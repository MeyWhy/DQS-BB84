"""
kme/main.py  -- v0.8.0

Key fixes vs v0.7:

1. QKDL URL routing (the transmission stall bug)
   KME now maintains a pool of QKDL URLs (QKDL_URLS env var, comma-separated).
   When a session is created, KME picks a free QKDL, stores the chosen URL
   in the session record, and returns it in the create + join responses.
   Alice and Bob use this URL for all quantum channel operations, so they
   always hit the same QKDL instance that holds their session.

2. acquire_qkd_lock() is now called atomically BEFORE any background task
   is dispatched, closing the race window that allowed two sessions to
   start concurrently.

3. QKDL pool locking: each QKDL URL has its own Redis lock, so multiple
   QKDL instances can serve different sessions simultaneously.
"""

import asyncio
import logging
import os
import sys
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, BackgroundTasks

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import (
    NodeRegistration, NodeInfo, NodeRole,
    SessionCreateReq, SessionJoinReq, SessionJoinResp,
    QubitUpload, MeasurementUpload,
    SiftUpload, SiftResult, KeyUpload,
    SessionStatusResponse, KeyStatus, WebhookEvent,
    NetworkInitReq, NetworkStopReq,
    new_session_id,
)
from kme.state_machine import SessionStatus
from collections import defaultdict
from kme.session_store import (
    get_redis, save_session, load_session, update_session,
    push_qubit_batch, pop_qubit_batch, qubit_batch_count,
    save_measurements, load_measurements,
    save_sift_upload, load_sift_upload,
    save_key_upload, load_key_upload,
    activate_key, consume_key, delete_session,
    list_open_sessions, list_active_sessions, get_active_qkd_session,
    acquire_qkd_lock, release_qkd_lock,
)
from kme.node_registry import (
    register_node, load_node, find_node_by_label,
    list_nodes, notify_node,
)

logger = logging.getLogger("kme")
logging.basicConfig(level=logging.INFO)

HTTP_TO = 30.0

#QKDL pool 
#Comma-separated list of QKDL base URLs.
#Example: QKDL_URLS=http://localhost:8003,http://localhost:8013
#Falls back to single QKDL_URL for backward compatibility.
_raw = os.getenv(
    "QKDL_URLS",
    os.getenv("QKDL_URL", "http://localhost:8003"),
)
QKDL_POOL: list[str] = [u.strip().rstrip("/") for u in _raw.split(",") if u.strip()]

METRICS = {
    "sessions_created":    0,
    "sessions_completed":  0,
    "sessions_aborted":    0,
    "total_qubits":        0,
    "total_batches":       0,
    "registry_hits":       0,
    "webhook_events":      0,
    "coordination_latency_ms": [],
    "session_latency_s":       [],
    "batch_latency_ms":        [],
    "active_nodes_peak":   0,
}

NODE_SESSION_COUNT: dict[str, int] = defaultdict(int)


#QKDL pool helpers 

def _qkdl_lock_key(qkdl_url: str) -> str:
    """Redis key for per-QKDL lock."""
    #Sanitise URL into a safe key suffix
    safe = qkdl_url.replace("://", "_").replace("/", "_").replace(":", "_")
    return f"kme:qkdl_lock:{safe}"


def _acquire_qkdl_lock(r, qkdl_url: str, session_id: str) -> bool:
    """Atomically lock a specific QKDL instance for session_id."""
    return bool(r.set(_qkdl_lock_key(qkdl_url), session_id, nx=True, ex=600))


def _release_qkdl_lock(r, qkdl_url: str, session_id: str) -> None:
    key = _qkdl_lock_key(qkdl_url)
    if r.get(key) == session_id:
        r.delete(key)


def _get_qkdl_lock_holder(r, qkdl_url: str) -> str | None:
    return r.get(_qkdl_lock_key(qkdl_url))


def pick_free_qkdl(r) -> str | None:
    """
    Return the URL of the first QKDL instance that is not currently locked.
    Returns None if all instances are busy.
    """
    for url in QKDL_POOL:
        if not _get_qkdl_lock_holder(r, url):
            return url
    return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"[KME] Started -- QKDL pool: {QKDL_POOL}")
    yield
    logger.info("[KME] Stopped")


app = FastAPI(
    title="KME - Key Management Entity",
    description=(
        "ETSI GS QKD 014 compliant Key Management Entity. "
        "Agent-driven BB84 QKD over distributed microservices."
    ),
    version="0.8.0",
    lifespan=lifespan,
)


#Helpers 

def _get_session_or_404(r, session_id: str) -> dict:
    session = load_session(r, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session


async def _notify(node_id: str, event: WebhookEvent) -> None:
    r    = get_redis()
    node = load_node(r, node_id)
    if node:
        await notify_node(node, event)
        METRICS["webhook_events"] += 1


#Node registry 

@app.post("/nodes/register", response_model=NodeInfo)
async def register(reg: NodeRegistration):
    r    = get_redis()
    info = register_node(r, reg)
    METRICS["registry_hits"] += 1
    count = len(list_nodes(r))
    if count > METRICS["active_nodes_peak"]:
        METRICS["active_nodes_peak"] = count
    NODE_SESSION_COUNT[info.node_id] = 0
    return info


@app.get("/nodes", response_model=list[NodeInfo])
async def get_nodes(role: str = None):
    r        = get_redis()
    role_enum = NodeRole(role) if role else None
    return list_nodes(r, role=role_enum)


@app.get("/nodes/{node_id}", response_model=NodeInfo)
async def get_node(node_id: str):
    r    = get_redis()
    node = load_node(r, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node


#Session lifecycle 

@app.post("/sessions", status_code=202)
@app.post("/keys",     status_code=202)
async def create_session(
    req: SessionCreateReq,
    background_tasks: BackgroundTasks,
):
    """
    Alice calls this to open a new QKD session.

    Steps (order matters for correctness):
      1. Pick a free QKDL instance from the pool.
      2. Lock it atomically -- if lost race, return 409.
      3. Find Bob node.
      4. Persist session record (includes qkdl_url).
      5. Dispatch background tasks.
    """
    r          = get_redis()
    session_id = new_session_id()

    #1 + 2: pick and lock a free QKDL 
    qkdl_url = pick_free_qkdl(r)
    if not qkdl_url:
        raise HTTPException(
            status_code=409,
            detail=f"All QKDL instances are busy. Pool: {QKDL_POOL}"
        )

    locked = _acquire_qkdl_lock(r, qkdl_url, session_id)
    if not locked:
        #Lost atomic race on this QKDL; try again (another request grabbed it)
        qkdl_url = pick_free_qkdl(r)
        if not qkdl_url:
            raise HTTPException(
                status_code=409,
                detail="All QKDL instances are busy (race)."
            )
        locked = _acquire_qkdl_lock(r, qkdl_url, session_id)
        if not locked:
            raise HTTPException(
                status_code=409,
                detail="All QKDL instances are busy."
            )

    #Also acquire the legacy global lock for backward compat with
    #get_active_qkd_session() used elsewhere.
    acquire_qkd_lock(r, session_id)

    #3: find receiver 
    bob_node = find_node_by_label(r, req.receiver_label)
    if not bob_node:
        _release_qkdl_lock(r, qkdl_url, session_id)
        release_qkd_lock(r, session_id)
        raise HTTPException(
            status_code=404,
            detail=f"Receiver node '{req.receiver_label}' not registered"
        )

    #4: persist session 
    session = {
        "session_id":       session_id,
        "status":           SessionStatus.INITIALIZING.value,

        "sender_node_id":   req.sender_node_id,
        "receiver_node_id": bob_node.node_id,

        "n_qubits":         req.n_qubits,
        "batch_size":       req.batch_size,
        "loss_rate":        req.loss_rate,
        "retry_enabled":    req.retry_enabled,

        #The QKDL URL assigned to this session 
        #Alice and Bob MUST use this URL for all quantum channel calls.
        "qkdl_url":         qkdl_url,

        "created_at":       time.time(),
        "started_at":       None,
        "sending_at":       None,
        "completed_at":     None,

        "key_status":       KeyStatus.NONE.value,
        "key_expires_at":   None,

        "n_delivered":      0,
        "n_sifted":         0,
        "qber":             0.0,

        "key_final":        "",
        "error_message":    "",
    }
    save_session(r, session)

    #5: background tasks 
    background_tasks.add_task(
        _init_qkdl, session_id, req.n_qubits, req.loss_rate, qkdl_url
    )
    background_tasks.add_task(
        _notify, bob_node.node_id,
        WebhookEvent(
            event="session_open",
            session_id=session_id,
            payload={
                "role":           "receiver",
                "sender_node_id": req.sender_node_id,
                "n_qubits":       req.n_qubits,
                "qkdl_url":       qkdl_url,   #Bob needs this too
            },
        )
    )

    logger.info(
        f"[KME] Session {session_id} created  "
        f"sender={req.sender_node_id[:8]} receiver={bob_node.label} "
        f"qkdl={qkdl_url}"
    )

    METRICS["sessions_created"] += 1
    NODE_SESSION_COUNT[req.sender_node_id] += 1
    NODE_SESSION_COUNT[bob_node.node_id]   += 1

    return {
        "session_id": session_id,
        "status":     "open",
        "qkdl_url":   qkdl_url,   #Alice stores this for _send_qubits
    }


@app.post("/sessions/{session_id}/join", response_model=SessionJoinResp)
async def join_session(session_id: str, req: SessionJoinReq):
    r       = get_redis()
    session = _get_session_or_404(r, session_id)

    if session["status"] not in [
        SessionStatus.WAITING.value,
        SessionStatus.INITIALIZING.value,
    ]:
        raise HTTPException(
            status_code=409,
            detail=f"Session not open (status={session['status']})"
        )

    update_session(
        r, session_id,
        status=SessionStatus.SENDING.value,
        started_at=time.time(),
        sending_at=time.time(),
    )

    coord_ms = (time.time() - session["created_at"]) * 1000
    METRICS["coordination_latency_ms"].append(round(coord_ms, 2))

    asyncio.create_task(_notify(
        session["sender_node_id"],
        WebhookEvent(
            event="receiver_joined",
            session_id=session_id,
            payload={"receiver_node_id": req.node_id},
        )
    ))

    logger.info(f"[KME] Bob {req.node_id[:8]} joined session {session_id}")

    return SessionJoinResp(
        session_id=session_id,
        role=NodeRole.RECEIVER,
        sender_node_id=session["sender_node_id"],
        n_qubits=session["n_qubits"],
        status="joined",
        qkdl_url=session.get("qkdl_url", ""),   #Bob uses this for polling
    )


@app.post("/sessions/{session_id}/qubits", status_code=202)
async def upload_qubits(session_id: str, req: QubitUpload):
    r = get_redis()
    _get_session_or_404(r, session_id)
    push_qubit_batch(r, session_id, req.batch.model_dump())

    METRICS["total_batches"] += 1
    METRICS["total_qubits"]  += len(req.batch.qubits)

    return {
        "session_id": session_id,
        "batch_id":   req.batch.batch_id,
        "queued":     True,
    }


@app.get("/sessions/{session_id}/qubits/next")
async def next_qubit_batch(session_id: str):
    r     = get_redis()
    batch = pop_qubit_batch(r, session_id)
    return {
        "session_id": session_id,
        "batch":      batch,
        "remaining":  qubit_batch_count(r, session_id),
    }


@app.post("/sessions/{session_id}/measurements", status_code=202)
async def upload_measurements(
    session_id: str,
    upload:     MeasurementUpload,
    background_tasks: BackgroundTasks,
):
    r = get_redis()
    _get_session_or_404(r, session_id)
    save_measurements(r, session_id, upload.model_dump())

    n = len(upload.measurements)
    update_session(r, session_id, n_delivered=n)

    session = load_session(r, session_id)
    background_tasks.add_task(
        _notify, session["sender_node_id"],
        WebhookEvent(
            event="measurements_ready",
            session_id=session_id,
            payload={"n_measurements": n},
        )
    )
    logger.info(f"[KME] {n} measurements posted for session {session_id}")
    return {"session_id": session_id, "n_received": n}


@app.get("/sessions/{session_id}/measurements")
async def get_measurements(session_id: str):
    r = get_redis()
    _get_session_or_404(r, session_id)
    meas = load_measurements(r, session_id)
    return {"session_id": session_id, "measurements": list(meas.values())}


@app.post("/sessions/{session_id}/sift", status_code=202)
async def upload_sift(
    session_id: str,
    upload:     SiftUpload,
    background_tasks: BackgroundTasks,
):
    r       = get_redis()
    session = _get_session_or_404(r, session_id)
    save_sift_upload(r, session_id, upload.model_dump())

    background_tasks.add_task(
        _notify, session["receiver_node_id"],
        WebhookEvent(
            event="sift_ready",
            session_id=session_id,
            payload={"sample_seed": upload.sample_seed},
        )
    )
    return {"session_id": session_id, "stored": True}


@app.get("/sessions/{session_id}/sift")
async def get_sift(session_id: str):
    r      = get_redis()
    _get_session_or_404(r, session_id)
    upload = load_sift_upload(r, session_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Sift data not yet available")
    return upload


@app.post("/sessions/{session_id}/key")
async def publish_key(
    session_id: str,
    upload:     KeyUpload,
    background_tasks: BackgroundTasks,
):
    r       = get_redis()
    session = _get_session_or_404(r, session_id)

    save_key_upload(r, session_id, upload.model_dump())

    if upload.status == "success":
        expires_at = activate_key(r, session_id)
        update_session(
            r, session_id,
            status=SessionStatus.DONE.value,
            completed_at=time.time(),
            key_final=upload.key_final,
            key_hash=upload.key_hash,
            qber=upload.qber,
            n_sifted=upload.n_sifted,
        )
        event   = "key_available"
        payload = {
            "key_status":     KeyStatus.ACTIVE.value,
            "key_expires_at": expires_at,
            "qber":           upload.qber,
        }
        elapsed = time.time() - session["created_at"]
        METRICS["sessions_completed"] += 1
        METRICS["session_latency_s"].append(round(elapsed, 3))
        logger.info(
            f"[KME] Key ACTIVE session={session_id} QBER={upload.qber*100:.2f}%"
        )
    else:
        update_session(
            r, session_id,
            status=SessionStatus.ABORTED.value,
            completed_at=time.time(),
            key_final="",
            qber=0.0,
            error_message=upload.error_message,
        )
        event   = "session_aborted"
        payload = {"reason": upload.error_message}
        METRICS["sessions_aborted"] += 1
        logger.warning(
            f"[KME] Session {session_id} aborted: {upload.error_message}"
        )

    background_tasks.add_task(
        _notify, session["receiver_node_id"],
        WebhookEvent(event=event, session_id=session_id, payload=payload)
    )
    background_tasks.add_task(_stop_qkdl, session_id)

    return {"session_id": session_id, "status": upload.status}


@app.post("/sessions/{session_id}/consume-key")
@app.post("/keys/{session_id}/consume")
async def consume_session_key(session_id: str):
    r      = get_redis()
    ok, key = consume_key(r, session_id)
    if not ok:
        session = load_session(r, session_id)
        status  = session.get("key_status") if session else "unknown"
        raise HTTPException(
            status_code=409,
            detail=f"Key not available: status={status}"
        )
    return {
        "session_id": session_id,
        "key_final":  key,
        "key_status": KeyStatus.CONSUMED.value,
    }


@app.get("/sessions/{session_id}", response_model=SessionStatusResponse)
@app.get("/keys/{session_id}",     response_model=SessionStatusResponse)
async def get_session(session_id: str):
    r       = get_redis()
    session = _get_session_or_404(r, session_id)

    created_at   = session.get("created_at",   time.time())
    completed_at = session.get("completed_at")
    elapsed_s    = (
        round(completed_at - created_at, 3)
        if completed_at
        else round(time.time() - created_at, 3)
    )

    progress = {"open": 5, "waiting": 5, "initializing": 10,
                "sending": 50, "done": 100, "aborted": 0}
    labels   = {
        "open":         "Waiting for receiver",
        "waiting":      "Waiting for receiver",
        "initializing": "Initialising quantum network",
        "sending":      "Quantum transmission",
        "done":         "Key generated",
        "aborted":      "Session aborted",
    }

    valid_data = {
        k: session[k]
        for k in SessionStatusResponse.model_fields
        if k in session
    }
    valid_data.update({
        "session_id":  session_id,
        "elapsed_s":   elapsed_s,
        "progress_pct": progress.get(session["status"], 0),
        "phase_label":  labels.get(session["status"], ""),
        "qkdl_url":    session.get("qkdl_url", ""),
    })

    return SessionStatusResponse(**valid_data)


@app.get("/sessions")
async def list_sessions(active_only: bool = True):
    r   = get_redis()
    ids = list_active_sessions(r) if active_only else (
        list_open_sessions(r) + list_active_sessions(r)
    )
    return {"sessions": ids, "count": len(ids)}


@app.delete("/sessions/{session_id}")
async def cancel_session(session_id: str):
    r = get_redis()
    _get_session_or_404(r, session_id)
    update_session(r, session_id, status=SessionStatus.ABORTED.value,
                   error_message="Cancelled by user")
    release_qkd_lock(r, session_id)
    asyncio.create_task(_stop_qkdl(session_id))
    return {"status": "cancelled", "session_id": session_id}


#QKDL coordination 

async def _init_qkdl(
    session_id: str,
    n_qubits:   int,
    loss_rate:  float,
    qkdl_url:   str,          #explicit -- no more global QKDL_URL guessing
) -> None:
    r = get_redis()
    try:
        async with httpx.AsyncClient(timeout=HTTP_TO) as client:
            resp = await client.post(
                f"{qkdl_url}/network/init",
                json=NetworkInitReq(
                    session_id=session_id,
                    n_qubits=n_qubits,
                    loss_rate=loss_rate,
                ).model_dump(),
            )
            resp.raise_for_status()
        logger.info(
            f"[KME] QKDL initialised session={session_id} url={qkdl_url}"
        )
    except Exception as e:
        logger.error(f"[KME] QKDL init failed session={session_id}: {e}")
        update_session(r, session_id,
                       status=SessionStatus.ABORTED.value,
                       error_message=f"QKDL init failed: {e}")
        _release_qkdl_lock(r, qkdl_url, session_id)
        release_qkd_lock(r, session_id)
        await _stop_qkdl(session_id)


async def _stop_qkdl(session_id: str) -> None:
    r = get_redis()
    session = load_session(r, session_id)
    qkdl_url = session.get("qkdl_url", QKDL_POOL[0]) if session else QKDL_POOL[0]

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(
                f"{qkdl_url}/network/stop",
                json={"session_id": session_id},
            )
    except Exception as e:
        logger.warning(f"[KME] QKDL teardown partial session={session_id}: {e}")
    finally:
        #Always release locks whether stop succeeded or not
        _release_qkdl_lock(r, qkdl_url, session_id)
        release_qkd_lock(r, session_id)


#Metrics + health 

@app.get("/metrics")
async def metrics():
    def avg(lst):
        return round(sum(lst) / len(lst), 3) if lst else 0.0

    r = get_redis()
    qkdl_status = {
        url: (_get_qkdl_lock_holder(r, url) or "free")
        for url in QKDL_POOL
    }

    return {
        "sessions_created":              METRICS["sessions_created"],
        "sessions_completed":            METRICS["sessions_completed"],
        "sessions_aborted":              METRICS["sessions_aborted"],
        "total_qubits":                  METRICS["total_qubits"],
        "total_batches":                 METRICS["total_batches"],
        "throughput_qubits_per_session": round(
            METRICS["total_qubits"] / max(METRICS["sessions_completed"], 1), 2
        ),
        "avg_session_latency_s":         avg(METRICS["session_latency_s"]),
        "avg_coordination_latency_ms":   avg(METRICS["coordination_latency_ms"]),
        "avg_batch_latency_ms":          avg(METRICS["batch_latency_ms"]),
        "registry_hits":                 METRICS["registry_hits"],
        "webhook_events":                METRICS["webhook_events"],
        "active_nodes_peak":             METRICS["active_nodes_peak"],
        "active_sessions":               len(list_active_sessions(get_redis())),
        "qkdl_pool":                     qkdl_status,
    }


@app.get("/health")
async def health():
    r = get_redis()
    try:
        r.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {
        "status":           "ok",
        "redis":            redis_ok,
        "active_sessions":  len(list_active_sessions(r)),
        "registered_nodes": len(list_nodes(r)),
        "qkdl_pool":        QKDL_POOL,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")