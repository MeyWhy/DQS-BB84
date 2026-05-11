"""
kme/main.py  — KME (Key Management Entity)
============================================
Port 8000.

The KME is now a registry + message bus, not a controller.
Nodes call the KME. The KME stores state and notifies peers.

ETSI GS QKD 014 endpoint aliases:
  POST /keys          = POST /sessions          (create session)
  GET  /keys/{key_ID} = GET  /sessions/{id}     (get session status)
  POST /keys/{key_ID}/consume = consume key (one-time)

What changed from v6 main.py:
  - Removed: _run_session background task (KME no longer calls nodes)
  - Removed: direct calls to Alice /emit
  - Removed: direct calls to Bob /session/register
  - Added:   POST /sessions        (Alice calls this to create)
  - Added:   POST /sessions/{id}/join (Bob calls this to join)
  - Added:   POST /sessions/{id}/qubits (Alice posts qubit batches)
  - Added:   GET  /sessions/{id}/qubits (QKDL polls next batch)
  - Added:   POST /sessions/{id}/measurements (Bob posts results)
  - Added:   GET  /sessions/{id}/measurements (Alice retrieves)
  - Added:   POST /sessions/{id}/sift   (Alice posts bases)
  - Added:   GET  /sessions/{id}/sift   (Bob retrieves Alice's bases)
  - Added:   POST /sessions/{id}/key    (Alice posts final key)
  - Added:   POST /nodes/register       (any node registers)
  - Added:   GET  /nodes               (list nodes)
  - Kept:    GET  /sessions/{id}       (polling, unchanged)
  - Kept:    DELETE /sessions/{id}     (cancel)
  - Kept:    GET  /health
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, BackgroundTasks

from shared.models import (
    NodeRegistration, NodeInfo, NodeRole,
    SessionCreateReq, SessionJoinReq, SessionJoinResp,
    QubitUpload, MeasurementUpload,
    SiftUpload, SiftResult, KeyUpload,
    SessionStatusResponse, KeyStatus, WebhookEvent,
    NetworkInitReq, NetworkStopReq,
    new_session_id,
)
from kme.session_store import (
    get_redis, save_session, load_session, update_session,
    push_qubit_batch, pop_qubit_batch, qubit_batch_count,
    save_measurements, load_measurements,
    save_sift_upload, load_sift_upload,
    save_key_upload, load_key_upload,
    activate_key, consume_key, delete_session,
    list_open_sessions, list_active_sessions,
)
from kme.node_registry import (
    register_node, load_node, find_node_by_label,
    list_nodes, notify_node,
)

logger    = logging.getLogger("kme")
logging.basicConfig(level=logging.INFO)

QKDL_URL = os.getenv("QKDL_URL",  "http://localhost:8003")
HTTP_TO  = 30.0


# ─────────────────────────────────────────────
# App
# ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[KME] Started — ETSI GS QKD 014 compliant")
    yield
    logger.info("[KME] Stopped")


app = FastAPI(
    title="KME — Key Management Entity",
    description=(
        "ETSI GS QKD 014 compliant Key Management Entity. "
        "Agent-driven BB84 QKD over distributed microservices."
    ),
    version="0.7.0",
    lifespan=lifespan,
)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _get_session_or_404(r, session_id: str) -> dict:
    session = load_session(r, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session


async def _notify(node_id: str, event: WebhookEvent) -> None:
    """Best-effort webhook — never raises."""
    r    = get_redis()
    node = load_node(r, node_id)
    if node:
        await notify_node(node, event)


# ─────────────────────────────────────────────
# Node registry endpoints
# ─────────────────────────────────────────────

@app.post("/nodes/register", response_model=NodeInfo)
async def register(reg: NodeRegistration):
    """
    Any node registers here on startup.
    Returns a node_id the node must keep for all subsequent calls.
    """
    r    = get_redis()
    info = register_node(r, reg)
    return info


@app.get("/nodes", response_model=list[NodeInfo])
async def get_nodes(role: str = None):
    r    = get_redis()
    role_enum = NodeRole(role) if role else None
    return list_nodes(r, role=role_enum)


@app.get("/nodes/{node_id}", response_model=NodeInfo)
async def get_node(node_id: str):
    r    = get_redis()
    node = load_node(r, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node


# ─────────────────────────────────────────────
# Session lifecycle — nodes drive it
# ─────────────────────────────────────────────

@app.post("/sessions", status_code=202)
@app.post("/keys",     status_code=202)    # ETSI alias
async def create_session(
    req: SessionCreateReq,
    background_tasks: BackgroundTasks,
):
    """
    Alice calls this to start a new QKD session.

    KME:
      1. Creates session record (status=open)
      2. Looks up the target Bob by label
      3. Notifies Bob via webhook {event: session_open}
      4. Returns session_id immediately (202 Accepted)

    Alice then:
      - Initialises the QKDL network herself (or KME can do it)
      - Starts posting qubit batches to POST /sessions/{id}/qubits
    """
    r          = get_redis()
    session_id = new_session_id()

    # Find target receiver
    bob_node = find_node_by_label(r, req.receiver_label)
    if not bob_node:
        raise HTTPException(
            status_code=404,
            detail=f"Receiver node '{req.receiver_label}' not registered"
        )

    # Create session record
    session = {
        "session_id":      session_id,
        "status":          "open",
        "sender_node_id":  req.sender_node_id,
        "receiver_node_id": bob_node.node_id,
        "n_qubits":        req.n_qubits,
        "batch_size":      req.batch_size,
        "loss_rate":       req.loss_rate,
        "retry_enabled":   req.retry_enabled,
        "created_at":      time.time(),
        "key_status":      KeyStatus.NONE.value,
        "key_expires_at":  None,
        "n_delivered":     0,
        "n_sifted":        0,
        "qber":            0.0,
        "key_final":       "",
        "error_message":   "",
    }
    save_session(r, session)

    # Init QKDL network in background
    background_tasks.add_task(
        _init_qkdl, session_id, req.n_qubits, req.loss_rate
    )

    # Notify Bob via webhook
    background_tasks.add_task(
        _notify, bob_node.node_id,
        WebhookEvent(
            event="session_open",
            session_id=session_id,
            payload={
                "role":            "receiver",
                "sender_node_id":  req.sender_node_id,
                "n_qubits":        req.n_qubits,
            },
        )
    )

    logger.info(
        f"[KME] Session {session_id} created — "
        f"sender={req.sender_node_id[:8]} receiver={bob_node.label}"
    )
    return {"session_id": session_id, "status": "open"}


@app.post("/sessions/{session_id}/join", response_model=SessionJoinResp)
async def join_session(session_id: str, req: SessionJoinReq):
    """
    Bob calls this after receiving the session_open webhook.
    KME marks session as 'joined' and notifies Alice.
    """
    r       = get_redis()
    session = _get_session_or_404(r, session_id)

    if session["status"] != "open":
        raise HTTPException(
            status_code=409,
            detail=f"Session not open (status={session['status']})"
        )

    update_session(r, session_id, status="joined")

    # Notify Alice that Bob is ready
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
    )


# ─────────────────────────────────────────────
# Qubit bus
# ─────────────────────────────────────────────

@app.post("/sessions/{session_id}/qubits", status_code=202)
async def upload_qubits(session_id: str, req: QubitUpload):
    """
    Alice posts qubit batches here.
    KME stores them; QKDL polls and transmits to Bob.
    """
    r = get_redis()
    _get_session_or_404(r, session_id)
    push_qubit_batch(r, session_id, req.batch.model_dump())
    return {"session_id": session_id, "batch_id": req.batch.batch_id,
            "queued": True}


@app.get("/sessions/{session_id}/qubits/next")
async def next_qubit_batch(session_id: str):
    """
    QKDL polls this to get the next batch to transmit.
    Returns null if queue is empty.
    """
    r     = get_redis()
    batch = pop_qubit_batch(r, session_id)
    return {"session_id": session_id, "batch": batch,
            "remaining": qubit_batch_count(r, session_id)}


# ─────────────────────────────────────────────
# Measurement bus
# ─────────────────────────────────────────────

@app.post("/sessions/{session_id}/measurements", status_code=202)
async def upload_measurements(
    session_id: str,
    upload: MeasurementUpload,
    background_tasks: BackgroundTasks,
):
    """
    Bob posts his measurements after receiving qubits from QKDL.
    KME stores them and notifies Alice they are ready.
    """
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
    """Alice retrieves Bob's measurements to run sifting locally."""
    r    = get_redis()
    _get_session_or_404(r, session_id)
    meas = load_measurements(r, session_id)
    return {"session_id": session_id, "measurements": list(meas.values())}


# ─────────────────────────────────────────────
# Sifting bus
# ─────────────────────────────────────────────

@app.post("/sessions/{session_id}/sift", status_code=202)
async def upload_sift(
    session_id: str,
    upload: SiftUpload,
    background_tasks: BackgroundTasks,
):
    """
    Alice posts her bases (sifting data) here.
    KME stores them and notifies Bob to retrieve and sift locally.
    """
    r = get_redis()
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
    """Bob retrieves Alice's bases to do local sifting."""
    r      = get_redis()
    _get_session_or_404(r, session_id)
    upload = load_sift_upload(r, session_id)
    if not upload:
        raise HTTPException(status_code=404,
                            detail="Sift data not yet available")
    return upload


# ─────────────────────────────────────────────
# Key publication
# ─────────────────────────────────────────────

@app.post("/sessions/{session_id}/key")
async def publish_key(
    session_id: str,
    upload: KeyUpload,
    background_tasks: BackgroundTasks,
):
    """
    Alice posts the final key result after local QBER + derivation.
    KME activates the key lifecycle and notifies Bob.
    """
    r       = get_redis()
    session = _get_session_or_404(r, session_id)

    save_key_upload(r, session_id, upload.model_dump())

    if upload.status == "success":
        expires_at = activate_key(r, session_id)
        update_session(
            r, session_id,
            status="done",
            key_final=upload.key_final,
            key_hash=upload.key_hash,
            qber=upload.qber,
            n_sifted=upload.n_sifted,
        )
        event = "key_available"
        payload = {
            "key_status":     KeyStatus.ACTIVE.value,
            "key_expires_at": expires_at,
            "qber":           upload.qber,
        }
        logger.info(
            f"[KME] Key ACTIVE session={session_id} "
            f"QBER={upload.qber*100:.2f}%"
        )
    else:
        update_session(
            r, session_id,
            status="aborted",
            error_message=upload.error_message,
        )
        event   = "session_aborted"
        payload = {"reason": upload.error_message}
        logger.warning(
            f"[KME] Session {session_id} aborted: {upload.error_message}"
        )

    background_tasks.add_task(
        _notify, session["receiver_node_id"],
        WebhookEvent(event=event, session_id=session_id, payload=payload)
    )

    # Teardown QKDL
    background_tasks.add_task(_stop_qkdl, session_id)

    return {"session_id": session_id, "status": upload.status}


# ─────────────────────────────────────────────
# Key consumption (ETSI one-time use)
# ─────────────────────────────────────────────

@app.post("/sessions/{session_id}/consume-key")
@app.post("/keys/{session_id}/consume")           # ETSI alias
async def consume_session_key(session_id: str):
    """One-time key retrieval. Returns 409 if already consumed or expired."""
    r   = get_redis()
    ok, key = consume_key(r, session_id)
    if not ok:
        session = load_session(r, session_id)
        status  = session.get("key_status") if session else "unknown"
        raise HTTPException(
            status_code=409,
            detail=f"Key not available: status={status}"
        )
    return {"session_id": session_id, "key_final": key,
            "key_status": KeyStatus.CONSUMED.value}


# ─────────────────────────────────────────────
# Session status polling (unchanged from v6)
# ─────────────────────────────────────────────

@app.get("/sessions/{session_id}", response_model=SessionStatusResponse)
@app.get("/keys/{session_id}",     response_model=SessionStatusResponse)
async def get_session(session_id: str):
    r       = get_redis()
    session = _get_session_or_404(r, session_id)
    return SessionStatusResponse(**{
        k: session.get(k, v)
        for k, v in SessionStatusResponse.model_fields.items()
        if k != "session_id"
    }, session_id=session_id)


@app.get("/sessions")
async def list_sessions(active_only: bool = True):
    r   = get_redis()
    ids = list_active_sessions(r) if active_only else list_open_sessions(r)
    return {"sessions": ids, "count": len(ids)}


@app.delete("/sessions/{session_id}")
async def cancel_session(session_id: str):
    r       = get_redis()
    session = _get_session_or_404(r, session_id)
    update_session(r, session_id, status="aborted",
                   error_message="Cancelled by user")
    asyncio.create_task(_stop_qkdl(session_id))
    return {"status": "cancelled", "session_id": session_id}


# ─────────────────────────────────────────────
# QKDL coordination (KME still manages network init/stop)
# ─────────────────────────────────────────────

async def _init_qkdl(session_id: str, n_qubits: int, loss_rate: float):
    """KME initialises the quantum network after session creation."""
    try:
        async with httpx.AsyncClient(timeout=HTTP_TO) as client:
            resp = await client.post(
                f"{QKDL_URL}/network/init",
                json=NetworkInitReq(
                    session_id=session_id,
                    n_qubits=n_qubits,
                    loss_rate=loss_rate,
                ).model_dump(),
            )
            resp.raise_for_status()
        logger.info(f"[KME] QKDL initialised for session {session_id}")
    except Exception as e:
        logger.error(f"[KME] QKDL init failed {session_id}: {e}")
        update_session(get_redis(), session_id, status="aborted",
                       error_message=f"QKDL init failed: {e}")


async def _stop_qkdl(session_id: str):
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(
                f"{QKDL_URL}/network/stop",
                json={"session_id": session_id},
            )
    except Exception as e:
        logger.warning(f"[KME] QKDL teardown partial {session_id}: {e}")


# ─────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────

@app.get("/health")
async def health():
    r = get_redis()
    try:
        r.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {
        "status":          "ok",
        "redis":           redis_ok,
        "active_sessions": len(list_active_sessions(r)),
        "registered_nodes": len(list_nodes(r)),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")