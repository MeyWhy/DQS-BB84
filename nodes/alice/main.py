import asyncio
import logging
import os
import random
import signal
import subprocess
import sys
import time
import uvicorn

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from node.base_node import BaseNode
from models import NodeRole, SessionCreateReq, Basis, WebhookEvent
from kme.session_store import save_alice_state
from celery import chord as celery_chord
from workers.qubit_tasks import send_batch_task
from workers.sifting_tasks import (
    assemble_and_sift_task,
    qber_key_task,
    notify_kme_task,
)
import httpx

logger     = logging.getLogger("alice")
logging.basicConfig(level=logging.INFO)

KME_URL    = os.getenv("KME_URL",   "http://localhost:8000")
MY_URL     = os.getenv("ALICE_URL", "http://localhost:8001")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
DISTANCE_KM = float(os.getenv("DISTANCE_KM", "0.0"))

#How long to wait for the per-session worker to connect to Redis
#before dispatching the chord.  2.5s is conservative; 1.5s usually works.
WORKER_WARMUP_S = float(os.getenv("WORKER_WARMUP_S", "2.5"))


def _session_queue(session_id: str) -> str:
    """Deterministic per-session queue name from the first 8 hex chars."""
    return f"qubit_send_{session_id.replace('-', '')[:8]}"


def _worker_name(session_id: str) -> str:
    """Celery worker node name  must be unique per host."""
    return f"qubit_{session_id.replace('-', '')[:8]}@%h"


def _spawn_session_worker(
    session_id: str,
    n_batches:  int = 20,
) -> subprocess.Popen:
   
    max_concurrency = int(os.getenv("WORKER_CONCURRENCY_MAX", "3"))
    concurrency     = min(n_batches, max_concurrency)

    q_name  = _session_queue(session_id)
    w_name  = _worker_name(session_id)
    cmd = [
        sys.executable, "-m", "celery",
        "-A", "workers.celery_config",
        "worker",
        "--queues",      q_name,
        "--concurrency", str(concurrency),
        "--loglevel",    "warning",
        "-n",            w_name,
    ]
    #Inherit environment (REDIS_URL, KME_URL, etc.) from Alice's process.
    #cwd = project root (two levels up from nodes/alice/).
    project_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..")
    )
    proc = subprocess.Popen(
        cmd,
        cwd=project_root,
        env=os.environ.copy(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    logger.info(
        f"[Alice] Worker spawned session={session_id[:8]} "
        f"queue={q_name} pid={proc.pid}"
    )
    return proc


def _terminate_session_worker(proc: subprocess.Popen, session_id: str) -> None:

    if proc is None or proc.poll() is not None:
        return   #already dead

    try:
        proc.terminate()   #SIGTERM → Celery warm shutdown
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()    #SIGKILL
            proc.wait()
        logger.info(
            f"[Alice] Worker terminated session={session_id[:8]} "
            f"pid={proc.pid}"
        )
    except Exception as e:
        logger.warning(
            f"[Alice] Worker termination error session={session_id[:8]}: {e}"
        )


class AliceNode(BaseNode):

    def __init__(self):
        super().__init__(
            role=NodeRole.SENDER,
            label=os.getenv("ALICE_LABEL", "alice-1"),
            callback_url=f"{MY_URL}/webhook",
        )
        #session_id → {n_qubits, batch_size, qkdl_url, bits, bases,
        #queue_name, worker_proc, done}
        self._alice_state: dict[str, dict] = {}

    def _active_sessions(self) -> list[str]:
        return [
            sid for sid, s in self._alice_state.items()
            if not s.get("done")
        ]

    
    #Session creation
    async def start_bb84_session(
        self,
        receiver_label:    str,
        n_qubits:          int   = 200,
        batch_size:        int   = BATCH_SIZE,
        loss_rate:         float = 0.0,
        distance_km:       float = 0.0, 
        retry_enabled:     bool  = False,
        interceptor_label: str   = None,
    ) -> dict:
        payload = SessionCreateReq(
            sender_node_id=self.node_id,
            receiver_label=receiver_label,
            n_qubits=n_qubits,
            batch_size=batch_size,
            loss_rate=loss_rate,
            distance_km=distance_km, 
            retry_enabled=retry_enabled,
            interceptor_label=interceptor_label or None,
        ).model_dump()

        resp = await self._client.post(f"{KME_URL}/sessions", json=payload)
        resp.raise_for_status()
        body       = resp.json()
        session_id = body["session_id"]
        qkdl_url   = body.get(
            "qkdl_url", os.getenv("QKDL_URL", "http://localhost:8003")
        )

        bits  = [random.randint(0, 1)       for _ in range(n_qubits)]
        bases = [random.choice(list(Basis)) for _ in range(n_qubits)]

        save_alice_state(
            session_id,
            bits=bits,
            bases=[b.value for b in bases],
        )

        #Spawn the dedicated worker for this session's queue NOW,
        #before receiver_joined fires, so the worker has WORKER_WARMUP_S
        #to connect to Redis before the chord is dispatched.
        q_name      = _session_queue(session_id)
        n_batches   = (n_qubits + batch_size - 1) // batch_size
        worker_proc = _spawn_session_worker(session_id, n_batches=n_batches)

        self._alice_state[session_id] = {
            "n_qubits":    n_qubits,
            "batch_size":  batch_size,
            "qkdl_url":    qkdl_url,
            "bits":        bits,
            "bases":       bases,
            "queue_name":  q_name,
            "worker_proc": worker_proc,
            "done":        False,
        }

        logger.info(
            f"[Alice] Session {session_id[:8]} created "
            f"n_qubits={n_qubits} qkdl={qkdl_url} queue={q_name}"
            + (f" interceptor={interceptor_label}" if interceptor_label else "")
        )
        return body


    #Webhook handlers
    async def on_receiver_joined(self, session_id: str, payload: dict) -> None:
        logger.info(
            f"[Alice] Receiver joined {session_id[:8]}  dispatching pipeline"
        )
        asyncio.create_task(self._dispatch_pipeline(session_id))

    async def on_key_available(self, session_id: str, payload: dict) -> None:
        logger.info(
            f"[Alice] Key active session={session_id[:8]} "
            f"QBER={payload.get('qber', 0)*100:.2f}%"
        )
        self._cleanup(session_id)

    async def on_session_aborted(self, session_id: str, payload: dict) -> None:
        logger.warning(
            f"[Alice] Session {session_id[:8]} aborted: "
            f"{payload.get('reason', '')}"
        )
        self._cleanup(session_id)

    
    #Pipeline dispatch : chord(QTT x N) | ST | QKT | NT
    async def _dispatch_pipeline(self, session_id: str) -> None:
        state = self._alice_state.get(session_id)
        if not state:
            return

        bits, bases   = state["bits"], state["bases"]
        n, batch_size = state["n_qubits"], state["batch_size"]
        qkdl_url      = state["qkdl_url"]
        q_name        = state["queue_name"]
        n_batches     = (n + batch_size - 1) // batch_size

        #Wait for the per-session worker to connect to Redis.
        #The worker was spawned in start_bb84_session(); by the time
        #receiver_joined fires (KME notifies Bob, Bob joins, KME notifies
        #Alice) at least 12s have already elapsed.  The remaining sleep
        #closes any gap on fast machines.
        await asyncio.sleep(WORKER_WARMUP_S)

        logger.info(
            f"[Alice] Dispatching pipeline session={session_id[:8]} "
            f"batches={n_batches} qkdl={qkdl_url} queue={q_name}"
        )

        #QTT signatures  each routed to the session-specific queue
        qtt_tasks = []
        for batch_id, start in enumerate(range(0, n, batch_size)):
            end    = min(start + batch_size, n)
            qubits = [
                {"qubit_id": i, "bit": bits[i], "basis": bases[i].value}
                for i in range(start, end)
            ]
            batch_payload = {
                "session_id": session_id,
                "batch_id":   batch_id,
                "qubits":     qubits,
            }
            #.set(queue=q_name) overrides any static task_routes entry
            #and pins this task to the per-session queue.
            qtt_tasks.append(
                send_batch_task.s(session_id, batch_payload, qkdl_url)
                               .set(queue=q_name)
            )

        session_meta = {
            "session_id": session_id,
            "n_qubits":   n,
            "kme_url":    KME_URL,
            "node_id":    self.node_id,
        }

        pipeline_callback = (
            assemble_and_sift_task.s(session_meta)
            | qber_key_task.s()
            | notify_kme_task.s()
        )

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: celery_chord(qtt_tasks)(pipeline_callback),
        )

        logger.info(
            f"[Alice] Pipeline dispatched session={session_id[:8]} "
            f"({n_batches} QTT → ST → QKT → NT) queue={q_name}"
        )

    def _cleanup(self, session_id: str) -> None:
        state = self._alice_state.pop(session_id, None)
        if state:
            state["done"] = True
            #Terminate the per-session worker in a background thread so
            #we don't block Alice's async event loop during shutdown.
            import threading
            threading.Thread(
                target=_terminate_session_worker,
                args=(state.get("worker_proc"), session_id),
                daemon=True,
                name=f"worker-cleanup-{session_id[:8]}",
            ).start()
        self._alice_state.pop(session_id, None)   #idempotent

    async def _poll_tick(self) -> None:
        """Defensive fallback: detect sessions that ended without a webhook."""
        for sid in list(self._alice_state.keys()):
            state = self._alice_state.get(sid)
            if not state or state.get("done"):
                continue
            try:
                data = await self.kme_get(f"/sessions/{sid}")
                if data.get("status") in ("aborted", "done"):
                    logger.info(
                        f"[Alice] Poll detected terminal status "
                        f"session={sid[:8]} status={data['status']}"
                    )
                    self._cleanup(sid)
            except Exception:
                pass

alice = AliceNode()
app   = alice.build_app(title="SAE-A  Alice (Sender)", port=8001)


@app.post("/start")
async def start_session(
    receiver_label:    str   = "bob-1",
    n_qubits:          int   = 200,
    batch_size:        int   = BATCH_SIZE,
    loss_rate:         float = 0.0,
    distance_km:       float = DISTANCE_KM, 
    retry_enabled:     bool  = False,
    interceptor_label: str   = None,
):
    if not alice.node_id:
        return JSONResponse(
            status_code=503,
            content={"error": "Not registered yet  retry in 1s"},
        )

    active = alice._active_sessions()
    if active:
        return JSONResponse(
            status_code=409,
            content={
                "error":      "Session already in progress on this node",
                "active":     active,
                "suggestion": "Wait for it to finish or use another alice instance",
            },
        )

    try:
        body = await alice.start_bb84_session(
            receiver_label=receiver_label,
            n_qubits=n_qubits,
            batch_size=batch_size,
            loss_rate=loss_rate,
            distance_km=distance_km, 
            retry_enabled=retry_enabled,
            interceptor_label=interceptor_label,
        )
    except httpx.HTTPStatusError as e:
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text
        return JSONResponse(
            status_code=e.response.status_code,
            content={"error": "KME rejected session", "detail": detail},
        )
    except Exception as e:
        logger.error(f"[Alice] /start unexpected error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

    session_id = body["session_id"]
    return {
        "session_id":  session_id,
        "status":      "created",
        "qkdl_url":    body.get("qkdl_url"),
        "queue_name":  _session_queue(session_id),
        "intercepted": body.get("intercepted", False),
        "poll_url":    f"{KME_URL}/sessions/{session_id}",
        "consume_url": f"{KME_URL}/sessions/{session_id}/consume-key",
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")