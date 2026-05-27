import asyncio
import logging
import os
import random
import sys
import uvicorn

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from node.base_node import BaseNode
from models import NodeRole, SessionCreateReq, Basis, WebhookEvent
from kme.session_store import save_alice_state
from celery import chain as celery_chain, chord as celery_chord
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


class AliceNode(BaseNode):

    def __init__(self):
        super().__init__(
            role=NodeRole.SENDER,
            label=os.getenv("ALICE_LABEL", "alice-1"),
            callback_url=f"{MY_URL}/webhook",
        )
        #Lightweight session tracking  only for concurrency guard + cleanup.
        #Bits/bases live in Redis (written at session start, deleted by QKT).
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
        retry_enabled:     bool  = False,
        interceptor_label: str   = None,
    ) -> dict:
        payload = SessionCreateReq(
            sender_node_id=self.node_id,
            receiver_label=receiver_label,
            n_qubits=n_qubits,
            batch_size=batch_size,
            loss_rate=loss_rate,
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

        #Generate bits and bases
        bits  = [random.randint(0, 1)       for _ in range(n_qubits)]
        bases = [random.choice(list(Basis)) for _ in range(n_qubits)]

        #Persist to Redis so ST worker can read them without calling Alice
        save_alice_state(
            session_id,
            bits=bits,
            bases=[b.value for b in bases],  #store as "Z"/"X" strings
        )

        #Lightweight in-process record  no bits/bases here
        self._alice_state[session_id] = {
            "n_qubits":   n_qubits,
            "batch_size": batch_size,
            "qkdl_url":   qkdl_url,
            "bits":       bits,    #kept for qubit batch building only
            "bases":      bases,
            "done":       False,
        }

        logger.info(
            f"[Alice] Session {session_id[:8]} created "
            f"n_qubits={n_qubits} qkdl={qkdl_url}"
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

    
    #Pipeline dispatch:  chord(QTT x N) | ST | QKT | NT
    async def _dispatch_pipeline(self, session_id: str) -> None:
        state = self._alice_state.get(session_id)
        if not state:
            return

        bits, bases   = state["bits"], state["bases"]
        n, batch_size = state["n_qubits"], state["batch_size"]
        qkdl_url      = state["qkdl_url"]
        n_batches     = (n + batch_size - 1) // batch_size

        logger.info(
            f"[Alice] Building pipeline session={session_id[:8]} "
            f"batches={n_batches} qkdl={qkdl_url}"
        )

        #QTT signatures (one per batch) 
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
            qtt_tasks.append(
                send_batch_task.s(session_id, batch_payload, qkdl_url)
            )

        #session_meta passed immutably through the pipeline 
        #node_id is passed so NT can sign the key upload to KME
        session_meta = {
            "session_id": session_id,
            "n_qubits":   n,
            "kme_url":    KME_URL,
            "node_id":    self.node_id,
        }

        #ST -> QKT -> NT chain (callback for the chord) 
        #assemble_and_sift_task.s(session_meta):
        #Celery prepends the chord results list as the first arg -> correct.
        #qber_key_task.s() + notify_kme_task.s():
        #Each receives the previous task's return value as first arg.
        pipeline_callback = (
            assemble_and_sift_task.s(session_meta)
            | qber_key_task.s()
            | notify_kme_task.s()
        )

        #Dispatch (non-blocking) 
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: celery_chord(qtt_tasks)(pipeline_callback),
        )

        logger.info(
            f"[Alice] Pipeline dispatched session={session_id[:8]} "
            f"({n_batches} QTT tasks -> ST -> QKT -> NT)"
        )

    #Cleanup + poll fallback
    def _cleanup(self, session_id: str) -> None:
        state = self._alice_state.get(session_id)
        if state:
            state["done"] = True
        self._alice_state.pop(session_id, None)

    async def _poll_tick(self) -> None:
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
        "intercepted": body.get("intercepted", False),
        "poll_url":    f"{KME_URL}/sessions/{session_id}",
        "consume_url": f"{KME_URL}/sessions/{session_id}/consume-key",
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")