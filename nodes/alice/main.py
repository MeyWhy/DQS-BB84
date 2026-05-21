import asyncio
import hashlib
import logging
import os
import random
import sys
import uvicorn

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from node.base_node import BaseNode
from models import (
    NodeRole, SessionCreateReq, QubitRecord, QubitBatch,
    SiftUpload, KeyUpload, Basis, WebhookEvent,
)
from bb84_logic import compute_qber, QBER_THRESHOLD
from celery import chord as celery_chord
from workers.qubit_tasks import send_batch_task
from workers.sifting_tasks import batch_complete_task
import httpx

logger     = logging.getLogger("alice")
logging.basicConfig(level=logging.INFO)

KME_URL    = os.getenv("KME_URL",    "http://localhost:8000")
MY_URL     = os.getenv("ALICE_URL",  "http://localhost:8001")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))


class AliceNode(BaseNode):

    def __init__(self):
        super().__init__(
            role=NodeRole.SENDER,
            label=os.getenv("ALICE_LABEL", "alice-1"),
            callback_url=f"{MY_URL}/webhook",
        )
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
        qkdl_url   = body.get("qkdl_url",
                               os.getenv("QKDL_URL", "http://localhost:8003"))

        bits  = [random.randint(0, 1)       for _ in range(n_qubits)]
        bases = [random.choice(list(Basis)) for _ in range(n_qubits)]

        self._alice_state[session_id] = {
            "bits":                bits,
            "bases":               bases,
            "n_qubits":            n_qubits,
            "batch_size":          batch_size,
            "qkdl_url":            qkdl_url,
            "transmission_done":   False,   #set when chord callback fires
            "measurements_ready":  False,   #set when Bob uploads
            "sifting_triggered":   False,
            "done":                False,
        }
        logger.info(
            f"[Alice] Session {session_id[:8]} created "
            f"n_qubits={n_qubits} qkdl={qkdl_url}"
        )
        return body


    #Webhook handlers
    async def on_receiver_joined(self, session_id: str, payload: dict) -> None:
        logger.info(
            f"[Alice] Receiver joined {session_id[:8]}  dispatching qubit chord"
        )
        asyncio.create_task(self._dispatch_qubit_chord(session_id))

    async def on_measurements_ready(self, session_id: str, payload: dict) -> None:
        """KME fires this when Bob uploads measurements."""
        state = self._alice_state.get(session_id)
        if not state or state.get("done"):
            return
        state["measurements_ready"] = True
        logger.info(
            f"[Alice] measurements_ready session={session_id[:8]} "
            f"n={payload.get('n_measurements', '?')}"
        )
        self._try_start_sifting(session_id)

    async def on_transmission_complete(self, session_id: str, payload: dict) -> None:
        #Chord callback fires this after all batch tasks complete
        state = self._alice_state.get(session_id)
        if not state or state.get("done"):
            return
        state["transmission_done"] = True
        n_del = payload.get("n_delivered", "?")
        n_q   = payload.get("n_qubits",    "?")
        logger.info(
            f"[Alice] transmission_complete session={session_id[:8]} "
            f"delivered={n_del}/{n_q}"
        )
        self._try_start_sifting(session_id)

    async def on_session_aborted(self, session_id: str, payload: dict) -> None:
        self._cleanup(session_id)
        logger.info(f"[Alice] Session {session_id[:8]} aborted  cleaned up")

    #Webhook dispatch override  add transmission_complete handler

    async def handle_webhook(self, event: WebhookEvent) -> None:
        sid = event.session_id
        if sid not in self._sessions:
            self._sessions[sid] = {"session_id": sid}
        self._sessions[sid]["last_event"] = event.event

        handler = {
            "session_open":          self.on_session_open,
            "receiver_joined":       self.on_receiver_joined,
            "measurements_ready":    self.on_measurements_ready,
            "transmission_complete": self.on_transmission_complete,   #← new
            "sift_ready":            self.on_sift_ready,
            "key_available":         self.on_key_available,
            "session_aborted":       self.on_session_aborted,
        }.get(event.event)

        if handler:
            asyncio.create_task(handler(sid, event.payload))
        else:
            logger.warning(f"[Alice] Unknown event: {event.event}")


    #Qubit transmission w/ Celery chord
    async def _dispatch_qubit_chord(self, session_id: str) -> None:
        """
        Build and dispatch a Celery chord:
          chord(send_batch_task x N_batches)(batch_complete_task)

        Each send_batch_task receives:
          - session_id
          - serialised QubitBatch dict
          - qkdl_url (explicit  no env var lookup in workers)

        The chord header tasks are independent and execute in parallel
        across the qubit_send worker pool.  The callback fires once all
        N tasks have returned results (success or failure).
        """
        state = self._alice_state.get(session_id)
        if not state:
            return

        bits, bases   = state["bits"], state["bases"]
        n, batch_size = state["n_qubits"], state["batch_size"]
        qkdl_url      = state["qkdl_url"]
        n_batches     = (n + batch_size - 1) // batch_size

        logger.info(
            f"[Alice] Dispatching chord session={session_id[:8]} "
            f"total={n} batches={n_batches} qkdl={qkdl_url}"
        )

        #Build batch task signatures (one per batch slice)
        batch_tasks = []
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
            batch_tasks.append(
                send_batch_task.s(session_id, batch_payload, qkdl_url)
            )

        #Chord callback receives all batch results + immutable session_meta
        session_meta = {
            "session_id":        session_id,
            "alice_callback_url": f"{MY_URL}/webhook",
            "n_qubits":          n,
        }
        callback = batch_complete_task.s(session_meta)

        #Dispatch  non-blocking from Alice's async perspective
        #The chord runs in the Celery worker pool; Alice's event loop is free.
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: celery_chord(batch_tasks)(callback),
        )
        logger.info(
            f"[Alice] Chord dispatched session={session_id[:8]} "
            f"({n_batches} tasks)"
        )

    #Sifting gate  wait for BOTH signals before starting
    def _try_start_sifting(self, session_id: str) -> None:
        """
        Start sifting only when BOTH conditions are true:
          1. transmission_done   chord says all qubits were sent
          2. measurements_ready  Bob has uploaded his measurements to KME

        This gate ensures Alice doesn't fetch an empty measurement set.
        Either signal can arrive first; whichever arrives second triggers
        the actual sifting work.
        """
        state = self._alice_state.get(session_id)
        if not state:
            return
        if state.get("sifting_triggered") or state.get("done"):
            return
        if not (state.get("transmission_done") and state.get("measurements_ready")):
            logger.debug(
                f"[Alice] Waiting for both signals session={session_id[:8]} "
                f"tx={state.get('transmission_done')} "
                f"meas={state.get('measurements_ready')}"
            )
            return

        state["sifting_triggered"] = True
        logger.info(
            f"[Alice] Both signals received  starting sifting "
            f"session={session_id[:8]}"
        )
        asyncio.create_task(self._run_sifting_and_key(session_id))


    #Sifting + key derivation (same as V7)
    async def _run_sifting_and_key(self, session_id: str) -> None:
        state = self._alice_state.get(session_id)
        if not state:
            return

        try:
            resp = await self._client.get(
                f"{KME_URL}/sessions/{session_id}/measurements",
                timeout=15.0,
            )
            resp.raise_for_status()
            raw_meas = resp.json().get("measurements", [])
        except Exception as e:
            logger.error(
                f"[Alice] Fetch measurements failed {session_id[:8]}: {e}"
            )
            await self._post_key(session_id, "aborted", error="FETCH_FAILED")
            self._cleanup(session_id)
            return

        if not raw_meas:
            logger.warning(
                f"[Alice] No measurements yet {session_id[:8]}  retry via poll"
            )
            state["sifting_triggered"] = False
            state["measurements_ready"] = False
            return

        alice_bits  = state["bits"]
        alice_bases = [b.value for b in state["bases"]]
        sample_seed = random.randint(0, 2**31)

        meas_by_id              = {m["qubit_id"]: m for m in raw_meas}
        alice_sifted, bob_sifted = [], []

        for qid in sorted(meas_by_id.keys()):
            if qid >= len(alice_bases):
                continue
            m = meas_by_id[qid]
            if alice_bases[qid] == m.get("basis"):
                alice_sifted.append(alice_bits[qid])
                bob_sifted.append(m.get("bit_result", 0))

        n_sifted = len(alice_sifted)
        logger.info(
            f"[Alice] Sifted session={session_id[:8]} "
            f"n_sifted={n_sifted}/{state['n_qubits']}"
        )

        try:
            await self._client.post(
                f"{KME_URL}/sessions/{session_id}/sift",
                json=SiftUpload(
                    session_id=session_id,
                    alice_bases=[
                        (qid, alice_bases[qid])
                        for qid in sorted(meas_by_id.keys())
                        if qid < len(alice_bases)
                    ],
                    sample_seed=sample_seed,
                ).model_dump(),
                timeout=10.0,
            )
        except Exception as e:
            logger.warning(
                f"[Alice] Post sift failed {session_id[:8]}: {e}"
            )

        if n_sifted < 10:
            await self._post_key(session_id, "aborted",
                                 error="INSUFFICIENT_BITS", n_sifted=n_sifted)
            self._cleanup(session_id)
            return

        qber, alice_final, _ = compute_qber(
            alice_sifted, bob_sifted, sample_seed=sample_seed
        )
        logger.info(
            f"[Alice] QBER={qber*100:.2f}% session={session_id[:8]} "
            f"threshold={QBER_THRESHOLD*100:.1f}%"
        )

        if qber > QBER_THRESHOLD:
            await self._post_key(session_id, "aborted",
                                 error="QBER_TOO_HIGH", n_sifted=n_sifted,
                                 qber=qber)
            self._cleanup(session_id)
            return

        key_final = "".join(map(str, alice_final))
        key_hash  = hashlib.sha256(bytes(alice_final)).hexdigest()
        await self._post_key(session_id, "success",
                             key_final=key_final, key_hash=key_hash,
                             qber=qber, n_sifted=n_sifted)
        logger.info(
            f"[Alice] Key posted session={session_id[:8]} "
            f"QBER={qber*100:.2f}% key_len={len(alice_final)}"
        )
        self._cleanup(session_id)

    async def _post_key(self, session_id, status,
                        key_final="", key_hash="",
                        qber=0.0, n_sifted=0, error=""):
        try:
            await self._client.post(
                f"{KME_URL}/sessions/{session_id}/key",
                json=KeyUpload(
                    session_id=session_id,
                    node_id=self.node_id,
                    key_final=key_final,
                    key_hash=key_hash,
                    qber=qber,
                    n_sifted=n_sifted,
                    status=status,
                    error_message=error,
                ).model_dump(),
                timeout=10.0,
            )
        except Exception as e:
            logger.error(
                f"[Alice] Post key failed {session_id[:8]}: {e}"
            )

    def _cleanup(self, session_id: str) -> None:
        state = self._alice_state.get(session_id)
        if state:
            state["done"] = True
        self._alice_state.pop(session_id, None)


    #Poll tick  defensive fallback (same as V7)
    async def _poll_tick(self) -> None:
        for sid in list(self._alice_state.keys()):
            state = self._alice_state.get(sid)
            if not state or state.get("done") or state.get("sifting_triggered"):
                continue
            try:
                data       = await self.kme_get(f"/sessions/{sid}")
                kme_status = data.get("status", "")
                if kme_status in ("aborted", "done"):
                    self._cleanup(sid)
                    continue
                if kme_status == "sending":
                    meas = (await self.kme_get(
                        f"/sessions/{sid}/measurements"
                    )).get("measurements", [])
                    if meas:
                        state["measurements_ready"] = True
                        self._try_start_sifting(sid)
            except Exception:
                pass



#FastAPI app
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
        "session_id":    session_id,
        "status":        "created",
        "qkdl_url":      body.get("qkdl_url"),
        "intercepted":   body.get("intercepted", False),
        "poll_url":      f"{KME_URL}/sessions/{session_id}",
        "consume_url":   f"{KME_URL}/sessions/{session_id}/consume-key",
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")