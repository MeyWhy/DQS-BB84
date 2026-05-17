"""
nodes/alice/main.py  -- v0.8.0

Fix: _send_qubits now uses the qkdl_url returned by KME in the session
create response, instead of the module-level QKDL_URL env var.
This ensures Alice always talks to the exact QKDL instance that KME
initialised the session on, even when multiple QKDL instances are running.
"""
import asyncio
import hashlib
import logging
import os
import random
import sys
import uvicorn

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from node.base_node import BaseNode
from models import (
    NodeRole, SessionCreateReq, QubitRecord, QubitBatch,
    SiftUpload, KeyUpload, Basis,
)
from bb84_logic import compute_qber, QBER_THRESHOLD

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

    async def start_bb84_session(
        self,
        receiver_label: str,
        n_qubits:       int   = 200,
        batch_size:     int   = BATCH_SIZE,
        loss_rate:      float = 0.0,
        retry_enabled:  bool  = False,
    ) -> str:
        resp = await self._client.post(
            f"{KME_URL}/sessions",
            json=SessionCreateReq(
                sender_node_id=self.node_id,
                receiver_label=receiver_label,
                n_qubits=n_qubits,
                batch_size=batch_size,
                loss_rate=loss_rate,
                retry_enabled=retry_enabled,
            ).model_dump(),
        )
        resp.raise_for_status()
        body       = resp.json()
        session_id = body["session_id"]

        #FIX: use the QKDL URL assigned by KME for this session 
        #Do NOT use the QKDL_URL env var -- that could point to a different
        #QKDL instance than the one KME initialised this session on.
        qkdl_url = body.get("qkdl_url", os.getenv("QKDL_URL", "http://localhost:8003"))

        bits  = [random.randint(0, 1)       for _ in range(n_qubits)]
        bases = [random.choice(list(Basis)) for _ in range(n_qubits)]

        self._alice_state[session_id] = {
            "bits":               bits,
            "bases":              bases,
            "n_qubits":           n_qubits,
            "batch_size":         batch_size,
            "qkdl_url":           qkdl_url,   #stored per-session
            "sifting_triggered":  False,
            "done":               False,
        }
        logger.info(
            f"[Alice] Session {session_id[:8]} created "
            f"n_qubits={n_qubits} qkdl={qkdl_url}"
        )
        return session_id

    #Webhook handlers 

    async def on_receiver_joined(self, session_id: str, payload: dict) -> None:
        logger.info(
            f"[Alice] Receiver joined {session_id[:8]} -- starting qubit send"
        )
        asyncio.create_task(self._send_qubits(session_id))

    async def on_measurements_ready(self, session_id: str, payload: dict) -> None:
        state = self._alice_state.get(session_id)
        if not state or state.get("sifting_triggered") or state.get("done"):
            return
        state["sifting_triggered"] = True
        logger.info(
            f"[Alice] measurements_ready → sifting {session_id[:8]} "
            f"n={payload.get('n_measurements', '?')}"
        )
        asyncio.create_task(self._run_sifting_and_key(session_id))

    async def on_session_aborted(self, session_id: str, payload: dict) -> None:
        self._cleanup(session_id)
        logger.info(f"[Alice] Session {session_id[:8]} aborted -- cleaned up")

    #Qubit transmission 

    async def _send_qubits(self, session_id: str) -> None:
        state = self._alice_state.get(session_id)
        if not state:
            logger.warning(f"[Alice] _send_qubits: no state for {session_id[:8]}")
            return

        bits, bases = state["bits"], state["bases"]
        n, batch_size = state["n_qubits"], state["batch_size"]

        #Use the per-session QKDL URL 
        qkdl_url  = state["qkdl_url"]
        n_batches = (n + batch_size - 1) // batch_size

        logger.info(
            f"[Alice] Transmission start session={session_id[:8]} "
            f"total={n} batch_size={batch_size} "
            f"n_batches={n_batches} qkdl={qkdl_url}"
        )

        n_delivered = 0
        for batch_id, start in enumerate(range(0, n, batch_size)):
            end    = min(start + batch_size, n)
            qubits = [
                {"qubit_id": i, "bit": bits[i], "basis": bases[i].value}
                for i in range(start, end)
            ]
            try:
                resp = await self._client.post(
                    f"{qkdl_url}/batch/send",       #← per-session URL
                    json={
                        "session_id": session_id,
                        "batch": {
                            "session_id": session_id,
                            "batch_id":   batch_id,
                            "qubits":     qubits,
                        },
                    },
                    timeout=120.0,
                )
                resp.raise_for_status()
                results     = resp.json().get("results", [])
                batch_deliv = sum(1 for r in results if r.get("delivered"))
                n_delivered += batch_deliv
                logger.info(
                    f"[Alice] Batch {batch_id}/{n_batches-1} "
                    f"session={session_id[:8]} "
                    f"batch_delivered={batch_deliv} total={n_delivered}"
                )
            except Exception as e:
                logger.warning(
                    f"[Alice] Batch {batch_id} FAILED "
                    f"session={session_id[:8]} qkdl={qkdl_url}: {e}"
                )
            await asyncio.sleep(0.002)

        logger.info(
            f"[Alice] All batches sent session={session_id[:8]} "
            f"delivered={n_delivered}/{n}"
        )

    #Sifting + key derivation 

    async def _run_sifting_and_key(self, session_id: str) -> None:
        state = self._alice_state.get(session_id)
        if not state:
            return

        #Fetch Bob's measurements
        try:
            resp = await self._client.get(
                f"{KME_URL}/sessions/{session_id}/measurements",
                timeout=15.0,
            )
            resp.raise_for_status()
            raw_meas = resp.json().get("measurements", [])
        except Exception as e:
            logger.error(
                f"[Alice] Fetch measurements failed session={session_id[:8]}: {e}"
            )
            await self._post_key(session_id, "aborted", error="FETCH_FAILED")
            self._cleanup(session_id)
            return

        logger.info(
            f"[Alice] Sifting session={session_id[:8]} "
            f"raw_meas_count={len(raw_meas)}"
        )

        if not raw_meas:
            logger.warning(
                f"[Alice] No measurements yet session={session_id[:8]} "
                f"-- will retry via poll"
            )
            state["sifting_triggered"] = False
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
                bob_sifted.append(m.get("bit_result", m.get("bit_res", 0)))

        n_sifted = len(alice_sifted)
        logger.info(
            f"[Alice] Sifting done session={session_id[:8]} "
            f"n_sifted={n_sifted}/{state['n_qubits']}"
        )

        #Post Alice's bases so Bob can sift locally
        try:
            alice_bases_payload = [
                (qid, alice_bases[qid])
                for qid in sorted(meas_by_id.keys())
                if qid < len(alice_bases)
            ]
            await self._client.post(
                f"{KME_URL}/sessions/{session_id}/sift",
                json=SiftUpload(
                    session_id=session_id,
                    alice_bases=alice_bases_payload,
                    sample_seed=sample_seed,
                ).model_dump(),
                timeout=10.0,
            )
        except Exception as e:
            logger.warning(
                f"[Alice] Post sift failed session={session_id[:8]}: {e}"
            )

        if n_sifted < 10:
            logger.warning(
                f"[Alice] INSUFFICIENT_BITS session={session_id[:8]} "
                f"n_sifted={n_sifted}"
            )
            await self._post_key(
                session_id, "aborted",
                error="INSUFFICIENT_BITS", n_sifted=n_sifted,
            )
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
            await self._post_key(
                session_id, "aborted",
                error="QBER_TOO_HIGH", n_sifted=n_sifted, qber=qber,
            )
            self._cleanup(session_id)
            return

        key_final = "".join(map(str, alice_final))
        key_hash  = hashlib.sha256(bytes(alice_final)).hexdigest()
        await self._post_key(
            session_id, "success",
            key_final=key_final, key_hash=key_hash,
            qber=qber, n_sifted=n_sifted,
        )
        logger.info(
            f"[Alice] Key posted session={session_id[:8]} "
            f"QBER={qber*100:.2f}% key_len={len(alice_final)}"
        )
        self._cleanup(session_id)

    async def _post_key(
        self, session_id, status,
        key_final="", key_hash="",
        qber=0.0, n_sifted=0, error="",
    ):
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
                f"[Alice] Post key failed session={session_id[:8]}: {e}"
            )

    def _cleanup(self, session_id: str) -> None:
        state = self._alice_state.get(session_id)
        if state:
            state["done"] = True
        self._alice_state.pop(session_id, None)

    #Poll-tick fallback 

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
                    meas = (
                        await self.kme_get(f"/sessions/{sid}/measurements")
                    ).get("measurements", [])
                    if meas and not state.get("sifting_triggered"):
                        logger.info(
                            f"[Alice] Poll fallback: triggering sifting "
                            f"session={sid[:8]} n_meas={len(meas)}"
                        )
                        state["sifting_triggered"] = True
                        asyncio.create_task(self._run_sifting_and_key(sid))

            except Exception:
                pass


#FastAPI app 

alice = AliceNode()
app   = alice.build_app(title="SAE-A -- Alice (Sender)", port=8001)


@app.post("/start")
async def start_session(
    receiver_label: str   = "bob-1",
    n_qubits:       int   = 200,
    batch_size:     int   = BATCH_SIZE,
    loss_rate:      float = 0.0,
    retry_enabled:  bool  = False,
):
    if not alice.node_id:
        return {"error": "Not registered yet -- retry in 1s"}, 503

    session_id = await alice.start_bb84_session(
        receiver_label=receiver_label,
        n_qubits=n_qubits,
        batch_size=batch_size,
        loss_rate=loss_rate,
        retry_enabled=retry_enabled,
    )
    return {
        "session_id":  session_id,
        "status":      "created",
        "poll_url":    f"{KME_URL}/sessions/{session_id}",
        "consume_url": f"{KME_URL}/sessions/{session_id}/consume-key",
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")