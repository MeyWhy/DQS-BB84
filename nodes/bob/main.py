"""
nodes/bob/main.py  — v0.8.0

Fix: _receive_and_measure now uses the qkdl_url returned in the
join_session response, instead of the module-level QKDL_URL env var.
Also reads qkdl_url from the session_open webhook payload as early hint.
"""
import asyncio
import logging
import os
import random
import sys
import time
import uvicorn

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from node.base_node import BaseNode
from models import (
    NodeRole, MeasurementUpload, MeasurementRecord, Basis,
)

logger  = logging.getLogger("bob")
logging.basicConfig(level=logging.INFO)

KME_URL  = os.getenv("KME_URL",  "http://localhost:8000")
MY_URL   = os.getenv("BOB_URL",  "http://localhost:8002")

QKDL_SECS_PER_QUBIT = 8.0
QKDL_FIXED_OVERHEAD  = 30.0


class BobNode(BaseNode):

    def __init__(self):
        super().__init__(
            role=NodeRole.RECEIVER,
            label=os.getenv("BOB_LABEL", "bob-1"),
            callback_url=f"{MY_URL}/webhook",
        )
        self._bob_state: dict[str, dict] = {}

    # ── Webhook handlers ──────────────────────────────────────────────────────

    async def on_session_open(self, session_id: str, payload: dict) -> None:
        # ── FIX: grab qkdl_url from payload early ─────────────────────────
        # KME now includes it in the session_open webhook so Bob knows which
        # QKDL to poll before join_session even returns.
        early_qkdl = payload.get(
            "qkdl_url",
            os.getenv("QKDL_URL", "http://localhost:8003"),
        )

        join_data = await self.join_session(session_id)
        # join_session response now carries qkdl_url; prefer it over payload
        qkdl_url  = join_data.get("qkdl_url", early_qkdl)

        n_qubits  = payload.get("n_qubits", 200)
        self._bob_state[session_id] = {
            "n_qubits":       n_qubits,
            "qkdl_url":       qkdl_url,   # stored per-session
            "measurements":   [],
            "sifted_bits":    [],
            "bob_final":      [],
            "measuring_done": False,
        }
        logger.info(
            f"[Bob] Joined session={session_id[:8]} "
            f"n_qubits={n_qubits} qkdl={qkdl_url}"
        )
        asyncio.create_task(self._receive_and_measure(session_id))

    async def on_sift_ready(self, session_id: str, payload: dict) -> None:
        logger.info(f"[Bob] Sift data ready session={session_id[:8]}")
        asyncio.create_task(self._do_local_sift(session_id))

    async def on_key_available(self, session_id: str, payload: dict) -> None:
        logger.info(
            f"[Bob] Key available session={session_id[:8]} "
            f"QBER={payload.get('qber', 0.0)*100:.2f}%"
        )

    async def on_session_aborted(self, session_id: str, payload: dict) -> None:
        logger.warning(
            f"[Bob] Session {session_id[:8]} aborted: "
            f"{payload.get('reason', '')}"
        )
        self._bob_state.pop(session_id, None)
        self._sessions.pop(session_id, None)

    # ── Qubit reception ───────────────────────────────────────────────────────

    async def _receive_and_measure(self, session_id: str) -> None:
        state = self._bob_state.get(session_id)
        if not state:
            return

        n_qubits = state["n_qubits"]
        qkdl_url = state["qkdl_url"]   # ← per-session QKDL URL
        measurements: list[MeasurementRecord] = []
        deadline = (
            time.time()
            + QKDL_FIXED_OVERHEAD
            + n_qubits * QKDL_SECS_PER_QUBIT
        )

        logger.info(
            f"[Bob] Receive loop started session={session_id[:8]} "
            f"target={n_qubits} qkdl={qkdl_url} "
            f"deadline_s={int(QKDL_FIXED_OVERHEAD + n_qubits * QKDL_SECS_PER_QUBIT)}"
        )

        while len(measurements) < n_qubits and time.time() < deadline:
            try:
                resp = await self._client.get(
                    f"{qkdl_url}/qubit/receive/{session_id}",  # ← per-session
                    timeout=5.0,
                )

                if resp.status_code == 404:
                    logger.warning(
                        f"[Bob] QKDL 404 session={session_id[:8]} "
                        f"qkdl={qkdl_url} got={len(measurements)}/{n_qubits} "
                        f"— QKDL session not found, stopping poll"
                    )
                    break

                if resp.status_code != 200:
                    await asyncio.sleep(0.2)
                    continue

                data = resp.json()

                if data.get("queue_empty"):
                    if len(measurements) > 0 and len(measurements) % 25 == 0:
                        logger.info(
                            f"[Bob] Queue empty mid-session={session_id[:8]} "
                            f"got={len(measurements)} "
                            f"remaining={n_qubits - len(measurements)}"
                        )
                    await asyncio.sleep(0.05)
                    continue

                qid        = data.get("qubit_id")
                raw_basis  = data.get("basis")
                bit_result = data.get("bit_result")

                if qid is None or raw_basis is None or bit_result is None:
                    await asyncio.sleep(0.02)
                    continue

                measurements.append(MeasurementRecord(
                    qubit_id=qid,
                    basis=Basis(raw_basis),
                    bit_result=bit_result,
                ))

                if len(measurements) % 25 == 0:
                    logger.info(
                        f"[Bob] Progress session={session_id[:8]} "
                        f"{len(measurements)}/{n_qubits}"
                    )

            except Exception as e:
                logger.warning(
                    f"[Bob] Receive error session={session_id[:8]}: {e}"
                )
                await asyncio.sleep(0.5)

        state["measurements"]   = measurements
        state["measuring_done"] = True

        complete = len(measurements) == n_qubits
        logger.info(
            f"[Bob] Receive loop complete session={session_id[:8]} "
            f"received={len(measurements)}/{n_qubits} "
            f"{'OK' if complete else 'PARTIAL/TIMEOUT'}"
        )

        if not measurements:
            logger.error(
                f"[Bob] Zero measurements — aborting post session={session_id[:8]}"
            )
            return

        await self._post_measurements(session_id, measurements)

    async def _post_measurements(
        self,
        session_id:   str,
        measurements: list[MeasurementRecord],
    ) -> None:
        upload = MeasurementUpload(
            session_id=session_id,
            node_id=self.node_id,
            measurements=measurements,
        )
        try:
            resp = await self._client.post(
                f"{KME_URL}/sessions/{session_id}/measurements",
                json=upload.model_dump(),
                timeout=30.0,
            )
            resp.raise_for_status()
            logger.info(
                f"[Bob] Posted {len(measurements)} measurements "
                f"session={session_id[:8]}"
            )
        except Exception as e:
            logger.error(
                f"[Bob] Failed to post measurements "
                f"session={session_id[:8]}: {e}"
            )

    # ── Local sifting ─────────────────────────────────────────────────────────

    async def _do_local_sift(self, session_id: str) -> None:
        state = self._bob_state.get(session_id)
        if not state:
            return

        try:
            sift_data = await self.kme_get(f"/sessions/{session_id}/sift")
        except Exception as e:
            logger.error(
                f"[Bob] Failed to get sift data session={session_id[:8]}: {e}"
            )
            return

        alice_bases_map: dict[int, str] = {
            qid: basis
            for qid, basis in sift_data.get("alice_bases", [])
        }
        sample_seed = sift_data.get("sample_seed", 0)

        measurements = state.get("measurements", [])
        sifted_bits: list[int] = []

        for meas in sorted(measurements, key=lambda m: m.qubit_id):
            qid = meas.qubit_id
            if qid in alice_bases_map and alice_bases_map[qid] == meas.basis.value:
                sifted_bits.append(meas.bit_result)

        import random as _r
        rng       = _r.Random(sample_seed)
        n         = len(sifted_bits)
        n_sample  = max(1, int(n * 0.20)) if n > 0 else 0
        sample_idx = (
            set(rng.sample(range(n), n_sample))
            if n >= n_sample > 0 else set()
        )
        bob_final = [b for i, b in enumerate(sifted_bits) if i not in sample_idx]

        state["sifted_bits"] = sifted_bits
        state["bob_final"]   = bob_final

        logger.info(
            f"[Bob] Local sift done session={session_id[:8]} "
            f"n_sifted={len(sifted_bits)} key_len={len(bob_final)}"
        )


# ── FastAPI app ───────────────────────────────────────────────────────────────

bob = BobNode()
app = bob.build_app(title="SAE-B — Bob (Receiver)", port=8002)


@app.get("/session/{session_id}/key")
async def get_local_key(session_id: str):
    state = bob._bob_state.get(session_id)
    if not state:
        return {"error": "Session not found"}, 404
    return {
        "session_id":  session_id,
        "sifted_bits": state.get("sifted_bits", []),
        "bob_final":   state.get("bob_final",   []),
        "n_sifted":    len(state.get("sifted_bits", [])),
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002, log_level="info")