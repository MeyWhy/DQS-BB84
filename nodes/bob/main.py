"""
nodes/bob/main.py  — SAE-B (Bob)
==================================
Port 8002.

Bob is now an active agent. He:
  1. Registers with KME on startup
  2. On webhook session_open: auto-joins the session
  3. Polls QKDL for incoming qubits and measures them
  4. Posts measurements to KME
  5. On webhook sift_ready: retrieves Alice's bases, does local sift
  6. On webhook key_available: key is ready

Compared to v6 bob.py:
  - Removed: /session/register endpoint (KME notifies Bob via webhook)
  - Removed: /sift endpoint (Bob now pulls Alice's bases from KME)
  - Removed: /session/{id}/sifted-bits endpoint (Alice reads from KME)
  - Added:   webhook handlers (on_session_open, on_sift_ready)
  - Added:   _receive_and_measure() — Bob polls QKDL himself
  - Added:   BaseNode inheritance
  - Kept:    local sifting logic (by qubit_id)
  - Kept:    Redis storage for measurements via KME bus
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
from shared.models import (
    NodeRole, MeasurementUpload, MeasurementRecord, Basis,
)

logger = logging.getLogger("bob")
logging.basicConfig(level=logging.INFO)

KME_URL    = os.getenv("KME_URL",   "http://localhost:8000")
QKDL_URL   = os.getenv("QKDL_URL", "http://localhost:8003")
MY_URL     = os.getenv("BOB_URL",   "http://localhost:8002")


class BobNode(BaseNode):

    def __init__(self):
        super().__init__(
            role=NodeRole.RECEIVER,
            label=os.getenv("BOB_LABEL", "bob-1"),
            callback_url=f"{MY_URL}/webhook",
        )
        # session_id → {n_qubits, measurements: [], sifted_bits: []}
        self._bob_state: dict[str, dict] = {}

    # ─────────────────────────────────────────
    # Webhook handlers
    # ─────────────────────────────────────────

    async def on_session_open(self, session_id: str, payload: dict) -> None:
        """
        KME notified Bob that a session is open for him.
        Bob auto-joins, then starts polling QKDL for qubits.
        """
        await self.join_session(session_id)
        self._bob_state[session_id] = {
            "n_qubits":    payload.get("n_qubits", 200),
            "measurements": [],
            "sifted_bits": [],
            "measuring_done": False,
        }
        logger.info(
            f"[Bob] Joined session {session_id[:8]} — "
            f"waiting for qubits"
        )
        # Start qubit reception loop
        asyncio.create_task(self._receive_and_measure(session_id))

    async def on_sift_ready(self, session_id: str, payload: dict) -> None:
        """
        Alice's bases are in the KME bus.
        Bob retrieves them and does local sifting.
        """
        logger.info(f"[Bob] Sift data ready session {session_id[:8]}")
        asyncio.create_task(self._do_local_sift(session_id))

    async def on_key_available(self, session_id: str, payload: dict) -> None:
        qber = payload.get("qber", 0.0)
        logger.info(
            f"[Bob] Key available session={session_id[:8]} "
            f"QBER={qber*100:.2f}% — ready for consumption"
        )

    # ─────────────────────────────────────────
    # Qubit reception (Bob polls QKDL directly)
    # ─────────────────────────────────────────

    async def _receive_and_measure(self, session_id: str) -> None:
        """
        Bob polls the QKDL service for incoming qubits.
        For each qubit received, Bob measures in a random basis.
        Accumulates measurements until n_qubits received or timeout.

        Why poll QKDL directly instead of KME?
        The QKDL has the live QuNetSim hosts — Bob must call
        QKDL's measurement API which blocks on the quantum network.
        KME is only a bus for classical data.
        """
        state    = self._bob_state.get(session_id)
        if not state:
            return

        n_qubits     = state["n_qubits"]
        measurements = []
        deadline     = time.time() + 120   # 2 min max

        while len(measurements) < n_qubits and time.time() < deadline:
            try:
                resp = await self._client.get(
                    f"{QKDL_URL}/qubit/receive/{session_id}",
                    timeout=5.0,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("qubit_id") is not None:
                        bob_basis  = random.choice(list(Basis))
                        bit_result = data.get("bit_result")

                        if bit_result is None:
                            # QKDL returns the raw qubit for Bob to measure
                            # In the full implementation, QKDL measures it
                            # and returns bit_result. Fallback: random.
                            bit_result = random.randint(0, 1)

                        measurements.append(MeasurementRecord(
                            qubit_id=data["qubit_id"],
                            basis=bob_basis,
                            bit_result=bit_result,
                        ))
                    elif data.get("queue_empty"):
                        await asyncio.sleep(0.1)
                    else:
                        await asyncio.sleep(0.05)
                else:
                    await asyncio.sleep(0.2)

            except Exception as e:
                logger.debug(f"[Bob] QKDL poll error: {e}")
                await asyncio.sleep(0.5)

        state["measurements"]   = measurements
        state["measuring_done"] = True

        logger.info(
            f"[Bob] Measured {len(measurements)}/{n_qubits} qubits "
            f"session={session_id[:8]}"
        )

        # Post measurements to KME bus
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
        resp = await self._client.post(
            f"{KME_URL}/sessions/{session_id}/measurements",
            json=upload.model_dump(),
        )
        resp.raise_for_status()
        logger.info(
            f"[Bob] Posted {len(measurements)} measurements "
            f"session={session_id[:8]}"
        )

    # ─────────────────────────────────────────
    # Local sifting (Bob-side)
    # ─────────────────────────────────────────

    async def _do_local_sift(self, session_id: str) -> None:
        """
        Retrieves Alice's bases from KME and computes Bob's sifted key.
        Sifted bits are stored locally — Bob can retrieve the key
        via POST /sessions/{id}/consume-key on the KME.
        """
        state = self._bob_state.get(session_id)
        if not state:
            return

        # Get Alice's bases from KME
        try:
            sift_data = await self.kme_get(f"/sessions/{session_id}/sift")
        except Exception as e:
            logger.error(f"[Bob] Failed to get sift data: {e}")
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

        # Apply same QBER sample removal as Alice
        import random as _r
        rng      = _r.Random(sample_seed)
        n        = len(sifted_bits)
        n_sample = max(1, int(n * 0.20)) if n > 0 else 0
        sample_idx = set(rng.sample(range(n), n_sample)) if n >= n_sample > 0 else set()
        bob_final  = [b for i, b in enumerate(sifted_bits) if i not in sample_idx]

        state["sifted_bits"] = sifted_bits
        state["bob_final"]   = bob_final

        logger.info(
            f"[Bob] Sifting done session={session_id[:8]} "
            f"n_sifted={len(sifted_bits)} key_len={len(bob_final)}"
        )

    # ─────────────────────────────────────────
    # Polling fallback
    # ─────────────────────────────────────────

    async def _poll_tick(self) -> None:
        """Check for open sessions Bob hasn't joined yet."""
        try:
            resp = await self._client.get(
                f"{KME_URL}/sessions", params={"active_only": True}
            )
            if resp.status_code != 200:
                return
            data = resp.json()
            for sid in data.get("sessions", []):
                if sid not in self._bob_state:
                    session = await self.get_session(sid)
                    if (session.get("status") == "open"
                            and session.get("receiver_node_id") == self.node_id):
                        await self.on_session_open(
                            sid, {"n_qubits": session.get("n_qubits", 200)}
                        )
        except Exception:
            pass


# ─────────────────────────────────────────────
# FastAPI app
# ─────────────────────────────────────────────

bob = BobNode()
app = bob.build_app(title="SAE-B — Bob (Receiver)", port=8002)


@app.get("/session/{session_id}/key")
async def get_local_key(session_id: str):
    """
    Returns Bob's locally computed sifted key.
    For demo/test use. In production, consume via KME /consume-key.
    """
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
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002, log_level="info")
