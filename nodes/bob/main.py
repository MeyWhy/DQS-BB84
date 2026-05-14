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
        #session_id -> {n_qubits, measurements: [], sifted_bits: []}
        self._bob_state: dict[str, dict] = {}

    async def on_session_open(self, session_id: str, payload: dict) -> None:
    
        await self.join_session(session_id)
        self._bob_state[session_id] = {
            "n_qubits":    payload.get("n_qubits", 200),
            "measurements": [],
            "sifted_bits": [],
            "measuring_done": False,
        }
        logger.info(
            f"[Bob] Joined session {session_id[:8]}   "
            f"waiting for qubits"
        )
        #start qubit reception loop
        asyncio.create_task(self._receive_and_measure(session_id))

    async def on_sift_ready(self, session_id: str, payload: dict) -> None:
 
        logger.info(f"[Bob] Sift data ready session {session_id[:8]}")
        asyncio.create_task(self._do_local_sift(session_id))

    async def on_key_available(self, session_id: str, payload: dict) -> None:
        qber = payload.get("qber", 0.0)
        logger.info(
            f"[Bob] Key available session={session_id[:8]} "
            f"QBER={qber*100:.2f}%   ready for consumption"
        )

    #Qubit reception (Bob polls QKDL directly)
    async def _receive_and_measure(self, session_id: str) -> None:
        state = self._bob_state.get(session_id)
        if not state:
            return

        n_qubits     = state["n_qubits"]
        measurements = []
        deadline     = time.time() + 120

        while len(measurements) < n_qubits and time.time() < deadline:
            try:
                resp = await self._client.get(
                    f"{QKDL_URL}/qubit/receive/{session_id}",
                    timeout=5.0,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("qubit_id") is not None:
                        #The QKDL has already measured the qubit with a random
                        #basis   we must use that exact measurement, not override it
                        raw_basis  = data.get("basis")
                        bit_result = data.get("bit_result")

                        if raw_basis is None or bit_result is None:
                            #Qubit was lost in transit   skip it
                            await asyncio.sleep(0.05)
                            continue

                        measurements.append(MeasurementRecord(
                            qubit_id=data["qubit_id"],
                            basis=Basis(raw_basis),   #use the QKDL's basis
                            bit_result=bit_result,     #use the QKDL's measurement
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

    #Local sifting (Bob-side)

    async def _do_local_sift(self, session_id: str) -> None:
  
        state = self._bob_state.get(session_id)
        if not state:
            return

        #Get Alice's bases from KME
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

        #Apply same QBER sample removal as Alice
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


bob = BobNode()
app = bob.build_app(title="SAE-B   Bob (Receiver)", port=8002)


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
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002, log_level="info")
