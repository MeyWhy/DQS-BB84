"""
Eve node that uses the intercept-resend eavesdropping type for BB84

How it works :
1.Alice calls post /sessions with interceptor_label="eve-1"

2.KME stores eve_node_id in the session and fires a session_open webhook
with role="monitor" to Eve so it can perceive the content of the session

3.Eve receives the webhook -> calls POST /intercept/{session_id} on the
   QKDL to register herself as a Midm

4.From that point on, every qubit Alice sends is intercepted inside
   _process_batch_sync (qunetsim_service.py) before reaching Bob:
   - Eve picks a random basis and "measures" Alice's qubit
   - Eve re-prepares a new qubit in her measured basis/bit
   - Bob receives Eve's re-prepared qubit, not Alice's original

5.When Alice and Bob sift on matching bases, ~25% of the surviving bits
   differ because Eve guessed the wrong basis half the time.  This exceeds
   QBER_THRESHOLD and the session is aborted with QBER_TOO_HIGH.

6.Eve polls GET /intercept/{session_id}/measurements to collect her own
   measurement log (alice_basis, eve_basis, basis_match, bits).
   She persists these for the thesis QBER comparison.

"""
import asyncio
import logging
import os
import sys
import uvicorn

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from node.base_node import BaseNode
from models import NodeRole

logger   = logging.getLogger("eve")
logging.basicConfig(level=logging.INFO)

KME_URL = os.getenv("KME_URL",  "http://localhost:8000")
MY_URL  = os.getenv("EVE_URL",  "http://localhost:8010")

#How long Eve retries registering on the QKDL before giving up.
#The QKDL session is created in a background task, so there's a small
#window where the session exists in KME but the QKDL hasn't started yet.
EVE_REGISTER_RETRIES = 8
EVE_REGISTER_DELAY   = 1.2   #seconds between retries


class EveNode(BaseNode):

    def __init__(self):
        super().__init__(
            role=NodeRole.MONITOR,
            label=os.getenv("EVE_LABEL", "eve-1"),
            callback_url=f"{MY_URL}/webhook",
        )
        #session_id -> {qkdl_url, registered, meas_log, done}
        self._eve_state: dict[str, dict] = {}

    
    #Webhook handlers
    async def on_session_open(self, session_id: str, payload: dict) -> None:
        role = payload.get("role", "")
        if role != "monitor":
            #Not our session — ignore
            return

        qkdl_url = payload.get("qkdl_url",
                               os.getenv("QKDL_URL", "http://localhost:8003"))
        n_qubits = payload.get("n_qubits", 0)

        self._eve_state[session_id] = {
            "qkdl_url":   qkdl_url,
            "n_qubits":   n_qubits,
            "registered": False,
            "meas_log":   [],
            "done":       False,
        }

        logger.info(
            f"[Eve] Session open — session={session_id[:8]} "
            f"n_qubits={n_qubits} qkdl={qkdl_url} — registering as interceptor"
        )
        asyncio.create_task(self._register_as_interceptor(session_id))

    async def on_key_available(self, session_id: str, payload: dict) -> None:
        #This fires if Eve somehow didn't raise the QBER enough.
        #Practically impossible with intercept-resend but handle gracefully.
        logger.warning(
            f"[Eve] Session {session_id[:8]} completed WITH a key — "
            f"QBER={payload.get('qber', 0)*100:.2f}%  "
            f"(Eve's attack may have been too lucky — re-run)"
        )
        asyncio.create_task(self._collect_measurements(session_id))
        self._finish(session_id)

    async def on_session_aborted(self, session_id: str, payload: dict) -> None:
        reason = payload.get("reason", "")
        qber   = payload.get("qber", 0.0)
        logger.info(
            f"[Eve] Session {session_id[:8]} aborted — "
            f"reason={reason} QBER={qber*100:.2f}%"
        )
        #Collect any remaining measurements from QKDL before session cleans up
        asyncio.create_task(self._collect_measurements(session_id))
        self._finish(session_id)

    #QKDL intercept registration
    async def _register_as_interceptor(self, session_id: str) -> None:
    
        state = self._eve_state.get(session_id)
        if not state:
            return

        qkdl_url = state["qkdl_url"]

        for attempt in range(1, EVE_REGISTER_RETRIES + 1):
            try:
                resp = await self._client.post(
                    f"{qkdl_url}/intercept/{session_id}",
                    json={
                        "session_id":  session_id,
                        "eve_node_id": self.node_id,
                        "eve_label":   self.label,
                    },
                    timeout=5.0,
                )

                if resp.status_code == 404:
                    #QKDL session not yet active — retry
                    logger.debug(
                        f"[Eve] QKDL not ready attempt={attempt}/{EVE_REGISTER_RETRIES} "
                        f"session={session_id[:8]}"
                    )
                    await asyncio.sleep(EVE_REGISTER_DELAY)
                    continue

                resp.raise_for_status()
                state["registered"] = True
                logger.info(
                    f"[Eve] *** Registered as interceptor "
                    f"session={session_id[:8]} qkdl={qkdl_url} "
                    f"(attempt {attempt}) ***"
                )
                return

            except Exception as e:
                logger.warning(
                    f"[Eve] Register attempt {attempt} failed "
                    f"session={session_id[:8]}: {e}"
                )
                await asyncio.sleep(EVE_REGISTER_DELAY)

        logger.error(
            f"[Eve] Failed to register as interceptor after "
            f"{EVE_REGISTER_RETRIES} attempts session={session_id[:8]}"
        )

    
    #Measurement collectio
    async def _collect_measurements(self, session_id: str) -> None:
      
        state = self._eve_state.get(session_id)
        if not state:
            return

        qkdl_url = state["qkdl_url"]
        try:
            resp = await self._client.get(
                f"{qkdl_url}/intercept/{session_id}/measurements",
                timeout=5.0,
            )
            resp.raise_for_status()
            data = resp.json()
            records = data.get("measurements", [])
            state["meas_log"].extend(records)

            n_intercepted = len(state["meas_log"])
            n_basis_match = sum(1 for m in state["meas_log"] if m.get("basis_match"))
            logger.info(
                f"[Eve] Measurements collected session={session_id[:8]} "
                f"n={n_intercepted} basis_match={n_basis_match} "
                f"({n_basis_match/max(n_intercepted,1)*100:.1f}%)"
            )
        except Exception as e:
            logger.warning(
                f"[Eve] Collect measurements failed session={session_id[:8]}: {e}"
            )

    def _finish(self, session_id: str) -> None:
        state = self._eve_state.get(session_id)
        if state:
            state["done"] = True

    
    #Stats helper
    def session_stats(self, session_id: str) -> dict:
        state = self._eve_state.get(session_id)
        if not state:
            return {"error": "Session not found"}

        log = state.get("meas_log", [])
        n   = len(log)
        if n == 0:
            return {
                "session_id":       session_id,
                "registered":       state.get("registered"),
                "n_intercepted":    0,
                "basis_match_rate": None,
                "theoretical_qber": 0.25,
                "meas_log":         [],
            }

        n_match = sum(1 for m in log if m.get("basis_match"))
        return {
            "session_id":       session_id,
            "registered":       state.get("registered"),
            "n_intercepted":    n,
            "basis_match_rate": round(n_match / n, 4),
            "theoretical_qber": 0.25,
            #QBER Eve would introduce: errors from wrong-basis interceptions
            #= 0.5 (prob wrong basis) * 0.5 (prob random bit is wrong) = 0.25
            "eve_induced_qber_theory": 0.25,
            "meas_log":         log,
        }



eve = EveNode()
app = eve.build_app(title="SAE-E — Eve (Monitor / Interceptor)", port=8010)


@app.get("/session/{session_id}/stats")
async def get_eve_stats(session_id: str):
  
    return eve.session_stats(session_id)


@app.get("/sessions")
async def list_eve_sessions():
    return {
        "sessions": [
            {
                "session_id": sid,
                "registered": s.get("registered"),
                "done":       s.get("done"),
                "n_meas":     len(s.get("meas_log", [])),
            }
            for sid, s in eve._eve_state.items()
        ]
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8010, log_level="info")
