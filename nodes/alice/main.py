"""
nodes/alice/main.py  — SAE-A (Alice)
======================================
Port 8001.

Alice is now an active agent. She:
  1. Registers with KME on startup
  2. On demand: creates a session (POST /sessions)
  3. On webhook receiver_joined: starts transmitting qubits
  4. On webhook measurements_ready: runs sifting + QBER locally
  5. Posts final key result to KME (POST /sessions/{id}/key)

BB84 logic is unchanged — same bb84_logic.py functions.
What changed: Alice drives the flow instead of being driven.

Compared to v6 alice.py:
  - Removed: /emit endpoint (orchestrator no longer calls Alice)
  - Removed: Celery chord/group for qubit sending (now async HTTP batches)
  - Added:   POST /start  (client calls Alice directly to start a session)
  - Added:   webhook handlers (on_receiver_joined, on_measurements_ready)
  - Added:   BaseNode inheritance
  - Kept:    _make_batches, BB84 logic (compute_qber, derive_final_key)
  - Kept:    qubit_id explicit in all batches

Why remove Celery here?
  Celery's chord was needed when the orchestrator was driving parallel
  qubit dispatch. Now Alice drives her own async loop — asyncio tasks
  replace the chord. For high-throughput production, Celery can be
  re-added as an implementation detail inside AliceNode._send_qubits().
"""

import asyncio
import hashlib
import logging
import os
import random
import sys
import time

import uvicorn
from fastapi import FastAPI

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from node.base_node import BaseNode
from shared.models import (
    NodeRole, SessionCreateReq,
    QubitRecord, QubitBatch, QubitUpload,
    SiftUpload, KeyUpload, Basis,
)
from shared.bb84_logic import compute_qber, QBER_THRESHOLD

logger = logging.getLogger("alice")
logging.basicConfig(level=logging.INFO)

KME_URL    = os.getenv("KME_URL",    "http://localhost:8000")
QKDL_URL   = os.getenv("QKDL_URL",  "http://localhost:8003")
MY_URL     = os.getenv("ALICE_URL",  "http://localhost:8001")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))


class AliceNode(BaseNode):
    """
    Alice: the sender node in BB84.
    Inherits registration, webhook handling, and polling from BaseNode.
    """

    def __init__(self):
        super().__init__(
            role=NodeRole.SENDER,
            label=os.getenv("ALICE_LABEL", "alice-1"),
            callback_url=f"{MY_URL}/webhook",
        )
        # session_id → {bits, bases, n_qubits, ...}
        self._alice_state: dict[str, dict] = {}

    # ─────────────────────────────────────────
    # Client-facing: start a new session
    # ─────────────────────────────────────────

    async def start_bb84_session(
        self,
        receiver_label: str,
        n_qubits:       int   = 200,
        batch_size:     int   = BATCH_SIZE,
        loss_rate:      float = 0.0,
        retry_enabled:  bool  = False,
    ) -> str:
        """
        Alice initiates a BB84 session with a named receiver.
        Returns session_id immediately (KME returns 202).
        """
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
        data       = resp.json()
        session_id = data["session_id"]

        # Generate bits and bases locally right away
        bits  = [random.randint(0, 1)       for _ in range(n_qubits)]
        bases = [random.choice(list(Basis)) for _ in range(n_qubits)]

        self._alice_state[session_id] = {
            "bits":       bits,
            "bases":      bases,
            "n_qubits":   n_qubits,
            "batch_size": batch_size,
            "loss_rate":  loss_rate,
        }

        logger.info(
            f"[Alice] Session {session_id[:8]} created — "
            f"n_qubits={n_qubits} receiver={receiver_label}"
        )
        return session_id

    # ─────────────────────────────────────────
    # Webhook handlers
    # ─────────────────────────────────────────

    async def on_receiver_joined(self, session_id: str, payload: dict) -> None:
        """
        Bob has joined → Alice starts sending qubits.
        Runs as an asyncio task (non-blocking).
        """
        logger.info(f"[Alice] Receiver joined session {session_id[:8]} — sending qubits")
        asyncio.create_task(self._send_qubits(session_id))

    async def on_measurements_ready(self, session_id: str, payload: dict) -> None:
        """
        Bob's measurements are in the KME bus.
        Alice fetches them, runs sifting + QBER, posts key.
        """
        logger.info(f"[Alice] Measurements ready session {session_id[:8]}")
        asyncio.create_task(self._run_sifting_and_key(session_id))

    # ─────────────────────────────────────────
    # BB84 logic — Alice-side
    # ─────────────────────────────────────────

    async def _send_qubits(self, session_id: str) -> None:
        """
        Batch-uploads qubit data to the KME bus.
        KME → QKDL will transmit to Bob.
        Uses explicit qubit_id for ordering invariance.
        """
        state      = self._alice_state.get(session_id)
        if not state:
            logger.error(f"[Alice] No state for session {session_id[:8]}")
            return

        bits       = state["bits"]
        bases      = state["bases"]
        n          = state["n_qubits"]
        batch_size = state["batch_size"]

        batch_id = 0
        for start in range(0, n, batch_size):
            end    = min(start + batch_size, n)
            qubits = [
                QubitRecord(qubit_id=i, bit=bits[i], basis=bases[i])
                for i in range(start, end)
            ]
            batch = QubitBatch(
                session_id=session_id,
                batch_id=batch_id,
                qubits=qubits,
            )
            await self._client.post(
                f"{KME_URL}/sessions/{session_id}/qubits",
                json=QubitUpload(
                    session_id=session_id,
                    batch=batch,
                ).model_dump(),
            )
            batch_id += 1

            # Small yield to keep event loop responsive
            await asyncio.sleep(0.01)

        logger.info(
            f"[Alice] {batch_id} batches uploaded session {session_id[:8]}"
        )

    async def _run_sifting_and_key(self, session_id: str) -> None:
        """
        Fetches Bob's measurements from KME, runs sifting locally,
        estimates QBER, derives key, posts result to KME.
        """
        state = self._alice_state.get(session_id)
        if not state:
            return

        # ── Fetch measurements ────────────────────────────
        resp = await self._client.get(
            f"{KME_URL}/sessions/{session_id}/measurements"
        )
        resp.raise_for_status()
        raw_meas = resp.json().get("measurements", [])

        # ── Sifting by qubit_id ───────────────────────────
        alice_bits  = state["bits"]
        alice_bases = [b.value for b in state["bases"]]
        sample_seed = random.randint(0, 2**31)

        bob_bases_map: dict[int, str] = {
            m["qubit_id"]: m["basis"]
            for m in raw_meas
        }

        alice_sifted: list[int] = []
        bob_sifted:   list[int] = []

        for qid in sorted(bob_bases_map.keys()):
            if qid >= len(alice_bases):
                continue
            meas = next((m for m in raw_meas if m["qubit_id"] == qid), None)
            if not meas:
                continue
            if alice_bases[qid] == bob_bases_map[qid]:
                alice_sifted.append(alice_bits[qid])
                bob_sifted.append(meas["bit_result"])

        n_sifted = len(alice_sifted)
        logger.info(
            f"[Alice] Sifting done session={session_id[:8]} "
            f"n_sifted={n_sifted}/{state['n_qubits']}"
        )

        # Post Alice's bases to KME so Bob can do his local sift
        await self._client.post(
            f"{KME_URL}/sessions/{session_id}/sift",
            json=SiftUpload(
                session_id=session_id,
                alice_bases=list(bob_bases_map.items()),
                sample_seed=sample_seed,
            ).model_dump(),
        )

        # ── QBER + key ────────────────────────────────────
        if n_sifted < 10:
            await self._post_key(session_id, status="aborted",
                                 error="INSUFFICIENT_BITS",
                                 n_sifted=n_sifted)
            return

        qber, alice_final, _ = compute_qber(
            alice_sifted, bob_sifted, sample_seed=sample_seed
        )

        if qber > QBER_THRESHOLD:
            await self._post_key(session_id, status="aborted",
                                 error="QBER_TOO_HIGH", n_sifted=n_sifted,
                                 qber=qber)
            return

        key_bytes = bytes(alice_final)
        key_final = "".join(map(str, alice_final))
        key_hash  = hashlib.sha256(key_bytes).hexdigest()

        await self._post_key(
            session_id, status="success",
            key_final=key_final, key_hash=key_hash,
            qber=qber, n_sifted=n_sifted,
        )
        logger.info(
            f"[Alice] Key posted session={session_id[:8]} "
            f"QBER={qber*100:.2f}% key={key_hash[:16]}..."
        )

    async def _post_key(
        self, session_id: str, status: str,
        key_final: str = "", key_hash: str = "",
        qber: float = 0.0, n_sifted: int = 0, error: str = "",
    ) -> None:
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
        )

    # ─────────────────────────────────────────
    # Polling fallback (override base)
    # ─────────────────────────────────────────

    async def _poll_tick(self) -> None:
        """
        For each active session, check if measurements are ready
        in case the webhook was missed.
        """
        for sid, state in list(self._alice_state.items()):
            if state.get("sifting_done"):
                continue
            try:
                data = await self.kme_get(f"/sessions/{sid}/measurements")
                meas = data.get("measurements", [])
                if meas and not state.get("sifting_triggered"):
                    state["sifting_triggered"] = True
                    asyncio.create_task(self._run_sifting_and_key(sid))
            except Exception:
                pass


# ─────────────────────────────────────────────
# FastAPI app
# ─────────────────────────────────────────────

alice = AliceNode()
app   = alice.build_app(title="SAE-A — Alice (Sender)", port=8001)


@app.post("/start")
async def start_session(
    receiver_label: str   = "bob-1",
    n_qubits:       int   = 200,
    batch_size:     int   = BATCH_SIZE,
    loss_rate:      float = 0.0,
    retry_enabled:  bool  = False,
):
    """
    Client calls this to start a BB84 session.
    Alice creates it with the KME and returns session_id.
    """
    if not alice.node_id:
        return {"error": "Not registered yet"}, 503

    session_id = await alice.start_bb84_session(
        receiver_label=receiver_label,
        n_qubits=n_qubits,
        batch_size=batch_size,
        loss_rate=loss_rate,
        retry_enabled=retry_enabled,
    )
    return {"session_id": session_id, "status": "created"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
