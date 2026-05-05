from __future__ import annotations
import logging
import random

import httpx
from celery import group, chain, chord

from nodes.base_node import BaseNode
from models import NodeRole, NodeCapabilities, QubitBatch, QubitRecord, Basis
from workers.qubit_tasks    import send_batch_task
from workers.sifting_tasks  import assemble_and_sift_task, qber_key_task
from workers.notify_tasks   import notify_orchestrator_task

logger = logging.getLogger("node.alice")


class AliceNode(BaseNode):
    """
    Sender node.  When the orchestrator assigns a session, Alice:
      1. Initialises QNS.
      2. Generates bits + bases.
      3. Dispatches the Celery batch chord.
    All BB84 logic is unchanged; only the trigger mechanism is new.
    """

    def __init__(
        self,
        orch_url:     str,
        qns_url:      str,
        callback_url: str,             # Alice's own FastAPI base URL
        **kwargs,
    ):
        super().__init__(
            role=NodeRole.SENDER,
            orch_url=orch_url,
            callback_url=callback_url,
            capabilities=NodeCapabilities(max_qubits=5000),
            **kwargs,
        )
        self.qns_url = qns_url.rstrip("/")

    # ── session handler ───────────────────────────────────────────────────────

    async def handle_session(self, session: dict) -> None:
        """
        Entry point called by the poll loop (or by POST /session/begin from orch).
        Mirrors the old orchestrator._run_session logic.
        """
        session_id = session["session_id"]
        n_qubits   = session["n_qubits"]
        batch_size = session.get("batch_size", 10)
        loss_rate  = session.get("loss_rate", 0.0)

        logger.info(f"[Alice] Handling session {session_id} ({n_qubits} qubits)")

        async with httpx.AsyncClient(timeout=30.0) as client:
            # 1. Init QNS
            try:
                resp = await client.post(f"{self.qns_url}/network/init", json={
                    "session_id": session_id,
                    "n_qubits":   n_qubits,
                    "loss_rate":  loss_rate,
                })
                resp.raise_for_status()
                logger.info(f"[Alice] QNS initialised — session {session_id}")
            except httpx.HTTPError as e:
                logger.error(f"[Alice] QNS init failed: {e}")
                await self._report_abort(session_id, f"QNS init failed: {e}")
                return

        # 2. Generate bits + bases, build batches
        bits, bases, batches = self._make_batches(session_id, n_qubits, batch_size)

        session_meta = {
            "session_id":  session_id,
            "n_qubits":    n_qubits,
            "alice_bits":  bits,
            "alice_bases": bases,
        }

        # 3. Dispatch Celery chord (unchanged from old alice.py)
        batch_group = group(
            send_batch_task.s(
                session_id=session_id,
                batch_payload=batch.model_dump(),
            )
            for batch in batches
        )

        pipeline = chord(batch_group)(
            chain(
                assemble_and_sift_task.s(session_meta=session_meta),
                qber_key_task.s(),
                notify_orchestrator_task.s(),
            )
        )

        logger.info(
            f"[Alice] Celery pipeline started session={session_id} "
            f"batches={len(batches)} task_id={pipeline.id}"
        )

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _make_batches(
        session_id: str,
        n_qubits:   int,
        batch_size: int,
    ) -> tuple[list[int], list[str], list[QubitBatch]]:
        bits  = [random.randint(0, 1) for _ in range(n_qubits)]
        bases = [random.choice(list(Basis)) for _ in range(n_qubits)]
        batches: list[QubitBatch] = []

        for batch_id, start in enumerate(range(0, n_qubits, batch_size)):
            end = min(start + batch_size, n_qubits)
            batches.append(QubitBatch(
                session_id=session_id,
                batch_id=batch_id,
                qubits=[
                    QubitRecord(qubit_id=i, bit=bits[i], basis=bases[i])
                    for i in range(start, end)
                ],
            ))

        return bits, [b.value for b in bases], batches

    async def _report_abort(self, session_id: str, reason: str) -> None:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(
                    f"{self.orch_url}/sessions/{session_id}/complete",
                    json={"status": "aborted", "error_message": reason},
                )
        except httpx.HTTPError as e:
            logger.warning(f"[Alice] Could not report abort for {session_id}: {e}")