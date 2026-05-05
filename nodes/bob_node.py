from __future__ import annotations
import logging

import httpx

from nodes.base_node import BaseNode
from models import NodeRole, NodeCapabilities

logger = logging.getLogger("node.bob")


class BobNode(BaseNode):
    """
    Receiver node.
    Bob's quantum work (measuring qubits through QNS) is triggered by the
    Celery qubit_tasks workers, not by the orchestrator directly.
    Bob's role here is to:
      1. Self-register and appear in /sessions/pending as a receiver.
      2. Claim the receiver slot when a session is available.
      3. Stay ready — the sifting Celery task calls /sift on bob.py (the FastAPI
         service) just as before.  That service is unchanged.
    """

    def __init__(
        self,
        orch_url:     str,
        callback_url: str,
        **kwargs,
    ):
        super().__init__(
            role=NodeRole.RECEIVER,
            orch_url=orch_url,
            callback_url=callback_url,
            capabilities=NodeCapabilities(max_qubits=5000),
            **kwargs,
        )

    async def handle_session(self, session: dict) -> None:
        """
        Bob has claimed the receiver slot.
        Nothing active to do here — the QNS workers push measurements into Redis
        and the Celery sifting task calls /sift on the Bob FastAPI service directly.

        This method exists so that:
        - Bob is marked as busy (in _active_sessions) for the duration.
        - If we add Eve or relay logic later, Bob can react here.

        We simply wait until the session reaches a terminal state, then return
        so the slot is freed.
        """
        session_id = session["session_id"]
        logger.info(f"[Bob] Receiver slot claimed for session {session_id}, standing by")

        async with httpx.AsyncClient(timeout=10.0) as client:
            while True:
                try:
                    resp  = await client.get(f"{self.orch_url}/sessions/{session_id}")
                    resp.raise_for_status()
                    data  = resp.json()
                    status = data.get("status")
                    if status in ("done", "aborted"):
                        logger.info(f"[Bob] Session {session_id} finished ({status}), releasing slot")
                        return
                except httpx.HTTPError as e:
                    logger.warning(f"[Bob] Status poll error for {session_id}: {e}")

                import asyncio
                await asyncio.sleep(1.0)