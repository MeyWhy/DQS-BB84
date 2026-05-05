from __future__ import annotations
import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from typing import Optional

import httpx

from models import (
    NodeRole, NodeCapabilities,
    NodeRegistrationReq, NodeRegistrationResp,
    JoinSessionReq, JoinSessionResp,
)
from state_machine import SessionStatusResponse

logger = logging.getLogger("node.base")


class BaseNode(ABC):
    """
    Abstract base for all QKD network nodes (Alice, Bob, Eve, relay, …).

    Lifecycle
    ---------
    1. Call ``await node.start()`` — registers with orchestrator, begins poll loop.
    2. The poll loop calls ``await node.handle_session(session)`` for each claimed session.
    3. Call ``await node.stop()`` for graceful shutdown.

    Subclasses must implement ``handle_session``.
    """

    def __init__(
        self,
        role:          NodeRole,
        orch_url:      str,
        callback_url:  str,                          # own base URL, e.g. "http://alice:8001"
        capabilities:  Optional[NodeCapabilities] = None,
        poll_interval: float = 0.5,                  # seconds between /sessions/pending polls
        heartbeat_interval: float = 20.0,            # seconds between /nodes/{id}/heartbeat
        node_id:       Optional[str] = None,
    ):
        self.node_id      = node_id or str(uuid.uuid4())
        self.role         = role
        self.orch_url     = orch_url.rstrip("/")
        self.callback_url = callback_url.rstrip("/")
        self.capabilities = capabilities or NodeCapabilities()
        self.poll_interval      = poll_interval
        self.heartbeat_interval = heartbeat_interval

        self._running   = False
        self._active_sessions: set[str] = set()   # session_ids being handled right now

    # ── public interface ──────────────────────────────────────────────────────

    async def start(self) -> None:
        await self._register()
        self._running = True
        asyncio.create_task(self._poll_loop(),      name=f"{self.node_id}-poll")
        asyncio.create_task(self._heartbeat_loop(), name=f"{self.node_id}-heartbeat")
        logger.info(f"[{self.role.value}] Node {self.node_id} started")

    async def stop(self) -> None:
        self._running = False
        await self._deregister()
        logger.info(f"[{self.role.value}] Node {self.node_id} stopped")

    @abstractmethod
    async def handle_session(self, session: dict) -> None:
        """
        Called once per claimed session.
        ``session`` is the raw dict from SessionStatusResponse.
        Must not raise — catch and report errors internally.
        """

    # ── registration ──────────────────────────────────────────────────────────

    async def _register(self) -> None:
        req = NodeRegistrationReq(
            node_id=self.node_id,
            role=self.role,
            callback_url=self.callback_url,
            capabilities=self.capabilities,
        )
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                resp = await client.post(f"{self.orch_url}/nodes/register", json=req.model_dump())
                resp.raise_for_status()
                logger.info(f"[{self.role.value}] Registered with orchestrator at {self.orch_url}")
            except httpx.HTTPError as e:
                logger.error(f"[{self.role.value}] Registration failed: {e}")
                raise

    async def _deregister(self) -> None:
        async with httpx.AsyncClient(timeout=5.0) as client:
            try:
                await client.delete(f"{self.orch_url}/nodes/{self.node_id}")
            except httpx.HTTPError as e:
                logger.warning(f"[{self.role.value}] Deregistration error: {e}")

    # ── polling ───────────────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        async with httpx.AsyncClient(timeout=10.0) as client:
            while self._running:
                try:
                    sessions = await self._get_pending_sessions(client)
                    for session in sessions:
                        sid = session["session_id"]
                        if sid in self._active_sessions:
                            continue   # already handling this one
                        joined = await self._try_join(client, sid)
                        if joined:
                            self._active_sessions.add(sid)
                            asyncio.create_task(
                                self._run_session_guarded(session),
                                name=f"{self.node_id}-session-{sid}",
                            )
                            break  # one session at a time per node instance
                except httpx.HTTPError as e:
                    logger.warning(f"[{self.role.value}] Poll error: {e}")
                await asyncio.sleep(self.poll_interval)

    async def _get_pending_sessions(self, client: httpx.AsyncClient) -> list[dict]:
        resp = await client.get(
            f"{self.orch_url}/sessions/pending",
            params={"role": self.role.value},
        )
        resp.raise_for_status()
        return resp.json()   # list of SessionStatusResponse dicts

    async def _try_join(self, client: httpx.AsyncClient, session_id: str) -> bool:
        req = JoinSessionReq(node_id=self.node_id, role=self.role)
        try:
            resp = await client.post(
                f"{self.orch_url}/sessions/{session_id}/join",
                json=req.model_dump(),
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("accepted", False)
        except httpx.HTTPError as e:
            logger.debug(f"[{self.role.value}] Join attempt failed for {session_id}: {e}")
            return False

    async def _run_session_guarded(self, session: dict) -> None:
        sid = session["session_id"]
        try:
            await self.handle_session(session)
        except Exception as e:
            logger.error(f"[{self.role.value}] Unhandled error in session {sid}: {e}", exc_info=True)
        finally:
            self._active_sessions.discard(sid)

    # ── heartbeat ─────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        async with httpx.AsyncClient(timeout=5.0) as client:
            while self._running:
                try:
                    resp = await client.post(f"{self.orch_url}/nodes/{self.node_id}/heartbeat")
                    if resp.status_code == 404:
                        # orchestrator evicted us — re-register
                        logger.warning(f"[{self.role.value}] Evicted, re-registering…")
                        await self._register()
                except httpx.HTTPError as e:
                    logger.warning(f"[{self.role.value}] Heartbeat error: {e}")
                await asyncio.sleep(self.heartbeat_interval)