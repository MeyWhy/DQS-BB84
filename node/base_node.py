"""
node/base_node.py
==================
Generic base class for all QKD network nodes.

Any new node (Alice, Bob, Eve, Relay) inherits from BaseNode and
overrides only the methods it needs. The base class handles:
  - Registration with the KME on startup
  - Webhook server (receives KME notifications)
  - Session state (local cache, source of truth is KME Redis)
  - HTTP client for KME/QKDL calls
  - Agent loop (poll-based fallback if webhook delivery fails)

To add a new node type:
  1. Create nodes/mynode/main.py
  2. class MyNode(BaseNode)
  3. Override on_session_open(), on_receiver_joined(), etc.
  4. Add to network.yaml

Architecture decision — webhook + polling hybrid:
  Webhooks give low latency when delivered.
  Polling every POLL_INTERVAL seconds is the fallback for
  missed webhooks (network glitch, node restart, etc.).
  This makes the system resilient without requiring a message broker.
"""

import asyncio
import logging
import os
import time
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from shared.models import (
    NodeRole, NodeRegistration, NodeInfo,
    WebhookEvent, SessionStatusResponse,
)

logger = logging.getLogger("node.base")

KME_URL       = os.getenv("KME_URL",       "http://localhost:8000")
POLL_INTERVAL = float(os.getenv("NODE_POLL_INTERVAL", "2.0"))
REGISTER_RETRY_DELAY = 3.0


class BaseNode(ABC):
    """
    Abstract base for all QKD nodes.

    Subclasses implement the on_* event handlers.
    The base class manages registration, webhooks, and the agent loop.
    """

    def __init__(
        self,
        role:         NodeRole,
        label:        str,
        callback_url: str,
        metadata:     dict = {},
    ):
        self.role         = role
        self.label        = label
        self.callback_url = callback_url
        self.metadata     = metadata

        # Assigned by KME on registration
        self.node_id: Optional[str] = None

        # Active sessions this node is participating in
        # session_id → local session state dict
        self._sessions: dict[str, dict] = {}

        self._client = httpx.AsyncClient(timeout=30.0)
        self._running = False

    # ─────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────

    async def start(self) -> None:
        """Register with KME and start agent loop."""
        self.node_id = await self._register()
        self._running = True
        logger.info(
            f"[{self.label}] Started — node_id={self.node_id[:8]} "
            f"role={self.role.value}"
        )
        asyncio.create_task(self._agent_loop())

    async def stop(self) -> None:
        self._running = False
        await self._client.aclose()
        logger.info(f"[{self.label}] Stopped")

    async def _register(self) -> str:
        """
        Register this node with the KME.
        Retries until KME is reachable (handles startup ordering).
        """
        while True:
            try:
                resp = await self._client.post(
                    f"{KME_URL}/nodes/register",
                    json=NodeRegistration(
                        role=self.role,
                        callback_url=self.callback_url,
                        label=self.label,
                        metadata=self.metadata,
                    ).model_dump(),
                )
                resp.raise_for_status()
                info = NodeInfo.model_validate(resp.json())
                logger.info(
                    f"[{self.label}] Registered → node_id={info.node_id[:8]}"
                )
                return info.node_id
            except (httpx.ConnectError, httpx.HTTPError) as e:
                logger.warning(
                    f"[{self.label}] KME unreachable, retrying in "
                    f"{REGISTER_RETRY_DELAY}s: {e}"
                )
                await asyncio.sleep(REGISTER_RETRY_DELAY)

    # ─────────────────────────────────────────
    # Webhook handler (called by FastAPI route)
    # ─────────────────────────────────────────

    async def handle_webhook(self, event: WebhookEvent) -> None:
        """
        Dispatches incoming KME notifications to the right handler.
        FastAPI route calls this; subclasses implement handlers.
        """
        sid = event.session_id
        logger.debug(f"[{self.label}] Webhook: {event.event} session={sid[:8]}")

        # Update local session cache
        if sid not in self._sessions:
            self._sessions[sid] = {"session_id": sid}
        self._sessions[sid]["last_event"] = event.event

        handler = {
            "session_open":     self.on_session_open,
            "receiver_joined":  self.on_receiver_joined,
            "measurements_ready": self.on_measurements_ready,
            "sift_ready":       self.on_sift_ready,
            "key_available":    self.on_key_available,
            "session_aborted":  self.on_session_aborted,
        }.get(event.event)

        if handler:
            asyncio.create_task(handler(sid, event.payload))
        else:
            logger.warning(f"[{self.label}] Unknown event: {event.event}")

    # ─────────────────────────────────────────
    # Event handlers — override in subclasses
    # ─────────────────────────────────────────

    async def on_session_open(self, session_id: str, payload: dict) -> None:
        """
        KME notified this node that a new session is open.
        Default: receivers auto-join; senders ignore (they created it).
        """
        if self.role == NodeRole.RECEIVER:
            await self.join_session(session_id)

    async def on_receiver_joined(self, session_id: str, payload: dict) -> None:
        """KME notified sender that receiver has joined."""
        pass   # Sender starts transmitting — overridden in AliceNode

    async def on_measurements_ready(self, session_id: str, payload: dict) -> None:
        """KME notified sender that Bob's measurements are ready."""
        pass   # Overridden in AliceNode

    async def on_sift_ready(self, session_id: str, payload: dict) -> None:
        """KME notified receiver that Alice's sift bases are ready."""
        pass   # Overridden in BobNode

    async def on_key_available(self, session_id: str, payload: dict) -> None:
        """Key is active and ready for consumption."""
        logger.info(
            f"[{self.label}] Key available session={session_id[:8]} "
            f"QBER={payload.get('qber', 0)*100:.2f}%"
        )

    async def on_session_aborted(self, session_id: str, payload: dict) -> None:
        """Session was aborted."""
        logger.warning(
            f"[{self.label}] Session {session_id[:8]} aborted: "
            f"{payload.get('reason', '')}"
        )
        self._sessions.pop(session_id, None)

    # ─────────────────────────────────────────
    # Common KME calls (used by subclasses)
    # ─────────────────────────────────────────

    async def join_session(self, session_id: str) -> dict:
        """Bob calls this to join an open session."""
        resp = await self._client.post(
            f"{KME_URL}/sessions/{session_id}/join",
            json={"node_id": self.node_id, "session_id": session_id},
        )
        resp.raise_for_status()
        data = resp.json()
        self._sessions[session_id] = data
        logger.info(f"[{self.label}] Joined session {session_id[:8]}")
        return data

    async def get_session(self, session_id: str) -> dict:
        """Poll KME for session status."""
        resp = await self._client.get(
            f"{KME_URL}/sessions/{session_id}"
        )
        resp.raise_for_status()
        return resp.json()

    async def kme_post(self, path: str, payload: dict) -> dict:
        """Generic POST to KME."""
        resp = await self._client.post(
            f"{KME_URL}{path}", json=payload
        )
        resp.raise_for_status()
        return resp.json()

    async def kme_get(self, path: str) -> dict:
        """Generic GET from KME."""
        resp = await self._client.get(f"{KME_URL}{path}")
        resp.raise_for_status()
        return resp.json()

    # ─────────────────────────────────────────
    # Agent loop — polling fallback
    # ─────────────────────────────────────────

    async def _agent_loop(self) -> None:
        """
        Polls KME periodically as a fallback for missed webhooks.
        Subclasses can override _poll_tick() for role-specific polling.
        """
        while self._running:
            try:
                await self._poll_tick()
            except Exception as e:
                logger.debug(f"[{self.label}] Poll tick error: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    async def _poll_tick(self) -> None:
        """Override in subclasses for role-specific polling behaviour."""
        pass

    # ─────────────────────────────────────────
    # FastAPI app factory
    # ─────────────────────────────────────────

    def build_app(self, title: str, port: int) -> FastAPI:
        """
        Creates the FastAPI application for this node.
        Registers the /webhook endpoint automatically.
        Subclasses call this and add their own routes.
        """
        node = self

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            await node.start()
            yield
            await node.stop()

        app = FastAPI(title=title, version="0.7.0", lifespan=lifespan)

        @app.post("/webhook")
        async def webhook(request: Request):
            """
            KME delivers events here.
            Registered as callback_url during node registration.
            """
            body = await request.json()
            event = WebhookEvent(**body)
            await node.handle_webhook(event)
            return {"status": "received"}

        @app.get("/health")
        async def health():
            return {
                "status":    "ok",
                "node_id":   node.node_id,
                "label":     node.label,
                "role":      node.role.value,
                "sessions":  list(node._sessions.keys()),
            }

        return app
