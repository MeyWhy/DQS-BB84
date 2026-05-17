"""
node/base_node.py  — v0.8.0

Fix: join_session() now returns the full response dict (including qkdl_url)
instead of storing it into self._sessions and returning it silently.
Bob's on_session_open handler picks up qkdl_url from this return value.
"""
import asyncio
import logging
import os
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, Request

from models import (
    NodeRole, NodeRegistration, NodeInfo,
    WebhookEvent, SessionStatusResponse,
)

logger = logging.getLogger("node.base")

KME_URL              = os.getenv("KME_URL",  "http://localhost:8000")
POLL_INTERVAL        = float(os.getenv("NODE_POLL_INTERVAL", "2.0"))
REGISTER_RETRY_DELAY = 3.0


class BaseNode(ABC):

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

        self.node_id: Optional[str] = None
        self._sessions: dict[str, dict] = {}
        self._client  = httpx.AsyncClient(timeout=30.0)
        self._running = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self.node_id = await self._register()
        self._running = True
        logger.info(
            f"[{self.label}] Started  node_id={self.node_id[:8]} "
            f"role={self.role.value}"
        )
        asyncio.create_task(self._agent_loop())

    async def stop(self) -> None:
        self._running = False
        await self._client.aclose()
        logger.info(f"[{self.label}] Stopped")

    async def _register(self) -> str:
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

    # ── Webhook dispatch ──────────────────────────────────────────────────────

    async def handle_webhook(self, event: WebhookEvent) -> None:
        sid = event.session_id
        logger.debug(
            f"[{self.label}] Webhook: {event.event} session={sid[:8]}"
        )

        if sid not in self._sessions:
            self._sessions[sid] = {"session_id": sid}
        self._sessions[sid]["last_event"] = event.event

        handler = {
            "session_open":       self.on_session_open,
            "receiver_joined":    self.on_receiver_joined,
            "measurements_ready": self.on_measurements_ready,
            "sift_ready":         self.on_sift_ready,
            "key_available":      self.on_key_available,
            "session_aborted":    self.on_session_aborted,
        }.get(event.event)

        if handler:
            asyncio.create_task(handler(sid, event.payload))
        else:
            logger.warning(f"[{self.label}] Unknown event: {event.event}")

    # ── Default handlers (override in subclasses) ─────────────────────────────

    async def on_session_open(self, session_id: str, payload: dict) -> None:
        if self.role == NodeRole.RECEIVER:
            await self.join_session(session_id)

    async def on_receiver_joined(self, session_id: str, payload: dict) -> None:
        pass

    async def on_measurements_ready(self, session_id: str, payload: dict) -> None:
        pass

    async def on_sift_ready(self, session_id: str, payload: dict) -> None:
        pass

    async def on_key_available(self, session_id: str, payload: dict) -> None:
        logger.info(
            f"[{self.label}] Key available session={session_id[:8]} "
            f"QBER={payload.get('qber', 0)*100:.2f}%"
        )

    async def on_session_aborted(self, session_id: str, payload: dict) -> None:
        logger.warning(
            f"[{self.label}] Session {session_id[:8]} aborted: "
            f"{payload.get('reason', '')}"
        )
        self._sessions.pop(session_id, None)

    # ── KME helpers ───────────────────────────────────────────────────────────

    async def join_session(self, session_id: str) -> dict:
        """
        Join a session and return the full response dict.
        The dict now includes qkdl_url so the caller can use it
        for quantum channel operations without relying on env vars.
        """
        resp = await self._client.post(
            f"{KME_URL}/sessions/{session_id}/join",
            json={"node_id": self.node_id, "session_id": session_id},
        )
        resp.raise_for_status()
        data = resp.json()
        self._sessions[session_id] = data
        logger.info(
            f"[{self.label}] Joined session={session_id[:8]} "
            f"qkdl={data.get('qkdl_url', 'unknown')}"
        )
        return data   # ← callers use data["qkdl_url"]

    async def get_session(self, session_id: str) -> dict:
        resp = await self._client.get(f"{KME_URL}/sessions/{session_id}")
        resp.raise_for_status()
        return resp.json()

    async def kme_post(self, path: str, payload: dict) -> dict:
        resp = await self._client.post(f"{KME_URL}{path}", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def kme_get(self, path: str) -> dict:
        resp = await self._client.get(f"{KME_URL}{path}")
        resp.raise_for_status()
        return resp.json()

    # ── Agent loop ────────────────────────────────────────────────────────────

    async def _agent_loop(self) -> None:
        while self._running:
            try:
                await self._poll_tick()
            except Exception as e:
                logger.debug(f"[{self.label}] Poll tick error: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    async def _poll_tick(self) -> None:
        pass

    # ── FastAPI app factory ───────────────────────────────────────────────────

    def build_app(self, title: str, port: int) -> FastAPI:
        node = self

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            await node.start()
            yield
            await node.stop()

        app = FastAPI(title=title, version="0.8.0", lifespan=lifespan)

        @app.post("/webhook")
        async def webhook(request: Request):
            body  = await request.json()
            event = WebhookEvent(**body)
            await node.handle_webhook(event)
            return {"status": "received"}

        @app.get("/health")
        async def health():
            return {
                "status":   "ok",
                "node_id":  node.node_id,
                "label":    node.label,
                "role":     node.role.value,
                "sessions": list(node._sessions.keys()),
            }

        return app