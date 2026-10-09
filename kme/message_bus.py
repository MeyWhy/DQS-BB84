import asyncio
import logging
import os
from typing import Awaitable, Callable, Optional

import redis
import redis.asyncio as aredis
from redis.exceptions import ResponseError

from models import BusEvent

logger = logging.getLogger("kme.bus")

REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_MAXLEN  = int(os.getenv("BUS_STREAM_MAXLEN", "1000"))
STREAM_TTL_S   = int(os.getenv("BUS_STREAM_TTL", "7200"))
GROUP          = "node"
BLOCK_MS       = 5000
MAX_ATTEMPTS   = 3
RETRY_DELAY_S  = 1.0


def stream_key(node_id: str) -> str:
    return f"kme:stream:node:{node_id}"


class EventBus:
    """Producer side (KME). One XADD per event, bounded stream, TTL refreshed."""

    def __init__(self):
        self._r = redis.from_url(REDIS_URL, decode_responses=True)

    def publish(self, node_id: str, event: BusEvent) -> str:
        key = stream_key(node_id)
        pipe = self._r.pipeline()
        pipe.xadd(key, {"data": event.model_dump_json()},
                  maxlen=STREAM_MAXLEN, approximate=True)
        pipe.expire(key, STREAM_TTL_S)
        msg_id, _ = pipe.execute()
        return msg_id


Handler = Callable[[BusEvent], Awaitable[None]]


class NodeConsumer:
    """Consumer side (nodes). At-least-once: ACK only after the handler succeeds."""

    def __init__(self, node_id: str, handler: Handler):
        self.node_id   = node_id
        self.handler   = handler
        self._key      = stream_key(node_id)
        self._r        = aredis.from_url(REDIS_URL, decode_responses=True)
        self._task: Optional[asyncio.Task] = None
        self._attempts: dict[str, int] = {}

    async def start(self) -> None:
        await self._ensure_group()   # id="0": events published before startup are kept
        self._task = asyncio.create_task(self._run(), name=f"bus-{self.node_id[:8]}")
        logger.info(f"[Bus] Consumer started node={self.node_id[:8]}")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._r.aclose()

    async def _ensure_group(self) -> None:
        try:
            await self._r.xgroup_create(self._key, GROUP, id="0", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def _run(self) -> None:
        cursor, regroup = "0", False   # "0": replay own pending, ">": new events
        while True:
            try:
                if regroup:            # key expired (TTL) -> group gone
                    await self._ensure_group()
                    regroup = False
                replay = cursor != ">"
                resp = await self._r.xreadgroup(
                    GROUP, self.node_id, {self._key: cursor},
                    count=16, block=None if replay else BLOCK_MS,
                )
                entries = resp[0][1] if resp else []
                if replay and not entries:
                    cursor = ">"
                    continue
                for msg_id, fields in entries:
                    if replay:
                        cursor = msg_id
                    if not await self._dispatch(msg_id, fields):
                        cursor = "0"   # keep order: replay from the failed one
                        await asyncio.sleep(RETRY_DELAY_S)
                        break
            except asyncio.CancelledError:
                raise
            except ResponseError as e:
                if "NOGROUP" in str(e):
                    regroup = True
                else:
                    logger.warning(f"[Bus] Redis error: {e}")
                    await asyncio.sleep(1.0)
            except Exception as e:
                logger.warning(f"[Bus] Connection lost, retrying: {e}")
                cursor = "0"
                await asyncio.sleep(1.0)

    async def _dispatch(self, msg_id: str, fields: Optional[dict]) -> bool:
        """True = done (ACKed or dropped), False = retry."""
        try:
            event = BusEvent.model_validate_json(fields["data"])
        except Exception:
            logger.error(f"[Bus] Malformed/trimmed event {msg_id} dropped")
            await self._r.xack(self._key, GROUP, msg_id)
            return True
        try:
            await self.handler(event)
        except Exception as e:
            n = self._attempts[msg_id] = self._attempts.get(msg_id, 0) + 1
            if n >= MAX_ATTEMPTS:
                logger.error(f"[Bus] '{event.event}' {msg_id} dropped after {n} attempts: {e}")
                self._attempts.pop(msg_id, None)
                await self._r.xack(self._key, GROUP, msg_id)
                return True
            logger.warning(f"[Bus] '{event.event}' failed (attempt {n}): {e}")
            return False
        self._attempts.pop(msg_id, None)
        await self._r.xack(self._key, GROUP, msg_id)
        return True
