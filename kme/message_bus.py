import json
import logging
import os
import threading
from typing import Callable, Optional

import redis

logger   = logging.getLogger("kme.bus")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


class MessageBus:


    def __init__(self):
        self._r = redis.from_url(REDIS_URL, decode_responses=True)

    def publish(
        self,
        channel: str,
        event:   str,
        session_id: str,
        payload: dict = {},
    ) -> int:
 
        message = json.dumps({
            "event":      event,
            "session_id": session_id,
            "payload":    payload,
        })
        n = self._r.publish(channel, message)
        logger.debug(f"[Bus] Published '{event}' → {channel} ({n} receivers)")
        return n

    def publish_to_node(
        self,
        node_id:    str,
        event:      str,
        session_id: str,
        payload:    dict = {},
    ) -> int:
        
        return self.publish(
            channel=f"kme:events:{node_id}",
            event=event,
            session_id=session_id,
            payload=payload,
        )

    def publish_to_session(
        self,
        session_id: str,
        event:      str,
        payload:    dict = {},
    ) -> int:

        return self.publish(
            channel=f"kme:session:{session_id}",
            event=event,
            session_id=session_id,
            payload=payload,
        )

    def broadcast(self, event: str, payload: dict = {}) -> int:
        return self.publish(
            channel="kme:events:broadcast",
            event=event,
            session_id="",
            payload=payload,
        )

class NodeSubscriber:


    def __init__(
        self,
        node_id: str,
        handler: Callable[[str, str, dict], None],
    ):
        self.node_id  = node_id
        self.handler  = handler
        self._thread: Optional[threading.Thread] = None
        self._stop    = threading.Event()

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._listen,
            daemon=True,
            name=f"bus-sub-{self.node_id[:8]}",
        )
        self._thread.start()
        logger.info(f"[Bus] Subscriber started for node {self.node_id[:8]}")

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3.0)

    def _listen(self) -> None:

        while not self._stop.is_set():
            try:
                r      = redis.from_url(REDIS_URL, decode_responses=True)
                pubsub = r.pubsub(ignore_subscribe_messages=True)

                # Subscribe to node-specific, broadcast, and session channels
                pubsub.subscribe(
                    f"kme:events:{self.node_id}",
                    "kme:events:broadcast",
                )

                for raw in pubsub.listen():
                    if self._stop.is_set():
                        break
                    if raw["type"] != "message":
                        continue
                    try:
                        msg = json.loads(raw["data"])
                        self.handler(
                            msg.get("event", ""),
                            msg.get("session_id", ""),
                            msg.get("payload", {}),
                        )
                    except Exception as e:
                        logger.warning(f"[Bus] Handler error: {e}")

            except Exception as e:
                if not self._stop.is_set():
                    logger.warning(f"[Bus] Redis disconnect, reconnecting: {e}")
                    import time
                    time.sleep(1.0)

    def subscribe_session(self, session_id: str) -> None:
        """
        Dynamically add a session channel subscription.
        Called when the node joins a session.
        Note: requires a new pubsub connection or psubscribe.
        Simple implementation: just re-listen (restart thread).
        """
        # In a full implementation, use psubscribe("kme:session:*")
        # For now, the node-specific channel carries all session events.
        pass