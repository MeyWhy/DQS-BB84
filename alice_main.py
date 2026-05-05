"""
Alice service — FastAPI wrapper around AliceNode.

Exposes:
  GET  /health          standard health check
  GET  /status          current load / active sessions
  POST /session/begin   called by orchestrator when both roles are filled
                        (redundant with polling but gives faster start for
                        latency-sensitive deployments)
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from nodes.alice_node import AliceNode

logger = logging.getLogger("alice.main")
logging.basicConfig(level=logging.INFO)

ORCH_URL     = os.getenv("ORCH_URL",     "http://localhost:8000")
QNS_URL      = os.getenv("QNS_URL",     "http://localhost:8003")
CALLBACK_URL = os.getenv("ALICE_URL",   "http://localhost:8001")

_node: AliceNode | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _node
    _node = AliceNode(
        orch_url=ORCH_URL,
        qns_url=QNS_URL,
        callback_url=CALLBACK_URL,
    )
    await _node.start()
    logger.info("[Alice] Service started")
    yield
    await _node.stop()
    logger.info("[Alice] Service stopped")


app = FastAPI(
    title="Alice Node",
    description="BB84 sender — self-registering node",
    version="0.7.0",
    lifespan=lifespan,
)


class BeginReq(BaseModel):
    session_id: str
    n_qubits:   int
    batch_size: int
    loss_rate:  float


@app.post("/session/begin")
async def session_begin(req: BeginReq):
    """
    Webhook called by the orchestrator after both roles are filled.
    Triggers handle_session directly without waiting for the next poll cycle.
    """
    if _node is None:
        raise HTTPException(status_code=503, detail="Node not initialised")

    session_id = req.session_id
    if session_id in _node._active_sessions:
        return {"status": "already_running", "session_id": session_id}

    _node._active_sessions.add(session_id)
    asyncio.create_task(
        _node._run_session_guarded(req.model_dump()),
        name=f"{_node.node_id}-session-{session_id}",
    )
    return {"status": "started", "session_id": session_id}


@app.get("/status")
async def status():
    if _node is None:
        raise HTTPException(status_code=503, detail="Node not initialised")
    return {
        "node_id":         _node.node_id,
        "role":            _node.role.value,
        "active_sessions": list(_node._active_sessions),
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "alice", "version": "0.7.0"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")