import asyncio
import logging
import time
import os
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI, HTTPException, BackgroundTasks
from state_machine import(
    OrchestratorSession, SessionStatus,SessionStatusResponse,
    session_to_response, new_session_id, TransitionError,)
from models import (NetworkInitReq, SessionCreateReq, NodeRole,
    NodeRegistrationReq, NodeRegistrationResp, NodeRecord,
    JoinSessionReq, JoinSessionResp,)
from orch_store import (get_redis, save_orch_session, load_orch_session,
    update_orch_session, list_active_sessions, list_all_sessions,list_waiting_sessions,
    save_node, load_node, update_node, delete_node, list_nodes,
    heartbeat_node, claim_session_role,)

logger=logging.getLogger("orchestrator")
logging.basicConfig(level=logging.INFO)

QNS_URL=os.getenv("QNS_URL", "http://localhost:8003")
HTTP_TIMEOUT=30.0

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[Orch] Service started")
    yield
    logger.info("[Orch] Service stopped")

 
app=FastAPI(
    title="KME - Key Management Entity",
    description="ESTI GS QKD 014 compliant QKD Key Management Entity."
                "Implements BB84 via distributed microservices.",
    version="0.7.0",
    lifespan=lifespan,)

def _abort(r, session:OrchestratorSession, msg:str)-> OrchestratorSession:
    try:
        session.transition(SessionStatus.ABORTED)
    except TransitionError:
        session.status=SessionStatus.ABORTED
    session.error_message=msg
    session.completed_at=time.time()
    update_orch_session(r, session)
    return session


async def _stop_qns(session_id: str) -> None:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(
                f"{QNS_URL}/network/stop",
                json={"session_id": session_id},
            )
    except Exception as e:
        logger.warning(f"[Orch] QNS Teardown partial {session_id}: {e}")

async def _notify_node(callback_url: str, path: str, payload: dict) -> None:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(f"{callback_url}{path}", json=payload)
    except Exception as e:
        logger.warning(f"[Orch] Node notify failed {callback_url}{path}: {e}")



 
@app.post("/nodes/register", response_model=NodeRegistrationResp)
async def register_node(req: NodeRegistrationReq):

    r = get_redis()
    node = NodeRecord(
        node_id=req.node_id,
        role=req.role,
        callback_url=req.callback_url,
        capabilities=req.capabilities,
        registered_at=time.time(),
    )
    save_node(r, node)
    logger.info(f"[Orch] Node registered: {req.node_id} role={req.role.value}")
    return NodeRegistrationResp(node_id=req.node_id, role=req.role, registered=True)
 
@app.post("/nodes/{node_id}/heartbeat")
async def node_heartbeat(node_id: str):
    """Nodes call this periodically to refresh their TTL in the registry."""
    r = get_redis()
    alive = heartbeat_node(r, node_id)
    if not alive:
        raise HTTPException(status_code=404, detail="Node not registered")
    return {"node_id": node_id, "status": "alive"}
 

@app.delete("/nodes/{node_id}")
async def deregister_node(node_id: str):
    """Called by a node on graceful shutdown."""
    r = get_redis()
    node = load_node(r, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    delete_node(r, node_id)
    logger.info(f"[Orch] Node deregistered: {node_id}")
    return {"node_id": node_id, "status": "deregistered"}

 
@app.get("/nodes")
async def list_all_nodes(role: NodeRole | None = None):
    r = get_redis()
    nodes = list_nodes(r, role=role)
    return {"nodes": [n.model_dump() for n in nodes], "count": len(nodes)}
 

@app.post("/keys", response_model=SessionStatusResponse)
@app.post("/sessions/start", response_model=SessionStatusResponse)
async def create_session(req: SessionCreateReq, background_tasks:BackgroundTasks):
    session_id=new_session_id()
    r = get_redis()
    session=OrchestratorSession(
        session_id=session_id,
        n_qubits=req.n_qubits,
        batch_size=req.batch_size,
        loss_rate=req.loss_rate,
    )
    session.transition(SessionStatus.INITIALIZING)
    save_orch_session(r, session)
 
    logger.info(f"[Orch] Session {session_id} created ({req.n_qubits} qubits)")
    return session_to_response(session)

#backward compatibilité again
@app.post("/session/start", response_model=SessionStatusResponse)
async def start_session_legacy(req: SessionCreateReq):
    return await create_session(req)


@app.get("/sessions/pending", response_model=list[SessionStatusResponse])
async def get_pending_sessions(role: NodeRole | None = None):
    r = get_redis()
    waiting_ids = list_waiting_sessions(r)
    result = []
    for sid in waiting_ids:
        session = load_orch_session(r, sid)
        if not session:
            continue
        # filter by role if requested: donc alice ne voit que les sessions sans sender et vice versa
        if role == NodeRole.SENDER   and session.sender_node_id   is not None:
            continue
        if role == NodeRole.RECEIVER and session.receiver_node_id is not None:
            continue
        result.append(session_to_response(session))
    return result

@app.post("/sessions/{session_id}/join", response_model=JoinSessionResp)
async def join_session(session_id: str, req: JoinSessionReq, background_tasks: BackgroundTasks):

    r = get_redis()
    session = load_orch_session(r, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    if session.status != SessionStatus.WAITING:
        return JoinSessionResp(
            session_id=session_id, role=req.role,
            accepted=False, detail=f"Session is {session.status.value}, not waiting",
        )
 
    # verify node exists
    node = load_node(r, req.node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not registered")
 
    # atomic claim
    claimed = claim_session_role(r, session_id, req.node_id, req.role)
    if not claimed:
        return JoinSessionResp(
            session_id=session_id, role=req.role,
            accepted=False, detail="Role already claimed by another node",
        )
 
    # update session record
    if req.role == NodeRole.SENDER:
        session.sender_node_id   = req.node_id
    else:
        session.receiver_node_id = req.node_id
 
    # mark node as busy
    node.current_session_id = session_id
    update_node(r, node)
 
    logger.info(f"[Orch] Session {session_id} — {req.role.value} claimed by {req.node_id}")
 
    if session.is_ready_to_start():
        # both roles filled -> start
        session.transition(SessionStatus.INITIALIZING)
        update_orch_session(r, session)
        logger.info(f"[Orch] Session {session_id} -> INITIALIZING")
 
        # signal Alice node to begin (non-blocking, best-effort)
        sender_node = load_node(r, session.sender_node_id)
        if sender_node:
            background_tasks.add_task(
                _notify_node,
                sender_node.callback_url,
                "/session/begin",
                {
                    "session_id": session_id,
                    "n_qubits":   session.n_qubits,
                    "batch_size": session.batch_size,
                    "loss_rate":  session.loss_rate,
                },
            )
    else:
        update_orch_session(r, session)
 
    return JoinSessionResp(session_id=session_id, role=req.role, accepted=True)

@app.get("/keys/{key_ID}", response_model=SessionStatusResponse)
@app.get("/sessions/{session_id}", response_model=SessionStatusResponse)
async def get_session_status(session_id: str=None, key_ID: str=None):
    sid=session_id or key_ID
    r = get_redis()
    session = load_orch_session(r, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    session.is_key_valid()
    update_orch_session(r, session)
    return session_to_response(session)

#backward compat
@app.get("/session/{session_id}", response_model=SessionStatusResponse)
async def get_session_status_legacy(session_id: str):
    return await get_session_status(session_id)

@app.post("/sessions/{session_id}/complete")
async def complete_session(session_id: str, result: dict):
    r = get_redis()
    session = load_orch_session(r, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    #idempotence: already done
    if session.is_terminal:
        logger.info(f"[Orch] Session {session_id} already finished == ignored")
        return {"status": "already_complete"}

    #transition vers sift
    try:
        if session.status == SessionStatus.SENDING:
            session.transition(SessionStatus.SIFTING)
    except TransitionError:
        pass

    #transition finale depending on res
    status = result.get("status", "aborted")
    if status == "success":
        session.transition(SessionStatus.DONE)
        session.key_final   = result.get("key_final", "")
        session.qber       = result.get("qber", 0.0)
        session.n_sifted   = result.get("n_sifted", 0)
        session.n_delivered = result.get("n_delivered", 0)
        session.activate_key()
    else:
        session.transition(SessionStatus.ABORTED)
        session.error_message = result.get("error_message", "Erreur inconnue")
        session.qber = result.get("qber", 1.0)

    update_orch_session(r, session)
    _release_session_nodes(r, session)

    #stop qnd in bg (best-effort)
    asyncio.create_task(_stop_qns(session_id))

    logger.info(
        f"[Orch] Session {session_id} -> {session.status.value} "
        f"QBER={session.qber*100:.1f}% "
        f"key={session.key_final[:16] if session.key_final else 'none'}..."
    )
    return {"status": "acknowledged", "session_id": session_id}

#backward compat
@app.post("/session/{session_id}/complete")
async def complete_session_legacy(session_id: str, result: dict):
    return await complete_session(session_id, result)


@app.delete("/sessions/{session_id}")
async def cancel_session(session_id: str):
    r = get_redis()
    session = load_orch_session(r, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session introuvable")

    if session.is_terminal:
        return {"status": "already_terminal", "session_id": session_id}

    _abort(r, session, "Annulé par l'utilisateur")
    _release_session_nodes(r, session)

    asyncio.create_task(_stop_qns(session_id))

    logger.info(f"[Orch] Session {session_id}  cancelled")
    return {"status": "cancelled", "session_id": session_id}

#backward compat 
@app.post("/session/{session_id}/complete")
async def complete_session_legacy(session_id: str, result: dict):
    return await complete_session(session_id, result)

@app.post("/sessions/{session_id}/consume-key")
async def consume_key(session_id: str):
    r =get_redis()
    session = load_orch_session(r, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    if not session.consume_key():
        raise HTTPException(
            status_code=409,
            detail=f"Key not available: status={session.key_status.value}"
        )

    update_orch_session(r, session)
    logger.info(f"[Orch] Key consumed for session {session_id}")

    return {
        "session_id": session_id,
        "key_final":  session.key_final,
        "key_status": session.key_status.value,
    }

#backward compat
@app.post("/session/{session_id}/consume-key")
async def consume_key_legacy(session_id: str):
    return await consume_key(session_id)


#pour liberer les nodes
def _release_session_nodes(r, session: OrchestratorSession) -> None:
    for node_id in (session.sender_node_id, session.receiver_node_id):
        if node_id:
            node = load_node(r, node_id)
            if node:
                node.current_session_id = None
                update_node(r, node)
 
@app.get("/sessions")
async def list_sessions(active_only: bool = True):
    r = get_redis()
    ids = list_active_sessions(r) if active_only else list_all_sessions(r)
    sessions = []
    for sid in ids:
        s = load_orch_session(r, sid)
        if s:
            sessions.append(session_to_response(s).model_dump())
    return {"sessions": sessions, "count": len(sessions)}



@app.get("/health")
async def health():
    r = get_redis()
    try:
        r.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    active = list_active_sessions(r)
    return {
        "status":          "ok",
        "redis":           redis_ok,
        "active_sessions": len(active),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
