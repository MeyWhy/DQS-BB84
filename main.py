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
from models import NetworkInitReq, SessionStartReq
from orch_store import (get_redis, save_orch_session, load_orch_session,
    update_orch_session, list_active_sessions, list_all_sessions,)

logger=logging.getLogger("orchestrator")
logging.basicConfig(level=logging.INFO)

QNS_URL=os.getenv("QNS_URL", "http://localhost:8003")
BOB_URL=os.getenv("BOB_URL", "http://localhost:8002")
ALICE_URL = os.getenv("ALICE_URL", "http://localhost:8001")
HTTP_TIMEOUT=30.0

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[Orch] Service started")
    yield
    logger.info("[Orch] Service stopped")

 
app=FastAPI(
    title="Orchestrator",
    description="Orchestrator BB84",
    version="0.6.0",
    lifespan=lifespan,)

async def _http(method: str, url: str, client: httpx.AsyncClient,  **kwargs) -> dict:
    try:
        resp=await getattr(client, method)(url, **kwargs)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=502,
            detail=f"Appel {method.upper()} {url} -> {e.response.status_code}: "f"{e.response.text[:200]}",)

def _abort(r, session:OrchestratorSession, msg:str)-> OrchestratorSession:
    try:
        session.transition(SessionStatus.ABORTED)
    except TransitionError:
        session.status=SessionStatus.ABORTED
    session.error_message=msg
    session.completed_at=time.time()
    update_orch_session(r, session)
    return session


async def _run_session(session_id: str)-> None:
    r=get_redis()
    session=load_orch_session(r,session_id)
    if not session:
        logger.error(f"[Orch] Session {session_id} introuvable au démarrage")
        return
    
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT)as client:
        #init network here
        try:
            await _http("post", f"{QNS_URL}/network/init", client, 
                       json=NetworkInitReq(
                           session_id=session_id, n_qubits=session.n_qubits, loss_rate=session.loss_rate,
                       ).model_dump())
            logger.info(f"[Orch] QNS initialisé — session {session_id}")
        except HTTPException as e:
            _abort(r, session, f"QNS init failed: {e.detail}")
            return

        #record bob
        try:
            await _http("post", f"{BOB_URL}/session/register", client, params={"session_id":session_id})
        except HTTPException as e:
            _abort(r, session, f"Bob register failed: {e.detail}")
            return 
    
        #send transition
        session.transition(SessionStatus.SENDING)
        update_orch_session(r,session)

        #lancer pipeline alice
        try:
            emit_data=await _http("post", f"{ALICE_URL}/emit", client,
                json={
                    "session_id": session_id,
                    "n_qubits": session.n_qubits,
                    "batch_size": session.batch_size,
                    "loss_rate": session.loss_rate,
                })
            session.celery_task_id=emit_data.get("celery_task_id")
            update_orch_session(r,session)
            logger.info(
                f"[Orch] Pipeline started session={session_id} "
                f"task={session.celery_task_id}"
            )
        except HTTPException as e:
            _abort(r, session, f"Alice emit failed: {e.detail}")
            return
        

@app.post("/session/start", response_model=SessionStatusResponse)
async def start_session(req: SessionStartReq, background_tasks:BackgroundTasks):
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
    
    background_tasks.add_task(_run_session, session_id)

 
    logger.info(f"[Orch] Session {session_id} created ({req.n_qubits} qubits)")
    return session_to_response(session)


@app.get("/session/{session_id}", response_model=SessionStatusResponse)
async def get_session_status(session_id: str):
    r = get_redis()
    session = load_orch_session(r, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    session.is_key_valid()
    update_orch_session(r, session)
    return session_to_response(session)


@app.post("/session/{session_id}/complete")
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

    #stop qnd in bg (best-effort)
    asyncio.create_task(_stop_qns(session_id))

    logger.info(
        f"[Orch] Session {session_id} → {session.status.value} "
        f"QBER={session.qber*100:.1f}% "
        f"key={session.key_final[:16] if session.key_final else 'none'}..."
    )
    return {"status": "acknowledged", "session_id": session_id}



async def _stop_qns(session_id: str) -> None:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(
                f"{QNS_URL}/network/stop",
                json={"session_id": session_id},
            )
            await client.delete(f"{BOB_URL}/session/{session_id}")
    except Exception as e:
        logger.warning(f"[Orch] Teardown partiel {session_id}: {e}")


@app.delete("/session/{session_id}")
async def cancel_session(session_id: str):
    r = get_redis()
    session = load_orch_session(r, session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session introuvable")

    if session.is_terminal:
        return {"status": "already_terminal", "session_id": session_id}

    _abort(r, session, "Annulé par l'utilisateur")
    asyncio.create_task(_stop_qns(session_id))

    logger.info(f"[Orch] Session {session_id}  cancelled")
    return {"status": "cancelled", "session_id": session_id}


@app.post("/session/{session_id}/consume-key")
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
