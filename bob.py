import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from models import ( SiftReq, SiftResp,)
from redis_store import (
    get_redis,
    load_all_bob_measurements,
    save_session_result,
    load_session_result,
    delete_session,
)
logger=logging.getLogger("bob")
logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[Bob] Service started")
    yield
    logger.info("[Bob] Service stopped")
 
 
app=FastAPI(
    title="SAE B - Secure Application Entity (Bob)",
    description="Receiver BB84",
    version="0.7.0",
    lifespan=lifespan,
)

@app.post("/session/register")
async def register_session(session_id: str):
    logger.info(f"[Bob] Session {session_id} registered")
    return {"statut": "ready", "session_id": session_id}

@app.post("/sift", response_model=SiftResp)
async def sift(req: SiftReq):
    r=get_redis()
    bob_meas=load_all_bob_measurements(r, req.session_id)
    if not bob_meas:
        raise HTTPException(
            status_code=404,
            detail=f"No measurements for session {req.session_id}",)
    
    alice_bases_map:dict[int, str]={qid: basis for qid, basis in req.alice_bases}
    bob_sifted_bits:list[int]=[]
    bob_bases_for_alice: list[tuple[int, str]]=[]
    matched_ids:list[int]=[]
    
    for qid in sorted(bob_meas.keys()):
        meas=bob_meas[qid]
        bob_bases_for_alice.append((qid, meas.basis.value))
        if qid in alice_bases_map and alice_bases_map[qid]==meas.basis.value:
            bob_sifted_bits.append(meas.bit_res)
            matched_ids.append(qid)
    
    
    import random
    rng=random.Random(req.sample_seed)
    n=len(bob_sifted_bits)
    n_sample=max(1, int(n*0.20)) if n>0 else 0
    sample_idx=set(rng.sample(range(n), n_sample)) if n>=n_sample >0 else set()
    bob_final_key=[
        b for i, b in enumerate(bob_sifted_bits)
        if i not in sample_idx
    ]
    save_session_result(r, f"{req.session_id}:bob", {
        "sifted_bits": bob_sifted_bits,
        "final_key_len": len(bob_final_key),
        "n_sifted": n,
        "matched_ids": matched_ids,
    })

    logger.info(
        f"[Bob] Session {req.session_id} sifted: "
        f"{len(bob_sifted_bits)} bits retained, {len(bob_final_key)} bits key final"
    )

    return SiftResp(
        session_id=req.session_id,
        bob_bases=bob_bases_for_alice,
        n_sifted=len(bob_sifted_bits),
        bob_key_len=len(bob_final_key),
        matched_ids=matched_ids,
        bob_sifted_bits=bob_sifted_bits,
    )

@app.get("/session/{session_id}/sifted-bits")
async def get_sifted_bits(session_id: str):
    r=get_redis()
    data=load_session_result(r,f"{session_id}:bob")
    
    if not data:
        raise HTTPException(status_code=404, detail="Sifting not done yet")
    return {
        "session_id":session_id,
        "sifted_bits":data["sifted_bits"],
        "n_sifted":data["n_sifted"],}

@app.delete("/session/{session_id}")
async def cleanup_session(session_id: str):
    r=get_redis()
    delete_session(r, session_id)
    r.delete(f"session:{session_id}:bob:result")
    return {"statut": "cleaned", "session_id": session_id}

 
@app.get("/health")
async def health():
    try:
        r = get_redis()
        r.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {"statut": "ok", "redis": redis_ok}
 
if __name__=="__main__":
    import uvicorn
    uvicorn.run(app,host="0.0.0.0", port=8002, log_level="warning")