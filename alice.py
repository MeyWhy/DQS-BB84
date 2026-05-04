import logging
import threading
import os
import random
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from celery import group, chain, chord
from celery.result import AsyncResult
from workers.qubit_tasks   import send_batch_task
from workers.sifting_tasks import assemble_and_sift_task, qber_key_task, notify_orchestrator_task
from workers.celery_config import celery_app
from models import (Basis, SessionStartReq,QubitBatch, QubitRecord,)
from pydantic import BaseModel

logger=logging.getLogger("alice")
logging.basicConfig(level=logging.INFO)

class EmitReq(BaseModel):
    session_id: str
    n_qubits: int
    batch_size: int
    loss_rate: float

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[Alice] Service started")
    yield
    logger.info("[Alice] Service stopped")

 
app=FastAPI(
    title="SAE-A - Secure Application Entity (Alice)",
    description="Emitter of qubits for BB84",
    version="0.7.0",
    lifespan=lifespan,)

#here alice gen les bits & bases et divides them as batches to be put in redis broker
def _make_batches(session_id:str, n_qubits:int, batch_size:int,)-> tuple[list[int], list[str], list[QubitBatch]]:
    bits=[random.randint(0,1) for _ in range(n_qubits)]
    bases=[random.choice(list(Basis)) for _ in range(n_qubits)]
    batches=[]
    batch_id=0

    for start in range(0, n_qubits, batch_size):
        end=min(start+batch_size,n_qubits)
        qubits=[
            QubitRecord(qubit_id=i, bit=bits[i], basis=bases[i])
            for i in range(start, end)
        ]
        batches.append(QubitBatch(
            session_id=session_id,
            batch_id=batch_id,
            qubits=qubits,
        ))
        batch_id+=1
    return bits, [b.value for b in bases], batches

@app.post("/emit")
async def emit(req: EmitReq):
    bits, bases, batches=_make_batches(session_id=req.session_id, n_qubits=req.n_qubits, batch_size=req.batch_size)
    session_meta={
        "session_id":req.session_id,
        "n_qubits": req.n_qubits,
        "alice_bits": bits,
        "alice_bases": bases,
    }

    batch_group=group(
        send_batch_task.s(
            session_id=req.session_id,
            batch_payload=batch.model_dump(),
        ) for batch in batches
    )

    pipeline=chord(batch_group)(
        chain(
            assemble_and_sift_task.s(session_meta=session_meta),
            qber_key_task.s(),
            notify_orchestrator_task.s(),
        )
    )

    logger.info(
        f"[Alice] Pipeline started session={req.session_id} "
        f"batches={len(batches)} task_id={pipeline.id}"
    )

    return {
        "session_id": req.session_id,
        "celery_task_id": pipeline.id,
        "n_batches": len(batches),
        "n_qubits": req.n_qubits,
   }

 
@app.get("/health")
async def health():
    return {"status": "ok", "service": "alice", "version": "0.6.0"}


if __name__=="__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="warning")
