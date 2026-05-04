from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from qunetsim.components import Host, Network
from qunetsim.backends import EQSNBackend
from qunetsim.objects import Qubit
import logging
import asyncio
from contextlib import asynccontextmanager
from typing import Optional
import random
import threading
import time
from models import(
    Basis,NetworkInitReq, NetworkInitResp,
    SendBatchReq, SendBatchResp, QubitBatch,
    QubitMeasurement, NetworkStopReq,
)
from redis_store import (get_redis, save_bob_measurements_batch, mark_delivered)
logging.basicConfig(level=logging.WARNING)
logger=logging.getLogger("qns")
_classical_inbox: dict[str, list[str]] = {}   # session_id → [hex, ...]
_classical_lock = threading.Lock()

class ClassicalSendReq(BaseModel):
    session_id: str
    payload_hex: str          #ciphertext as hex string
    mode: str = "direct"      #direct or broadcast

class ClassicalSendResp(BaseModel):
    session_id: str
    delivered: bool
    mode: str

class ClassicalRecvResp(BaseModel):
    session_id: str
    payload_hex: str          # "" if nothing wait
    available: bool
class NetworkSession:
    def __init__(self, session_id: str, loss_rate:float=0.0):
        self.session_id= session_id
        self.loss_rate= loss_rate
        self.backend= EQSNBackend()
        self.network= Network.get_instance()
        self.alice_host:Optional[Host] = None
        self.bob_host:Optional[Host] = None
        self._send_lock= threading.Lock()
        self._active= False
    
    def start(self):
        self.network.start(['Alice', 'Bob'], self.backend)
        self.alice_host=Host('Alice', self.backend)
        self.bob_host=Host('Bob', self.backend)
        self.alice_host.add_connection('Bob')
        self.bob_host.add_connection('Alice')
        
        if self.loss_rate>0:
            self.network.packet_drop_rate=self.loss_rate
        self.alice_host.start()
        self.bob_host.start()

        self.network.add_host(self.alice_host)
        self.network.add_host(self.bob_host)
        self._active=True
        logger.info(f"[QNS] Session {self.session_id} started")
     
    def stop(self):
        if self._active:
            try:
                self.network.stop(stop_hosts=True)
            except Exception as e:
                logger.warning(f"[QNS] Error stop: {e}")
            self._active=False
            logger.info(f"[QNS] Session {self.session_id} stopped")

    def is_active(self)->bool:
        return self._active

_sessions:dict[str, NetworkSession]={}
_sessions_lock=threading.Lock()

def _process_batch_sync(
        session:NetworkSession, 
        batch: QubitBatch,
)-> list[dict]:
    results=[]
    for qrec in batch.qubits:
        qid=qrec.qubit_id
        bit=qrec.bit
        basis=qrec.basis
        #simuler perte 
        if session.loss_rate>0 and random.random() < session.loss_rate:
            results.append({
                "qubit_id":qid,
                "delivered":False,
                "bob_basis":None,
                "bob_bit":None,
            })
            continue
        
        bob_res={}
        bob_ready=threading.Event()
        
        def bob_receive(res=bob_res, ready=bob_ready):
            q=session.bob_host.get_qubit("Alice", wait=3)
            if q is None:
                res['bit']=None
                res['basis']=None
            else:
                bob_basis=random.choice(list(Basis))
                if bob_basis==Basis.DIAGONAL:
                    q.H()
                res["bit"]=q.measure()
                res['basis']=bob_basis.value
            ready.set()

        t_bob=threading.Thread(target=bob_receive, daemon=True)    
        t_bob.start()
        time.sleep(0.01)

        with session._send_lock:
            q=Qubit(session.alice_host)
            if bit==1:
                q.X()
            if basis==Basis.DIAGONAL:
                q.H()
            session.alice_host.send_qubit('Bob', q, await_ack=False)
        bob_ready.wait(timeout=4.0)
        t_bob.join(timeout=4.0)

        delivered=bob_res.get('bit') is not None
        results.append({
            "qubit_id": qid,
            "delivered":delivered,
            "bob_basis": bob_res.get('basis'),
            "bob_bit":bob_res.get('bit'),
        })

    return results

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[QNS] Service Started")
    yield
    with _sessions_lock:
        for session in _sessions.values():
            session.stop()
    logger.info("[QNS] Service stopped")
   
app=FastAPI(
    title="Quantum Network Service",
    description="QuNetSim wrapper for BB84",
    version="0.5.0",
    lifespan=lifespan,
) 
@app.post("/network/init", response_model=NetworkInitResp)
async def init_network(req: NetworkInitReq):
    with _sessions_lock:
        dead=[sid for sid, s in _sessions.items() if not s.is_active()]
        for sid in dead:
            del _sessions[sid]
        
        if req.session_id in _sessions:
            return NetworkInitResp( session_id=req.session_id, statut="ready", message="Already active session")
        
        if _sessions:
            raise HTTPException(
                status_code=409,
                detail= "a session is already active")
        
    loop=asyncio.get_event_loop()
    session=NetworkSession(req.session_id, loss_rate=req.loss_rate)

    try:
        await loop.run_in_executor(None, session.start)
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))
 
    with _sessions_lock:
        _sessions[req.session_id] = session
 
    return NetworkInitResp(
        session_id=req.session_id,
        statut="ready",
        message=f"network ready for {req.n_qubits} qubits",
    )

@app.post("/batch/send", response_model=SendBatchResp)
async def send_batch(req: SendBatchReq):
    with _sessions_lock:
        session=_sessions.get(req.session_id)
    
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    loop=asyncio.get_event_loop()
    results= await loop.run_in_executor(None, _process_batch_sync,session, req.batch)
    
    r=get_redis()
    measurements=[]
    delivered_ids=[]
    
    for res in results:
        if res['delivered'] and res['bob_bit'] is not None:
            measurements.append(QubitMeasurement(
                qubit_id=res["qubit_id"], 
                basis=Basis(res["bob_basis"]), 
                bit_res=res["bob_bit"],
            ))
            delivered_ids.append(res["qubit_id"])
    if measurements:
        save_bob_measurements_batch(r, req.session_id, measurements)
        mark_delivered(r, req.session_id, delivered_ids)
    return SendBatchResp(
        session_id=req.session_id,
        batch_id=req.batch.batch_id,
        results=results,
    )

@app.post("/classical/send", response_model=ClassicalSendResp)
async def send_classical(req: ClassicalSendReq):
    with _sessions_lock:
        session = _sessions.get(req.session_id)

    if not session or not session.is_active():
        raise HTTPException(status_code=404, detail="Session not found or inactive")

    loop = asyncio.get_event_loop()

    def _do_send():
        # Bob listens first
        received = {}
        ready    = threading.Event()

        def _bob_listen():
            msgs = session.bob_host.get_classical("Alice", wait=10)
            if msgs:
                m       = msgs[-1] if isinstance(msgs, list) else msgs
                content = getattr(m, "content", m)
                received["hex"] = content if isinstance(content, str) else content.hex()
            ready.set()

        threading.Thread(target=_bob_listen, daemon=True).start()
        time.sleep(0.05)

        # Alice sends
        if req.mode == "broadcast":
            session.alice_host.send_broadcast(req.payload_hex, ["Bob"])
        else:
            session.alice_host.send_classical("Bob", req.payload_hex)

        ready.wait(timeout=10.0)

        if "hex" in received:
            with _classical_lock:
                _classical_inbox.setdefault(req.session_id, []).append(received["hex"])
            return True
        return False

    delivered = await loop.run_in_executor(None, _do_send)

    return ClassicalSendResp(
        session_id=req.session_id,
        delivered=delivered,
        mode=req.mode,
    )


@app.get("/classical/recv/{session_id}", response_model=ClassicalRecvResp)
async def recv_classical(session_id: str):
    """
    Retrieves the next classical message from Bob's inbox.
    Returns available=False if nothing is waiting.
    """
    with _classical_lock:
        inbox = _classical_inbox.get(session_id, [])
        if inbox:
            return ClassicalRecvResp(
                session_id=session_id,
                payload_hex=inbox.pop(0),
                available=True,
            )
    return ClassicalRecvResp(
        session_id=session_id,
        payload_hex="",
        available=False,
    )

@app.post("/network/stop")
async def stop_network(req: NetworkStopReq):
    with _sessions_lock:
        session=_sessions.pop(req.session_id, None)
    with _classical_lock:
        _classical_inbox.pop(req.session_id, None)
    if session:
        loop=asyncio.get_event_loop()
        await loop.run_in_executor(None, session.stop)
    return {"statut": "stopped", "session_id": req.session_id}    

 
@app.get("/health")
async def health():
    with _sessions_lock:
        active = list(_sessions.keys())
    return {"statut": "ok", "active_sessions": active}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003, log_level="warning")