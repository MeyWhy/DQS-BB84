from __future__ import annotations
import logging
import os
import random
import threading
import time
from contextlib import asynccontextmanager
from typing import Optional

import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

try:
    from models import (
        Basis, NetworkInitReq, NetworkInitResp,
        MeasurementRecord, SendBatchReq, SendBatchResp,
        QubitBatch, NetworkStopReq,
        InterceptRegisterReq, InterceptRegisterResp,
    )
except ModuleNotFoundError:
    from models import (
        Basis, NetworkInitReq, NetworkInitResp,
        MeasurementRecord, SendBatchReq, SendBatchResp,
        QubitBatch, NetworkStopReq,
        InterceptRegisterReq, InterceptRegisterResp,
    )

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("qkdl")



#Classical channel models
class ClassicalSendReq(BaseModel):
    session_id:  str
    payload_hex: str
    mode:        str = "direct"


class ClassicalSendResp(BaseModel):
    session_id: str
    delivered:  bool
    mode:       str


class ClassicalRecvResp(BaseModel):
    session_id:  str
    payload_hex: str
    available:   bool



#Post-stop cooldown
COOLDOWN_S       = 2.5
_cooldown_until  = 0.0
_cooldown_lock   = threading.Lock()


def _set_cooldown() -> None:
    with _cooldown_lock:
        global _cooldown_until
        _cooldown_until = time.time() + COOLDOWN_S


def _cooldown_remaining() -> float:
    with _cooldown_lock:
        return max(0.0, _cooldown_until - time.time())


def _clear_cooldown() -> None:
    with _cooldown_lock:
        global _cooldown_until
        _cooldown_until = 0.0



#NetworkSession
#How long to wait after starting Bob's thread before Alice sends.
#EQSN has no "ready" callback; this small delay ensures get_qubit()
#is blocking before the qubit arrives.
BOB_READY_DELAY = 0.015   #15 ms : empirically sufficient for EQSN


class NetworkSession:
    def __init__(self, session_id: str, loss_rate: float = 0.0):
        self.session_id  = session_id
        self.loss_rate   = loss_rate

        self.backend     = None
        self.network     = None
        self.alice_host  = None
        self.bob_host    = None
        self._active     = False

        sid6             = session_id.replace("-", "")[:6]
        self._alice_name = f"Alice-{sid6}"
        self._bob_name   = f"Bob-{sid6}"

        #Measurement results
        self._meas_queue: list[dict] = []
        self._meas_lock   = threading.Lock()

        #Eve intercept state
        self._eve_registered  = False
        self._eve_label       = ""
        self._eve_meas_queue: list[dict] = []
        self._eve_meas_lock   = threading.Lock()

    def start(self) -> None:
        from qunetsim.components import Host, Network
        from qunetsim.backends import EQSNBackend

        self.backend = EQSNBackend()
        self.network = Network.get_instance()
        self.network.start([self._alice_name, self._bob_name], self.backend)

        self.alice_host = Host(self._alice_name, self.backend)
        self.bob_host   = Host(self._bob_name,   self.backend)

        self.alice_host.add_connection(self._bob_name)
        self.bob_host.add_connection(self._alice_name)

        if self.loss_rate > 0:
            self.network.packet_drop_rate = self.loss_rate

        self.alice_host.start()
        self.bob_host.start()
        self.network.add_host(self.alice_host)
        self.network.add_host(self.bob_host)

        self._active = True
        logger.info(f"[QKDL] Session {self.session_id[:8]} started")

    def stop(self) -> None:
        if not self._active:
            return

        try:
            if self.alice_host:
                try:
                    self.alice_host.stop()
                except Exception as e:
                    logger.debug(f"[QKDL] alice_host.stop(): {e}")
            if self.bob_host:
                try:
                    self.bob_host.stop()
                except Exception as e:
                    logger.debug(f"[QKDL] bob_host.stop(): {e}")
            self.network.stop(stop_hosts=False)
        except Exception as e:
            logger.warning(f"[QKDL] Network stop error: {e}")
        finally:
            self._active = False
            try:
                from qunetsim.components import Network as _Net
                _Net._instance = None
            except Exception as e:
                logger.debug(f"[QKDL] Singleton reset: {e}")
            _set_cooldown()
            logger.info(f"[QKDL] Session {self.session_id[:8]} stopped (cooldown {COOLDOWN_S}s)")

    def is_active(self) -> bool:
        return self._active

    def push_measurement(self, result: dict) -> None:
        with self._meas_lock:
            self._meas_queue.append(result)

    def pop_measurement(self) -> Optional[dict]:
        with self._meas_lock:
            return self._meas_queue.pop(0) if self._meas_queue else None

    def measurement_count(self) -> int:
        with self._meas_lock:
            return len(self._meas_queue)

    def push_eve_measurement(self, result: dict) -> None:
        with self._eve_meas_lock:
            self._eve_meas_queue.append(result)

    def pop_eve_measurement(self) -> Optional[dict]:
        with self._eve_meas_lock:
            return self._eve_meas_queue.pop(0) if self._eve_meas_queue else None

    def eve_measurement_count(self) -> int:
        with self._eve_meas_lock:
            return len(self._eve_meas_queue)

    def register_interceptor(self, eve_node_id: str, eve_label: str) -> None:
        self._eve_registered = True
        self._eve_label      = eve_label
        logger.warning(
            f"[QKDL] *** Eve registered session={self.session_id[:8]} "
            f"label={eve_label} ***"
        )


#Session registry
_sessions:      dict[str, NetworkSession] = {}
_sessions_lock  = threading.Lock()
_classical_inbox: dict[str, list[str]] = {}
_classical_lock   = threading.Lock()


#Eve intercept-resend
def _eve_intercept_qubit(
    alice_bit:   int,
    alice_basis: Basis,
    session:     NetworkSession,
    qubit_id:    int,
) -> tuple[int, str]:
    eve_basis = random.choice(list(Basis))
    eve_bit   = alice_bit if eve_basis == alice_basis else random.randint(0, 1)

    session.push_eve_measurement({
        "qubit_id":    qubit_id,
        "alice_basis": alice_basis.value,
        "alice_bit":   alice_bit,
        "eve_basis":   eve_basis.value,
        "eve_bit":     eve_bit,
        "basis_match": eve_basis == alice_basis,
    })
    return eve_bit, eve_basis.value



#Core: per-qubit Bob-first send
def _send_one_qubit(
    session:    NetworkSession,
    qid:        int,
    bit:        int,
    basis:      Basis,
) -> dict:
    
    from qunetsim.objects import Qubit

    bob_result: dict = {}
    bob_done    = threading.Event()

    def _bob_recv(
        res=bob_result, done=bob_done,
        bh=session.bob_host, an=session._alice_name,
    ):
        q = bh.get_qubit(an, wait=5)
        if q is None:
            res["bit"]   = None
            res["basis"] = None
        else:
            b = random.choice(list(Basis))
            if b == Basis.DIAGONAL:
                q.H()
            res["bit"]   = q.measure()
            res["basis"] = b.value
        done.set()

    t = threading.Thread(
        target=_bob_recv, daemon=True,
        name=f"bob-{session.session_id[:6]}-q{qid}",
    )
    t.start()

    #Give Bob's thread time to enter get_qubit() before Alice sends
    time.sleep(BOB_READY_DELAY)

    #Alice prepares and sends
    q = Qubit(session.alice_host)
    if bit == 1:
        q.X()
    if basis == Basis.DIAGONAL:
        q.H()
    session.alice_host.send_qubit(session._bob_name, q, await_ack=False)

    #Wait for Bob
    bob_done.wait(timeout=6.0)
    t.join(timeout=6.0)

    delivered = bob_result.get("bit") is not None
    return {
        "qubit_id":  qid,
        "delivered": delivered,
        "bob_basis": bob_result.get("basis"),
        "bob_bit":   bob_result.get("bit"),
    }


def _process_batch_sync(
    session: NetworkSession,
    batch:   QubitBatch,
) -> list[dict]:
    results = []

    for qrec in batch.qubits:
        qid   = qrec.qubit_id
        bit   = qrec.bit
        basis = qrec.basis

        #Loss model (before Eve  a lost qubit is never intercepted)
        if session.loss_rate > 0 and random.random() < session.loss_rate:
            results.append({
                "qubit_id": qid, "delivered": False,
                "bob_basis": None, "bob_bit": None,
            })
            continue

        #Eve intercept-resend (pure Python, no QuNetSim)
        if session._eve_registered:
            eve_bit, eve_basis_val = _eve_intercept_qubit(bit, basis, session, qid)
            bob_basis = random.choice(list(Basis))
            bob_bit   = eve_bit if bob_basis.value == eve_basis_val else random.randint(0, 1)
            results.append({
                "qubit_id":  qid,
                "delivered": True,
                "bob_basis": bob_basis.value,
                "bob_bit":   bob_bit,
            })
            session.push_measurement({
                "qubit_id":    qid,
                "basis":       bob_basis.value,
                "bit_result":  bob_bit,
                "delivered":   True,
                "intercepted": True,
            })
            continue

        #Normal QuNetSim path
        res = _send_one_qubit(session, qid, bit, basis)
        results.append(res)

        if res["delivered"]:
            session.push_measurement({
                "qubit_id":    qid,
                "basis":       res["bob_basis"],
                "bit_result":  res["bob_bit"],
                "delivered":   True,
                "intercepted": False,
            })

    return results


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[QKDL] Service started")
    yield
    with _sessions_lock:
        sessions = list(_sessions.values())
    for s in sessions:
        s.stop()
    logger.info("[QKDL] Service stopped")


app = FastAPI(
    title="QKDL - QKD Link Layer (QuNetSim)",
    description="Quantum transport layer for BB84",
    version="1.1.0",
    lifespan=lifespan,
)


@app.post("/network/init", response_model=NetworkInitResp)
async def init_network(req: NetworkInitReq):
    remaining = _cooldown_remaining()
    if remaining > 0:
        raise HTTPException(
            status_code=503,
            detail=f"QKDL cooling down. Retry in {remaining:.1f}s.",
        )

    with _sessions_lock:
        dead = [sid for sid, s in _sessions.items() if not s.is_active()]
        for sid in dead:
            del _sessions[sid]

        if req.session_id in _sessions:
            return NetworkInitResp(session_id=req.session_id, statut="ready", message="Already active")

        if _sessions:
            active = list(_sessions.keys())
            raise HTTPException(
                status_code=409,
                detail=f"Session already active: {active[0][:8]}. Stop it first.",
            )

    session = NetworkSession(req.session_id, loss_rate=req.loss_rate)
    loop    = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, session.start)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    with _sessions_lock:
        _sessions[req.session_id] = session

    return NetworkInitResp(
        session_id=req.session_id,
        statut="ready",
        message=f"Network ready for {req.n_qubits} qubits",
    )


@app.post("/network/stop")
async def stop_network(req: NetworkStopReq):
    with _sessions_lock:
        session = _sessions.pop(req.session_id, None)
    with _classical_lock:
        _classical_inbox.pop(req.session_id, None)
    if session:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, session.stop)
    return {"statut": "stopped", "session_id": req.session_id}


@app.post("/network/reset")
async def reset_network():
    loop = asyncio.get_event_loop()
    with _sessions_lock:
        sessions_to_stop = list(_sessions.values())
        _sessions.clear()
    with _classical_lock:
        _classical_inbox.clear()
    for s in sessions_to_stop:
        await loop.run_in_executor(None, s.stop)
    try:
        from qunetsim.components import Network as _Net
        _Net.get_instance().stop(stop_hosts=True)
    except Exception:
        pass
    _clear_cooldown()
    return {"statut": "reset", "stopped": len(sessions_to_stop)}


@app.post("/intercept/{session_id}", response_model=InterceptRegisterResp)
async def register_interceptor(session_id: str, req: InterceptRegisterReq):
    with _sessions_lock:
        session = _sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session {session_id[:8]} not yet active. Retry in 1s.")
    if not session.is_active():
        raise HTTPException(status_code=409, detail="Session is not active")
    session.register_interceptor(req.eve_node_id, req.eve_label)
    return InterceptRegisterResp(
        session_id=session_id, registered=True,
        message=f"Eve '{req.eve_label}' registered as interceptor",
    )


@app.get("/intercept/{session_id}/measurements")
async def eve_measurements(session_id: str):
    with _sessions_lock:
        session = _sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    records = []
    while True:
        m = session.pop_eve_measurement()
        if m is None:
            break
        records.append(m)
    return {"session_id": session_id, "intercepted_n": len(records),
            "measurements": records, "eve_label": session._eve_label}


@app.post("/batch/send", response_model=SendBatchResp)
async def send_batch(req: SendBatchReq):
    with _sessions_lock:
        session = _sessions.get(req.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    loop    = asyncio.get_event_loop()
    results = await loop.run_in_executor(None, _process_batch_sync, session, req.batch)
    return SendBatchResp(session_id=req.session_id, batch_id=req.batch.batch_id, results=results)


@app.get("/qubit/receive/{session_id}")
async def receive_qubit(session_id: str):
    with _sessions_lock:
        session = _sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    meas = session.pop_measurement()
    if meas:
        return {
            "session_id": session_id, "qubit_id": meas["qubit_id"],
            "basis": meas["basis"], "bit_result": meas["bit_result"],
            "delivered": meas["delivered"], "queue_empty": False,
            "remaining": session.measurement_count(),
        }
    return {"session_id": session_id, "qubit_id": None, "bit_result": None,
            "queue_empty": True, "remaining": 0}


@app.get("/qubit/count/{session_id}")
async def qubit_count(session_id: str):
    with _sessions_lock:
        session = _sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"session_id": session_id, "count": session.measurement_count()}


@app.post("/classical/send", response_model=ClassicalSendResp)
async def send_classical(req: ClassicalSendReq):
    with _sessions_lock:
        session = _sessions.get(req.session_id)
    if not session or not session.is_active():
        raise HTTPException(status_code=404, detail="Session not found or inactive")

    loop = asyncio.get_event_loop()

    def _do_send() -> bool:
        received = {}
        ready    = threading.Event()

        def _bob_listen():
            msgs    = session.bob_host.get_classical("Alice", wait=10)
            if msgs:
                m       = msgs[-1] if isinstance(msgs, list) else msgs
                content = getattr(m, "content", m)
                received["hex"] = content if isinstance(content, str) else content.hex()
            ready.set()

        threading.Thread(target=_bob_listen, daemon=True).start()
        time.sleep(0.05)

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
    return ClassicalSendResp(session_id=req.session_id, delivered=delivered, mode=req.mode)


@app.get("/classical/recv/{session_id}", response_model=ClassicalRecvResp)
async def recv_classical(session_id: str):
    with _classical_lock:
        inbox = _classical_inbox.get(session_id, [])
        if inbox:
            return ClassicalRecvResp(session_id=session_id, payload_hex=inbox.pop(0), available=True)
    return ClassicalRecvResp(session_id=session_id, payload_hex="", available=False)


@app.get("/health")
async def health():
    remaining = _cooldown_remaining()
    with _sessions_lock:
        active = list(_sessions.keys())
        counts = {sid: _sessions[sid].measurement_count() for sid in active}
        eve_on = {sid: _sessions[sid]._eve_registered for sid in active}
    return {
        "statut": "ok", "active_sessions": active,
        "measurement_queues": counts, "eve_active": eve_on,
        "cooldown_remaining": round(remaining, 2),
        "ready": remaining == 0 and not active,
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("QKDL_PORT", "8003"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")