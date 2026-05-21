from fastapi import FastAPI, HTTPException
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
import os

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


#Cooldown state
COOLDOWN_S        = 2.5
_cooldown_until:  float          = 0.0
_cooldown_lock:   threading.Lock = threading.Lock()


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

class NetworkSession:
    def __init__(self, session_id: str, loss_rate: float = 0.0):
        self.session_id  = session_id
        self.loss_rate   = loss_rate
        self.backend     = None
        self.network     = None
        self.alice_host  = None
        self.bob_host    = None
        self._send_lock  = threading.Lock()
        self._active     = False
        self._meas_queue: list[dict] = []
        self._meas_lock  = threading.Lock()
        sid6             = session_id.replace("-", "")[:6]
        self._alice_name = f"Alice-{sid6}"
        self._bob_name   = f"Bob-{sid6}"

        #Eve intercept state
        #When Eve registers via POST /intercept/{session_id}, this is set.
        #_process_batch_sync checks this flag per qubit and routes through Eve.
        self._eve_registered: bool = False
        self._eve_label:      str  = ""
        #Eve's own measurement log  for thesis stats
        self._eve_meas_queue: list[dict] = []
        self._eve_meas_lock  = threading.Lock()

    def register_interceptor(self, eve_node_id: str, eve_label: str) -> None:
        self._eve_registered = True
        self._eve_label      = eve_label
        logger.warning(
            f"[QKDL] *** Eve registered as interceptor "
            f"session={self.session_id[:8]} label={eve_label} ***"
        )

    def start(self):
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
        logger.info(f"[QKDL] Session {self.session_id} started")

    def stop(self):
        if self._active:
            try:
                self.network.stop(stop_hosts=True)
            except Exception as e:
                logger.warning(f"[QKDL] Stop error: {e}")
            self._active = False

            try:
                if hasattr(Network, '_instance'):
                    Network._instance = None
                time.sleep(0.3)
            except Exception as e:
                logger.warning(f"[QKDL] Singleton reset error: {e}")

            _set_cooldown()
            logger.info(
                f"[QKDL] Session {self.session_id} stopped "
                f"(cooldown {COOLDOWN_S}s)"
            )

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
            return (self._eve_meas_queue.pop(0)
                    if self._eve_meas_queue else None)

    def eve_measurement_count(self) -> int:
        with self._eve_meas_lock:
            return len(self._eve_meas_queue)


_sessions:     dict[str, NetworkSession] = {}
_sessions_lock = threading.Lock()

_classical_inbox: dict[str, list[str]] = {}
_classical_lock   = threading.Lock()


#Qubit processing  the core of BB84 and the Eve intercept

def _eve_intercept_qubit(
    alice_bit:   int,
    alice_basis: Basis,
    session:     NetworkSession,
    qubit_id:    int,
) -> tuple[int, str]:
    """
    Simulate Eve's intercept-resend attack on one qubit

    Eve picks a random basis and measures Alice's qubit.
    She then re-prepares a new qubit in her measured basis and bit,
    which is what Bob will ultimately receive.

    This is the intercept-resend attack.  When Eve guesses the wrong basis
    (~50% of the time) she sends Bob a qubit that is random from his
    perspective, introducing ~25% errors in the sifted key (QBER~~ 0.25).

    Returns:
        (bob_bit, bob_basis_value)  what Bob's detector will see.
    """
    eve_basis = random.choice(list(Basis))

    #Eve measures Alice's qubit in her chosen basis.
    #If eve_basis == alice_basis: she gets alice_bit (correct).
    #If eve_basis != alice_basis: quantum mechanics gives her a random bit.
    if eve_basis == alice_basis:
        eve_bit = alice_bit
    else:
        eve_bit = random.randint(0, 1)

    #Log Eve's measurement for thesis stats
    session.push_eve_measurement({
        "qubit_id":        qubit_id,
        "alice_basis":     alice_basis.value,
        "alice_bit":       alice_bit,
        "eve_basis":       eve_basis.value,
        "eve_bit":         eve_bit,
        "basis_match":     eve_basis == alice_basis,
    })

    #Eve re-prepares a new qubit: bit=eve_bit, basis=eve_basis.
    #This is what Bob will receive and measure  he has no idea it's a
    #re-prepared qubit rather than Alice's original.
    return eve_bit, eve_basis.value


def _process_batch_sync(
    session: NetworkSession,
    batch:   QubitBatch,
) -> list[dict]:
    results = []

    for qrec in batch.qubits:
        qid   = qrec.qubit_id
        bit   = qrec.bit
        basis = qrec.basis

        #Loss model (applied before Eve  a lost qubit is never intercepted)
        if session.loss_rate > 0 and random.random() < session.loss_rate:
            results.append({
                "qubit_id": qid, "delivered": False,
                "bob_basis": None, "bob_bit": None,
            })
            continue

        #Eve intercept-resend 
        #If Eve is registered, she intercepts the qubit here before it
        #reaches Bob's detector.  The re-prepared values replace the
        #original Alice values for Bob's measurement.
        if session._eve_registered:
            bit, basis_value = _eve_intercept_qubit(bit, basis, session, qid)
            #Bob will measure what Eve re-prepared
            bob_bit   = bit
            bob_basis = random.choice(list(Basis))   #Bob still picks randomly
            #When bob_basis == eve_basis (~50%): Bob gets the correct eve_bit.
            #When bob_basis != eve_basis (~50%): Bob gets a random result.
            #Combined with Alice's sifting: only basis-matched pairs survive.
            #Net effect: ~25% of surviving pairs have errors → QBER ≈ 0.25.
            if bob_basis.value != basis_value:
                bob_bit = random.randint(0, 1)
            results.append({
                "qubit_id":  qid,
                "delivered": True,
                "bob_basis": bob_basis.value,
                "bob_bit":   bob_bit,
            })
            session.push_measurement({
                "qubit_id":   qid,
                "basis":      bob_basis.value,
                "bit_result": bob_bit,
                "delivered":  True,
                "intercepted": True,
            })
            continue
        #

        #Normal (non-intercepted) path: QuNetSim quantum channel
        bob_res   = {}
        bob_ready = threading.Event()

        def bob_receive(
            res=bob_res, ready=bob_ready,
            bh=session.bob_host, an=session._alice_name,
        ):
            q = bh.get_qubit(an, wait=3)
            if q is None:
                res["bit"]   = None
                res["basis"] = None
            else:
                bob_basis = random.choice(list(Basis))
                if bob_basis == Basis.DIAGONAL:
                    q.H()
                res["bit"]   = q.measure()
                res["basis"] = bob_basis.value
            ready.set()

        t_bob = threading.Thread(
            target=bob_receive, daemon=True,
            name=f"bob-recv-{session.session_id[:6]}-q{qid}",
        )
        t_bob.start()
        time.sleep(0.003)

        with session._send_lock:
            q = Qubit(session.alice_host)
            if bit == 1:
                q.X()
            if basis == Basis.DIAGONAL:
                q.H()
            session.alice_host.send_qubit(
                session._bob_name, q, await_ack=False
            )

        bob_ready.wait(timeout=4.0)
        t_bob.join(timeout=4.0)

        delivered = bob_res.get("bit") is not None
        results.append({
            "qubit_id":  qid,
            "delivered": delivered,
            "bob_basis": bob_res.get("basis"),
            "bob_bit":   bob_res.get("bit"),
        })

        if delivered:
            session.push_measurement({
                "qubit_id":   qid,
                "basis":      bob_res.get("basis"),
                "bit_result": bob_res.get("bit"),
                "delivered":  True,
                "intercepted": False,
            })

    return results


#App

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[QKDL] Service started")
    yield
    with _sessions_lock:
        for session in _sessions.values():
            session.stop()
    logger.info("[QKDL] Service stopped")


app = FastAPI(
    title="QKDL - QKD Link Layer (QuNetSim)",
    description="Quantum transport layer for BB84",
    version="0.9.0",
    lifespan=lifespan,
)


@app.post("/network/init", response_model=NetworkInitResp)
async def init_network(req: NetworkInitReq):
    remaining = _cooldown_remaining()
    if remaining > 0:
        raise HTTPException(
            status_code=503,
            detail=(
                f"QKDL cooling down after previous session stop. "
                f"Retry in {remaining:.1f}s."
            ),
        )

    with _sessions_lock:
        dead = [sid for sid, s in _sessions.items() if not s.is_active()]
        for sid in dead:
            del _sessions[sid]

        if req.session_id in _sessions:
            return NetworkInitResp(
                session_id=req.session_id,
                statut="ready",
                message="Already active",
            )

        if _sessions:
            active = list(_sessions.keys())
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Session already active: {active[0][:8]}. "
                    f"Stop it first via POST /network/stop."
                ),
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

    for session in sessions_to_stop:
        await loop.run_in_executor(None, session.stop)

    try:
        from qunetsim.components import Network as _Net
        _Net.get_instance().stop(stop_hosts=True)
    except Exception:
        pass

    _clear_cooldown()
    return {"statut": "reset", "stopped": len(sessions_to_stop)}


#Eve intercept registration endpoint
#Eve calls this after receiving her session_open webhook.
#Once registered, every qubit in this session routes through _eve_intercept_qubit.

@app.post("/intercept/{session_id}", response_model=InterceptRegisterResp)
async def register_interceptor(session_id: str, req: InterceptRegisterReq):
    with _sessions_lock:
        session = _sessions.get(session_id)

    if not session:
        #QKDL may still be initialising  return 202 and Eve will retry
        raise HTTPException(
            status_code=404,
            detail=f"Session {session_id[:8]} not yet active on this QKDL. "
                   f"Retry in 1s."
        )

    if not session.is_active():
        raise HTTPException(status_code=409, detail="Session is not active")

    session.register_interceptor(req.eve_node_id, req.eve_label)
    return InterceptRegisterResp(
        session_id=session_id,
        registered=True,
        message=f"Eve '{req.eve_label}' registered as interceptor",
    )


@app.post("/batch/send", response_model=SendBatchResp)
async def send_batch(req: SendBatchReq):
    with _sessions_lock:
        session = _sessions.get(req.session_id)

    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    loop    = asyncio.get_event_loop()
    results = await loop.run_in_executor(
        None, _process_batch_sync, session, req.batch
    )

    return SendBatchResp(
        session_id=req.session_id,
        batch_id=req.batch.batch_id,
        results=results,
    )


@app.get("/qubit/receive/{session_id}")
async def receive_qubit(session_id: str):
    with _sessions_lock:
        session = _sessions.get(session_id)

    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    meas = session.pop_measurement()
    if meas:
        return {
            "session_id":  session_id,
            "qubit_id":    meas["qubit_id"],
            "basis":       meas["basis"],
            "bit_result":  meas["bit_result"],
            "delivered":   meas["delivered"],
            "queue_empty": False,
            "remaining":   session.measurement_count(),
        }

    return {
        "session_id":  session_id,
        "qubit_id":    None,
        "bit_result":  None,
        "queue_empty": True,
        "remaining":   0,
    }


@app.get("/qubit/count/{session_id}")
async def qubit_count(session_id: str):
    with _sessions_lock:
        session = _sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"session_id": session_id, "count": session.measurement_count()}


#Eve's own measurement log  she polls this to log her interceptions

@app.get("/intercept/{session_id}/measurements")
async def eve_measurements(session_id: str):
    """
    Eve polls this to retrieve her own measurement records.
    Each record contains: qubit_id, alice_basis, alice_bit, eve_basis,
    eve_bit, basis_match.  This is the data for the thesis QBER plot.
    """
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

    return {
        "session_id":    session_id,
        "intercepted_n": len(records),
        "measurements":  records,
        "eve_label":     session._eve_label,
    }


@app.post("/classical/send", response_model=ClassicalSendResp)
async def send_classical(req: ClassicalSendReq):
    with _sessions_lock:
        session = _sessions.get(req.session_id)

    if not session or not session.is_active():
        raise HTTPException(
            status_code=404, detail="Session not found or inactive"
        )

    loop = asyncio.get_event_loop()

    def _do_send() -> bool:
        received = {}
        ready    = threading.Event()

        def _bob_listen():
            msgs    = session.bob_host.get_classical("Alice", wait=10)
            if msgs:
                m       = msgs[-1] if isinstance(msgs, list) else msgs
                content = getattr(m, "content", m)
                received["hex"] = (
                    content if isinstance(content, str) else content.hex()
                )
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
                _classical_inbox.setdefault(req.session_id, []).append(
                    received["hex"]
                )
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


@app.get("/health")
async def health():
    remaining = _cooldown_remaining()
    with _sessions_lock:
        active  = list(_sessions.keys())
        counts  = {sid: _sessions[sid].measurement_count() for sid in active}
        eve_on  = {sid: _sessions[sid]._eve_registered for sid in active}
    return {
        "statut":             "ok",
        "active_sessions":    active,
        "measurement_queues": counts,
        "eve_active":         eve_on,
        "cooldown_remaining": round(remaining, 2),
        "ready":              remaining == 0 and not active,
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("QKDL_PORT", "8003"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")