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

from optical.channel import StatisticalChannel, FiberChannel

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

_ENCODE: dict[tuple[str, int], str] = {
    ("Z", 0): "H",
    ("Z", 1): "V",
    ("X", 0): "D",
    ("X", 1): "A",
}
_DECODE: dict[str, tuple[str, int]] = {v: k for k, v in _ENCODE.items()}

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


COOLDOWN_S      = 2.5
_cooldown_until = 0.0
_cooldown_lock  = threading.Lock()

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


BOB_READY_DELAY = 0.015   #15 ms: enough for EQSN get_qubit() to block first


class NetworkSession:
    def __init__(
        self,
        session_id:  str,
        loss_rate:   float = 0.0,
        distance_km: float = 0.0,
    ):
        self.session_id  = session_id
        self.loss_rate   = loss_rate
        self.distance_km = distance_km

       
        if distance_km > 0.0:
            self.channel = FiberChannel(
                distance_km=distance_km,
                csv_path="optical/data/attenuation_table.csv",
            )
        else:
            self.channel = StatisticalChannel(loss_rate)

        #QuNetSim objects (initialised in start())
        self.backend    = None
        self.network    = None
        self.alice_host = None
        self.bob_host   = None
        self._active    = False

        sid6              = session_id.replace("-", "")[:6]
        self._alice_name  = f"Alice-{sid6}"
        self._bob_name    = f"Bob-{sid6}"

        #Measurement results
        self._meas_queue: list[dict] = []
        self._meas_lock  = threading.Lock()

        #Eve intercept state
        self._eve_registered = False
        self._eve_label      = ""
        self._eve_meas_queue: list[dict] = []
        self._eve_meas_lock  = threading.Lock()

    
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

        #IMPORTANT: do NOT set network.packet_drop_rate here.
        #All loss is owned by the optical channel model, not QuNetSim.
        #Setting packet_drop_rate would double-count photon loss.

        self.alice_host.start()
        self.bob_host.start()
        self.network.add_host(self.alice_host)
        self.network.add_host(self.bob_host)

        #Reset per-session state on the optical model
        self.channel.reset_session()

        self._active = True
        logger.info(
            f"[QKDL] Session {self.session_id[:8]} started "
            f"channel={type(self.channel).__name__} "
            f"distance={self.distance_km} km"
        )

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
            logger.info(
                f"[QKDL] Session {self.session_id[:8]} stopped "
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



PULSE_PERIOD_NS = 1000.0   #1 MHz clock → 1000 ns per qubit slot


def _send_one_qubit_qns(
    session:       NetworkSession,
    qid:           int,
    effective_bit:  int,    #may differ from original if drift flipped the state
    effective_basis: Basis, #may differ from original if drift flipped the state
) -> dict:
    """
    Send a single qubit through QuNetSim and return Bob's measurement.
    Uses the drift-corrected (effective) bit and basis so that polarization
    rotation is faithfully represented in the quantum simulation.
    """
    from qunetsim.objects import Qubit

    bob_result: dict = {}
    bob_done = threading.Event()

    def _bob_recv(
        res=bob_result, done=bob_done,
        bh=session.bob_host, an=session._alice_name,
    ):
        q = bh.get_qubit(an, wait=5)
        if q is None:
            res["bit"]   = None
            res["basis"] = None
        else:
            bob_b = random.choice(list(Basis))
            if bob_b == Basis.DIAGONAL:
                q.H()
            res["bit"]   = q.measure()
            res["basis"] = bob_b.value
        done.set()

    t = threading.Thread(
        target=_bob_recv, daemon=True,
        name=f"bob-{session.session_id[:6]}-q{qid}",
    )
    t.start()

    #Give Bob's thread time to enter get_qubit() before Alice sends
    time.sleep(BOB_READY_DELAY)

    #Alice prepares qubit in the drift-corrected state
    q = Qubit(session.alice_host)
    if effective_bit == 1:
        q.X()
    if effective_basis == Basis.DIAGONAL:
        q.H()
    session.alice_host.send_qubit(session._bob_name, q, await_ack=False)

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
    """
    Process one batch of qubits.

    For each qubit the pipeline is:

      [Optical: attenuation + drift + detector]
            ↓ photon lost?  → delivered=False, done
            ↓ dark count?   → synthetic measurement, done
            ↓ photon survived (possibly drift-modified state)
      [Eve: intercept-resend]  ← only if _eve_registered
            ↓
      [QuNetSim: real qubit send/receive]
    """
    results = []

    for qrec in batch.qubits:
        qid   = qrec.qubit_id
        bit   = qrec.bit
        basis = qrec.basis
        t_ns  = qid * PULSE_PERIOD_NS

        
        #Ansys attenuation + OU drift + detector
        photon_in  = {"qubit_id": qid, "bit": bit, "basis": basis.value}
        transmitted = session.channel.transmit(photon_in, t_ns=t_ns)

        if transmitted is None:
            #Photon lost in fiber or missed by detector
            results.append({
                "qubit_id":  qid,
                "delivered": False,
                "bob_basis": None,
                "bob_bit":   None,
            })
            continue

        #Dark count: synthetic measurement, no QuNetSim needed
        if transmitted.get("dark_count", False):
            bob_basis = random.choice(list(Basis))
            bob_bit   = random.randint(0, 1)
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
                "intercepted": False,
                "dark_count":  True,
            })
            continue


        #Extract drift-corrected state for QuNetSim preparation
        effective_basis_str = transmitted.get("basis", basis.value)
        effective_bit       = transmitted.get("bit",   bit)
        try:
            effective_basis = Basis(effective_basis_str)
        except ValueError:
            effective_basis = basis


        #Eve intercept-resend (after optical, before QuNetSim)
        if session._eve_registered:
            eve_bit, eve_basis_val = _eve_intercept_qubit(
                effective_bit, effective_basis, session, qid
            )
            #Bob measures Eve's resent photon - pure Python, no QuNetSim.
            #Eve's resent photon is a fresh coherent state; QuNetSim would
            #just add overhead without changing the probability model.
            bob_basis = random.choice(list(Basis))
            bob_bit   = (
                eve_bit if bob_basis.value == eve_basis_val
                else random.randint(0, 1)
            )
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

        
        #QuNetSim: real qubit send/receive
        res = _send_one_qubit_qns(
            session,
            qid,
            effective_bit,
            effective_basis,
        )
        results.append(res)

        if res["delivered"]:
            session.push_measurement({
                "qubit_id":    qid,
                "basis":       res["bob_basis"],
                "bit_result":  res["bob_bit"],
                "delivered":   True,
                "intercepted": False,
                "dark_count":  False,
            })

    return results


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[QKDL] Service started (QuNetSim + Optical merged)")
    yield
    with _sessions_lock:
        sessions = list(_sessions.values())
    for s in sessions:
        s.stop()
    logger.info("[QKDL] Service stopped")


app = FastAPI(
    title="QKDL - QKD Link Layer (QuNetSim + Optical)",
    description="Quantum transport via QuNetSim + Ansys optical channel",
    version="2.0.0",
    lifespan=lifespan,
)

_sessions:        dict[str, NetworkSession] = {}
_sessions_lock    = threading.Lock()
_classical_inbox: dict[str, list[str]] = {}
_classical_lock   = threading.Lock()


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
            return NetworkInitResp(
                session_id=req.session_id, statut="ready",
                message="Already active",
            )

        if _sessions:
            active = list(_sessions.keys())
            raise HTTPException(
                status_code=409,
                detail=f"Session already active: {active[0][:8]}. Stop it first.",
            )

    session = NetworkSession(
        req.session_id,
        loss_rate=req.loss_rate,
        distance_km=req.distance_km,
    )
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, session.start)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    with _sessions_lock:
        _sessions[req.session_id] = session

    return NetworkInitResp(
        session_id=req.session_id,
        statut="ready",
        message=f"Network ready - channel={type(session.channel).__name__} d={req.distance_km}km",
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
        raise HTTPException(
            status_code=404,
            detail=f"Session {session_id[:8]} not yet active. Retry in 1s.",
        )
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
    return {
        "session_id":    session_id,
        "intercepted_n": len(records),
        "measurements":  records,
        "eve_label":     session._eve_label,
    }


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
            "session_id": session_id,
            "qubit_id":   meas["qubit_id"],
            "basis":      meas["basis"],
            "bit_result": meas["bit_result"],
            "delivered":  meas["delivered"],
            "queue_empty": False,
            "remaining":  session.measurement_count(),
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
                _classical_inbox.setdefault(
                    req.session_id, []
                ).append(received["hex"])
            return True
        return False

    delivered = await loop.run_in_executor(None, _do_send)
    return ClassicalSendResp(
        session_id=req.session_id, delivered=delivered, mode=req.mode
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
        session_id=session_id, payload_hex="", available=False
    )


@app.get("/channel/status/{session_id}")
async def channel_status(session_id: str):
    with _sessions_lock:
        session = _sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    ch = session.channel
    report = {
        "session_id":   session_id,
        "channel_type": type(ch).__name__,
        "channel":      ch.describe(),
    }

    if hasattr(ch, "qber_floor"):
        physical_floor = ch.qber_floor()
        report["qber_budget"] = {
            "physical_floor": round(physical_floor, 8),
            "eve_threshold":  round(max(0.0, 0.11 - physical_floor), 8),
            "margin_note":    "Eve detectable if measured_QBER > physical_floor + margin",
        }

    if hasattr(ch, "detector") and ch.detector:
        report["detector_counters"] = ch.detector.counters()

    if hasattr(ch, "drift") and ch.drift and hasattr(ch.drift, "describe"):
        report["drift_state"] = ch.drift.describe()

    return report


@app.get("/health")
async def health():
    remaining = _cooldown_remaining()
    with _sessions_lock:
        active = list(_sessions.keys())
        counts = {sid: _sessions[sid].measurement_count() for sid in active}
        eve_on = {sid: _sessions[sid]._eve_registered for sid in active}
        detector_stats = {}
        for sid in active:
            ch    = _sessions[sid].channel
            entry = {}
            if hasattr(ch, "detector") and ch.detector:
                entry["detector"] = ch.detector.counters()
            if hasattr(ch, "drift") and ch.drift and hasattr(ch.drift, "describe"):
                entry["drift"] = ch.drift.describe()
            detector_stats[sid] = entry
    return {
        "statut":             "ok",
        "active_sessions":    active,
        "measurement_queues": counts,
        "eve_active":         eve_on,
        "detector_stats":     detector_stats,
        "cooldown_remaining": round(remaining, 2),
        "ready":              remaining == 0 and not active,
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("QKDL_PORT", "8003"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")