import json
import os
import time
from typing import Optional

import redis

from models import (
    KeyStatus, SessionStatusResponse,
    QubitBatch, MeasurementUpload, SiftUpload, KeyUpload,
)

REDIS_URL   = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SESSION_TTL = 7200   #2 hours


def get_redis() -> redis.Redis:
    return redis.from_url(REDIS_URL, decode_responses=True)



def _ks(sid: str)       -> str: return f"kme:session:{sid}"
def _kq(sid: str)       -> str: return f"kme:session:{sid}:qubits"
def _km(sid: str)       -> str: return f"kme:session:{sid}:meas"
def _ksift(sid: str)    -> str: return f"kme:session:{sid}:sift"
def _kkey(sid: str)     -> str: return f"kme:session:{sid}:key"



def save_session(r: redis.Redis, session: dict) -> None:
    sid = session["session_id"]
    r.set(_ks(sid), json.dumps(session), ex=SESSION_TTL)

    if session.get("status") == "open":
        r.sadd("kme:sessions:open",   sid)
        r.sadd("kme:sessions:active", sid)
    elif session.get("status") in ("done", "aborted"):
        r.srem("kme:sessions:open",   sid)
        r.srem("kme:sessions:active", sid)


def load_session(r: redis.Redis, session_id: str) -> Optional[dict]:
    raw = r.get(_ks(session_id))
    return json.loads(raw) if raw else None


def update_session(r: redis.Redis, session_id: str, **fields) -> None:
    session = load_session(r, session_id)
    if session:
        session.update(fields)
        save_session(r, session)


def list_open_sessions(r: redis.Redis) -> list[str]:
    return list(r.smembers("kme:sessions:open"))


def list_active_sessions(r: redis.Redis) -> list[str]:
    return list(r.smembers("kme:sessions:active"))


#Qubit bus 
def push_qubit_batch(r: redis.Redis, session_id: str, batch: dict) -> None:
    r.rpush(_kq(session_id), json.dumps(batch))
    r.expire(_kq(session_id), SESSION_TTL)


def pop_qubit_batch(r: redis.Redis, session_id: str) -> Optional[dict]:
    raw = r.lpop(_kq(session_id))
    return json.loads(raw) if raw else None


def qubit_batch_count(r: redis.Redis, session_id: str) -> int:
    return r.llen(_kq(session_id))


#measurement bus
def save_measurements(r: redis.Redis, session_id: str,
                       upload: dict) -> None:
    meas_list = upload.get("measurements", [])
    if not meas_list:
        return
    mapping = {
        str(m["qubit_id"]): json.dumps(m)
        for m in meas_list
    }
    r.hset(_km(session_id), mapping=mapping)
    r.expire(_km(session_id), SESSION_TTL)


def load_measurements(r: redis.Redis, session_id: str) -> dict[int, dict]:
    raw = r.hgetall(_km(session_id))
    return {int(k): json.loads(v) for k, v in raw.items()}


#sifting bus
def save_sift_upload(r: redis.Redis, session_id: str, upload: dict) -> None:
    r.set(_ksift(session_id), json.dumps(upload), ex=SESSION_TTL)


def load_sift_upload(r: redis.Redis, session_id: str) -> Optional[dict]:
    raw = r.get(_ksift(session_id))
    return json.loads(raw) if raw else None


#key save
def save_key_upload(r: redis.Redis, session_id: str, upload: dict) -> None:
    r.set(_kkey(session_id), json.dumps(upload), ex=SESSION_TTL)


def load_key_upload(r: redis.Redis, session_id: str) -> Optional[dict]:
    raw = r.get(_kkey(session_id))
    return json.loads(raw) if raw else None


#key lifecycle management
KEY_TTL = int(os.getenv("BB84_KEY_TTL", "300"))


def activate_key(r: redis.Redis, session_id: str) -> float:
    expires_at = time.time() + KEY_TTL
    update_session(r, session_id,
                   key_status=KeyStatus.ACTIVE.value,
                   key_expires_at=expires_at)
    return expires_at


def consume_key(r: redis.Redis, session_id: str) -> tuple[bool, Optional[str]]:

    session = load_session(r, session_id)
    if not session:
        return False, None

    key_status  = session.get("key_status", KeyStatus.NONE.value)
    expires_at  = session.get("key_expires_at")

    if key_status != KeyStatus.ACTIVE.value:
        return False, None
    if expires_at and time.time() > expires_at:
        update_session(r, session_id, key_status=KeyStatus.EXPIRED.value)
        return False, None

    update_session(r, session_id, key_status=KeyStatus.CONSUMED.value)
    return True, session.get("key_final", "")



def delete_session(r: redis.Redis, session_id: str) -> None:
    r.delete(
        _ks(session_id), _kq(session_id),
        _km(session_id), _ksift(session_id), _kkey(session_id),
    )
    r.srem("kme:sessions:open",   session_id)
    r.srem("kme:sessions:active", session_id)