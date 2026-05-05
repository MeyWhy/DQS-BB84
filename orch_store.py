import json
import os
import redis
from typing import Optional
from state_machine import OrchestratorSession, SessionStatus
from models import NodeRole, NodeRecord

REDIS_URL=os.getenv("REDIS_URL", "redis://localhost:6379/0")
SESSION_TTL=7200  
NODE_TTL= 60

def get_redis()->redis.Redis:
    return redis.from_url(REDIS_URL, decode_responses=True)

def _skey(session_id: str) -> str:
    return f"orch:session:{session_id}"

def _nkey(node_id: str) -> str:
    return f"orch:node:{node_id}"

def save_orch_session(r: redis.Redis, session: OrchestratorSession) -> None:
    pipe=r.pipeline()
    pipe.set(_skey(session.session_id), session.model_dump_json(), ex=SESSION_TTL)
    pipe.sadd("orch:sessions:all", session.session_id)
    if not session.is_terminal:
        pipe.sadd("orch:sessions:active", session.session_id)
        if session.status==SessionStatus.WAITING:
            pipe.sadd("orch:session:waiting", session.session_id)
    else:
        pipe.srem("orch:sessions:active", session.session_id)
        pipe.srem("orch:session:waiting", session.session_id)
    pipe.execute()

def load_orch_session(
    r: redis.Redis, session_id: str
) -> Optional[OrchestratorSession]:
    raw=r.get(_skey(session_id))
    if not raw:
        return None
    return OrchestratorSession.model_validate_json(raw)


def update_orch_session(r: redis.Redis,session: OrchestratorSession,) -> None:
    pipe=r.pipeline()
    pipe.set(_skey(session.session_id), session.model_dump_json(), ex=SESSION_TTL)
    if session.is_terminal:
        pipe.srem("orch:sessions:active", session.session_id)
        pipe.srem("orch:session:waiting", session.session_id)
    elif session.status==SessionStatus.WAITING:
        pipe.sadd("orch:session:waiting", session.session_id)
    else:
        pipe.srem("orch:session:waiting", session.session_id)
    pipe.execute()

def list_active_sessions(r: redis.Redis) -> list[str]:
    return list(r.smembers("orch:sessions:active"))

def list_all_sessions(r: redis.Redis) -> list[str]:
    return list(r.smembers("orch:sessions:all"))

def list_waiting_sessions(r: redis.Redis) -> list[str]:
    return list(r.smembers("orch:sessions:waiting"))

 
def save_node(r: redis.Redis, node: NodeRecord) -> None:
    pipe = r.pipeline()
    pipe.set(_nkey(node.node_id), node.model_dump_json(), ex=NODE_TTL)
    pipe.sadd("orch:nodes:all", node.node_id)
    pipe.sadd(f"orch:nodes:role:{node.role.value}", node.node_id)
    pipe.execute()
 
 
def load_node(r: redis.Redis, node_id: str) -> Optional[NodeRecord]:
    raw = r.get(_nkey(node_id))
    if not raw:
        return None
    return NodeRecord.model_validate_json(raw)
 
 
def update_node(r: redis.Redis, node: NodeRecord) -> None:
    r.set(_nkey(node.node_id), node.model_dump_json(), ex=NODE_TTL)
 
 
def delete_node(r: redis.Redis, node_id: str) -> None:
    node = load_node(r, node_id)
    pipe = r.pipeline()
    pipe.delete(_nkey(node_id))
    pipe.srem("orch:nodes:all", node_id)
    if node:
        pipe.srem(f"orch:nodes:role:{node.role.value}", node_id)
    pipe.execute()
 
 
def list_nodes(r: redis.Redis, role: Optional[NodeRole] = None) -> list[NodeRecord]:
    if role:
        ids = r.smembers(f"orch:nodes:role:{role.value}")
    else:
        ids = r.smembers("orch:nodes:all")
 
    nodes = []
    for nid in ids:
        node = load_node(r, nid)
        if node:                  # expired TTL ->>> load returns None, skip silently
            nodes.append(node)
        else:
            # clean up stale set entry
            r.srem("orch:nodes:all", nid)
            if role:
                r.srem(f"orch:nodes:role:{role.value}", nid)
    return nodes
 
 
def heartbeat_node(r: redis.Redis, node_id: str) -> bool:
    raw = r.get(_nkey(node_id))
    if not raw:
        return False
    r.expire(_nkey(node_id), NODE_TTL)
    return True

def claim_session_role(
    r: redis.Redis,
    session_id: str,
    node_id: str,
    role: NodeRole,
) -> bool:
    claim_key = f"orch:session:{session_id}:claim:{role.value}"
    acquired  = r.set(claim_key, node_id, nx=True, ex=SESSION_TTL)
    return bool(acquired)