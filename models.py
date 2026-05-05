from __future__ import annotations
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field
import uuid

class Basis(str, Enum):
    RECTILINEAR="Z"
    DIAGONAL="X"

def new_session_id() ->str:
    return str(uuid.uuid4())

class NetworkInitReq(BaseModel):
    session_id:str
    n_qubits:int= Field(gt=0, le=10000)
    loss_rate:float=Field(default=0.0, ge=0.0, le=1.0)

class NetworkInitResp(BaseModel):
    session_id: str
    statut:str
    message: str=""

class QubitRecord(BaseModel):
    qubit_id:int
    bit:int
    basis:Basis

#batch it so we can diminuer overhead de redis
class QubitBatch(BaseModel):
    session_id:str
    batch_id:int
    qubits:list[QubitRecord]

class QubitBatchResult(BaseModel):
    session_id: str
    batch_id: int
    delivered:list[int] #qubit ids qui ont ete livres
    failed:list[int] #qubit ids failed

class SendBatchReq(BaseModel):
    session_id: str
    batch:      QubitBatch

class SendBatchResp(BaseModel):
    session_id: str
    batch_id:   int
    results:    list[dict]   

class SessionMeta(BaseModel):
    session_id:str
    n_qubits: int
    batch_size:int
    loss_rate:float
    bits: list[int]
    bases:list[str]
    statut: str="init"
    sample_seed: Optional[int]=None

class QubitMeasurement(BaseModel):
    qubit_id:int
    basis: Basis
    bit_res:int=Field(ge=0, le=1)

class NetworkStopReq(BaseModel):
    session_id:str

class SessionCreateReq(BaseModel):
    n_qubits:int=Field(default=200, gt=0, le=5000)
    loss_rate:float=Field(default=0.0, ge=0.0, le=1.0)
    batch_size: int=Field(default=10, gt=0, le=100)

SessionStartReq=SessionCreateReq #backward compat pour eviter les soucis if old file calls

class NodeRole(str, Enum):
    SENDER   = "sender"    #Alice
    RECEIVER = "receiver"  #Bob
    RELAY    = "relay"     #future
    EVE      = "eve"       #eavesdropper simulation for later

class NodeCapabilities(BaseModel):
    max_qubits: int = 5000
    supported_loss_rates: list[float] = Field(default_factory=list)
    protocol_version: str = "bb84-v1"


class NodeRegistrationReq(BaseModel):
    node_id:    str
    role:   NodeRole
    callback_url: str
    capabilities: NodeCapabilities=Field(default_factory=NodeCapabilities)

class NodeRegistrationResp(BaseModel):
    node_id:    str
    role:   NodeRole
    registered:    bool

class NodeRecord(BaseModel):
    node_id: str
    role: NodeRole
    callback_url: str
    capabilities: NodeCapabilities
    registered_at: float
    current_session_id: Optional[str]=None

class JoinSessionReq(BaseModel):
    node_id: str
    role: NodeRole

class JoinSessionResp(BaseModel):
    session_id: str
    role: NodeRole
    accepted: bool
    detail: str=""

class SiftReq(BaseModel):
    session_id:str
    alice_bases:list[tuple[int, str]]
    sample_seed: int= Field(ge=0)

class SiftResp(BaseModel):
    session_id:str
    bob_bases: list[tuple[int, str]]
    n_sifted:int
    bob_key_len:int
    matched_ids: list[int]
    bob_sifted_bits: list[int]

class ErrorCode:
    SESSION_NOT_FOUND="SESSION_NOT_FOUND"
    NETWORK_UNAVAILABLE="NETWORK_UNAVAILABLE"
    QBER_TOO_HIGH="QBER_TOO_HIGH"
    INSUFFICIENT_BITS="INSUFFICIENT_BITS"
    TIMEOUT="TIMEOUT"
    INTERNAL_ERROR="INTERNAL_ERROR"