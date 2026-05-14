from __future__ import annotations
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field
import uuid


class Basis(str, Enum):
    RECTILINEAR = "Z"
    DIAGONAL    = "X"


def new_session_id() -> str:
    return str(uuid.uuid4())


def new_node_id() -> str:
    return str(uuid.uuid4())

class NodeRole(str, Enum):
    SENDER   = "sender"     #Alice equivalent
    RECEIVER = "receiver"   #Bob equivalent
    RELAY    = "relay"      #future: intermediate node
    MONITOR  = "monitor"    #future: Eve / passive listener


class NodeRegistration(BaseModel):

    role:        NodeRole
    callback_url: str          #URL the KME calls for webhook notifications
    label:       str = ""      #name
    metadata:    dict = {}     #extensible: location, capabilities, etc.


class NodeInfo(BaseModel):
    """KME's view of a registered node."""
    node_id:      str
    role:         NodeRole
    callback_url: str
    label:        str   = ""
    metadata:     dict  = {}
    registered_at: float = 0.0


class SessionCreateReq(BaseModel):

    sender_node_id:   str
    receiver_label:   str        #label of the target Bob node
    n_qubits:         int   = Field(default=200, ge=0, le=5000)
    batch_size:       int   = Field(default=10,  gt=0,  le=100)
    loss_rate:        float = Field(default=0.0, ge=0.0, le=1.0)
    retry_enabled:    bool  = False


class SessionJoinReq(BaseModel):

    node_id:   str
    session_id: str


class SessionJoinResp(BaseModel):
    session_id:      str
    role:            NodeRole
    sender_node_id:  str
    n_qubits:        int
    status:          str


class QubitRecord(BaseModel):
    qubit_id: int
    bit:      int   = Field(ge=0, le=1)
    basis:    Basis


class QubitBatch(BaseModel):
    session_id: str
    batch_id:   int
    qubits:     list[QubitRecord]


class QubitUpload(BaseModel):

    session_id: str
    batch:      QubitBatch


class MeasurementRecord(BaseModel):
    qubit_id:   int
    basis:      Basis
    bit_result: int = Field(ge=0, le=1)


class MeasurementUpload(BaseModel):

    session_id:    str
    node_id:       str
    measurements:  list[MeasurementRecord]


class SiftUpload(BaseModel):
    
    session_id:   str
    alice_bases:  list[tuple[int, str]]   #[(qubit_id, basis_str), ...]
    sample_seed:  int = Field(ge=0)


class SiftResult(BaseModel):

    session_id:    str
    node_id:       str
    bob_bases:     list[tuple[int, str]]
    n_sifted:      int
    bob_sifted_bits: list[int]


class KeyStatus(str, Enum):
    NONE     = "none"
    ACTIVE   = "active"
    CONSUMED = "consumed"
    EXPIRED  = "expired"


class KeyUpload(BaseModel):

    session_id:  str
    node_id:     str
    key_final:   str          #the key material (or hash for security)
    key_hash:    str          #SHA-256
    qber:        float
    n_sifted:    int
    status:      str          #"success" | "aborted"
    error_message: str = ""


class NetworkInitReq(BaseModel):
    session_id: str
    n_qubits:   int   = Field(gt=0, le=10000)
    loss_rate:  float = Field(default=0.0, ge=0.0, le=1.0)


class NetworkInitResp(BaseModel):
    session_id: str
    statut:     str
    message:    str = ""


class SendBatchReq(BaseModel):
    session_id: str
    batch:      QubitBatch


class SendBatchResp(BaseModel):
    session_id: str
    batch_id:   int
    results:    list[dict]


class NetworkStopReq(BaseModel):
    session_id: str


class SessionStatusResponse(BaseModel):

    session_id:     str
    status:         str
    n_qubits:       int   = 0
    n_delivered:    int   = 0
    n_sifted:       int   = 0
    qber:           float = 0.0
    key_final:      str   = ""
    key_status:     KeyStatus = KeyStatus.NONE
    key_expires_at: Optional[float] = None
    error_message:  str   = ""
    elapsed_s:      float = 0.0
    progress_pct:   float = 0.0
    phase_label:    str   = ""

    #ETSI GS QKD 014 aliases
    @property
    def key_ID(self)   -> str: return self.session_id
    @property
    def key(self)      -> str: return self.key_final
    @property
    def key_size(self) -> int: return self.n_sifted


class WebhookEvent(BaseModel):

    event:      str          #"session_open" | "qubits_ready" |
                             #"measurements_ready" | "sift_ready" |
                             #"key_available" | "session_aborted"
    session_id: str
    payload:    dict = {}    #event-specific data


class SiftReq(BaseModel):
    session_id:  str
    alice_bases: list[tuple[int, str]]
    sample_seed: int = Field(ge=0)


class SiftResp(BaseModel):
    session_id:      str
    bob_bases:       list[tuple[int, str]]
    n_sifted:        int
    bob_key_len:     int
    matched_ids:     list[int]
    bob_sifted_bits: list[int]

class ErrorCode:
    SESSION_NOT_FOUND   = "SESSION_NOT_FOUND"
    NODE_NOT_FOUND      = "NODE_NOT_FOUND"
    SESSION_NOT_OPEN    = "SESSION_NOT_OPEN"
    NETWORK_UNAVAILABLE = "NETWORK_UNAVAILABLE"
    QBER_TOO_HIGH       = "QBER_TOO_HIGH"
    INSUFFICIENT_BITS   = "INSUFFICIENT_BITS"
    TIMEOUT             = "TIMEOUT"
    INTERNAL_ERROR      = "INTERNAL_ERROR"