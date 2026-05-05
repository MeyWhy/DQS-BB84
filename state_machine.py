from __future__ import annotations
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field
import uuid
import time
import os

KEY_TTL_SECONDS=int(os.getenv("BB84_KEY_TTL", "180"))


class KeyStatus(str, Enum):
    NONE    =   "none"
    ACTIVE      =   "active"
    CONSUMED    =   "consumed"
    EXPIRED     =   "expired"


class SessionStatus(str, Enum):
    CREATED      = "created"       #Session créee, pas encore demarré
    WAITING      = "waiting"       #en attente de nodes
    INITIALIZING = "initializing"  #QNS + Bob en cours d'init
    SENDING      = "sending"       #Qubits en transit (workers Celery)
    SIFTING      = "sifting"       #Chord done, sifting en cours
    DONE         = "done"          #Cle gen avec succes
    ABORTED      = "aborted"       #Erreur non recup


#Transitions valides: etat_courant->>> {etats_suivants autorises}
VALID_TRANSITIONS: dict[SessionStatus, set[SessionStatus]] = {
    SessionStatus.CREATED:      {SessionStatus.WAITING, SessionStatus.ABORTED},
    SessionStatus.WAITING:      {SessionStatus.INITIALIZING, SessionStatus.ABORTED},
    SessionStatus.INITIALIZING: {SessionStatus.SENDING,      SessionStatus.ABORTED},
    SessionStatus.SENDING:      {SessionStatus.SIFTING,      SessionStatus.ABORTED},
    SessionStatus.SIFTING:      {SessionStatus.DONE,         SessionStatus.ABORTED},
    SessionStatus.DONE:         set(),
    SessionStatus.ABORTED:      set(),
}

#etats terminaux => aucune transition possible
TERMINAL_STATES = {SessionStatus.DONE, SessionStatus.ABORTED}


class TransitionError(Exception):
    pass


def validate_transition(current: SessionStatus, target: SessionStatus) -> None:
    if target not in VALID_TRANSITIONS.get(current, set()):
        raise TransitionError(
            f"Transition interdite : {current.value} -> {target.value}. "
            f"Transitions valides : {[s.value for s in VALID_TRANSITIONS[current]]}"
        )


class OrchestratorSession(BaseModel):

    session_id:   str
    status:       SessionStatus = SessionStatus.CREATED
    n_qubits:     int
    batch_size:   int
    loss_rate:    float
    
    key_status:     KeyStatus=KeyStatus.NONE
    key_expires_at: Optional[float] = None

    sender_node_id:    Optional[str] =None
    receiver_node_id:   Optional[str]= None

    #timestamps pour metrics de latence
    created_at:      float = Field(default_factory=time.time)
    started_at:      Optional[float] = None
    sending_at:      Optional[float] = None
    sifting_at:      Optional[float] = None
    completed_at:    Optional[float] = None

    #res intermediaire
    celery_task_id:  Optional[str] = None   #id du chord celery
    n_delivered:     int   = 0
    n_sifted:        int   = 0
    qber:            float = 0.0
    key_final:        str   = ""
    error_message:   str   = ""

    def transition(self, target: SessionStatus) -> "OrchestratorSession":
        validate_transition(self.status, target)
        self.status = target

        now = time.time()
        if target == SessionStatus.INITIALIZING:
            self.started_at  = now
        elif target == SessionStatus.SENDING:
            self.sending_at  = now
        elif target == SessionStatus.SIFTING:
            self.sifting_at  = now
        elif target in TERMINAL_STATES:
            self.completed_at = now

        return self

    def activate_key(self) -> None:
        self.key_status = KeyStatus.ACTIVE
        self.key_expires_at = time.time() + KEY_TTL_SECONDS

    def consume_key(self) -> bool:
        if not self.is_key_valid():
            return False
        self.key_status = KeyStatus.CONSUMED
        return True
    
    def is_key_valid(self) -> bool:
        if self.key_status != KeyStatus.ACTIVE:
            return False
        if self.key_expires_at and time.time() > self.key_expires_at:
            self.key_status = KeyStatus.EXPIRED
            return False
        return True
    
    def is_ready_to_start(self)-> bool:
        return (self.sender_node_id is not None and self.receiver_node_id is not None)
    
    @property
    def elapsed_s(self) -> float:
        if self.completed_at and self.created_at:
            return self.completed_at - self.created_at
        return time.time() - self.created_at

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATES

class SessionStatusResponse(BaseModel):
    session_id:    str
    status:        SessionStatus
    elapsed_s:     float
    n_qubits:      int
    n_delivered:   int   = 0
    n_sifted:      int   = 0
    qber:          float = 0.0
    key_final:      str   = ""
    error_message: str   = ""

    #champs de progression pour le client not necessary
    progress_pct:  float = 0.0   # 0-100
    phase_label:   str   = ""

    key_status:     KeyStatus=KeyStatus.NONE
    key_expires_at: Optional[float] = None

    sender_node_id:   Optional[str] = None
    receiver_node_id: Optional[str] = None

def session_to_response(s: OrchestratorSession) -> SessionStatusResponse:
    s.is_key_valid()

    progress = {
        SessionStatus.CREATED:      0.0,
        SessionStatus.WAITING:      5.0,
        SessionStatus.INITIALIZING: 10.0,
        SessionStatus.SENDING:      40.0,
        SessionStatus.SIFTING:      80.0,
        SessionStatus.DONE:         100.0,
        SessionStatus.ABORTED:      0.0,
    }
    labels = {
        SessionStatus.CREATED:      "En attente",
        SessionStatus.WAITING:      "Attente des nodes",
        SessionStatus.INITIALIZING: "Initialisation reseau quantique",
        SessionStatus.SENDING:      "Transmission des qubits",
        SessionStatus.SIFTING:      "Sifting & calcul QBER",
        SessionStatus.DONE:         "Cle generee",
        SessionStatus.ABORTED:      "Session abandonnee",
    }

    return SessionStatusResponse(
        session_id=s.session_id,
        status=s.status,
        elapsed_s=round(s.elapsed_s, 2),
        n_qubits=s.n_qubits,
        n_delivered=s.n_delivered,
        n_sifted=s.n_sifted,
        qber=s.qber,
        key_final=s.key_final,
        error_message=s.error_message,
        progress_pct=progress.get(s.status, 0.0),
        phase_label=labels.get(s.status, ""),
        key_status=s.key_status,
        key_expires_at=s.key_expires_at,
        sender_node_id=s.sender_node_id,
        receiver_node_id=s.receiver_node_id,
    )


def new_session_id() -> str:
    return str(uuid.uuid4())
