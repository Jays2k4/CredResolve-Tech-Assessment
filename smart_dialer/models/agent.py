from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Set
import time


class AgentState(str, Enum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DIALING = "DIALING"
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP_UP"
    PAUSED = "PAUSED"


VALID_AGENT_TRANSITIONS: dict[AgentState, Set[AgentState]] = {
    AgentState.OFFLINE: {AgentState.AVAILABLE},
    AgentState.AVAILABLE: {
        AgentState.RESERVED,
        AgentState.PAUSED,
        AgentState.OFFLINE,
    },
    AgentState.RESERVED: {
        AgentState.DIALING,
        AgentState.AVAILABLE,  # Call cancelled or setup aborted
        AgentState.OFFLINE,
    },
    AgentState.DIALING: {
        AgentState.CONNECTED,
        AgentState.AVAILABLE,  # Call failed, unanswered, busy, or canceled
        AgentState.OFFLINE,
    },
    AgentState.CONNECTED: {
        AgentState.WRAP_UP,
        AgentState.OFFLINE,  # Abrupt agent disconnection
    },
    AgentState.WRAP_UP: {
        AgentState.AVAILABLE,
        AgentState.PAUSED,
        AgentState.OFFLINE,
    },
    AgentState.PAUSED: {
        AgentState.AVAILABLE,
        AgentState.OFFLINE,
    },
}


class InvalidAgentStateTransitionError(Exception):
    """Raised when an illegal agent state transition is attempted."""
    pass


@dataclass
class Agent:
    id: str
    name: str
    state: AgentState = AgentState.OFFLINE
    current_call_id: Optional[str] = None
    locked_by_worker_id: Optional[str] = None
    lease_expires_at: Optional[float] = None
    version: int = 1
    updated_at: float = field(default_factory=time.time)

    def can_transition_to(self, new_state: AgentState) -> bool:
        if self.state == new_state:
            return True  # Idempotent no-op
        return new_state in VALID_AGENT_TRANSITIONS.get(self.state, set())

    def transition_to(self, new_state: AgentState, worker_id: Optional[str] = None) -> None:
        if self.state == new_state:
            return
        if not self.can_transition_to(new_state):
            raise InvalidAgentStateTransitionError(
                f"Agent {self.id} cannot transition from {self.state.value} to {new_state.value}"
            )
        self.state = new_state
        self.version += 1
        self.updated_at = time.time()
        if new_state in (AgentState.AVAILABLE, AgentState.OFFLINE, AgentState.PAUSED):
            self.current_call_id = None
            self.locked_by_worker_id = None
            self.lease_expires_at = None
        elif worker_id:
            self.locked_by_worker_id = worker_id
