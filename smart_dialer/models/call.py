from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Set, List
import time


class CallState(str, Enum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    CONNECTED = "CONNECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


# Monotonic progression ranks for evaluating out-of-order events
CALL_STATE_PROGRESSION = {
    CallState.QUEUED: 10,
    CallState.RESERVED: 20,
    CallState.INITIATED: 30,
    CallState.RINGING: 40,
    CallState.ANSWERED: 50,
    CallState.CONNECTED: 60,
    CallState.COMPLETED: 100,  # Terminal
    CallState.FAILED: 100,     # Terminal
    CallState.CANCELLED: 100,  # Terminal
}

TERMINAL_CALL_STATES = {CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED}

VALID_CALL_TRANSITIONS: dict[CallState, Set[CallState]] = {
    CallState.QUEUED: {CallState.RESERVED, CallState.CANCELLED},
    CallState.RESERVED: {CallState.INITIATED, CallState.CANCELLED, CallState.FAILED},
    CallState.INITIATED: {
        CallState.RINGING,
        CallState.ANSWERED,  # Provider may skip RINGING webhook directly to ANSWERED
        CallState.FAILED,
        CallState.CANCELLED,
    },
    CallState.RINGING: {
        CallState.ANSWERED,
        CallState.FAILED,
        CallState.CANCELLED,
    },
    CallState.ANSWERED: {
        CallState.CONNECTED,
        CallState.FAILED,     # Abandoned before agent connect, or early hangup
        CallState.COMPLETED,  # Out of order or rapid borrower hangup
    },
    CallState.CONNECTED: {
        CallState.COMPLETED,
        CallState.FAILED,
    },
    CallState.COMPLETED: set(),  # Terminal
    CallState.FAILED: set(),     # Terminal
    CallState.CANCELLED: set(),  # Terminal
}


class InvalidCallStateTransitionError(Exception):
    """Raised when an illegal call state transition is attempted."""
    pass


@dataclass
class CallEventRecord:
    event_id: str
    state: CallState
    timestamp: float = field(default_factory=time.time)
    note: Optional[str] = None


@dataclass
class Call:
    id: str
    campaign_id: str
    borrower_id: str
    phone_number: str
    agent_id: Optional[str] = None
    provider_name: Optional[str] = None
    provider_call_id: Optional[str] = None
    state: CallState = CallState.QUEUED
    created_at: float = field(default_factory=time.time)
    initiated_at: Optional[float] = None
    answered_at: Optional[float] = None
    connected_at: Optional[float] = None
    ended_at: Optional[float] = None
    failure_reason: Optional[str] = None
    version: int = 1
    processed_event_ids: Set[str] = field(default_factory=set)
    history: List[CallEventRecord] = field(default_factory=list)

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_CALL_STATES

    def can_transition_to(self, new_state: CallState) -> bool:
        if self.state == new_state:
            return True  # Idempotent
        if self.is_terminal:
            return False  # Terminal states cannot be undone by stale events
        return new_state in VALID_CALL_TRANSITIONS.get(self.state, set())

    def apply_event(self, event_id: str, new_state: CallState, note: Optional[str] = None) -> bool:
        """
        Applies a state transition idempotently.
        Returns True if state was updated, False if skipped due to deduplication or out-of-order stale event.
        """
        # Deduplication
        if event_id in self.processed_event_ids:
            return False
        self.processed_event_ids.add(event_id)

        # Idempotent match
        if self.state == new_state:
            self.history.append(CallEventRecord(event_id, new_state, time.time(), f"Duplicate ignored: {note}"))
            return False

        # If call is already terminal, ignore subsequent active events (e.g. COMPLETED arrived before late ANSWERED/RINGING)
        if self.is_terminal:
            self.history.append(
                CallEventRecord(event_id, new_state, time.time(), f"Discarded out-of-order event for terminal call ({self.state.value})")
            )
            return False

        # If incoming event is regressive in progression rank (e.g. RINGING arriving after ANSWERED)
        if CALL_STATE_PROGRESSION[new_state] < CALL_STATE_PROGRESSION[self.state]:
            self.history.append(
                CallEventRecord(event_id, new_state, time.time(), f"Ignored regressive event {new_state.value} (current: {self.state.value})")
            )
            return False

        if not self.can_transition_to(new_state):
            # Allow skipping intermediary states if provider drops them (e.g., INITIATED -> ANSWERED)
            if CALL_STATE_PROGRESSION[new_state] > CALL_STATE_PROGRESSION[self.state]:
                pass
            else:
                raise InvalidCallStateTransitionError(
                    f"Call {self.id} cannot transition from {self.state.value} to {new_state.value}"
                )

        # State transition
        self.state = new_state
        self.version += 1
        now = time.time()
        if new_state == CallState.INITIATED:
            self.initiated_at = now
        elif new_state == CallState.ANSWERED:
            self.answered_at = now
        elif new_state == CallState.CONNECTED:
            self.connected_at = now
        elif new_state in TERMINAL_CALL_STATES:
            self.ended_at = now
            if new_state == CallState.FAILED and note:
                self.failure_reason = note

        self.history.append(CallEventRecord(event_id, new_state, now, note))
        return True
