from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
from ..models.agent import Agent, AgentState
from ..models.borrower import Borrower, BorrowerStatus
from ..models.call import Call, CallState


class StorageInterface(ABC):
    @abstractmethod
    def save_agent(self, agent: Agent) -> None:
        pass

    @abstractmethod
    def get_agent(self, agent_id: str) -> Optional[Agent]:
        pass

    @abstractmethod
    def list_available_agents(self, limit: int = 100) -> List[Agent]:
        pass

    @abstractmethod
    def reserve_agent_atomic(
        self, agent_id: str, worker_id: str, expected_version: int, lease_duration_sec: float
    ) -> bool:
        """Atomic Compare-And-Swap (CAS) to reserve an agent without race conditions."""
        pass

    @abstractmethod
    def update_agent_state(
        self, agent_id: str, new_state: AgentState, current_call_id: Optional[str] = None
    ) -> bool:
        pass

    @abstractmethod
    def save_borrower(self, borrower: Borrower) -> None:
        pass

    @abstractmethod
    def get_borrower(self, borrower_id: str) -> Optional[Borrower]:
        pass

    @abstractmethod
    def reserve_next_borrower_atomic(
        self, campaign_id: str, worker_id: str, lease_duration_sec: float
    ) -> Optional[Borrower]:
        pass

    @abstractmethod
    def update_borrower_status(self, borrower_id: str, status: BorrowerStatus) -> bool:
        pass

    @abstractmethod
    def save_call(self, call: Call) -> None:
        pass

    @abstractmethod
    def get_call(self, call_id: str) -> Optional[Call]:
        pass

    @abstractmethod
    def list_active_calls(self, campaign_id: Optional[str] = None) -> List[Call]:
        pass

    @abstractmethod
    def record_call_event_idempotent(
        self, call_id: str, event_id: str, new_state: CallState, note: Optional[str] = None
    ) -> bool:
        pass

    @abstractmethod
    def recover_expired_leases(self, now: float) -> Dict[str, int]:
        """Reconcile orphaned agents, borrowers, and calls from crashed workers."""
        pass
