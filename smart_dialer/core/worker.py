from __future__ import annotations
import uuid
import time
import asyncio
import logging
from typing import Optional, List, Dict
from ..storage.base import StorageInterface
from ..models.agent import AgentState
from ..models.call import Call, CallState
from ..models.borrower import BorrowerStatus
from ..safety.safety_controller import SafetyPass
from ..telecom.provider import TelecomProvider

logger = logging.getLogger(__name__)


class DistributedDialerWorker:
    """
    Represents an independent worker node (Worker 1 .. Worker N) in a distributed cluster.
    Competes with other workers for available agents and borrowers using atomic CAS.
    """

    def __init__(
        self,
        worker_id: str,
        campaign_id: str,
        storage: StorageInterface,
        provider: TelecomProvider,
        lease_duration_sec: float = 5.0,
    ):
        self.worker_id = worker_id
        self.campaign_id = campaign_id
        self.storage = storage
        self.provider = provider
        self.lease_duration_sec = lease_duration_sec
        self.is_alive = True
        self.reserved_agents: List[str] = []
        self.reserved_borrowers: List[str] = []

    def simulate_crash(self) -> None:
        """
        Simulates an abrupt worker death (SIGKILL / power loss).
        Leaves any held agent and borrower locks unreleased.
        """
        logger.warning(f"Worker {self.worker_id} crashed abruptly!")
        self.is_alive = False

    async def execute_pacing_cycle(
        self, safety_pass: SafetyPass, simulated_answer: bool = True
    ) -> List[Call]:
        if not self.is_alive:
            raise RuntimeError(f"Worker {self.worker_id} is dead/crashed.")

        if not safety_pass.is_valid():
            return []

        allocated_calls: List[Call] = []
        for _ in range(safety_pass.authorized_calls):
            # 1. Atomic reservation of next borrower
            borrower = self.storage.reserve_next_borrower_atomic(
                self.campaign_id, self.worker_id, self.lease_duration_sec
            )
            if not borrower:
                break
            self.reserved_borrowers.append(borrower.id)

            # 2. Atomic reservation of available agent
            agent_cand = None
            avail = self.storage.list_available_agents(limit=5)
            for a in avail:
                if self.storage.reserve_agent_atomic(a.id, self.worker_id, a.version, self.lease_duration_sec):
                    agent_cand = a
                    self.reserved_agents.append(a.id)
                    break

            # 3. Create call record
            call_id = f"call_{uuid.uuid4().hex[:8]}"
            call = Call(
                id=call_id,
                campaign_id=self.campaign_id,
                borrower_id=borrower.id,
                phone_number=borrower.phone_number,
                agent_id=agent_cand.id if agent_cand else None,
                provider_name=self.provider.name,
                state=CallState.INITIATED,
                initiated_at=time.time(),
            )
            self.storage.save_call(call)

            if agent_cand:
                self.storage.update_agent_state(agent_cand.id, AgentState.DIALING, current_call_id=call_id)

            # Check if crashed before telecom handshake
            if not self.is_alive:
                logger.warning(f"Worker {self.worker_id} died mid-flight after reserving!")
                return allocated_calls

            # 4. Initiate with telecom provider
            res = await self.provider.initiate_call(call_id, borrower.phone_number, simulated_answer=simulated_answer)
            if res.success:
                call.provider_call_id = res.provider_call_id
                self.storage.save_call(call)
                allocated_calls.append(call)

        return allocated_calls


class LeaseReconciliationService:
    """
    Background supervisor sweeper.
    Scans for expired leases from crashed or stalled workers and safely restores state.
    """

    def __init__(self, storage: StorageInterface):
        self.storage = storage

    def run_reconciliation(self, now: Optional[float] = None) -> Dict[str, int]:
        current_time = now if now is not None else time.time()
        results = self.storage.recover_expired_leases(current_time)
        if results.get("agents", 0) > 0 or results.get("borrowers", 0) > 0:
            logger.info(f"Lease reconciliation recovered: {results}")
        return results
