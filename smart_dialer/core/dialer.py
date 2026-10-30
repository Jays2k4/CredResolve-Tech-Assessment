# WHY THIS DESIGN:
# The SmartDialer pipeline enforces the core architectural invariant:
# Campaign -> Pacing Engine -> Safety Controller -> Call Allocator -> Telecom Provider.
# The CallAllocator physically refuses to initiate dials unless presented with an authentic
# SafetyPass token issued by the SafetyController.
# This prevents race conditions and architectural bypasses, ensuring predictive dialing
# can never accidentally dial without safety approval.
from __future__ import annotations
import uuid
import time
import asyncio
import logging
from enum import Enum
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from ..models.agent import Agent, AgentState
from ..models.call import Call, CallState
from ..models.borrower import Borrower, BorrowerStatus
from ..storage.base import StorageInterface
from ..telecom.provider import TelecomProvider, CallInitiateResult
from ..telecom.circuit_breaker import TelecomCircuitBreaker
from ..pacing.progressive import ProgressivePacingEngine, PacingProposal
from ..pacing.predictive import PredictivePacingEngine
from ..safety.safety_controller import SafetyController, SafetyPass, SafetyDecision, SafetyBypassProhibitedError
from .event_handler import ResilientEventHandler

logger = logging.getLogger(__name__)


class DialerMode(str, Enum):
    PROGRESSIVE = "PROGRESSIVE"
    PREDICTIVE = "PREDICTIVE"


@dataclass
class TickDiagnostics:
    cycle: int
    mode: str
    available_agents: int
    connected_agents: int
    in_flight_calls: int
    pending_borrowers: int
    estimated_answer_rate: float
    expected_connections: int
    pacing_proposed_calls: int
    pacing_reason: str
    safety_decision: str
    safety_authorized_calls: int
    safety_reason: str
    dispatched_calls: int


class CallAllocator:
    """
    Subcomponent responsible for reserving borrower and agent records and dispatching to carrier.
    MANDATORY RULE: The Call Allocator MUST receive a valid SafetyPass.
    Attempting to dial without a SafetyPass immediately triggers SafetyBypassProhibitedError.
    """

    def __init__(
        self,
        storage: StorageInterface,
        provider: TelecomProvider,
        circuit_breaker: Optional[TelecomCircuitBreaker] = None,
        worker_id: str = "allocator_main",
    ):
        self.storage = storage
        self.provider = provider
        self.circuit_breaker = circuit_breaker
        self.worker_id = worker_id

    async def dispatch_authorized_calls(
        self,
        campaign_id: str,
        safety_pass: SafetyPass,
        simulated_answer_prob: float = 0.5,
        avg_talk_time_sec: float = 2.0,
    ) -> List[Call]:
        # Enforce Non-Bypassable Safety Check
        if not isinstance(safety_pass, SafetyPass):
            raise SafetyBypassProhibitedError("SECURITY VIOLATION: Execution attempted without an authentic SafetyPass!")

        if not safety_pass.is_valid():
            logger.info(f"SafetyPass {safety_pass.pass_id} has 0 authorized calls or has expired.")
            return []

        calls_to_make = safety_pass.authorized_calls
        dispatched_calls: List[Call] = []

        for _ in range(calls_to_make):
            # 1. Atomically reserve next borrower
            borrower = self.storage.reserve_next_borrower_atomic(campaign_id, self.worker_id, 30.0)
            if not borrower:
                break  # No more pending borrowers

            # 2. Check if agent should be reserved ahead (Progressive) or on-answer (Predictive)
            assigned_agent_id: Optional[str] = None
            available_agents = self.storage.list_available_agents(limit=1)
            if available_agents:
                candidate = available_agents[0]
                if self.storage.reserve_agent_atomic(candidate.id, self.worker_id, candidate.version, 30.0):
                    assigned_agent_id = candidate.id

            call_id = f"call_{uuid.uuid4().hex[:10]}"
            new_call = Call(
                id=call_id,
                campaign_id=campaign_id,
                borrower_id=borrower.id,
                phone_number=borrower.phone_number,
                agent_id=assigned_agent_id,
                provider_name=self.provider.name,
                state=CallState.INITIATED,
                initiated_at=time.time(),
            )
            self.storage.save_call(new_call)

            if assigned_agent_id:
                self.storage.update_agent_state(assigned_agent_id, AgentState.DIALING, current_call_id=call_id)

            # 3. Hand off to telecom provider
            import random
            will_answer = random.random() < simulated_answer_prob
            res: CallInitiateResult = await self.provider.initiate_call(
                call_id=call_id,
                phone_number=borrower.phone_number,
                simulated_answer=will_answer,
                talk_time_sec=avg_talk_time_sec,
            )

            if res.success:
                new_call.provider_call_id = res.provider_call_id
                self.storage.save_call(new_call)
                dispatched_calls.append(new_call)
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()
            else:
                # Carrier rejected call
                if self.circuit_breaker:
                    self.circuit_breaker.record_failure()
                self.storage.record_call_event_idempotent(
                    call_id, f"fail_init_{uuid.uuid4().hex[:6]}", CallState.FAILED, res.error_message
                )
                if assigned_agent_id:
                    self.storage.update_agent_state(assigned_agent_id, AgentState.AVAILABLE)
                self.storage.update_borrower_status(borrower.id, BorrowerStatus.PENDING)

        return dispatched_calls


class SmartDialer:
    """
    Complete SmartDialer pipeline:
    Campaign -> Pacing Engine -> Safety Controller -> Call Allocator -> Telecom Provider
    """

    def __init__(
        self,
        campaign_id: str,
        storage: StorageInterface,
        provider: TelecomProvider,
        mode: DialerMode = DialerMode.PROGRESSIVE,
        max_multiplier_ceiling: float = 3.0,
        enforce_worst_case_cap: bool = True,
    ):
        self.campaign_id = campaign_id
        self.storage = storage
        self.provider = provider
        self.mode = mode
        self.enforce_worst_case_cap = enforce_worst_case_cap

        self.circuit_breaker = TelecomCircuitBreaker(failure_threshold=5, recovery_time_sec=5.0)
        self.event_handler = ResilientEventHandler(storage, self.circuit_breaker)
        self.provider.register_webhook_handler(self.event_handler.handle_webhook)

        self.progressive_pacing = ProgressivePacingEngine()
        self.predictive_pacing = PredictivePacingEngine()
        self.safety_controller = SafetyController(
            max_multiplier_ceiling=max_multiplier_ceiling,
            enforce_worst_case_cap=enforce_worst_case_cap,
        )
        self.call_allocator = CallAllocator(storage, provider, circuit_breaker=self.circuit_breaker)

        self.total_cycles = 0
        self.total_calls_initiated = 0
        self.history_proposals: List[PacingProposal] = []
        self.history_passes: List[SafetyPass] = []
        self.last_tick_diagnostics: Optional[TickDiagnostics] = None
        self._step_lock = asyncio.Lock()

    async def step(
        self,
        override_answer_rate: Optional[float] = None,
        avg_talk_time_sec: float = 2.0,
    ) -> SafetyPass:
        """
        Executes one full iteration of the dialer loop.
        """
        async with self._step_lock:
            self.total_cycles += 1

            # 1. Inspect live system capacity
            agent_counts = self.storage.count_agents_by_state()
            available_agents = agent_counts.get(AgentState.AVAILABLE, 0)
            connected_agents = agent_counts.get(AgentState.CONNECTED, 0)

            active_calls = self.storage.list_active_calls(self.campaign_id)
            in_flight_calls = sum(1 for c in active_calls if c.state in (CallState.INITIATED, CallState.RINGING))
            pending_borrowers = self.storage.count_pending_borrowers(self.campaign_id)

            # 2. Pacing Engine proposals
            if self.mode == DialerMode.PROGRESSIVE:
                proposal = self.progressive_pacing.calculate_pacing(
                    available_agents=available_agents,
                    in_flight_calls=in_flight_calls,
                    pending_borrowers=pending_borrowers,
                    recent_answer_rate=override_answer_rate or 0.5,
                )
            else:
                proposal = self.predictive_pacing.calculate_pacing(
                    available_agents=available_agents,
                    connected_agents=connected_agents,
                    in_flight_calls=in_flight_calls,
                    pending_borrowers=pending_borrowers,
                    override_answer_rate=override_answer_rate,
                )

            total_active_workforce = sum(
                agent_counts.get(s, 0)
                for s in (AgentState.AVAILABLE, AgentState.CONNECTED, AgentState.DIALING, AgentState.RESERVED, AgentState.WRAP_UP)
            )

            # Count only agents whose release is bounded by an enforced rule:
            # specifically, agents in WRAP_UP with a hard wrap-up deadline shorter than setup window.
            # Calls reaching average talk time are soft estimates and must NOT be in guaranteed_freed.
            wrap_up_agents = agent_counts.get(AgentState.WRAP_UP, 0)
            guaranteed_freed = wrap_up_agents

            # 3. SAFETY CONTROLLER: The Non-Bypassable Boundary
            safety_pass = self.safety_controller.evaluate(
                proposal=proposal,
                available_agents=available_agents,
                connected_agents=connected_agents,
                in_flight_calls=in_flight_calls,
                circuit_breaker=self.circuit_breaker,
                total_workforce=total_active_workforce,
                guaranteed_freed_agents=guaranteed_freed,
            )
            self.history_passes.append(safety_pass)

            # When workforce falls below in-flight calls (e.g. abrupt agent drop),
            # cancel surplus ringing calls immediately to protect against abandonment.
            if in_flight_calls > total_active_workforce:
                self.cancel_surplus_ringing_calls(target_capacity=total_active_workforce)

            # 4. Call Allocator Dispatches ONLY with SafetyPass
            dispatched_count = 0
            if safety_pass.authorized_calls > 0:
                effective_answer_prob = override_answer_rate if override_answer_rate is not None else 0.5
                dispatched = await self.call_allocator.dispatch_authorized_calls(
                    campaign_id=self.campaign_id,
                    safety_pass=safety_pass,
                    simulated_answer_prob=effective_answer_prob,
                    avg_talk_time_sec=avg_talk_time_sec,
                )
                dispatched_count = len(dispatched)
                self.total_calls_initiated += dispatched_count

            # Record human-readable explainable diagnostics for this tick
            self.last_tick_diagnostics = TickDiagnostics(
                cycle=self.total_cycles,
                mode=self.mode.value,
                available_agents=available_agents,
                connected_agents=connected_agents,
                in_flight_calls=in_flight_calls,
                pending_borrowers=pending_borrowers,
                estimated_answer_rate=proposal.target_answer_rate,
                expected_connections=proposal.expected_connections,
                pacing_proposed_calls=proposal.suggested_calls,
                pacing_reason=proposal.reason,
                safety_decision=safety_pass.decision.value,
                safety_authorized_calls=safety_pass.authorized_calls,
                safety_reason=safety_pass.reason,
                dispatched_calls=dispatched_count,
            )

            return safety_pass

    def cancel_surplus_ringing_calls(self, target_capacity: int) -> int:
        """
        When agent workforce drops below in-flight calls, cancels surplus
        ringing calls (NOT answered or connected ones) immediately to eliminate abandonment risk.
        Returns the number of cancelled ringing calls.
        """
        active_calls = self.storage.list_active_calls(self.campaign_id)
        ringing_calls = [c for c in active_calls if c.state in (CallState.INITIATED, CallState.RINGING)]
        in_flight_count = len(ringing_calls)
        surplus = max(0, in_flight_count - target_capacity)
        cancelled = 0
        for i in range(surplus):
            call_to_cancel = ringing_calls[i]
            event_id = f"evt_cancel_drop_{uuid.uuid4().hex[:8]}"
            self.storage.record_call_event_idempotent(
                call_to_cancel.id,
                event_id,
                CallState.CANCELLED,
                note="Cancelled surplus ringing call due to sudden agent workforce drop",
            )
            self.storage.update_borrower_status(call_to_cancel.borrower_id, BorrowerStatus.PENDING)
            cancelled += 1
        return cancelled

    def get_metrics(self) -> Dict[str, Any]:
        agent_counts = self.storage.count_agents_by_state()
        total_agents = sum(agent_counts.values())
        connected = agent_counts.get(AgentState.CONNECTED, 0)
        utilization = (connected / total_agents * 100.0) if total_agents > 0 else 0.0

        return {
            "mode": self.mode.value,
            "total_cycles": self.total_cycles,
            "agent_counts": {k.value: v for k, v in agent_counts.items()},
            "agent_utilization_pct": round(utilization, 2),
            "total_calls_initiated": self.total_calls_initiated,
            "abandoned_calls": self.event_handler.abandoned_calls_count,
            "duplicate_events_ignored": self.event_handler.duplicate_events_ignored,
            "circuit_breaker_state": self.circuit_breaker.state.value,
            "safety_decisions": {k.value: v for k, v in self.safety_controller.total_decisions.items()},
        }
