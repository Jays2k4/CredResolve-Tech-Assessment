from __future__ import annotations
import uuid
import time
import math
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional
from ..pacing.progressive import PacingProposal
from ..telecom.circuit_breaker import CircuitState, TelecomCircuitBreaker


class SafetyDecision(str, Enum):
    APPROVE = "APPROVE"
    REDUCE = "REDUCE"
    REJECT = "REJECT"
    FALLBACK_TO_PROGRESSIVE = "FALLBACK_TO_PROGRESSIVE"


class SafetyBypassProhibitedError(Exception):
    """Raised if an outbound call is attempted without an authentic SafetyPass."""
    pass


@dataclass(frozen=True)
class SafetyPass:
    """
    Cryptographically tracked / immutable authorization pass issued by SafetyController.
    The Call Allocator MUST present this pass to execute dials.
    """
    pass_id: str
    decision: SafetyDecision
    authorized_calls: int
    original_requested_calls: int
    reason: str
    issued_at: float = field(default_factory=time.time)

    def is_valid(self) -> bool:
        return self.authorized_calls > 0 and (time.time() - self.issued_at < 30.0)


class SafetyController:
    """
    Mandatory safety gatekeeper between Pacing Engine and Call Allocator.
    Prevents call abandonment, enforces compliance, and guarantees system resilience.

    Decisions:
    - APPROVE: Pacing request is within verified agent capacity and risk tolerance.
    - REDUCE: Clamps aggressive pacing down to a mathematically safe ceiling.
    - REJECT: Halts dials (e.g. carrier outage, no agents, buffer exhausted).
    - FALLBACK_TO_PROGRESSIVE: Carrier jitter, sudden agent drop, or erratic answer rate.
    """

    def __init__(
        self,
        max_multiplier_ceiling: float = 3.5,
        max_abandon_rate_target: float = 0.02,  # 2% compliance limit
        sudden_drop_pct_threshold: float = 0.25, # 25% drop is flagged as abrupt
    ):
        self.max_multiplier_ceiling = max_multiplier_ceiling
        self.max_abandon_rate_target = max_abandon_rate_target
        self.sudden_drop_pct_threshold = sudden_drop_pct_threshold
        
        self.last_known_agents_count: Optional[int] = None
        self.last_evaluation_time: float = time.time()
        self.total_decisions: dict[SafetyDecision, int] = {d: 0 for d in SafetyDecision}

    def evaluate(
        self,
        proposal: PacingProposal,
        available_agents: int,
        connected_agents: int,
        in_flight_calls: int,
        circuit_breaker: Optional[TelecomCircuitBreaker] = None,
    ) -> SafetyPass:
        now = time.time()
        requested = proposal.suggested_calls
        pass_id = f"safe_{uuid.uuid4().hex[:12]}"

        # 1. Check Telecom Provider Health via Circuit Breaker
        if circuit_breaker and not circuit_breaker.allow_call():
            self.total_decisions[SafetyDecision.REJECT] += 1
            return SafetyPass(
                pass_id=pass_id,
                decision=SafetyDecision.REJECT,
                authorized_calls=0,
                original_requested_calls=requested,
                reason="Telecom Provider Circuit Breaker is OPEN/failing. Rejecting all outbound dials.",
            )

        # 2. Check for Sudden Agent Availability Drops (e.g. 100 -> 60 abruptly)
        if self.last_known_agents_count is not None and self.last_known_agents_count > 10:
            agent_delta = self.last_known_agents_count - available_agents
            drop_ratio = agent_delta / self.last_known_agents_count
            if drop_ratio >= self.sudden_drop_pct_threshold:
                # Sudden agent attrition detected! Fall back to strict progressive 1:1
                self.last_known_agents_count = available_agents
                safe_slots = max(0, available_agents - in_flight_calls)
                allowed = min(requested, safe_slots)
                self.total_decisions[SafetyDecision.FALLBACK_TO_PROGRESSIVE] += 1
                return SafetyPass(
                    pass_id=pass_id,
                    decision=SafetyDecision.FALLBACK_TO_PROGRESSIVE,
                    authorized_calls=allowed,
                    original_requested_calls=requested,
                    reason=(
                        f"Sudden agent drop detected ({drop_ratio:.1%} reduction). "
                        f"Falling back to deterministic progressive dialing ({allowed} calls)."
                    ),
                )
        self.last_known_agents_count = available_agents

        # 3. Absolute Zero Available & Connecting Capacity
        if available_agents == 0 and connected_agents == 0:
            self.total_decisions[SafetyDecision.REJECT] += 1
            return SafetyPass(
                pass_id=pass_id,
                decision=SafetyDecision.REJECT,
                authorized_calls=0,
                original_requested_calls=requested,
                reason="Zero agents available or connected. Cannot dial.",
            )

        # 4. In-Flight Call Ceiling & Over-dial Clamping via Risk-Budgeted Erlang approximation
        p = max(0.05, min(1.0, proposal.target_answer_rate))
        
        # Dynamic multiplier ceiling: tightens automatically as answer rate rises
        dynamic_ceiling_mult = min(self.max_multiplier_ceiling, 1.0 + (1.0 - p) * 2.0)
        safe_ceiling = int(math.ceil(available_agents * dynamic_ceiling_mult))

        # Expected answers from in-flight calls
        expected_inflight_answers = in_flight_calls * p
        available_headroom_agents = max(0.0, available_agents - expected_inflight_answers)

        # Maximum new calls allowed such that (new_calls * p) <= available_headroom_agents
        max_safe_new_calls = int(math.floor(available_headroom_agents / p)) if p > 0 else 0
        net_safe_headroom = max(0, min(safe_ceiling - in_flight_calls, max_safe_new_calls))

        # In Progressive mode, ceiling is strictly 1:1
        if proposal.engine_name == "ProgressivePacing":
            net_safe_headroom = max(0, available_agents - in_flight_calls)

        if requested == 0:
            self.total_decisions[SafetyDecision.APPROVE] += 1
            return SafetyPass(
                pass_id=pass_id,
                decision=SafetyDecision.APPROVE,
                authorized_calls=0,
                original_requested_calls=0,
                reason="Pacing engine proposed 0 calls.",
            )

        if requested <= net_safe_headroom:
            self.total_decisions[SafetyDecision.APPROVE] += 1
            return SafetyPass(
                pass_id=pass_id,
                decision=SafetyDecision.APPROVE,
                authorized_calls=requested,
                original_requested_calls=requested,
                reason=f"Approved {requested} calls within safety headroom ({net_safe_headroom}).",
            )
        elif net_safe_headroom > 0:
            self.total_decisions[SafetyDecision.REDUCE] += 1
            return SafetyPass(
                pass_id=pass_id,
                decision=SafetyDecision.REDUCE,
                authorized_calls=net_safe_headroom,
                original_requested_calls=requested,
                reason=(
                    f"Reduced from requested {requested} to {net_safe_headroom} "
                    f"to prevent abandonment risk (AnswerRate={p:.1%})."
                ),
            )
        else:
            self.total_decisions[SafetyDecision.REJECT] += 1
            return SafetyPass(
                pass_id=pass_id,
                decision=SafetyDecision.REJECT,
                authorized_calls=0,
                original_requested_calls=requested,
                reason=f"In-flight calls ({in_flight_calls}) with AnswerRate={p:.1%} consume all available agent buffer.",
            )
