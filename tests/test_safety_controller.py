import pytest
from smart_dialer.safety.safety_controller import (
    SafetyController,
    SafetyDecision,
    SafetyPass,
    SafetyBypassProhibitedError,
)
from smart_dialer.pacing.progressive import PacingProposal
from smart_dialer.telecom.circuit_breaker import TelecomCircuitBreaker, CircuitState
from smart_dialer.core.dialer import CallAllocator
from smart_dialer.storage.sqlite_store import SQLiteStorage
from smart_dialer.telecom.mock_provider_a import MockProviderA


def test_safety_controller_approve_and_reduce():
    ctrl = SafetyController(max_multiplier_ceiling=3.0)

    # 1. Proposal within safe limit -> APPROVE
    proposal = PacingProposal("Predictive", suggested_calls=15, reason="test", target_answer_rate=0.5, expected_connections=7)
    s_pass = ctrl.evaluate(proposal, available_agents=10, connected_agents=5, in_flight_calls=2)
    assert s_pass.decision == SafetyDecision.APPROVE
    assert s_pass.authorized_calls == 15

    # 2. Over-aggressive proposal -> REDUCE
    aggressive_proposal = PacingProposal("Predictive", suggested_calls=50, reason="aggressive", target_answer_rate=0.2, expected_connections=10)
    # Safe ceiling: 10 * 3.0 = 30. in_flight = 5. Net headroom = 25.
    s_pass_reduced = ctrl.evaluate(aggressive_proposal, available_agents=10, connected_agents=5, in_flight_calls=5)
    assert s_pass_reduced.decision == SafetyDecision.REDUCE
    assert s_pass_reduced.authorized_calls == 25


def test_safety_controller_rejects_on_carrier_outage():
    ctrl = SafetyController()
    cb = TelecomCircuitBreaker()
    # Trip circuit breaker
    for _ in range(5):
        cb.record_failure()
    assert cb.state == CircuitState.OPEN

    proposal = PacingProposal("Predictive", suggested_calls=20, reason="test", target_answer_rate=0.5, expected_connections=10)
    s_pass = ctrl.evaluate(proposal, available_agents=10, connected_agents=5, in_flight_calls=0, circuit_breaker=cb)

    assert s_pass.decision == SafetyDecision.REJECT
    assert s_pass.authorized_calls == 0


def test_safety_controller_detects_sudden_agent_drop():
    ctrl = SafetyController(sudden_drop_pct_threshold=0.25)
    # Initial steady state: 100 agents
    prop1 = PacingProposal("Predictive", 20, "steady", 0.5, 10)
    ctrl.evaluate(prop1, available_agents=100, connected_agents=10, in_flight_calls=5)

    # Sudden agent drop: 100 -> 60 (40% drop)
    prop2 = PacingProposal("Predictive", 30, "aggressive", 0.5, 15)
    s_pass = ctrl.evaluate(prop2, available_agents=60, connected_agents=10, in_flight_calls=5)

    assert s_pass.decision == SafetyDecision.FALLBACK_TO_PROGRESSIVE
    # Clamped to progressive available slots: 60 - 5 = 55 (min with requested 30 = 30)
    assert s_pass.authorized_calls == 30


@pytest.mark.asyncio
async def test_call_allocator_enforces_safety_pass_non_bypassable():
    storage = SQLiteStorage(":memory:")
    provider = MockProviderA()
    allocator = CallAllocator(storage, provider)

    # Attempting to call without SafetyPass MUST raise SafetyBypassProhibitedError
    with pytest.raises(SafetyBypassProhibitedError):
        await allocator.dispatch_authorized_calls(
            campaign_id="camp1",
            safety_pass=None,  # type: ignore
        )
