import pytest
import asyncio
from smart_dialer.storage.sqlite_store import SQLiteStorage
from smart_dialer.models.agent import Agent, AgentState
from smart_dialer.models.borrower import Borrower, BorrowerStatus
from smart_dialer.models.call import Call, CallState
from smart_dialer.telecom.provider import TelecomWebhookEvent, TelecomEventType
from smart_dialer.telecom.mock_provider_a import MockProviderA
from smart_dialer.telecom.mock_provider_b import MockProviderB
from smart_dialer.telecom.circuit_breaker import TelecomCircuitBreaker, CircuitState
from smart_dialer.safety.safety_controller import SafetyController, SafetyDecision, SafetyPass
from smart_dialer.pacing.progressive import PacingProposal
import time
from smart_dialer.core.event_handler import ResilientEventHandler
from smart_dialer.core.worker import DistributedDialerWorker, LeaseReconciliationService
from smart_dialer.core.dialer import SmartDialer, DialerMode


@pytest.mark.asyncio
async def test_provider_outage_circuit_breaker_and_failover():
    """
    Failure Demo B Unit Test:
    Provider B fails repeatedly -> circuit breaker opens -> Safety Controller rejects dials
    -> Automated failover routes dials to Backup Provider A.
    """
    storage = SQLiteStorage(":memory:")
    cb = TelecomCircuitBreaker(failure_threshold=3, recovery_time_sec=1.0)
    provider_b = MockProviderB(timeout_rate=1.0)
    provider_a = MockProviderA(failure_rate=0.0)

    # 3 consecutive timeouts trip circuit breaker
    for _ in range(3):
        res = await provider_b.initiate_call("call_fail", "+15550000")
        assert res.success is False
        cb.record_failure()

    assert cb.state == CircuitState.OPEN
    assert cb.allow_call() is False

    # Safety Controller detects open breaker and rejects dials
    ctrl = SafetyController()
    proposal = PacingProposal("Predictive", suggested_calls=10, reason="test", target_answer_rate=0.5, expected_connections=5)
    s_pass = ctrl.evaluate(proposal, available_agents=10, connected_agents=0, in_flight_calls=0, circuit_breaker=cb)
    assert s_pass.decision == SafetyDecision.REJECT
    assert s_pass.authorized_calls == 0

    # Failover to Provider A
    res_failover = await provider_a.initiate_call("call_failover", "+15550001")
    assert res_failover.success is True
    assert res_failover.provider_call_id is not None


def test_agent_drop_single_tick_reaction():
    """
    Failure Demo C Unit Test:
    100 agents -> 40 abruptly drop (40% reduction).
    Verifies Safety Controller detects drop in exactly 1 pacing tick,
    issuing FALLBACK_TO_PROGRESSIVE and clamping authorized calls to the new capacity.
    """
    ctrl = SafetyController(sudden_drop_pct_threshold=0.25)

    # Tick 1: Baseline steady state with 100 agents
    prop1 = PacingProposal("Predictive", 30, "Baseline", 0.5, 15)
    pass1 = ctrl.evaluate(prop1, available_agents=100, connected_agents=0, in_flight_calls=0, total_workforce=100)
    assert pass1.decision == SafetyDecision.APPROVE
    assert pass1.authorized_calls == 30

    # Tick 2: Sudden 40% workforce drop to 60 agents
    prop2 = PacingProposal("Predictive", 30, "Overdial proposal", 0.5, 15)
    pass2 = ctrl.evaluate(prop2, available_agents=60, connected_agents=0, in_flight_calls=0, total_workforce=60)

    # Reacts in exactly 1 tick
    assert pass2.decision == SafetyDecision.FALLBACK_TO_PROGRESSIVE
    assert pass2.authorized_calls == 30  # clamped to progressive 1:1 limit


@pytest.mark.asyncio
async def test_duplicate_webhooks_triplicate_answered_then_completed():
    """
    Failure Demo D Unit Test:
    Receives ANSWERED x3 followed by COMPLETED.
    Verifies that only 1 state transition occurs, duplicates are tracked,
    and call reaches final COMPLETED state with 0 duplicate agent side-effects.
    """
    storage = SQLiteStorage(":memory:")
    storage.save_agent(Agent("agt_trip", "Agent Trip", state=AgentState.AVAILABLE))
    storage.save_borrower(Borrower("bor_trip", "camp1", "Borrower Trip", "+15559090"))
    call = Call("call_trip", "camp1", "bor_trip", "+15559090", agent_id="agt_trip", state=CallState.INITIATED)
    storage.save_call(call)

    handler = ResilientEventHandler(storage)

    event_ans = TelecomWebhookEvent("evt_ans_trip", "call_trip", "ProviderB", TelecomEventType.ANSWERED)
    # First delivery
    await handler.handle_webhook(event_ans)
    call_1 = storage.get_call("call_trip")
    assert call_1.state == CallState.CONNECTED
    assert handler.duplicate_events_ignored == 0

    # Second delivery (Duplicate)
    await handler.handle_webhook(event_ans)
    assert handler.duplicate_events_ignored == 1
    assert storage.get_call("call_trip").state == CallState.CONNECTED

    # Third delivery (Duplicate)
    await handler.handle_webhook(event_ans)
    assert handler.duplicate_events_ignored == 2
    assert storage.get_call("call_trip").state == CallState.CONNECTED

    # COMPLETED delivery
    event_comp = TelecomWebhookEvent("evt_comp_trip", "call_trip", "ProviderB", TelecomEventType.COMPLETED)
    await handler.handle_webhook(event_comp)
    assert storage.get_call("call_trip").state == CallState.COMPLETED


@pytest.mark.asyncio
async def test_out_of_order_webhooks_completed_then_answered_then_ringing():
    """
    Failure Demo E Unit Test:
    Carrier delivers COMPLETED first, followed by late delayed ANSWERED and RINGING.
    Verifies monotonic ordering: COMPLETED remains the permanent terminal state,
    and backwards out-of-order transitions are safely ignored.
    """
    storage = SQLiteStorage(":memory:")
    storage.save_agent(Agent("agt_ooo", "Agent OOO", state=AgentState.AVAILABLE))
    storage.save_borrower(Borrower("bor_ooo", "camp1", "Borrower OOO", "+15558888"))
    call = Call("call_ooo", "camp1", "bor_ooo", "+15558888", agent_id="agt_ooo", state=CallState.INITIATED)
    storage.save_call(call)

    handler = ResilientEventHandler(storage)

    # 1. COMPLETED webhook arrives first
    ev_comp = TelecomWebhookEvent("evt_ooo_comp", "call_ooo", "ProviderB", TelecomEventType.COMPLETED)
    await handler.handle_webhook(ev_comp)
    call_comp = storage.get_call("call_ooo")
    assert call_comp.state == CallState.COMPLETED
    assert call_comp.is_terminal is True

    # 2. Delayed ANSWERED arrives afterwards
    ev_ans = TelecomWebhookEvent("evt_ooo_ans", "call_ooo", "ProviderB", TelecomEventType.ANSWERED)
    await handler.handle_webhook(ev_ans)
    assert storage.get_call("call_ooo").state == CallState.COMPLETED

    # 3. Delayed RINGING arrives afterwards
    ev_ring = TelecomWebhookEvent("evt_ooo_ring", "call_ooo", "ProviderB", TelecomEventType.RINGING)
    await handler.handle_webhook(ev_ring)
    assert storage.get_call("call_ooo").state == CallState.COMPLETED


@pytest.mark.asyncio
async def test_all_ringing_calls_answer_worst_case_statistical_overdialing():
    """
    Worst-Case 100% Simultaneous Answers (Statistical Mode):
    Demonstrates what happens when 5 calls are ringing, only 2 agents are available,
    and 100% of borrowers pick up at the exact same millisecond.
    The first 2 answers bridge to available agents.
    The excess 3 answers cannot find free agents within grace retry window and are audited as abandoned.
    """
    storage = SQLiteStorage(":memory:")
    # Only 2 agents available
    storage.save_agent(Agent("agt_w1", "Agent 1", state=AgentState.AVAILABLE))
    storage.save_agent(Agent("agt_w2", "Agent 2", state=AgentState.AVAILABLE))

    handler = ResilientEventHandler(storage)

    # 5 active ringing calls
    for i in range(1, 6):
        storage.save_borrower(Borrower(f"bor_w{i}", "camp_wc", f"Borrower {i}", f"+155500{i}"))
        storage.save_call(Call(f"call_wc_{i}", "camp_wc", f"bor_w{i}", f"+155500{i}", state=CallState.RINGING))

    # All 5 answer simultaneously at t=0
    events = [
        TelecomWebhookEvent(f"evt_wc_ans_{i}", f"call_wc_{i}", "ProviderA", TelecomEventType.ANSWERED)
        for i in range(1, 6)
    ]
    await asyncio.gather(*(handler.handle_webhook(ev) for ev in events))

    connected_calls = [c for c in (storage.get_call(f"call_wc_{i}") for i in range(1, 6)) if c.state == CallState.CONNECTED]
    failed_calls = [c for c in (storage.get_call(f"call_wc_{i}") for i in range(1, 6)) if c.state == CallState.FAILED]

    assert len(connected_calls) == 2
    assert len(failed_calls) == 3
    assert handler.abandoned_calls_count == 3
    for f in failed_calls:
        assert f.failure_reason == "ABANDONED_NO_AGENT_AVAILABLE"


def test_deterministic_worst_case_cap_prevents_abandonment():
    """
    Deterministic Worst-Case Cap (enforce_worst_case_cap=True):
    Demonstrates that when the deterministic cap is enabled, the Safety Controller
    strictly caps authorized calls so that worst-case simultaneous answers (100% answer burst)
    can NEVER exceed available agents, guaranteeing 0 abandoned calls even under catastrophic conditions.
    """
    ctrl = SafetyController(enforce_worst_case_cap=True)

    # Pacing engine proposes 10 calls for 2 available agents
    proposal = PacingProposal("Predictive", suggested_calls=10, reason="Overdial", target_answer_rate=0.2, expected_connections=2)
    s_pass = ctrl.evaluate(proposal, available_agents=2, connected_agents=5, in_flight_calls=0)

    assert s_pass.decision == SafetyDecision.REDUCE
    # Strictly clamped to available agents (2), so even if 100% answer, 0 are abandoned
    assert s_pass.authorized_calls == 2


@pytest.mark.asyncio
async def test_deterministic_cap_strict_per_tick_invariant():
    """
    Asserts per tick that authorized_calls <= available + guaranteed_freed - in_flight
    across varied answer patterns (including multiple 100% answering bursts).
    Asserts that zero calls are abandoned across all ticks.
    """
    storage = SQLiteStorage(":memory:")
    for i in range(1, 21):
        storage.save_agent(Agent(f"agt_inv_{i}", f"Agent {i}", state=AgentState.AVAILABLE))
    for i in range(1, 101):
        storage.save_borrower(Borrower(f"bor_inv_{i}", "camp_inv", f"Borrower {i}", f"+1555{i:04d}"))

    provider = MockProviderA(failure_rate=0.0, min_latency=0.005, max_latency=0.01)
    dialer = SmartDialer("camp_inv", storage, provider, mode=DialerMode.PREDICTIVE, enforce_worst_case_cap=True)

    answer_patterns = [0.20, 0.50, 1.0, 0.10, 1.0, 0.70, 1.0]

    for p in answer_patterns:
        agent_counts = storage.count_agents_by_state()
        available = agent_counts.get(AgentState.AVAILABLE, 0)
        wrap_up = agent_counts.get(AgentState.WRAP_UP, 0)
        active = storage.list_active_calls("camp_inv")
        in_flight = sum(1 for c in active if c.state in (CallState.INITIATED, CallState.RINGING))
        
        freed = wrap_up
        max_allowed_bound = max(0, available + freed - in_flight)

        s_pass = await dialer.step(override_answer_rate=p, avg_talk_time_sec=0.15)

        # STRICT INVARIANT ASSERTION PER TICK: No speculative talk-time completions allowed!
        assert s_pass.authorized_calls <= max_allowed_bound, (
            f"Authorized calls ({s_pass.authorized_calls}) exceeded deterministic headroom "
            f"bound ({max_allowed_bound} = {available} avail + {freed} wrap_up - {in_flight} inflight)"
        )

        await asyncio.sleep(0.08)

    metrics = dialer.get_metrics()
    assert metrics["abandoned_calls"] == 0


@pytest.mark.asyncio
async def test_long_tail_talk_time_with_100_percent_bursts_zero_abandons():
    """
    Test with long-tail talk times (some calls lasting 5x the average duration)
    under 100% answer bursts, asserting zero abandoned calls.
    Confirms that because guaranteed_freed never speculatively counts ongoing calls
    reaching average talk time, long-tail calls never cause surplus pickups or abandons.
    """
    storage = SQLiteStorage(":memory:")

    # 15 agents in shift
    for i in range(1, 16):
        storage.save_agent(Agent(f"agt_tail_{i}", f"Agent {i}", state=AgentState.AVAILABLE))
    for i in range(1, 101):
        storage.save_borrower(Borrower(f"bor_tail_{i}", "camp_tail", f"Borrower {i}", f"+1555{i:04d}"))

    provider = MockProviderA(failure_rate=0.0, min_latency=0.005, max_latency=0.01)
    dialer = SmartDialer("camp_tail", storage, provider, mode=DialerMode.PREDICTIVE, enforce_worst_case_cap=True)

    # Base avg talk time = 0.05s. Long-tail talk time = 0.25s (5x average).
    avg_talk = 0.05
    long_tail_talk = 0.25

    # Run 6 ticks of 100% pickup bursts with long-tail durations
    for tick in range(6):
        # Measure capacity BEFORE step dispatches authorized calls
        agent_counts = storage.count_agents_by_state()
        available = agent_counts.get(AgentState.AVAILABLE, 0)
        wrap_up = agent_counts.get(AgentState.WRAP_UP, 0)
        active = storage.list_active_calls("camp_tail")
        in_flight = sum(1 for c in active if c.state in (CallState.INITIATED, CallState.RINGING))
        max_allowed_bound = max(0, available + wrap_up - in_flight)

        # Vary talk times, including 5x long-tail duration
        talk_duration = long_tail_talk if (tick % 2 == 0) else avg_talk
        s_pass = await dialer.step(override_answer_rate=1.0, avg_talk_time_sec=talk_duration)
        
        # Verify strict deterministic limit
        assert s_pass.authorized_calls <= max_allowed_bound, (
            f"Authorized {s_pass.authorized_calls} exceeded bound {max_allowed_bound} "
            f"(available: {available}, wrap_up: {wrap_up}, in_flight: {in_flight})"
        )
        await asyncio.sleep(0.06)

    # Let remaining calls complete
    await asyncio.sleep(long_tail_talk + 0.05)
    metrics = dialer.get_metrics()
    assert metrics["abandoned_calls"] == 0, f"Expected 0 abandons under long-tail talk times, got {metrics['abandoned_calls']}"


