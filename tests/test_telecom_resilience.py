import pytest
import asyncio
from smart_dialer.telecom.provider import TelecomWebhookEvent, TelecomEventType
from smart_dialer.telecom.mock_provider_a import MockProviderA
from smart_dialer.telecom.mock_provider_b import MockProviderB
from smart_dialer.telecom.circuit_breaker import TelecomCircuitBreaker, CircuitState
from smart_dialer.storage.sqlite_store import SQLiteStorage
from smart_dialer.models.call import Call, CallState
from smart_dialer.models.borrower import Borrower, BorrowerStatus
from smart_dialer.core.event_handler import ResilientEventHandler


@pytest.mark.asyncio
async def test_provider_a_clean_lifecycle():
    provider = MockProviderA(failure_rate=0.0, min_latency=0.001, max_latency=0.002)
    events_received = []

    async def on_event(ev: TelecomWebhookEvent):
        events_received.append(ev.event_type)

    provider.register_webhook_handler(on_event)
    res = await provider.initiate_call("call_test_a", "+15551234", simulated_answer=True, talk_time_sec=0.05)
    assert res.success is True

    await asyncio.sleep(0.35)
    assert TelecomEventType.RINGING in events_received
    assert TelecomEventType.ANSWERED in events_received
    assert TelecomEventType.COMPLETED in events_received


@pytest.mark.asyncio
async def test_duplicate_event_handling_idempotency():
    storage = SQLiteStorage(":memory:")
    from smart_dialer.models.agent import Agent, AgentState
    storage.save_agent(Agent(id="agt_dup_test", name="Agent Dup", state=AgentState.AVAILABLE))

    call = Call("c_dup", "camp1", "b1", "+15559999", agent_id="agt_dup_test", state=CallState.INITIATED)
    storage.save_call(call)
    handler = ResilientEventHandler(storage)

    event = TelecomWebhookEvent(
        event_id="evt_same_123",
        call_id="c_dup",
        provider_name="ProviderB",
        event_type=TelecomEventType.ANSWERED,
    )

    # First event application
    await handler.handle_webhook(event)
    call_after_1 = storage.get_call("c_dup")
    assert call_after_1.state in (CallState.ANSWERED, CallState.CONNECTED)

    # Duplicate identical event application
    await handler.handle_webhook(event)
    assert handler.duplicate_events_ignored == 1
    call_after_2 = storage.get_call("c_dup")
    assert call_after_2.state == call_after_1.state


def test_circuit_breaker_transitions():
    cb = TelecomCircuitBreaker(failure_threshold=3, recovery_time_sec=0.1, success_threshold_to_close=2)
    assert cb.state == CircuitState.CLOSED
    assert cb.allow_call() is True

    # 3 failures trip circuit
    cb.record_failure()
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitState.OPEN
    assert cb.allow_call() is False

    # Wait recovery timeout
    import time
    time.sleep(0.12)
    # Probing in HALF_OPEN
    assert cb.allow_call() is True
    assert cb.state == CircuitState.HALF_OPEN

    # 2 successes restore to CLOSED
    cb.record_success()
    cb.record_success()
    assert cb.state == CircuitState.CLOSED
