import pytest
from smart_dialer.models.agent import Agent, AgentState, InvalidAgentStateTransitionError
from smart_dialer.models.call import Call, CallState, InvalidCallStateTransitionError


def test_agent_state_valid_lifecycle():
    agent = Agent(id="agt_1", name="Agent 1", state=AgentState.OFFLINE)
    agent.transition_to(AgentState.AVAILABLE)
    assert agent.state == AgentState.AVAILABLE

    agent.transition_to(AgentState.RESERVED, worker_id="w1")
    assert agent.state == AgentState.RESERVED
    assert agent.locked_by_worker_id == "w1"

    agent.transition_to(AgentState.DIALING)
    assert agent.state == AgentState.DIALING

    agent.transition_to(AgentState.CONNECTED)
    assert agent.state == AgentState.CONNECTED

    agent.transition_to(AgentState.WRAP_UP)
    assert agent.state == AgentState.WRAP_UP

    agent.transition_to(AgentState.AVAILABLE)
    assert agent.state == AgentState.AVAILABLE
    assert agent.locked_by_worker_id is None


def test_agent_invalid_transition_raises():
    agent = Agent(id="agt_2", name="Agent 2", state=AgentState.OFFLINE)
    with pytest.raises(InvalidAgentStateTransitionError):
        # Cannot jump from OFFLINE directly to CONNECTED
        agent.transition_to(AgentState.CONNECTED)


def test_call_lifecycle_and_idempotency():
    call = Call(
        id="c1",
        campaign_id="camp1",
        borrower_id="b1",
        phone_number="+15550001",
        state=CallState.INITIATED,
    )

    # First RINGING event
    assert call.apply_event("evt_1", CallState.RINGING) is True
    assert call.state == CallState.RINGING

    # Duplicate RINGING event with same event_id
    assert call.apply_event("evt_1", CallState.RINGING) is False
    assert call.state == CallState.RINGING

    # ANSWERED event
    assert call.apply_event("evt_2", CallState.ANSWERED) is True
    assert call.state == CallState.ANSWERED

    # CONNECTED event
    assert call.apply_event("evt_3", CallState.CONNECTED) is True
    assert call.state == CallState.CONNECTED

    # COMPLETED event
    assert call.apply_event("evt_4", CallState.COMPLETED) is True
    assert call.state == CallState.COMPLETED


def test_call_out_of_order_resilience():
    """
    Simulates telecom provider sending COMPLETED before ANSWERED,
    or late ANSWERED after call has already COMPLETED.
    """
    call = Call(
        id="c2",
        campaign_id="camp1",
        borrower_id="b2",
        phone_number="+15550002",
        state=CallState.INITIATED,
    )

    # Telecom skips directly to COMPLETED (e.g. quick hangup webhook race)
    assert call.apply_event("evt_comp", CallState.COMPLETED) is True
    assert call.state == CallState.COMPLETED
    assert call.is_terminal is True

    # Late ANSWERED arriving out-of-order after COMPLETED
    applied = call.apply_event("evt_late_ans", CallState.ANSWERED)
    assert applied is False
    # State MUST remain COMPLETED (monotonic progression)
    assert call.state == CallState.COMPLETED
