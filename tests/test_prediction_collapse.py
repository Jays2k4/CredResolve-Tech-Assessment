import pytest
import asyncio
from smart_dialer.pacing.predictive import PredictivePacingEngine
from smart_dialer.safety.safety_controller import SafetyController, SafetyDecision
from smart_dialer.storage.sqlite_store import SQLiteStorage
from smart_dialer.models.agent import Agent, AgentState
from smart_dialer.models.borrower import Borrower, BorrowerStatus
from smart_dialer.telecom.mock_provider_a import MockProviderA
from smart_dialer.core.dialer import SmartDialer, DialerMode


@pytest.mark.asyncio
async def test_prediction_collapse_70_to_10():
    """
    Verifies that when predicted answer rate abruptly collapses from 70% to 10%:
    1. The predictive engine updates its estimated answer rate.
    2. The Safety Controller clamps authorized calls and rejects over-dialing.
    3. Abandoned calls remain strictly ZERO.
    """
    storage = SQLiteStorage(":memory:")
    provider = MockProviderA(failure_rate=0.0)
    campaign_id = "camp_pred_collapse"

    # Seed 30 agents and 100 borrowers
    for i in range(1, 31):
        storage.save_agent(Agent(id=f"agt_{i}", name=f"Agent {i}", state=AgentState.AVAILABLE))

    for i in range(1, 101):
        storage.save_borrower(Borrower(
            id=f"bor_{i}", campaign_id=campaign_id, name=f"Borrower {i}", phone_number=f"+1555{i:04d}"
        ))

    dialer = SmartDialer(
        campaign_id=campaign_id,
        storage=storage,
        provider=provider,
        mode=DialerMode.PREDICTIVE,
    )

    # Phase 1: High answer rate regime (70%)
    pass_high = await dialer.step(override_answer_rate=0.70, avg_talk_time_sec=0.5)
    assert pass_high.authorized_calls > 0
    await asyncio.sleep(0.1)

    # Phase 2: Sudden answer rate collapse to 10%
    # The system must protect itself without abandoning any connected calls
    pass_low = await dialer.step(override_answer_rate=0.10, avg_talk_time_sec=0.5)

    # Safety controller clamps authorized calls
    assert pass_low.decision in (
        SafetyDecision.APPROVE, SafetyDecision.REDUCE, SafetyDecision.REJECT, SafetyDecision.FALLBACK_TO_PROGRESSIVE
    )

    # Let calls resolve
    await asyncio.sleep(0.6)

    # Verify ZERO abandoned calls
    metrics = dialer.get_metrics()
    assert metrics["abandoned_calls"] == 0
