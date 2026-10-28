import pytest
from smart_dialer.pacing.progressive import ProgressivePacingEngine
from smart_dialer.pacing.predictive import PredictivePacingEngine


def test_progressive_pacing_strictly_one_to_one():
    engine = ProgressivePacingEngine()

    # 10 available agents, 2 in flight -> 8 open slots -> 8 suggested calls
    proposal = engine.calculate_pacing(
        available_agents=10,
        in_flight_calls=2,
        pending_borrowers=50,
        recent_answer_rate=0.5,
    )
    assert proposal.suggested_calls == 8

    # When pending borrowers is smaller than available agents
    proposal2 = engine.calculate_pacing(
        available_agents=10,
        in_flight_calls=0,
        pending_borrowers=3,
        recent_answer_rate=0.5,
    )
    assert proposal2.suggested_calls == 3


def test_predictive_pacing_calculation():
    engine = PredictivePacingEngine(
        default_answer_rate=0.20,  # 20% answer rate = 5x overdial
        avg_setup_time_sec=10.0,
        default_talk_time_sec=100.0,
    )

    # 10 available agents + 10 connected agents (1 expected to free up during setup) = 11 total capacity
    # 0 in-flight calls
    # 11 / 0.20 = 55 suggested dials
    proposal = engine.calculate_pacing(
        available_agents=10,
        connected_agents=10,
        in_flight_calls=0,
        pending_borrowers=200,
        override_answer_rate=0.20,
    )
    assert proposal.suggested_calls == 55
    assert proposal.expected_connections == 11


def test_predictive_pacing_ema_updates():
    engine = PredictivePacingEngine(default_answer_rate=0.50, ema_alpha=0.5)

    # Observe answered call
    engine.update_metrics(answered=True, talk_time_sec=120.0)
    # 0.5 * 1.0 + 0.5 * 0.5 = 0.75
    assert abs(engine.current_answer_rate - 0.75) < 1e-4
