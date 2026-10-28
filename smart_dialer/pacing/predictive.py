from __future__ import annotations
import math
from typing import Optional
from .progressive import PacingProposal


class PredictivePacingEngine:
    """
    Predictive Pacing Engine:
    Calculates aggressive outbound dialing over-ratio based on:
    - Current available agents (A)
    - Connected agents expected to become free during call setup (R)
    - Historical and recent borrower answer rate (p)
    - Call setup latency / ring time (t_setup) vs average call duration (T_talk)

    Formula:
      Expected capacity at pickup = A + (Connected_Agents * (t_setup / T_talk))
      Unchecked Proposed Dials = Expected capacity / p
    """

    def __init__(
        self,
        default_answer_rate: float = 0.50,
        avg_setup_time_sec: float = 15.0,
        default_talk_time_sec: float = 90.0,
        ema_alpha: float = 0.2,
    ):
        self.name = "PredictivePacing"
        self.current_answer_rate = default_answer_rate
        self.avg_setup_time_sec = avg_setup_time_sec
        self.avg_talk_time_sec = default_talk_time_sec
        self.ema_alpha = ema_alpha

    def update_metrics(self, answered: bool, talk_time_sec: Optional[float] = None) -> None:
        """Update rolling stats using Exponential Moving Average."""
        obs_rate = 1.0 if answered else 0.0
        self.current_answer_rate = (
            self.ema_alpha * obs_rate + (1 - self.ema_alpha) * self.current_answer_rate
        )
        self.current_answer_rate = max(0.05, min(0.95, self.current_answer_rate))

        if talk_time_sec and talk_time_sec > 0:
            self.avg_talk_time_sec = (
                self.ema_alpha * talk_time_sec + (1 - self.ema_alpha) * self.avg_talk_time_sec
            )

    def calculate_pacing(
        self,
        available_agents: int,
        connected_agents: int,
        in_flight_calls: int,
        pending_borrowers: int,
        override_answer_rate: Optional[float] = None,
    ) -> PacingProposal:
        p = override_answer_rate if override_answer_rate is not None else self.current_answer_rate
        p = max(0.05, min(1.0, p))

        # Expected agents to wrap up during call setup window
        expected_freed_agents = connected_agents * (self.avg_setup_time_sec / max(1.0, self.avg_talk_time_sec))

        # Total expected capacity when calls connect
        total_expected_capacity = available_agents + expected_freed_agents

        # Net capacity accounting for existing calls already in flight
        expected_in_flight_answers = in_flight_calls * p
        net_expected_capacity = max(0.0, total_expected_capacity - expected_in_flight_answers)

        # Raw suggested outbound calls: Capacity / p
        raw_suggested = int(math.floor(net_expected_capacity / p))
        suggested = min(raw_suggested, pending_borrowers)
        suggested = max(0, suggested)

        expected_conn = int(round(suggested * p))
        reason = (
            f"Predictive dial calculation: AnswerRate={p:.1%}, "
            f"AvailableAgents={available_agents}, ConnectedAgents={connected_agents}, "
            f"ExpectedFreed={expected_freed_agents:.1f}, InFlight={in_flight_calls}. "
            f"Calculated over-dial ratio={1.0/p:.2f}x to target {expected_conn} answers."
        )

        return PacingProposal(
            engine_name=self.name,
            suggested_calls=suggested,
            reason=reason,
            target_answer_rate=p,
            expected_connections=expected_conn,
        )
