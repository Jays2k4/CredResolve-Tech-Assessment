from __future__ import annotations
from dataclasses import dataclass


@dataclass
class PacingProposal:
    engine_name: str
    suggested_calls: int
    reason: str
    target_answer_rate: float
    expected_connections: int


class ProgressivePacingEngine:
    """
    Progressive Dialing:
    Deterministic 1 available agent -> 1 outbound call.
    Zero risk of call abandonment, but agents may sit idle if answer rates are low.
    """

    def __init__(self):
        self.name = "ProgressivePacing"

    def calculate_pacing(
        self,
        available_agents: int,
        in_flight_calls: int,
        pending_borrowers: int,
        recent_answer_rate: float = 0.50,
    ) -> PacingProposal:
        # Effective slots = available agents - in flight calls already heading towards them
        open_agent_slots = max(0, available_agents - in_flight_calls)
        suggested = min(open_agent_slots, pending_borrowers)

        return PacingProposal(
            engine_name=self.name,
            suggested_calls=suggested,
            reason=(
                f"Progressive 1:1 rule. Available agents: {available_agents}, "
                f"in-flight calls: {in_flight_calls}, open slots: {open_agent_slots}. "
                f"Pending borrowers: {pending_borrowers}."
            ),
            target_answer_rate=recent_answer_rate,
            expected_connections=int(suggested * recent_answer_rate),
        )
