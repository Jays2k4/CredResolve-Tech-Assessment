from __future__ import annotations
import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

from ..storage.sqlite_store import SQLiteStorage
from ..models.agent import Agent, AgentState
from ..models.borrower import Borrower, BorrowerStatus
from ..telecom.mock_provider_a import MockProviderA
from ..telecom.mock_provider_b import MockProviderB
from ..core.dialer import SmartDialer, DialerMode, TickDiagnostics
from ..safety.safety_controller import SafetyDecision


@dataclass
class ScenarioConfig:
    name: str
    description: str
    num_agents: int = 30
    num_borrowers: int = 300
    answer_rate: float = 0.50
    avg_talk_time_sec: float = 0.25  # Time-scaled for fast simulation
    dynamic_answer_rate: bool = False
    use_provider_b: bool = False
    inject_agent_drop_cycle: Optional[int] = None
    agent_drop_count: int = 8
    cycles: int = 15


@dataclass
class ScenarioResult:
    scenario_name: str
    mode: str
    total_calls_initiated: int
    total_calls_connected: int
    abandoned_calls: int
    abandon_rate_pct: float
    avg_agent_utilization_pct: float
    peak_agent_utilization_pct: float
    safety_decisions: Dict[str, int]
    provider_name: str
    notes: str
    representative_tick: Optional[TickDiagnostics] = None


class DialerSimulator:
    """
    Simulation testbench for Scenarios A, B, C, and D.
    Evaluates both Progressive and Predictive dialing modes against different
    telecom and borrower behavioral conditions.
    """

    def __init__(self, config: ScenarioConfig, mode: DialerMode = DialerMode.PREDICTIVE, seed: Optional[int] = None):
        self.config = config
        self.mode = mode
        self.seed = seed
        if seed is not None:
            random.seed(seed)
        self.storage = SQLiteStorage(":memory:")

        # Choose provider
        if config.use_provider_b:
            self.provider = MockProviderB(
                timeout_rate=0.05, duplicate_event_rate=0.10, out_of_order_rate=0.10,
                min_latency=0.01, max_latency=0.03
            )
        else:
            self.provider = MockProviderA(failure_rate=0.02, min_latency=0.005, max_latency=0.015)

        self.campaign_id = f"camp_{config.name.lower()}"
        self.dialer = SmartDialer(
            campaign_id=self.campaign_id,
            storage=self.storage,
            provider=self.provider,
            mode=mode,
            max_multiplier_ceiling=3.0,
        )

        self._seed_data()

    def _seed_data(self) -> None:
        # Seed Agents
        for i in range(1, self.config.num_agents + 1):
            agent = Agent(id=f"agent_{i:03d}", name=f"Agent {i}", state=AgentState.AVAILABLE)
            self.storage.save_agent(agent)

        # Seed Borrowers
        for i in range(1, self.config.num_borrowers + 1):
            borrower = Borrower(
                id=f"bor_{i:04d}",
                campaign_id=self.campaign_id,
                name=f"Borrower {i}",
                phone_number=f"+1555{i:06d}",
                status=BorrowerStatus.PENDING,
                debt_amount=random.uniform(500, 8000),
            )
            self.storage.save_borrower(borrower)

    async def run(self) -> ScenarioResult:
        utilization_samples: List[float] = []
        representative_tick: Optional[TickDiagnostics] = None

        current_p = self.config.answer_rate
        talk_time = self.config.avg_talk_time_sec

        for cycle in range(1, self.config.cycles + 1):
            # Dynamic conditions in Scenario D (Answer rate 20-80%, Talk times 90-180s)
            if self.config.dynamic_answer_rate:
                current_p = 0.20 + 0.60 * random.random()
                talk_time = random.uniform(0.18, 0.36)

            # Test Sudden Agent Drop
            if self.config.inject_agent_drop_cycle == cycle:
                avail = self.storage.list_available_agents(limit=self.config.agent_drop_count)
                for a in avail:
                    self.storage.update_agent_state(a.id, AgentState.OFFLINE)

            # Step dialer
            await self.dialer.step(override_answer_rate=current_p, avg_talk_time_sec=talk_time)

            if cycle == 2 and self.dialer.last_tick_diagnostics:
                representative_tick = self.dialer.last_tick_diagnostics

            # Allow calls to initiate, ring, and bridge
            await asyncio.sleep(0.08)

            # Record utilization while calls are connected
            metrics = self.dialer.get_metrics()
            utilization_samples.append(metrics["agent_utilization_pct"])

            # Allow talk duration progress
            await asyncio.sleep(talk_time * 0.7)

        # Allow trailing calls to wrap up
        await asyncio.sleep(talk_time + 0.1)

        final_metrics = self.dialer.get_metrics()
        cur = self.storage._conn.execute("SELECT count(*) as cnt FROM call_events WHERE state = 'CONNECTED'")
        row = cur.fetchone()
        total_connected = row["cnt"] if row else 0

        total_init = final_metrics["total_calls_initiated"]
        abandoned = final_metrics["abandoned_calls"]
        abandon_rate = (abandoned / total_connected * 100.0) if total_connected > 0 else 0.0

        avg_util = sum(utilization_samples) / max(1, len(utilization_samples))
        peak_util = max(utilization_samples) if utilization_samples else 0.0

        notes = (
            f"Cycles: {self.config.cycles}, Answer rate: {current_p:.1%}. "
            f"Duplicates ignored: {final_metrics['duplicate_events_ignored']}."
        )

        return ScenarioResult(
            scenario_name=self.config.name,
            mode=self.mode.value,
            total_calls_initiated=total_init,
            total_calls_connected=total_connected,
            abandoned_calls=abandoned,
            abandon_rate_pct=round(abandon_rate, 2),
            avg_agent_utilization_pct=round(avg_util, 2),
            peak_agent_utilization_pct=round(peak_util, 2),
            safety_decisions=final_metrics["safety_decisions"],
            provider_name=self.provider.name,
            notes=notes,
            representative_tick=representative_tick,
        )
