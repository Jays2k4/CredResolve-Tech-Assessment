from __future__ import annotations
import asyncio
import random
import statistics
from typing import Dict, List, Any
from smart_dialer.simulation.simulator import DialerSimulator, ScenarioConfig, ScenarioResult
from smart_dialer.core.dialer import DialerMode


async def run_scenario_across_seeds(config: ScenarioConfig, mode: DialerMode, seeds: List[int]) -> List[ScenarioResult]:
    sem = asyncio.Semaphore(6)

    async def run_single(s: int):
        async with sem:
            sim = DialerSimulator(config, mode=mode, seed=s)
            return await sim.run()

    return await asyncio.gather(*(run_single(s) for s in seeds))


async def main():
    print("=" * 105)
    print("         SMARTDIALER 20-SEED MONTE CARLO EVALUATION (SCENARIOS A, B, C, D)")
    print("=" * 105)
    print("Evaluating 20 random seeds (seeds 1..20) per scenario to measure statistical distribution.\n")

    seeds = list(range(1, 21))

    scenarios = [
        ScenarioConfig(
            name="Scenario_A",
            description="Low Answer Rate (20%), Avg Talk Time 120s (Provider A)",
            num_agents=30,
            num_borrowers=300,
            answer_rate=0.20,
            avg_talk_time_sec=0.25,
            use_provider_b=False,
            cycles=12,
        ),
        ScenarioConfig(
            name="Scenario_B",
            description="Medium Answer Rate (50%), Avg Talk Time 90s (Provider A)",
            num_agents=30,
            num_borrowers=300,
            answer_rate=0.50,
            avg_talk_time_sec=0.25,
            use_provider_b=False,
            cycles=12,
        ),
        ScenarioConfig(
            name="Scenario_C",
            description="High Answer Rate (70%), Avg Talk Time 180s (Provider A)",
            num_agents=30,
            num_borrowers=300,
            answer_rate=0.70,
            avg_talk_time_sec=0.25,
            use_provider_b=False,
            cycles=12,
        ),
        ScenarioConfig(
            name="Scenario_D",
            description="Dynamic (20-80%), Talk Times, Drops & Provider B Jitter",
            num_agents=30,
            num_borrowers=350,
            dynamic_answer_rate=True,
            avg_talk_time_sec=0.25,
            use_provider_b=True,
            inject_agent_drop_cycle=6,
            agent_drop_count=6,
            cycles=12,
        ),
    ]

    summary_rows = []

    for cfg in scenarios:
        print(f"Running 20 seeds for {cfg.name}...")

        # Run Progressive across 20 seeds
        prog_results = await run_scenario_across_seeds(cfg, DialerMode.PROGRESSIVE, seeds)
        prog_utils = [r.avg_agent_utilization_pct for r in prog_results]
        prog_abands = [r.abandoned_calls for r in prog_results]

        # Run Predictive across 20 seeds
        pred_results = await run_scenario_across_seeds(cfg, DialerMode.PREDICTIVE, seeds)
        pred_utils = [r.avg_agent_utilization_pct for r in pred_results]
        pred_abands = [r.abandoned_calls for r in pred_results]

        summary_rows.append({
            "scenario": cfg.name,
            "mode": "PROGRESSIVE",
            "util_mean": statistics.mean(prog_utils),
            "util_min": min(prog_utils),
            "util_max": max(prog_utils),
            "aband_mean": statistics.mean(prog_abands),
            "aband_min": min(prog_abands),
            "aband_max": max(prog_abands),
        })

        summary_rows.append({
            "scenario": cfg.name,
            "mode": "PREDICTIVE",
            "util_mean": statistics.mean(pred_utils),
            "util_min": min(pred_utils),
            "util_max": max(pred_utils),
            "aband_mean": statistics.mean(pred_abands),
            "aband_min": min(pred_abands),
            "aband_max": max(pred_abands),
        })

    # Print Formatted Report Table
    print("\n" + "=" * 105)
    print("                     20-SEED BENCHMARK SUMMARY (MEAN & MIN/MAX)")
    print("=" * 105)
    print(f"{'Scenario':<12} | {'Mode':<12} | {'Mean Util %':<12} | {'Min/Max Util %':<18} | {'Mean Aband':<12} | {'Min/Max Aband'}")
    print("-" * 105)
    for row in summary_rows:
        util_range = f"[{row['util_min']:.1f}% - {row['util_max']:.1f}%]"
        aband_range = f"[{row['aband_min']} - {row['aband_max']}]"
        print(
            f"{row['scenario']:<12} | {row['mode']:<12} | {row['util_mean']:<11.2f}% | "
            f"{util_range:<18} | {row['aband_mean']:<12.2f} | {aband_range}"
        )
    print("=" * 105)


if __name__ == "__main__":
    asyncio.run(main())
