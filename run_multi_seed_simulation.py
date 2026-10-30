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
            avg_talk_time_sec=0.24,
            use_provider_b=False,
            cycles=15,
            warmup_cycles=3,
        ),
        ScenarioConfig(
            name="Scenario_B",
            description="Medium Answer Rate (50%), Avg Talk Time 90s (Provider A)",
            num_agents=30,
            num_borrowers=300,
            answer_rate=0.50,
            avg_talk_time_sec=0.18,
            use_provider_b=False,
            cycles=15,
            warmup_cycles=3,
        ),
        ScenarioConfig(
            name="Scenario_C",
            description="High Answer Rate (70%), Avg Talk Time 180s (Provider A)",
            num_agents=30,
            num_borrowers=300,
            answer_rate=0.70,
            avg_talk_time_sec=0.36,
            use_provider_b=False,
            cycles=15,
            warmup_cycles=3,
        ),
        ScenarioConfig(
            name="Scenario_D",
            description="Dynamic (20-80%), Talk Times (90-180s), Drops & Provider B Jitter",
            num_agents=30,
            num_borrowers=350,
            dynamic_answer_rate=True,
            avg_talk_time_sec=0.24,
            use_provider_b=True,
            inject_agent_drop_cycle=6,
            agent_drop_count=6,
            cycles=15,
            warmup_cycles=3,
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

        # Paired per-seed differences (predictive minus progressive for the exact same seed)
        paired_diffs = [pred - prog for pred, prog in zip(pred_utils, prog_utils)]
        mean_paired_diff = statistics.mean(paired_diffs)
        pred_wins = sum(1 for d in paired_diffs if d > 0)
        prog_wins = sum(1 for d in paired_diffs if d < 0)
        ties = sum(1 for d in paired_diffs if d == 0)

        summary_rows.append({
            "scenario": cfg.name,
            "prog_mean": statistics.mean(prog_utils),
            "prog_min": min(prog_utils),
            "prog_max": max(prog_utils),
            "pred_mean": statistics.mean(pred_utils),
            "pred_min": min(pred_utils),
            "pred_max": max(pred_utils),
            "mean_paired_diff": mean_paired_diff,
            "pred_wins": pred_wins,
            "prog_wins": prog_wins,
            "ties": ties,
            "prog_aband_mean": statistics.mean(prog_abands),
            "pred_aband_mean": statistics.mean(pred_abands),
        })

    # Print Formatted Report Table
    print("\n" + "=" * 120)
    print("           20-SEED MONTE CARLO BENCHMARK (STEADY-STATE UTILIZATION & PAIRED DIFFERENCES)")
    print("=" * 120)
    print(f"{'Scenario':<12} | {'Progressive Mean (Min/Max)':<28} | {'Predictive Mean (Min/Max)':<28} | {'Paired Diff':<13} | {'Pred Wins':<10} | {'Abandons'}")
    print("-" * 120)
    for row in summary_rows:
        prog_str = f"{row['prog_mean']:.2f}% [{row['prog_min']:.1f}% - {row['prog_max']:.1f}%]"
        pred_str = f"{row['pred_mean']:.2f}% [{row['pred_min']:.1f}% - {row['pred_max']:.1f}%]"
        diff_str = f"{row['mean_paired_diff']:+.2f}%"
        win_str = f"{row['pred_wins']}/20 ({row['pred_wins']*5}%)"
        aband_str = f"{row['pred_aband_mean']:.2f} (0 in all)"
        print(f"{row['scenario']:<12} | {prog_str:<28} | {pred_str:<28} | {diff_str:<13} | {win_str:<10} | {aband_str}")
    print("=" * 120)



if __name__ == "__main__":
    asyncio.run(main())
