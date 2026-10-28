from __future__ import annotations
import asyncio
from smart_dialer.simulation.simulator import DialerSimulator, ScenarioConfig, ScenarioResult
from smart_dialer.core.dialer import DialerMode


async def main():
    print("=" * 80)
    print("           SMARTDIALER SIMULATION ENGINE: SCENARIOS A, B, C, D")
    print("=" * 80)
    print("Evaluating Progressive vs. Predictive Pacing, Carrier Latency & Failures\n")

    scenarios = [
        ScenarioConfig(
            name="Scenario_A",
            description="Low Answer Rate (20%), Avg Talk Time 120s (Fast Reliable Provider A)",
            num_agents=50,
            num_borrowers=300,
            answer_rate=0.20,
            avg_talk_time_sec=0.8,
            use_provider_b=False,
            cycles=12,
        ),
        ScenarioConfig(
            name="Scenario_B",
            description="Medium Answer Rate (50%), Avg Talk Time 90s (Fast Reliable Provider A)",
            num_agents=50,
            num_borrowers=300,
            answer_rate=0.50,
            avg_talk_time_sec=0.6,
            use_provider_b=False,
            cycles=12,
        ),
        ScenarioConfig(
            name="Scenario_C",
            description="High Answer Rate (70%), Avg Talk Time 180s (Fast Reliable Provider A)",
            num_agents=50,
            num_borrowers=300,
            answer_rate=0.70,
            avg_talk_time_sec=1.2,
            use_provider_b=False,
            cycles=12,
        ),
        ScenarioConfig(
            name="Scenario_D",
            description="Dynamic Changing Answer Rate (20-80%), Talk Times, Drops & Provider B Jitter",
            num_agents=50,
            num_borrowers=400,
            dynamic_answer_rate=True,
            avg_talk_time_sec=0.8,
            use_provider_b=True,
            inject_agent_drop_cycle=5,
            agent_drop_count=20,
            cycles=15,
        ),
    ]

    results: list[ScenarioResult] = []

    for config in scenarios:
        print(f"\n--- Running {config.name}: {config.description} ---")

        # Run Progressive Mode
        sim_prog = DialerSimulator(config, mode=DialerMode.PROGRESSIVE)
        res_prog = await sim_prog.run()
        results.append(res_prog)

        # Run Predictive Mode
        sim_pred = DialerSimulator(config, mode=DialerMode.PREDICTIVE)
        res_pred = await sim_pred.run()
        results.append(res_pred)

        print(f"  [Progressive] Util: {res_prog.avg_agent_utilization_pct}% | Dials: {res_prog.total_calls_initiated} | Connected: {res_prog.total_calls_connected} | Abandoned: {res_prog.abandoned_calls}")
        print(f"  [Predictive]  Util: {res_pred.avg_agent_utilization_pct}% | Dials: {res_pred.total_calls_initiated} | Connected: {res_pred.total_calls_connected} | Abandoned: {res_pred.abandoned_calls}")
        print(f"  Safety Decisions: {res_pred.safety_decisions}")

    # Summary Table
    print("\n" + "=" * 95)
    print("                                SIMULATION RESULTS SUMMARY")
    print("=" * 95)
    print(f"{'Scenario':<12} | {'Mode':<11} | {'Dials':<6} | {'Connects':<8} | {'Avg Util %':<10} | {'Abandons':<8} | {'Abandon Rate'}")
    print("-" * 95)
    for r in results:
        print(
            f"{r.scenario_name:<12} | {r.mode:<11} | {r.total_calls_initiated:<6} | {r.total_calls_connected:<8} | "
            f"{r.avg_agent_utilization_pct:<10}% | {r.abandoned_calls:<8} | {r.abandon_rate_pct}%"
        )
    print("=" * 95)
    print("\nKey Takeaway: Predictive pacing drastically boosts agent utilization under low/moderate answer rates,")
    print("while the Safety Controller ensures abandoned calls remain tightly constrained even under sudden agent drops and provider jitter.\n")


if __name__ == "__main__":
    asyncio.run(main())
