from __future__ import annotations
import asyncio
from smart_dialer.simulation.load_test import run_full_pipeline_load_test, run_concurrent_load_test


async def main():
    print("=" * 80)
    print("            SMARTDIALER CONCURRENCY & PIPELINE LOAD TEST")
    print("=" * 80)

    # 1. Full Pipeline Stress Test
    print("\n[PART 1] FULL PIPELINE LOAD TEST (Pacing -> Safety -> Allocator -> Carrier -> Webhooks)")
    print("Running 10 concurrent worker engines simultaneously on shared state...")
    res_full = await run_full_pipeline_load_test(num_workers=10, num_agents=50, num_borrowers=500, cycles_per_worker=8)

    print(f"  * Workers:                    {res_full['num_workers']}")
    print(f"  * Elapsed Time:               {res_full['elapsed_seconds']} seconds")
    print(f"  * Calls Processed:            {res_full['total_calls_created']}")
    print(f"  * Pipeline Throughput:        {res_full['throughput_calls_sec']} calls/second")
    print(f"  * Double-Allocated Agents:    {res_full['double_allocated_agents']} (Expected: 0)")
    print(f"  * Duplicate Borrowers:        {res_full['duplicate_borrowers']} (Expected: 0)")
    print(f"  * Illegal State Transitions:  {res_full['illegal_state_transitions']} (Expected: 0)")
    if res_full["concurrency_safe"]:
        print("  >>> INVARIANTS: [PASSED] 100% Concurrency Safe across full pipeline.")
    else:
        print("  >>> INVARIANTS: [FAILED] Violation detected!")

    # 2. Raw CAS Contention Benchmark
    print("\n[PART 2] LOW-LEVEL ATOMIC CAS CONTENTION BENCHMARK")
    print("Measuring raw SQLite conditional write throughput under extreme parallel contention...")
    res_cas = await run_concurrent_load_test(num_workers=15, num_agents=100, num_borrowers=800, cycles_per_worker=10)

    print(f"  * Workers:                    {res_cas['num_workers']}")
    print(f"  * Elapsed Time:               {res_cas['elapsed_seconds']} seconds")
    print(f"  * Total Locks Acquired:       {res_cas['total_calls_created']}")
    print(f"  * CAS Contention Throughput:  {res_cas['throughput_ops_sec']} ops/second")
    print(f"  * Double-Allocated Agents:    {res_cas['double_allocated_agents']} (Expected: 0)")
    if res_cas["concurrency_safe"]:
        print("  >>> INVARIANTS: [PASSED] 100% Isolation. Zero race conditions.")
    else:
        print("  >>> INVARIANTS: [FAILED] Violation detected!")

    print("\n" + "=" * 80)
    print("                      ALL LOAD TESTS COMPLETED")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
