from __future__ import annotations
import asyncio
from smart_dialer.simulation.load_test import run_concurrent_load_test


async def main():
    print("=" * 75)
    print("        SMARTDIALER HIGH-CONCURRENCY DISTRIBUTED LOAD TEST")
    print("=" * 75)
    print("Simulating 15 concurrent distributed workers contending for agents & borrowers...")
    print("Testing atomic Compare-And-Swap (CAS) isolation and zero double-allocations.\n")

    res = await run_concurrent_load_test(
        num_workers=15,
        num_agents=100,
        num_borrowers=800,
        cycles_per_worker=10,
    )

    print(f"Workers Running Concurrently: {res['num_workers']}")
    print(f"Elapsed Time:                 {res['elapsed_seconds']} seconds")
    print(f"Total Calls Created:          {res['total_calls_created']}")
    print(f"Throughput:                   {res['throughput_ops_sec']} ops/second")
    print(f"Double-Allocated Agents:      {res['double_allocated_agents']} (Expected: 0)")
    print(f"Duplicate Borrowers:          {res['duplicate_borrowers']} (Expected: 0)")
    print("-" * 75)
    if res["concurrency_safe"]:
        print("RESULT: [PASSED] Zero race conditions. Atomic CAS guaranteed 100% isolation.")
    else:
        print("RESULT: [FAILED] Concurrency violation detected!")
    print("=" * 75)


if __name__ == "__main__":
    asyncio.run(main())
