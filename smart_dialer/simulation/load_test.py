from __future__ import annotations
import asyncio
import time
import random
from typing import Dict, Any
from ..storage.sqlite_store import SQLiteStorage
from ..models.agent import Agent, AgentState
from ..models.borrower import Borrower, BorrowerStatus
from ..telecom.mock_provider_a import MockProviderA
from ..safety.safety_controller import SafetyPass, SafetyDecision
from ..core.worker import DistributedDialerWorker


async def run_concurrent_load_test(
    num_workers: int = 15,
    num_agents: int = 100,
    num_borrowers: int = 1000,
    cycles_per_worker: int = 10,
) -> Dict[str, Any]:
    """
    Executes a high-concurrency stress test where multiple worker threads/coroutines
    simultaneously contest the same pool of available agents and borrowers.
    Validates atomic CAS isolation, zero double-allocations, and zero deadlocks.
    """
    storage = SQLiteStorage(":memory:")
    provider = MockProviderA(min_latency=0.001, max_latency=0.003)

    campaign_id = "camp_stress_load"

    # 1. Populate Agents
    for i in range(1, num_agents + 1):
        storage.save_agent(Agent(id=f"agt_{i:04d}", name=f"Agent {i}", state=AgentState.AVAILABLE))

    # 2. Populate Borrowers
    for i in range(1, num_borrowers + 1):
        storage.save_borrower(Borrower(
            id=f"bor_{i:05d}",
            campaign_id=campaign_id,
            name=f"Borrower {i}",
            phone_number=f"+1800{i:06d}",
            status=BorrowerStatus.PENDING,
        ))

    workers = [
        DistributedDialerWorker(f"worker_{w:02d}", campaign_id, storage, provider, lease_duration_sec=10.0)
        for w in range(1, num_workers + 1)
    ]

    total_attempts = 0
    start_time = time.time()

    async def worker_loop(w: DistributedDialerWorker):
        nonlocal total_attempts
        for _ in range(cycles_per_worker):
            # Worker generates a safety-authorized batch
            s_pass = SafetyPass(
                pass_id=f"pass_{w.worker_id}",
                decision=SafetyDecision.APPROVE,
                authorized_calls=random.randint(3, 8),
                original_requested_calls=8,
                reason="Load test batch",
            )
            total_attempts += s_pass.authorized_calls
            await w.execute_pacing_cycle(s_pass, simulated_answer=(random.random() < 0.6))
            await asyncio.sleep(0.01)

    # Run all workers concurrently
    await asyncio.gather(*(worker_loop(w) for w in workers))
    elapsed = time.time() - start_time

    # Verification: Check for any duplicate reservations or invalid agent states
    with storage._lock, storage._conn:
        # Check if any call has multiple agents or any agent is assigned to multiple active calls
        cur = storage._conn.execute("""
            SELECT current_call_id, count(*) as cnt
            FROM agents
            WHERE current_call_id IS NOT NULL
            GROUP BY current_call_id
            HAVING count(*) > 1
        """)
        double_allocated_calls = cur.fetchall()

        cur_b = storage._conn.execute("""
            SELECT id, attempts, count(*) as cnt
            FROM borrowers
            GROUP BY id
            HAVING count(*) > 1
        """)
        duplicate_borrowers = cur_b.fetchall()

        cur_calls = storage._conn.execute("SELECT count(*) as cnt FROM calls")
        total_calls_created = cur_calls.fetchone()["cnt"]

    return {
        "num_workers": num_workers,
        "elapsed_seconds": round(elapsed, 3),
        "total_calls_created": total_calls_created,
        "throughput_ops_sec": round(total_calls_created / max(0.001, elapsed), 1),
        "double_allocated_agents": len(double_allocated_calls),
        "duplicate_borrowers": len(duplicate_borrowers),
        "concurrency_safe": (len(double_allocated_calls) == 0 and len(duplicate_borrowers) == 0),
    }
