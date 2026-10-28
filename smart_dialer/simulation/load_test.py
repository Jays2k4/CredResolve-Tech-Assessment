from __future__ import annotations
import asyncio
import time
import random
from typing import Dict, Any
from ..storage.sqlite_store import SQLiteStorage
from ..models.agent import Agent, AgentState
from ..models.borrower import Borrower, BorrowerStatus
from ..telecom.mock_provider_a import MockProviderA
from ..safety.safety_controller import SafetyController
from ..pacing.progressive import ProgressivePacingEngine
from ..pacing.predictive import PredictivePacingEngine
from ..core.dialer import SmartDialer, CallAllocator, DialerMode
from ..core.worker import DistributedDialerWorker


async def run_full_pipeline_load_test(
    num_workers: int = 10,
    num_agents: int = 50,
    num_borrowers: int = 500,
    cycles_per_worker: int = 8,
) -> Dict[str, Any]:
    """
    Full End-to-End Pipeline Stress Test:
    Spawns multiple workers simultaneously driving the full SmartDialer stack:
      Pacing Proposal -> Safety Controller Evaluation -> SafetyPass Generation ->
      Atomic CAS Lock (Borrower & Agent) -> Telecom Provider Dispatch -> Webhook Processing.
    Validates high throughput and all distributed system invariants.
    """
    storage = SQLiteStorage(":memory:")
    provider = MockProviderA(min_latency=0.001, max_latency=0.003, failure_rate=0.01)
    campaign_id = "camp_full_load"

    # 1. Seed Agents
    for i in range(1, num_agents + 1):
        storage.save_agent(Agent(id=f"agt_load_{i:04d}", name=f"Agent {i}", state=AgentState.AVAILABLE))

    # 2. Seed Borrowers
    for i in range(1, num_borrowers + 1):
        storage.save_borrower(Borrower(
            id=f"bor_load_{i:05d}",
            campaign_id=campaign_id,
            name=f"Borrower {i}",
            phone_number=f"+1800{i:06d}",
            status=BorrowerStatus.PENDING,
        ))

    # Central SmartDialer managing campaign state with concurrent worker tasks driving the pipeline
    dialer = SmartDialer(
        campaign_id=campaign_id,
        storage=storage,
        provider=provider,
        mode=DialerMode.PREDICTIVE,
        max_multiplier_ceiling=3.0,
    )

    start_time = time.time()

    async def worker_task(worker_id: int):
        for _ in range(cycles_per_worker):
            await dialer.step(override_answer_rate=0.45, avg_talk_time_sec=0.1)
            await asyncio.sleep(0.005)

    await asyncio.gather(*(worker_task(w) for w in range(num_workers)))
    elapsed = time.time() - start_time

    # Allow trailing webhooks to settle
    await asyncio.sleep(0.2)

    # Invariants Verification
    with storage._lock, storage._conn:
        # Invariant 1: No double-allocated agents
        cur_agents = storage._conn.execute("""
            SELECT current_call_id, count(*) as cnt
            FROM agents
            WHERE current_call_id IS NOT NULL
            GROUP BY current_call_id
            HAVING count(*) > 1
        """)
        double_allocated = cur_agents.fetchall()

        # Invariant 2: No duplicate borrower allocation
        cur_bor = storage._conn.execute("""
            SELECT id, count(*) as cnt
            FROM borrowers
            WHERE status = 'RESERVED'
            GROUP BY id
            HAVING count(*) > 1
        """)
        duplicate_borrowers = cur_bor.fetchall()

        # Invariant 3: Total calls created
        cur_calls = storage._conn.execute("SELECT count(*) as cnt FROM calls")
        total_calls = cur_calls.fetchone()["cnt"]

        # Invariant 4: No illegal call states
        cur_illegal = storage._conn.execute("""
            SELECT count(*) as cnt FROM calls
            WHERE state NOT IN (
                'QUEUED', 'RESERVED', 'INITIATED', 'RINGING',
                'ANSWERED', 'CONNECTED', 'COMPLETED', 'FAILED', 'CANCELLED'
            )
        """)
        illegal_states = cur_illegal.fetchone()["cnt"]

    concurrency_safe = (
        len(double_allocated) == 0 and
        len(duplicate_borrowers) == 0 and
        illegal_states == 0
    )

    return {
        "test_type": "Full End-to-End Pipeline (Pacing -> Safety -> Allocator -> Carrier)",
        "num_workers": num_workers,
        "elapsed_seconds": round(elapsed, 3),
        "total_calls_created": total_calls,
        "throughput_calls_sec": round(total_calls / max(0.001, elapsed), 1),
        "double_allocated_agents": len(double_allocated),
        "duplicate_borrowers": len(duplicate_borrowers),
        "illegal_state_transitions": illegal_states,
        "concurrency_safe": concurrency_safe,
    }


async def run_concurrent_load_test(
    num_workers: int = 15,
    num_agents: int = 100,
    num_borrowers: int = 1000,
    cycles_per_worker: int = 10,
) -> Dict[str, Any]:
    """
    Low-Level Atomic CAS Contention Benchmark:
    Measures raw SQLite conditional write throughput under extreme worker contention.
    """
    storage = SQLiteStorage(":memory:")
    provider = MockProviderA(min_latency=0.001, max_latency=0.003)
    campaign_id = "camp_stress_cas"

    for i in range(1, num_agents + 1):
        storage.save_agent(Agent(id=f"agt_{i:04d}", name=f"Agent {i}", state=AgentState.AVAILABLE))

    for i in range(1, num_borrowers + 1):
        storage.save_borrower(Borrower(
            id=f"bor_{i:05d}",
            campaign_id=campaign_id,
            name=f"Borrower {i}",
            phone_number=f"+1800{i:06d}",
            status=BorrowerStatus.PENDING,
        ))

    from ..safety.safety_controller import SafetyPass, SafetyDecision
    workers = [
        DistributedDialerWorker(f"worker_{w:02d}", campaign_id, storage, provider, lease_duration_sec=10.0)
        for w in range(1, num_workers + 1)
    ]

    start_time = time.time()

    async def worker_loop(w: DistributedDialerWorker):
        for _ in range(cycles_per_worker):
            s_pass = SafetyPass(
                pass_id=f"pass_{w.worker_id}",
                decision=SafetyDecision.APPROVE,
                authorized_calls=random.randint(3, 8),
                original_requested_calls=8,
                reason="Load test batch",
            )
            await w.execute_pacing_cycle(s_pass, simulated_answer=(random.random() < 0.6))
            await asyncio.sleep(0.01)

    await asyncio.gather(*(worker_loop(w) for w in workers))
    elapsed = time.time() - start_time

    with storage._lock, storage._conn:
        cur = storage._conn.execute("""
            SELECT current_call_id, count(*) as cnt
            FROM agents
            WHERE current_call_id IS NOT NULL
            GROUP BY current_call_id
            HAVING count(*) > 1
        """)
        double_allocated = cur.fetchall()

        cur_calls = storage._conn.execute("SELECT count(*) as cnt FROM calls")
        total_calls_created = cur_calls.fetchone()["cnt"]

    return {
        "test_type": "Raw SQLite Atomic CAS Contention",
        "num_workers": num_workers,
        "elapsed_seconds": round(elapsed, 3),
        "total_calls_created": total_calls_created,
        "throughput_ops_sec": round(total_calls_created / max(0.001, elapsed), 1),
        "double_allocated_agents": len(double_allocated),
        "concurrency_safe": len(double_allocated) == 0,
    }
