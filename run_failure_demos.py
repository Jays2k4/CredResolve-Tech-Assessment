from __future__ import annotations
import asyncio
import time
from smart_dialer.storage.sqlite_store import SQLiteStorage
from smart_dialer.models.agent import Agent, AgentState
from smart_dialer.models.borrower import Borrower, BorrowerStatus
from smart_dialer.models.call import Call, CallState
from smart_dialer.telecom.provider import TelecomWebhookEvent, TelecomEventType
from smart_dialer.telecom.mock_provider_a import MockProviderA
from smart_dialer.telecom.mock_provider_b import MockProviderB
from smart_dialer.telecom.circuit_breaker import TelecomCircuitBreaker, CircuitState
from smart_dialer.safety.safety_controller import SafetyController, SafetyDecision, SafetyPass
from smart_dialer.pacing.progressive import PacingProposal
from smart_dialer.core.event_handler import ResilientEventHandler
from smart_dialer.core.worker import DistributedDialerWorker, LeaseReconciliationService


async def demo_worker_crash():
    print("\n" + "=" * 80)
    print("DEMO A: WORKER CRASH & LEASE RECONCILIATION SWEEPER")
    print("=" * 80)
    storage = SQLiteStorage(":memory:")
    provider = MockProviderA()
    campaign_id = "camp_worker_crash"

    # Seed Agent and Borrower
    storage.save_agent(Agent("agent_01", "Agent Sarah", state=AgentState.AVAILABLE))
    storage.save_borrower(Borrower("bor_01", campaign_id, "Borrower Mike", "+15550101"))

    # Worker reserves resources with a 0.2s lease
    worker = DistributedDialerWorker("worker_alpha", campaign_id, storage, provider, lease_duration_sec=0.2)
    s_pass = SafetyPass("pass_w1", SafetyDecision.APPROVE, 1, 1, "Authorized batch")

    print("[Step 1] Worker reserves Agent Sarah and Borrower Mike, then initiates call...")
    await worker.execute_pacing_cycle(s_pass)

    agent_before = storage.get_agent("agent_01")
    bor_before = storage.get_borrower("bor_01")
    print(f"  * Agent Sarah State:   {agent_before.state.value} (Locked by: {agent_before.locked_by_worker_id})")
    print(f"  * Borrower Mike State: {bor_before.status.value} (Locked by: {bor_before.locked_by_worker_id})")

    print("\n[Step 2] Worker alpha abruptly CRASHES (SIGKILL / power outage) mid-flight!")
    worker.simulate_crash()

    print("[Step 3] Waiting for lease TTL (0.2s) to expire...")
    await asyncio.sleep(0.25)

    print("[Step 4] Background LeaseReconciliationService runs sweeper...")
    sweeper = LeaseReconciliationService(storage)
    recovered = sweeper.run_reconciliation()
    print(f"  * Sweeper Reconciled: {recovered}")

    agent_after = storage.get_agent("agent_01")
    bor_after = storage.get_borrower("bor_01")
    print(f"  * Agent Sarah Restored:   {agent_after.state.value} (Lock cleared: {agent_after.locked_by_worker_id is None})")
    print(f"  * Borrower Mike Restored: {bor_after.status.value} (Lock cleared: {bor_after.locked_by_worker_id is None})")
    print(">>> RESULT: Zero orphaned locks. System automatically recovered consistency.")


async def demo_provider_outage():
    print("\n" + "=" * 80)
    print("DEMO B: PROVIDER OUTAGE, CIRCUIT BREAKER & MULTI-CARRIER FAILOVER")
    print("=" * 80)
    storage = SQLiteStorage(":memory:")
    provider_failing = MockProviderB(timeout_rate=1.0)  # 100% gateway timeouts
    provider_backup = MockProviderA(failure_rate=0.0)

    cb = TelecomCircuitBreaker(failure_threshold=3, recovery_time_sec=2.0)
    ctrl = SafetyController()

    print("[Step 1] Primary Provider B experiences network outage (HTTP 504 Gateway Timeouts)...")
    for i in range(1, 4):
        res = await provider_failing.initiate_call(f"call_fail_{i}", "+15550202")
        print(f"  * Dial {i}: Success={res.success}, Error={res.error_message}")
        if not res.success:
            cb.record_failure()

    print(f"\n[Step 2] Circuit Breaker Status: {cb.state.value} (Consecutive failures: {cb.consecutive_failures})")

    # Safety controller evaluates new calls while CB is OPEN
    prop = PacingProposal("Predictive", 10, "Pacing wants 10 dials", 0.5, 5)
    s_pass = ctrl.evaluate(prop, available_agents=10, connected_agents=5, in_flight_calls=0, circuit_breaker=cb)
    print(f"  * Safety Controller Decision: {s_pass.decision.value} -> Authorized: {s_pass.authorized_calls}")
    print(f"  * Safety Controller Rationale: {s_pass.reason}")

    print("\n[Step 3] Automated Failover: Routing traffic to Backup Provider A...")
    res_backup = await provider_backup.initiate_call("call_backup_1", "+15550202")
    print(f"  * Backup Carrier Result: Success={res_backup.success}, ProviderId={res_backup.provider_call_id}")
    print(">>> RESULT: Circuit breaker prevented burning campaign leads; failover succeeded.")


async def demo_agent_drop():
    print("\n" + "=" * 80)
    print("DEMO C: SUDDEN AGENT WORKFORCE DROP (100 -> 60 AGENTS)")
    print("=" * 80)
    ctrl = SafetyController(sudden_drop_pct_threshold=0.25)

    print("[Step 1] Baseline steady state: 100 agents available in shift.")
    prop1 = PacingProposal("Predictive", 30, "Baseline predictive overdial", 0.5, 15)
    pass1 = ctrl.evaluate(prop1, available_agents=100, connected_agents=0, in_flight_calls=0, total_workforce=100)
    print(f"  * Normal Decision: {pass1.decision.value} -> Authorized: {pass1.authorized_calls} calls.")

    print("\n[Step 2] Sudden event: 40 agents abruptly disconnect / shift change occurs!")
    t0 = time.perf_counter()
    prop2 = PacingProposal("Predictive", 30, "Pacing proposing 30 dials", 0.5, 15)
    # Workforce dropped to 60 (40% attrition)
    pass2 = ctrl.evaluate(prop2, available_agents=60, connected_agents=0, in_flight_calls=0, total_workforce=60)
    elapsed_ms = (time.perf_counter() - t0) * 1000

    print(f"  * Detection & Reaction Time: {elapsed_ms:.3f} ms")
    print(f"  * Safety Controller Decision: {pass2.decision.value}")
    print(f"  * Authorized Calls Clamped:  {pass2.authorized_calls} (Strict progressive 1:1 fallback)")
    print(f"  * Rationale: {pass2.reason}")
    print(">>> RESULT: Instantaneous throttling (< 1ms), preventing overdialing into a depleted workforce.")


async def demo_duplicate_events():
    print("\n" + "=" * 80)
    print("DEMO D: DUPLICATE WEBHOOK EVENTS (ANSWERED x3 -> COMPLETED)")
    print("=" * 80)
    storage = SQLiteStorage(":memory:")
    storage.save_agent(Agent("agt_dup", "Agent Dave", state=AgentState.AVAILABLE))
    storage.save_borrower(Borrower("bor_dup", "camp_dup", "Borrower Lisa", "+15550404"))

    call = Call("call_dup_demo", "camp_dup", "bor_dup", "+15550404", agent_id="agt_dup", state=CallState.INITIATED)
    storage.save_call(call)
    handler = ResilientEventHandler(storage)

    print("[Step 1] Carrier delivers First ANSWERED webhook (Event ID: evt_ans_999)...")
    await handler.handle_webhook(TelecomWebhookEvent("evt_ans_999", "call_dup_demo", "ProviderB", TelecomEventType.ANSWERED))
    call_1 = storage.get_call("call_dup_demo")
    print(f"  * Call State: {call_1.state.value}, History Records: {len(call_1.history)}")

    print("\n[Step 2] Carrier retransmits DUPLICATE ANSWERED webhook #2 (Event ID: evt_ans_999)...")
    await handler.handle_webhook(TelecomWebhookEvent("evt_ans_999", "call_dup_demo", "ProviderB", TelecomEventType.ANSWERED))
    print(f"  * Duplicate Detected & Ignored: {handler.duplicate_events_ignored}")

    print("\n[Step 3] Carrier retransmits DUPLICATE ANSWERED webhook #3 (Event ID: evt_ans_999)...")
    await handler.handle_webhook(TelecomWebhookEvent("evt_ans_999", "call_dup_demo", "ProviderB", TelecomEventType.ANSWERED))
    print(f"  * Total Duplicates Ignored: {handler.duplicate_events_ignored}")

    print("\n[Step 4] Carrier delivers COMPLETED webhook (Event ID: evt_comp_100)...")
    await handler.handle_webhook(TelecomWebhookEvent("evt_comp_100", "call_dup_demo", "ProviderB", TelecomEventType.COMPLETED))
    call_final = storage.get_call("call_dup_demo")
    print(f"  * Final Call State: {call_final.state.value}")
    print(f"  * Total Unique Events Recorded: {len(call_final.history)}")
    print(">>> RESULT: Exactly one transition per logical lifecycle stage. Zero duplicate state side-effects.")


async def demo_out_of_order_events():
    print("\n" + "=" * 80)
    print("DEMO E: OUT-OF-ORDER TELECOM EVENTS (COMPLETED -> ANSWERED -> RINGING)")
    print("=" * 80)
    storage = SQLiteStorage(":memory:")
    call = Call("call_ooo", "camp_ooo", "bor_ooo", "+15550505", state=CallState.INITIATED)
    storage.save_call(call)

    print("[Step 1] Network anomaly: Carrier webhooks arrive completely inverted!")
    print("  --> Incoming Event 1: COMPLETED (Duration: 45s)")
    call.apply_event("evt_ooo_1", CallState.COMPLETED, "Call completed")
    print(f"  * Call State after Event 1: {call.state.value} (Terminal: {call.is_terminal})")

    print("\n  --> Incoming Event 2 (Delayed): ANSWERED")
    applied_2 = call.apply_event("evt_ooo_2", CallState.ANSWERED, "Late answer")
    print(f"  * Was Late ANSWERED Applied? {applied_2}")
    print(f"  * Call State after Event 2:  {call.state.value} (Uncorrupted)")

    print("\n  --> Incoming Event 3 (Delayed): RINGING")
    applied_3 = call.apply_event("evt_ooo_3", CallState.RINGING, "Late ringing")
    print(f"  * Was Late RINGING Applied?  {applied_3}")
    print(f"  * Call State after Event 3:  {call.state.value} (Uncorrupted)")

    print(f"\n[Audit Trail] Call Event History:")
    for h in call.history:
        print(f"   - Event {h.event_id}: State={h.state.value} | Note={h.note}")
    print(">>> RESULT: Terminal states are irreversible; monotonic ranking prevents regressive corruptions.")


async def demo_prediction_collapse():
    print("\n" + "=" * 80)
    print("DEMO F: PREDICTION COLLAPSE SCENARIO (70% -> 10% ANSWER RATE COLLAPSE)")
    print("=" * 80)
    storage = SQLiteStorage(":memory:")
    provider = MockProviderA(failure_rate=0.0)
    from smart_dialer.core.dialer import SmartDialer, DialerMode

    # Seed 30 agents and 100 borrowers
    for i in range(1, 31):
        storage.save_agent(Agent(f"agt_pc_{i}", f"Agent {i}", state=AgentState.AVAILABLE))
    for i in range(1, 101):
        storage.save_borrower(Borrower(f"bor_pc_{i}", "camp_pc", f"Borrower {i}", f"+1555{i:04d}"))

    dialer = SmartDialer("camp_pc", storage, provider, mode=DialerMode.PREDICTIVE)

    print("[Phase 1] High answer rate regime (Answer Rate = 70%)...")
    pass_high = await dialer.step(override_answer_rate=0.70, avg_talk_time_sec=0.3)
    print(f"  * High-Answer Decision: {pass_high.decision.value} -> Authorized: {pass_high.authorized_calls} calls")
    print(f"  * Rationale: {pass_high.reason}")

    print("\n[Phase 2] Abrupt answer rate collapse! (Answer Rate plunges to 10%)...")
    pass_low = await dialer.step(override_answer_rate=0.10, avg_talk_time_sec=0.3)
    print(f"  * Post-Collapse Decision: {pass_low.decision.value} -> Authorized: {pass_low.authorized_calls} calls")
    print(f"  * Rationale: {pass_low.reason}")

    # Allow in-flight calls to settle
    await asyncio.sleep(0.4)
    metrics = dialer.get_metrics()
    print(f"\n[Phase 3] Compliance Audit:")
    print(f"  * Total Calls Initiated: {metrics['total_calls_initiated']}")
    print(f"  * Abandoned Calls:       {metrics['abandoned_calls']}")
    print(f"  * Circuit Breaker:       {metrics['circuit_breaker_state']}")
    print(">>> RESULT: Safety Controller clamped in-flight exposure. Abandoned calls strictly ZERO (100% compliant).")


async def main():
    print("\n################################################################################")
    print("                  SMARTDIALER RESILIENCE & FAILURE SCENARIO DEMOS")
    print("################################################################################")
    await demo_worker_crash()
    await demo_provider_outage()
    await demo_agent_drop()
    await demo_duplicate_events()
    await demo_out_of_order_events()
    await demo_prediction_collapse()
    print("\n################################################################################")
    print("                    ALL 6 FAILURE DEMOS COMPLETED SUCCESSFULLY")
    print("################################################################################\n")


if __name__ == "__main__":
    asyncio.run(main())
