import pytest
import time
from smart_dialer.storage.sqlite_store import SQLiteStorage
from smart_dialer.models.agent import Agent, AgentState
from smart_dialer.models.borrower import Borrower, BorrowerStatus
from smart_dialer.models.call import Call, CallState
from smart_dialer.core.worker import DistributedDialerWorker, LeaseReconciliationService
from smart_dialer.telecom.mock_provider_a import MockProviderA
from smart_dialer.safety.safety_controller import SafetyPass, SafetyDecision


@pytest.mark.asyncio
async def test_worker_crash_and_lease_recovery():
    """
    Scenario from PDF:
    Agent reserved > Borrower reserved > Call initiated > Worker crashes.
    What happens when the system comes back?
    """
    storage = SQLiteStorage(":memory:")
    provider = MockProviderA()
    campaign_id = "camp_crash_test"

    # Seed agent and borrower
    agent = Agent(id="agt_crash_target", name="Agent Vulnerable", state=AgentState.AVAILABLE)
    storage.save_agent(agent)

    borrower = Borrower(id="bor_crash_target", campaign_id=campaign_id, name="Target", phone_number="+15551111")
    storage.save_borrower(borrower)

    # Short lease of 0.1 seconds for testing
    worker = DistributedDialerWorker("worker_dying", campaign_id, storage, provider, lease_duration_sec=0.1)

    s_pass = SafetyPass("pass_1", SafetyDecision.APPROVE, 1, 1, "Testing crash")
    await worker.execute_pacing_cycle(s_pass)

    # Inspect state: Agent and Borrower are reserved/dialing by worker_dying
    mid_agent = storage.get_agent("agt_crash_target")
    assert mid_agent.state in (AgentState.RESERVED, AgentState.DIALING)
    assert mid_agent.locked_by_worker_id == "worker_dying"

    mid_borrower = storage.get_borrower("bor_crash_target")
    assert mid_borrower.status == BorrowerStatus.RESERVED
    assert mid_borrower.locked_by_worker_id == "worker_dying"

    # Worker crashes abruptly!
    worker.simulate_crash()
    assert worker.is_alive is False

    # Wait for lease to expire
    time.sleep(0.15)

    # Supervisor runs lease reconciliation (or new worker comes back online)
    reconciler = LeaseReconciliationService(storage)
    reconciled = reconciler.run_reconciliation()

    assert reconciled["agents"] == 1
    assert reconciled["borrowers"] == 1

    # Verify agent and borrower have been safely restored and unlocked
    restored_agent = storage.get_agent("agt_crash_target")
    assert restored_agent.state == AgentState.AVAILABLE
    assert restored_agent.locked_by_worker_id is None
    assert restored_agent.lease_expires_at is None

    restored_borrower = storage.get_borrower("bor_crash_target")
    assert restored_borrower.status == BorrowerStatus.PENDING
    assert restored_borrower.locked_by_worker_id is None
    assert restored_borrower.lease_expires_at is None
