import pytest
from smart_dialer.storage.sqlite_store import SQLiteStorage
from smart_dialer.models.agent import Agent, AgentState
from smart_dialer.models.borrower import Borrower, BorrowerStatus


def test_concurrent_agent_reservation_atomic_cas():
    """
    Simulates Worker 1 and Worker 2 querying the exact same available agent at the same time.
    Verifies that only ONE succeeds, while the other fails cleanly.
    """
    storage = SQLiteStorage(":memory:")
    agent = Agent(id="agt_race", name="Agent Contested", state=AgentState.AVAILABLE, version=1)
    storage.save_agent(agent)

    # Both workers see the agent with version 1
    w1_agent = storage.get_agent("agt_race")
    w2_agent = storage.get_agent("agt_race")
    assert w1_agent.version == 1
    assert w2_agent.version == 1

    # Worker 1 executes atomic CAS reservation
    w1_success = storage.reserve_agent_atomic(
        agent_id="agt_race", worker_id="worker_1", expected_version=w1_agent.version, lease_duration_sec=30.0
    )
    assert w1_success is True

    # Worker 2 attempts atomic CAS reservation with the stale version 1
    w2_success = storage.reserve_agent_atomic(
        agent_id="agt_race", worker_id="worker_2", expected_version=w2_agent.version, lease_duration_sec=30.0
    )
    # MUST FAIL: rowcount == 0 because version bumped to 2 and state is RESERVED
    assert w2_success is False

    # Verify agent state in database
    final_agent = storage.get_agent("agt_race")
    assert final_agent.state == AgentState.RESERVED
    assert final_agent.locked_by_worker_id == "worker_1"
    assert final_agent.version == 2


def test_atomic_borrower_allocation():
    """
    Simulates concurrent worker threads acquiring borrowers from a shared campaign queue.
    Guarantees no two workers get the same borrower.
    """
    storage = SQLiteStorage(":memory:")
    campaign_id = "camp_borrower_race"

    for i in range(1, 11):
        storage.save_borrower(Borrower(
            id=f"bor_{i}",
            campaign_id=campaign_id,
            name=f"Borrower {i}",
            phone_number=f"+1555{i:04d}",
            status=BorrowerStatus.PENDING,
        ))

    allocated_ids = set()
    for worker_idx in range(10):
        borrower = storage.reserve_next_borrower_atomic(
            campaign_id=campaign_id, worker_id=f"worker_{worker_idx}", lease_duration_sec=30.0
        )
        assert borrower is not None
        assert borrower.id not in allocated_ids
        allocated_ids.add(borrower.id)

    # Queue should now be exhausted
    no_more = storage.reserve_next_borrower_atomic(campaign_id, "worker_extra", 30.0)
    assert no_more is None
