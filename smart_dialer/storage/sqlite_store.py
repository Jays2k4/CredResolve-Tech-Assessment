from __future__ import annotations
import sqlite3
import threading
import time
from typing import Optional, List, Dict, Any, Set
from .base import StorageInterface
from ..models.agent import Agent, AgentState
from ..models.borrower import Borrower, BorrowerStatus
from ..models.call import Call, CallState, CallEventRecord, TERMINAL_CALL_STATES


class SQLiteStorage(StorageInterface):
    """
    Production-grade embedded storage engine using SQLite with WAL (Write-Ahead Logging).
    Guarantees strict ACID isolation and atomic Compare-And-Swap (CAS) state changes
    preventing dual allocation across concurrent worker threads or processes.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        with self._lock, self._conn:
            self._conn.execute("PRAGMA journal_mode = WAL;")
            self._conn.execute("PRAGMA synchronous = NORMAL;")
            self._conn.execute("PRAGMA busy_timeout = 30000;")

            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS agents (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    current_call_id TEXT,
                    locked_by_worker_id TEXT,
                    lease_expires_at REAL,
                    version INTEGER NOT NULL DEFAULT 1,
                    updated_at REAL NOT NULL
                );
            """)

            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS borrowers (
                    id TEXT PRIMARY KEY,
                    campaign_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    phone_number TEXT NOT NULL,
                    status TEXT NOT NULL,
                    debt_amount REAL DEFAULT 0.0,
                    attempts INTEGER DEFAULT 0,
                    max_attempts INTEGER DEFAULT 3,
                    locked_by_worker_id TEXT,
                    lease_expires_at REAL,
                    version INTEGER NOT NULL DEFAULT 1,
                    updated_at REAL NOT NULL
                );
            """)

            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS calls (
                    id TEXT PRIMARY KEY,
                    campaign_id TEXT NOT NULL,
                    borrower_id TEXT NOT NULL,
                    phone_number TEXT NOT NULL,
                    agent_id TEXT,
                    provider_name TEXT,
                    provider_call_id TEXT,
                    state TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    initiated_at REAL,
                    answered_at REAL,
                    connected_at REAL,
                    ended_at REAL,
                    failure_reason TEXT,
                    version INTEGER NOT NULL DEFAULT 1
                );
            """)

            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS call_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    call_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    note TEXT,
                    UNIQUE(call_id, event_id),
                    FOREIGN KEY(call_id) REFERENCES calls(id)
                );
            """)

            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_agents_state ON agents(state);")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_borrowers_status ON borrowers(campaign_id, status);")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_calls_state ON calls(state);")

    def save_agent(self, agent: Agent) -> None:
        with self._lock, self._conn:
            self._conn.execute("""
                INSERT INTO agents (id, name, state, current_call_id, locked_by_worker_id, lease_expires_at, version, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    state = excluded.state,
                    current_call_id = excluded.current_call_id,
                    locked_by_worker_id = excluded.locked_by_worker_id,
                    lease_expires_at = excluded.lease_expires_at,
                    version = excluded.version,
                    updated_at = excluded.updated_at;
            """, (
                agent.id, agent.name, agent.state.value, agent.current_call_id,
                agent.locked_by_worker_id, agent.lease_expires_at, agent.version, agent.updated_at
            ))

    def get_agent(self, agent_id: str) -> Optional[Agent]:
        with self._lock:
            cur = self._conn.execute("SELECT * FROM agents WHERE id = ?", (agent_id,))
            row = cur.fetchone()
            if not row:
                return None
            return Agent(
                id=row["id"],
                name=row["name"],
                state=AgentState(row["state"]),
                current_call_id=row["current_call_id"],
                locked_by_worker_id=row["locked_by_worker_id"],
                lease_expires_at=row["lease_expires_at"],
                version=row["version"],
                updated_at=row["updated_at"]
            )

    def list_available_agents(self, limit: int = 100) -> List[Agent]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM agents WHERE state = ? ORDER BY updated_at ASC LIMIT ?",
                (AgentState.AVAILABLE.value, limit)
            )
            rows = cur.fetchall()
            return [
                Agent(
                    id=r["id"],
                    name=r["name"],
                    state=AgentState(r["state"]),
                    current_call_id=r["current_call_id"],
                    locked_by_worker_id=r["locked_by_worker_id"],
                    lease_expires_at=r["lease_expires_at"],
                    version=r["version"],
                    updated_at=r["updated_at"]
                ) for r in rows
            ]

    def count_agents_by_state(self) -> Dict[AgentState, int]:
        with self._lock:
            cur = self._conn.execute("SELECT state, count(*) as cnt FROM agents GROUP BY state")
            counts = {s: 0 for s in AgentState}
            for row in cur.fetchall():
                try:
                    counts[AgentState(row["state"])] = row["cnt"]
                except ValueError:
                    pass
            return counts

    def reserve_agent_atomic(
        self, agent_id: str, worker_id: str, expected_version: int, lease_duration_sec: float
    ) -> bool:
        """
        Atomic Compare-And-Swap (CAS).
        Ensures that if two workers race to reserve the same agent, only one gets rowcount == 1.
        """
        now = time.time()
        lease_expires = now + lease_duration_sec
        with self._lock, self._conn:
            cur = self._conn.execute("""
                UPDATE agents
                SET state = ?,
                    locked_by_worker_id = ?,
                    lease_expires_at = ?,
                    version = version + 1,
                    updated_at = ?
                WHERE id = ? AND state = ? AND version = ?
            """, (
                AgentState.RESERVED.value,
                worker_id,
                lease_expires,
                now,
                agent_id,
                AgentState.AVAILABLE.value,
                expected_version
            ))
            return cur.rowcount == 1

    def update_agent_state(
        self, agent_id: str, new_state: AgentState, current_call_id: Optional[str] = None
    ) -> bool:
        now = time.time()
        with self._lock, self._conn:
            locked_by = None if new_state in (AgentState.AVAILABLE, AgentState.OFFLINE, AgentState.PAUSED) else None
            lease_exp = None if new_state in (AgentState.AVAILABLE, AgentState.OFFLINE, AgentState.PAUSED) else None
            
            if new_state in (AgentState.AVAILABLE, AgentState.OFFLINE, AgentState.PAUSED):
                cur = self._conn.execute("""
                    UPDATE agents
                    SET state = ?,
                        current_call_id = NULL,
                        locked_by_worker_id = NULL,
                        lease_expires_at = NULL,
                        version = version + 1,
                        updated_at = ?
                    WHERE id = ?
                """, (new_state.value, now, agent_id))
            else:
                cur = self._conn.execute("""
                    UPDATE agents
                    SET state = ?,
                        current_call_id = COALESCE(?, current_call_id),
                        version = version + 1,
                        updated_at = ?
                    WHERE id = ?
                """, (new_state.value, current_call_id, now, agent_id))
            return cur.rowcount == 1

    def save_borrower(self, borrower: Borrower) -> None:
        with self._lock, self._conn:
            self._conn.execute("""
                INSERT INTO borrowers (id, campaign_id, name, phone_number, status, debt_amount, attempts, max_attempts, locked_by_worker_id, lease_expires_at, version, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    phone_number = excluded.phone_number,
                    status = excluded.status,
                    debt_amount = excluded.debt_amount,
                    attempts = excluded.attempts,
                    max_attempts = excluded.max_attempts,
                    locked_by_worker_id = excluded.locked_by_worker_id,
                    lease_expires_at = excluded.lease_expires_at,
                    version = excluded.version,
                    updated_at = excluded.updated_at;
            """, (
                borrower.id, borrower.campaign_id, borrower.name, borrower.phone_number,
                borrower.status.value, borrower.debt_amount, borrower.attempts, borrower.max_attempts,
                borrower.locked_by_worker_id, borrower.lease_expires_at, borrower.version, borrower.updated_at
            ))

    def get_borrower(self, borrower_id: str) -> Optional[Borrower]:
        with self._lock:
            cur = self._conn.execute("SELECT * FROM borrowers WHERE id = ?", (borrower_id,))
            row = cur.fetchone()
            if not row:
                return None
            return Borrower(
                id=row["id"],
                campaign_id=row["campaign_id"],
                name=row["name"],
                phone_number=row["phone_number"],
                status=BorrowerStatus(row["status"]),
                debt_amount=row["debt_amount"],
                attempts=row["attempts"],
                max_attempts=row["max_attempts"],
                locked_by_worker_id=row["locked_by_worker_id"],
                lease_expires_at=row["lease_expires_at"],
                version=row["version"],
                updated_at=row["updated_at"]
            )

    def reserve_next_borrower_atomic(
        self, campaign_id: str, worker_id: str, lease_duration_sec: float
    ) -> Optional[Borrower]:
        """Atomically picks and locks the next pending borrower."""
        now = time.time()
        lease_expires = now + lease_duration_sec
        with self._lock, self._conn:
            cur = self._conn.execute("""
                SELECT id, version FROM borrowers
                WHERE campaign_id = ? AND status = ? AND attempts < max_attempts
                ORDER BY updated_at ASC LIMIT 1
            """, (campaign_id, BorrowerStatus.PENDING.value))
            row = cur.fetchone()
            if not row:
                return None
            borrower_id = row["id"]
            expected_ver = row["version"]

            update_cur = self._conn.execute("""
                UPDATE borrowers
                SET status = ?,
                    locked_by_worker_id = ?,
                    lease_expires_at = ?,
                    attempts = attempts + 1,
                    version = version + 1,
                    updated_at = ?
                WHERE id = ? AND status = ? AND version = ?
            """, (
                BorrowerStatus.RESERVED.value,
                worker_id,
                lease_expires,
                now,
                borrower_id,
                BorrowerStatus.PENDING.value,
                expected_ver
            ))
            if update_cur.rowcount == 1:
                return self.get_borrower(borrower_id)
            return None

    def update_borrower_status(self, borrower_id: str, status: BorrowerStatus) -> bool:
        now = time.time()
        with self._lock, self._conn:
            cur = self._conn.execute("""
                UPDATE borrowers
                SET status = ?,
                    locked_by_worker_id = NULL,
                    lease_expires_at = NULL,
                    version = version + 1,
                    updated_at = ?
                WHERE id = ?
            """, (status.value, now, borrower_id))
            return cur.rowcount == 1

    def count_pending_borrowers(self, campaign_id: str) -> int:
        with self._lock:
            cur = self._conn.execute(
                "SELECT count(*) as cnt FROM borrowers WHERE campaign_id = ? AND status = ? AND attempts < max_attempts",
                (campaign_id, BorrowerStatus.PENDING.value)
            )
            row = cur.fetchone()
            return row["cnt"] if row else 0

    def save_call(self, call: Call) -> None:
        with self._lock, self._conn:
            self._conn.execute("""
                INSERT INTO calls (id, campaign_id, borrower_id, phone_number, agent_id, provider_name, provider_call_id, state, created_at, initiated_at, answered_at, connected_at, ended_at, failure_reason, version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    agent_id = excluded.agent_id,
                    provider_name = excluded.provider_name,
                    provider_call_id = excluded.provider_call_id,
                    state = excluded.state,
                    initiated_at = excluded.initiated_at,
                    answered_at = excluded.answered_at,
                    connected_at = excluded.connected_at,
                    ended_at = excluded.ended_at,
                    failure_reason = excluded.failure_reason,
                    version = excluded.version;
            """, (
                call.id, call.campaign_id, call.borrower_id, call.phone_number,
                call.agent_id, call.provider_name, call.provider_call_id, call.state.value,
                call.created_at, call.initiated_at, call.answered_at, call.connected_at,
                call.ended_at, call.failure_reason, call.version
            ))

    def get_call(self, call_id: str) -> Optional[Call]:
        with self._lock:
            cur = self._conn.execute("SELECT * FROM calls WHERE id = ?", (call_id,))
            row = cur.fetchone()
            if not row:
                return None
            
            # Fetch event history
            ev_cur = self._conn.execute("SELECT * FROM call_events WHERE call_id = ? ORDER BY timestamp ASC", (call_id,))
            history = [
                CallEventRecord(
                    event_id=er["event_id"],
                    state=CallState(er["state"]),
                    timestamp=er["timestamp"],
                    note=er["note"]
                ) for er in ev_cur.fetchall()
            ]
            processed_event_ids = {h.event_id for h in history}

            return Call(
                id=row["id"],
                campaign_id=row["campaign_id"],
                borrower_id=row["borrower_id"],
                phone_number=row["phone_number"],
                agent_id=row["agent_id"],
                provider_name=row["provider_name"],
                provider_call_id=row["provider_call_id"],
                state=CallState(row["state"]),
                created_at=row["created_at"],
                initiated_at=row["initiated_at"],
                answered_at=row["answered_at"],
                connected_at=row["connected_at"],
                ended_at=row["ended_at"],
                failure_reason=row["failure_reason"],
                version=row["version"],
                processed_event_ids=processed_event_ids,
                history=history
            )

    def list_active_calls(self, campaign_id: Optional[str] = None) -> List[Call]:
        with self._lock:
            terminals = tuple(s.value for s in TERMINAL_CALL_STATES)
            query = f"SELECT id FROM calls WHERE state NOT IN ({','.join(['?']*len(terminals))})"
            params: list[Any] = list(terminals)
            if campaign_id:
                query += " AND campaign_id = ?"
                params.append(campaign_id)
            cur = self._conn.execute(query, params)
            call_ids = [r["id"] for r in cur.fetchall()]
        return [self.get_call(cid) for cid in call_ids if self.get_call(cid) is not None]  # type: ignore

    def record_call_event_idempotent(
        self, call_id: str, event_id: str, new_state: CallState, note: Optional[str] = None
    ) -> bool:
        """
        Idempotently applies the event to call state machine and records in call_events.
        If duplicate event arrives, it is safely ignored.
        """
        now = time.time()
        with self._lock, self._conn:
            cur = self._conn.execute(
                "SELECT 1 FROM call_events WHERE call_id = ? AND event_id = ?",
                (call_id, event_id)
            )
            if cur.fetchone() is not None:
                return False

            call = self.get_call(call_id)
            if not call:
                return False

            applied = call.apply_event(event_id, new_state, note)
            if applied:
                self.save_call(call)
                self._conn.execute("""
                    INSERT INTO call_events (call_id, event_id, state, timestamp, note)
                    VALUES (?, ?, ?, ?, ?)
                """, (call_id, event_id, new_state.value, now, note))
                return True
            else:
                try:
                    self._conn.execute("""
                        INSERT INTO call_events (call_id, event_id, state, timestamp, note)
                        VALUES (?, ?, ?, ?, ?)
                    """, (call_id, event_id, new_state.value, now, note))
                except sqlite3.IntegrityError:
                    pass
                return False

    def recover_expired_leases(self, now: float) -> Dict[str, int]:
        """
        CRITICAL FOR WORKER CRASH RECOVERY:
        Finds any agents or borrowers reserved with an expired lease (worker crashed),
        and safely reconciles them back to healthy available states.
        """
        reconciled = {"agents": 0, "borrowers": 0, "orphaned_calls": 0}
        with self._lock, self._conn:
            # 1. Agents stuck in RESERVED or DIALING past lease expiration
            cur = self._conn.execute("""
                UPDATE agents
                SET state = ?,
                    locked_by_worker_id = NULL,
                    lease_expires_at = NULL,
                    current_call_id = NULL,
                    version = version + 1,
                    updated_at = ?
                WHERE state IN (?, ?) AND lease_expires_at IS NOT NULL AND lease_expires_at < ?
            """, (
                AgentState.AVAILABLE.value,
                now,
                AgentState.RESERVED.value,
                AgentState.DIALING.value,
                now
            ))
            reconciled["agents"] = cur.rowcount

            # 2. Borrowers stuck in RESERVED past lease
            cur_b = self._conn.execute("""
                UPDATE borrowers
                SET status = ?,
                    locked_by_worker_id = NULL,
                    lease_expires_at = NULL,
                    version = version + 1,
                    updated_at = ?
                WHERE status = ? AND lease_expires_at IS NOT NULL AND lease_expires_at < ?
            """, (
                BorrowerStatus.PENDING.value,
                now,
                BorrowerStatus.RESERVED.value,
                now
            ))
            reconciled["borrowers"] = cur_b.rowcount

            # 3. Calls stuck in INITIATED with no activity
            # (handled by caller checking active calls if needed)

        return reconciled
