from __future__ import annotations
import time
from enum import Enum
from dataclasses import dataclass
from typing import Optional


class CircuitState(str, Enum):
    CLOSED = "CLOSED"       # Healthy: full traffic permitted
    OPEN = "OPEN"           # Outage: fast-fail or switch to fallback
    HALF_OPEN = "HALF_OPEN" # Probing recovery with small canary traffic


@dataclass
class CircuitBreakerStats:
    state: CircuitState
    consecutive_failures: int
    consecutive_successes: int
    last_failure_time: Optional[float]
    total_calls: int
    failed_calls: int


class TelecomCircuitBreaker:
    """
    Monitors telecom provider reliability.
    Protects the dialer from burning borrowers during telecom carrier outages.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_time_sec: float = 10.0,
        success_threshold_to_close: int = 3,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_time_sec = recovery_time_sec
        self.success_threshold_to_close = success_threshold_to_close

        self.state = CircuitState.CLOSED
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self.last_failure_time: Optional[float] = None
        self.total_calls = 0
        self.failed_calls = 0

    def allow_call(self) -> bool:
        now = time.time()
        if self.state == CircuitState.CLOSED:
            return True
        elif self.state == CircuitState.OPEN:
            if self.last_failure_time and (now - self.last_failure_time >= self.recovery_time_sec):
                self.state = CircuitState.HALF_OPEN
                self.consecutive_successes = 0
                return True
            return False
        elif self.state == CircuitState.HALF_OPEN:
            # Allow limited canary calls
            return True
        return False

    def record_success(self) -> None:
        self.total_calls += 1
        self.consecutive_failures = 0
        if self.state == CircuitState.HALF_OPEN:
            self.consecutive_successes += 1
            if self.consecutive_successes >= self.success_threshold_to_close:
                self.state = CircuitState.CLOSED
                self.consecutive_successes = 0

    def record_failure(self) -> None:
        self.total_calls += 1
        self.failed_calls += 1
        self.consecutive_failures += 1
        self.last_failure_time = time.time()

        if self.state in (CircuitState.CLOSED, CircuitState.HALF_OPEN):
            if self.consecutive_failures >= self.failure_threshold:
                self.state = CircuitState.OPEN

    @property
    def is_healthy(self) -> bool:
        return self.state == CircuitState.CLOSED

    def get_stats(self) -> CircuitBreakerStats:
        return CircuitBreakerStats(
            state=self.state,
            consecutive_failures=self.consecutive_failures,
            consecutive_successes=self.consecutive_successes,
            last_failure_time=self.last_failure_time,
            total_calls=self.total_calls,
            failed_calls=self.failed_calls,
        )
