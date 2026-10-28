from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import time


class BorrowerStatus(str, Enum):
    PENDING = "PENDING"
    RESERVED = "RESERVED"
    IN_PROGRESS = "IN_PROGRESS"
    CONTACTED = "CONTACTED"
    UNREACHABLE = "UNREACHABLE"


@dataclass
class Borrower:
    id: str
    campaign_id: str
    name: str
    phone_number: str
    status: BorrowerStatus = BorrowerStatus.PENDING
    debt_amount: float = 0.0
    attempts: int = 0
    max_attempts: int = 3
    locked_by_worker_id: Optional[str] = None
    lease_expires_at: Optional[float] = None
    version: int = 1
    updated_at: float = field(default_factory=time.time)
