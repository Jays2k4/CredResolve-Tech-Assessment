from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Optional, Awaitable, List
from enum import Enum
import time


class TelecomEventType(str, Enum):
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass
class TelecomWebhookEvent:
    event_id: str
    call_id: str
    provider_name: str
    event_type: TelecomEventType
    timestamp: float = field(default_factory=time.time)
    duration_sec: Optional[float] = None
    error_message: Optional[str] = None


@dataclass
class CallInitiateResult:
    success: bool
    provider_call_id: Optional[str] = None
    error_message: Optional[str] = None


WebhookHandler = Callable[[TelecomWebhookEvent], Awaitable[None]]


class TelecomProvider(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def register_webhook_handler(self, handler: WebhookHandler) -> None:
        """Register the callback handler that receives call lifecycle webhooks."""
        pass

    @abstractmethod
    async def initiate_call(
        self, call_id: str, phone_number: str, simulated_answer: bool = True, talk_time_sec: float = 60.0
    ) -> CallInitiateResult:
        """Initiates an outbound call to the telecom network."""
        pass

    @abstractmethod
    async def hangup(self, provider_call_id: str) -> bool:
        """Terminates an ongoing call."""
        pass
