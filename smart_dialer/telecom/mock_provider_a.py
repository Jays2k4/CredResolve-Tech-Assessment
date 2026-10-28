from __future__ import annotations
import asyncio
import uuid
import time
from typing import Optional, List
from .provider import TelecomProvider, WebhookHandler, CallInitiateResult, TelecomWebhookEvent, TelecomEventType


class MockProviderA(TelecomProvider):
    """
    Provider A:
    - Fast response times
    - High reliability (low failure rate ~2%)
    - Strictly ordered, clean webhook events
    """

    def __init__(self, failure_rate: float = 0.02, min_latency: float = 0.01, max_latency: float = 0.03):
        self._name = "Provider_A_Tier1"
        self.failure_rate = failure_rate
        self.min_latency = min_latency
        self.max_latency = max_latency
        self._webhook_handlers: List[WebhookHandler] = []
        self._active_calls: set[str] = set()

    @property
    def name(self) -> str:
        return self._name

    def register_webhook_handler(self, handler: WebhookHandler) -> None:
        self._webhook_handlers.append(handler)

    async def _emit(self, event: TelecomWebhookEvent) -> None:
        for handler in self._webhook_handlers:
            asyncio.create_task(handler(event))

    async def initiate_call(
        self, call_id: str, phone_number: str, simulated_answer: bool = True, talk_time_sec: float = 2.0
    ) -> CallInitiateResult:
        # Simulate network latency
        await asyncio.sleep(self.min_latency)
        provider_call_id = f"pra_{uuid.uuid4().hex[:10]}"

        # Check failure rate
        import random
        if random.random() < self.failure_rate:
            # Immediate network refusal / sip error
            asyncio.create_task(self._emit(TelecomWebhookEvent(
                event_id=f"evt_{uuid.uuid4().hex[:8]}",
                call_id=call_id,
                provider_name=self.name,
                event_type=TelecomEventType.FAILED,
                error_message="SIP 503 Service Unavailable"
            )))
            return CallInitiateResult(success=False, error_message="Carrier network error")

        self._active_calls.add(provider_call_id)
        # Asynchronously simulate telecom lifecycle in proper order
        asyncio.create_task(self._simulate_call_lifecycle(call_id, provider_call_id, simulated_answer, talk_time_sec))
        return CallInitiateResult(success=True, provider_call_id=provider_call_id)

    async def _simulate_call_lifecycle(
        self, call_id: str, provider_call_id: str, simulated_answer: bool, talk_time_sec: float
    ) -> None:
        # 1. Ringing
        await asyncio.sleep(0.05)
        if provider_call_id not in self._active_calls:
            return
        await self._emit(TelecomWebhookEvent(
            event_id=f"evt_{uuid.uuid4().hex[:8]}",
            call_id=call_id,
            provider_name=self.name,
            event_type=TelecomEventType.RINGING
        ))

        # 2. Answered or No Answer
        await asyncio.sleep(0.05)
        if provider_call_id not in self._active_calls:
            return

        if not simulated_answer:
            # Borrower didn't answer / busy
            await self._emit(TelecomWebhookEvent(
                event_id=f"evt_{uuid.uuid4().hex[:8]}",
                call_id=call_id,
                provider_name=self.name,
                event_type=TelecomEventType.FAILED,
                error_message="NO_ANSWER"
            ))
            self._active_calls.discard(provider_call_id)
            return

        await self._emit(TelecomWebhookEvent(
            event_id=f"evt_{uuid.uuid4().hex[:8]}",
            call_id=call_id,
            provider_name=self.name,
            event_type=TelecomEventType.ANSWERED
        ))

        # 3. In-call talk duration
        await asyncio.sleep(talk_time_sec)
        if provider_call_id in self._active_calls:
            self._active_calls.discard(provider_call_id)
            await self._emit(TelecomWebhookEvent(
                event_id=f"evt_{uuid.uuid4().hex[:8]}",
                call_id=call_id,
                provider_name=self.name,
                event_type=TelecomEventType.COMPLETED,
                duration_sec=talk_time_sec
            ))

    async def hangup(self, provider_call_id: str) -> bool:
        if provider_call_id in self._active_calls:
            self._active_calls.discard(provider_call_id)
            return True
        return False
