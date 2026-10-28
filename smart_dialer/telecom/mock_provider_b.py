from __future__ import annotations
import asyncio
import uuid
import random
import time
from typing import Optional, List
from .provider import TelecomProvider, WebhookHandler, CallInitiateResult, TelecomWebhookEvent, TelecomEventType


class MockProviderB(TelecomProvider):
    """
    Provider B:
    - Slower network latency
    - Occasional timeouts
    - Emits duplicate events (e.g. duplicate ANSWERED)
    - Emits out-of-order events (e.g. COMPLETED arriving before ANSWERED)
    """

    def __init__(
        self,
        timeout_rate: float = 0.10,
        duplicate_event_rate: float = 0.20,
        out_of_order_rate: float = 0.20,
        min_latency: float = 0.05,
        max_latency: float = 0.15,
    ):
        self._name = "Provider_B_BudgetRoutes"
        self.timeout_rate = timeout_rate
        self.duplicate_event_rate = duplicate_event_rate
        self.out_of_order_rate = out_of_order_rate
        self.min_latency = min_latency
        self.max_latency = max_latency
        self._webhook_handlers: List[WebhookHandler] = []
        self._active_calls: set[str] = set()

    @property
    def name(self) -> str:
        return self._name

    def register_webhook_handler(self, handler: WebhookHandler) -> None:
        self._webhook_handlers.append(handler)

    async def _emit(self, event: TelecomWebhookEvent, duplicate_count: int = 1) -> None:
        for _ in range(duplicate_count):
            for handler in self._webhook_handlers:
                asyncio.create_task(handler(event))

    async def initiate_call(
        self, call_id: str, phone_number: str, simulated_answer: bool = True, talk_time_sec: float = 2.0
    ) -> CallInitiateResult:
        delay = random.uniform(self.min_latency, self.max_latency)
        await asyncio.sleep(delay)

        # 1. Simulate occasional network timeout
        if random.random() < self.timeout_rate:
            return CallInitiateResult(success=False, error_message="HTTP 504 Gateway Timeout")

        provider_call_id = f"prb_{uuid.uuid4().hex[:10]}"
        self._active_calls.add(provider_call_id)

        asyncio.create_task(self._simulate_chaotic_lifecycle(call_id, provider_call_id, simulated_answer, talk_time_sec))
        return CallInitiateResult(success=True, provider_call_id=provider_call_id)

    async def _simulate_chaotic_lifecycle(
        self, call_id: str, provider_call_id: str, simulated_answer: bool, talk_time_sec: float
    ) -> None:
        # Check if we should simulate out-of-order events
        is_out_of_order = random.random() < self.out_of_order_rate and simulated_answer

        if is_out_of_order:
            # CHAOTIC CASE: Emit COMPLETED event FIRST, then late ANSWERED or RINGING
            completed_evt_id = f"evt_comp_{uuid.uuid4().hex[:6]}"
            await asyncio.sleep(0.04)
            await self._emit(TelecomWebhookEvent(
                event_id=completed_evt_id,
                call_id=call_id,
                provider_name=self.name,
                event_type=TelecomEventType.COMPLETED,
                duration_sec=talk_time_sec
            ))
            # Delayed ANSWERED event arriving later
            await asyncio.sleep(0.03)
            await self._emit(TelecomWebhookEvent(
                event_id=f"evt_ans_late_{uuid.uuid4().hex[:6]}",
                call_id=call_id,
                provider_name=self.name,
                event_type=TelecomEventType.ANSWERED
            ))
            self._active_calls.discard(provider_call_id)
            return

        # Normal sequence with duplicate injection
        # 1. Ringing
        await asyncio.sleep(random.uniform(0.02, 0.06))
        if provider_call_id not in self._active_calls:
            return
        
        await self._emit(TelecomWebhookEvent(
            event_id=f"evt_rng_{uuid.uuid4().hex[:6]}",
            call_id=call_id,
            provider_name=self.name,
            event_type=TelecomEventType.RINGING
        ))

        # 2. Answered or No Answer
        await asyncio.sleep(random.uniform(0.03, 0.08))
        if provider_call_id not in self._active_calls:
            return

        if not simulated_answer:
            await self._emit(TelecomWebhookEvent(
                event_id=f"evt_fail_{uuid.uuid4().hex[:6]}",
                call_id=call_id,
                provider_name=self.name,
                event_type=TelecomEventType.FAILED,
                error_message="BUSY_OR_UNANSWERED"
            ))
            self._active_calls.discard(provider_call_id)
            return

        # ANSWERED event (potentially duplicated 2x or 3x)
        ans_evt_id = f"evt_ans_{uuid.uuid4().hex[:6]}"
        dup_count = 3 if (random.random() < self.duplicate_event_rate) else 1
        await self._emit(TelecomWebhookEvent(
            event_id=ans_evt_id,
            call_id=call_id,
            provider_name=self.name,
            event_type=TelecomEventType.ANSWERED
        ), duplicate_count=dup_count)

        # 3. Call conversation
        await asyncio.sleep(talk_time_sec)
        if provider_call_id in self._active_calls:
            self._active_calls.discard(provider_call_id)
            await self._emit(TelecomWebhookEvent(
                event_id=f"evt_comp_{uuid.uuid4().hex[:6]}",
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
