# WHY THIS DESIGN:
# The ResilientEventHandler manages the external chaos of telecom carriers.
# Real telecom providers deliver duplicate webhooks, out-of-order events (e.g. COMPLETED
# arriving before ANSWERED), and abrupt disconnects.
# This handler enforces idempotency via database unique constraints and monotonic
# progression ranks, ensuring the system state never regresses.
# Upon borrower pickup, it bridges to reserved agents or allocates from the available pool,
# allowing a brief grace interval for wrapping-up agents before flagging compliance abandonment.
from __future__ import annotations
import time
import asyncio
import logging
from typing import Optional
from ..models.agent import AgentState
from ..models.call import CallState, Call
from ..models.borrower import BorrowerStatus
from ..storage.base import StorageInterface
from ..telecom.provider import TelecomWebhookEvent, TelecomEventType
from ..telecom.circuit_breaker import TelecomCircuitBreaker

logger = logging.getLogger(__name__)


class ResilientEventHandler:
    """
    Handles incoming telecom webhooks with idempotency, out-of-order resilience,
    and agent/call synchronization.
    """

    def __init__(
        self,
        storage: StorageInterface,
        circuit_breaker: Optional[TelecomCircuitBreaker] = None,
        wrap_up_duration_sec: float = 0.05,
    ):
        self.storage = storage
        self.circuit_breaker = circuit_breaker
        self.wrap_up_duration_sec = wrap_up_duration_sec
        self.abandoned_calls_count = 0
        self.total_events_processed = 0
        self.duplicate_events_ignored = 0
        self.out_of_order_events_handled = 0

    async def handle_webhook(self, event: TelecomWebhookEvent) -> None:
        self.total_events_processed += 1
        call_id = event.call_id
        call = self.storage.get_call(call_id)
        if not call:
            logger.warning(f"Webhook received for unknown call {call_id}")
            return

        # 1. Map Telecom Event to CallState
        if event.event_type == TelecomEventType.RINGING:
            target_state = CallState.RINGING
        elif event.event_type == TelecomEventType.ANSWERED:
            target_state = CallState.ANSWERED
        elif event.event_type == TelecomEventType.COMPLETED:
            target_state = CallState.COMPLETED
        elif event.event_type == TelecomEventType.FAILED:
            target_state = CallState.FAILED
        else:
            return

        # 2. Record Event Idempotently in Storage
        applied = self.storage.record_call_event_idempotent(
            call_id=call_id,
            event_id=event.event_id,
            new_state=target_state,
            note=event.error_message or f"Event from {event.provider_name}",
        )

        if not applied:
            # Duplicate or out-of-order regressive event
            self.duplicate_events_ignored += 1
            return

        # Reload updated call
        call = self.storage.get_call(call_id)
        if not call:
            return

        # 3. Synchronize Agent State Machine
        if target_state == CallState.RINGING:
            if call.agent_id:
                agent = self.storage.get_agent(call.agent_id)
                if agent and agent.state == AgentState.RESERVED:
                    self.storage.update_agent_state(agent.id, AgentState.DIALING, current_call_id=call.id)

        elif target_state == CallState.ANSWERED:
            if self.circuit_breaker:
                self.circuit_breaker.record_success()

            # Allocate or bridge to agent
            assigned_agent_id = call.agent_id
            if not assigned_agent_id:
                # Predictive mode: borrower picked up, allocate next available agent atomically
                # Retry across brief intervals (up to 0.2s) to allow agents in wrap-up to complete
                for attempt in range(6):
                    available_agents = self.storage.list_available_agents(limit=5)
                    for cand in available_agents:
                        if self.storage.reserve_agent_atomic(cand.id, "allocator", cand.version, 60.0):
                            assigned_agent_id = cand.id
                            call.agent_id = assigned_agent_id
                            self.storage.save_call(call)
                            break
                    if assigned_agent_id:
                        break
                    await asyncio.sleep(0.03)

            if assigned_agent_id:
                # Successfully bridged to agent
                self.storage.update_agent_state(assigned_agent_id, AgentState.CONNECTED, current_call_id=call.id)
                self.storage.record_call_event_idempotent(
                    call.id, f"internal_bridge_{event.event_id}", CallState.CONNECTED, "Agent bridged"
                )
            else:
                # CRITICAL COMPLIANCE FAILURE: Abandoned call!
                self.abandoned_calls_count += 1
                logger.error(f"COMPLIANCE ALERT: Call {call.id} answered but NO agent available! Abandoning call.")
                self.storage.record_call_event_idempotent(
                    call.id, f"abandon_{event.event_id}", CallState.FAILED, "ABANDONED_NO_AGENT_AVAILABLE"
                )

        elif target_state == CallState.COMPLETED:
            # Call finished talking
            if call.agent_id:
                agent = self.storage.get_agent(call.agent_id)
                if agent:
                    # Transition to WRAP_UP
                    self.storage.update_agent_state(agent.id, AgentState.WRAP_UP)
                    # Automatically release back to AVAILABLE after wrap-up
                    asyncio.create_task(self._release_agent_after_wrapup(agent.id))

            # Mark borrower as contacted
            self.storage.update_borrower_status(call.borrower_id, BorrowerStatus.CONTACTED)

        elif target_state == CallState.FAILED:
            if self.circuit_breaker and event.error_message and any(
                err in event.error_message for err in ("SIP", "Timeout", "503", "504", "Gateway", "Carrier")
            ):
                self.circuit_breaker.record_failure()

            # Release reserved agent if any
            if call.agent_id:
                self.storage.update_agent_state(call.agent_id, AgentState.AVAILABLE)

            # Mark borrower for retry or unreachable
            borrower = self.storage.get_borrower(call.borrower_id)
            if borrower:
                if borrower.attempts >= borrower.max_attempts:
                    self.storage.update_borrower_status(borrower.id, BorrowerStatus.UNREACHABLE)
                else:
                    self.storage.update_borrower_status(borrower.id, BorrowerStatus.PENDING)

    async def _release_agent_after_wrapup(self, agent_id: str) -> None:
        await asyncio.sleep(self.wrap_up_duration_sec)
        agent = self.storage.get_agent(agent_id)
        if agent and agent.state == AgentState.WRAP_UP:
            self.storage.update_agent_state(agent_id, AgentState.AVAILABLE)
