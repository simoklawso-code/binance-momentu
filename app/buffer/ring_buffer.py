"""
Priority-Based Lossy Ring Buffer (§9, §10, §11).

Bounded capacity (default 10,000 events). Above the pressure watermark,
enters LOSSY_MODE: drop P3 first, then P2 if still necessary, then
(only under emergency pressure) controlled P1 degradation. P0 is never
silently dropped. If P0 alone threatens unbounded growth, the buffer
raises DATA_OVERLOAD / INGESTION_DEGRADED instead of silently expanding
or dropping P0 (§10).

Closed Klines (KlineEvent.is_closed == True) are P0 by construction
(assigned at the normalization layer, not here) and are therefore
covered by the "P0 never silently dropped" guarantee (§11). This module
does not special-case klines directly — it only enforces the priority
contract; the normalizer is responsible for assigning P0 correctly.

Safety principle honored here: DATA LOSS > SIGNAL GENERATION. This
buffer never blocks the ingestion loop and never crashes on overflow —
it degrades in a documented, observable way.
"""

from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from app.domain.enums import EventPriority, IngestionState
from app.domain.events import DomainEvent

logger = logging.getLogger(__name__)


@dataclass
class BufferMetrics:
    capacity: int
    size: int = 0
    utilization: float = 0.0
    state: IngestionState = IngestionState.NORMAL
    counts_by_priority: dict[EventPriority, int] = field(default_factory=dict)
    dropped_total: int = 0
    dropped_by_priority: dict[EventPriority, int] = field(default_factory=dict)
    p0_protected_overflow_events: int = 0


class BufferOverloadError(Exception):
    """Raised (and caught by the caller) when P0 pressure alone would
    require unbounded growth. The caller must translate this into a
    protective NO_TRADE / DATA_OVERLOAD system state (§10) rather than
    treating it as a normal exception."""


class PriorityRingBuffer:
    """Thread-safe, priority-aware, bounded event buffer.

    Not asyncio-native on purpose: the ingestion WebSocket loops run in
    dedicated threads (§44), so this buffer is protected with a plain
    lock rather than an asyncio.Queue. The Main Async Event Loop drains
    it via `pop_batch()` from a single consumer coroutine wrapped in
    `run_in_executor`, or a dedicated drain thread — see
    app/ingestion/ingestion_manager.py.
    """

    def __init__(
        self,
        capacity: int = 10_000,
        pressure_high_watermark: float = 0.80,
        p0_overload_watermark: float = 0.98,
        recovery_watermark: float = 0.60,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._high_watermark = pressure_high_watermark
        self._p0_overload_watermark = p0_overload_watermark
        self._recovery_watermark = recovery_watermark

        self._queues: dict[EventPriority, deque[DomainEvent]] = {
            p: deque() for p in EventPriority
        }
        self._lock = threading.RLock()
        self._state = IngestionState.NORMAL

        self._dropped_total = 0
        self._dropped_by_priority: dict[EventPriority, int] = {p: 0 for p in EventPriority}
        self._p0_protected_overflow_events = 0

    # -- capacity / state -----------------------------------------------

    @property
    def size(self) -> int:
        with self._lock:
            return sum(len(q) for q in self._queues.values())

    @property
    def utilization(self) -> float:
        return self.size / self._capacity

    @property
    def state(self) -> IngestionState:
        with self._lock:
            return self._state

    # -- push -------------------------------------------------------------

    def push(self, event: DomainEvent) -> bool:
        """Push one event. Returns True if accepted, False if dropped.

        Never raises for normal pressure — degrades per §10. Raises
        BufferOverloadError only in the true P0-overflow emergency case,
        so the caller can trigger DATA_OVERLOAD / INGESTION_DEGRADED and
        a protective NO_TRADE state upstream.
        """
        with self._lock:
            current_size = self.size

            if current_size < self._capacity:
                self._queues[event.priority].append(event)
                self._update_state_locked()
                return True

            # Buffer is full — apply lossy-mode drop policy (§10).
            if self._drop_lower_priority_locked(event.priority):
                self._queues[event.priority].append(event)
                self._update_state_locked()
                return True

            # Could not free space by dropping lower/equal priority.
            if event.priority == EventPriority.P0:
                # P0 must never be silently dropped. Raise a controlled
                # overload signal instead of expanding unbounded or
                # dropping silently.
                self._p0_protected_overflow_events += 1
                self._state = IngestionState.DATA_OVERLOAD
                logger.critical(
                    "P0 overflow: buffer full of P0/P1 events, cannot admit "
                    "new P0 event %s/%s without unbounded growth",
                    event.exchange, event.symbol,
                )
                raise BufferOverloadError(
                    f"P0 event for {event.exchange}:{event.symbol} could not "
                    "be admitted without unbounded buffer growth"
                )

            # Non-P0 event with nothing left to evict: drop it.
            self._dropped_total += 1
            self._dropped_by_priority[event.priority] += 1
            self._update_state_locked()
            return False

    def _drop_lower_priority_locked(self, incoming_priority: EventPriority) -> bool:
        """Attempt to free exactly one slot by dropping the oldest event
        from the lowest-priority non-empty queue that is strictly lower
        priority (larger enum value) than nothing is dropped to admit P0/P1
        beyond documented emergency rules. Returns True if a slot was freed.

        Drop order (§10): P3 first, then P2, then — only if the incoming
        event is P0 and P1 must be sacrificed under emergency pressure —
        the oldest P1. P0 itself is never chosen for eviction here.
        """
        drop_order = [EventPriority.P3, EventPriority.P2]
        for p in drop_order:
            if self._queues[p]:
                self._queues[p].popleft()
                self._dropped_total += 1
                self._dropped_by_priority[p] += 1
                return True

        # Emergency degradation: only sacrifice P1 to admit a P0 event,
        # and only once P2/P3 are already exhausted.
        if incoming_priority == EventPriority.P0 and self._queues[EventPriority.P1]:
            self._queues[EventPriority.P1].popleft()
            self._dropped_total += 1
            self._dropped_by_priority[EventPriority.P1] += 1
            logger.warning("Emergency degradation: dropped P1 event to admit P0 event")
            return True

        return False

    def _update_state_locked(self) -> None:
        utilization = self.size / self._capacity
        if self._state == IngestionState.DATA_OVERLOAD:
            # Only recover from DATA_OVERLOAD via explicit acknowledge_recovery()
            # (see below) — never silently, since a caller may need to
            # replay/verify data integrity first.
            return
        if utilization >= self._p0_overload_watermark:
            self._state = IngestionState.LOSSY_MODE
        elif utilization >= self._high_watermark:
            self._state = IngestionState.LOSSY_MODE
        elif utilization <= self._recovery_watermark:
            self._state = IngestionState.NORMAL

    def acknowledge_recovery(self) -> None:
        """Explicit transition out of DATA_OVERLOAD once the caller has
        verified pressure has normalized (§10 step 6: "recover
        automatically when pressure normalizes" — implemented as an
        explicit call from the ingestion manager's monitoring loop so the
        recovery decision is observable and testable, not implicit)."""
        with self._lock:
            if self.utilization <= self._recovery_watermark:
                self._state = IngestionState.NORMAL
                logger.info("Buffer pressure normalized — leaving DATA_OVERLOAD/LOSSY_MODE")

    # -- pop ----------------------------------------------------------------

    def pop_batch(self, max_items: int = 500) -> list[DomainEvent]:
        """Drain up to max_items, highest priority first (P0 before P1
        before P2 before P3), oldest-first within a priority class."""
        out: list[DomainEvent] = []
        with self._lock:
            for p in (EventPriority.P0, EventPriority.P1, EventPriority.P2, EventPriority.P3):
                q = self._queues[p]
                while q and len(out) < max_items:
                    out.append(q.popleft())
            self._update_state_locked()
        return out

    def pop_one(self) -> Optional[DomainEvent]:
        batch = self.pop_batch(max_items=1)
        return batch[0] if batch else None

    # -- metrics --------------------------------------------------------------

    def metrics(self) -> BufferMetrics:
        with self._lock:
            counts = {p: len(self._queues[p]) for p in EventPriority}
            return BufferMetrics(
                capacity=self._capacity,
                size=sum(counts.values()),
                utilization=sum(counts.values()) / self._capacity,
                state=self._state,
                counts_by_priority=counts,
                dropped_total=self._dropped_total,
                dropped_by_priority=dict(self._dropped_by_priority),
                p0_protected_overflow_events=self._p0_protected_overflow_events,
            )
