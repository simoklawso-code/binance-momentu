"""Adapts PriorityRingBuffer to the EventSink interface the exchange
adapters depend on, so ingestion code never imports buffer internals
directly (§5: ingestion must not perform strategy/heavy calculations or
know about storage — and, symmetrically, shouldn't be coupled to one
specific buffer implementation either)."""

from __future__ import annotations

import logging

from app.buffer.ring_buffer import BufferOverloadError, PriorityRingBuffer
from app.domain.events import DomainEvent
from app.exchanges.base import EventSink

logger = logging.getLogger(__name__)


class RingBufferSink(EventSink):
    def __init__(self, buffer: PriorityRingBuffer) -> None:
        self._buffer = buffer

    def push(self, event: DomainEvent) -> bool:
        try:
            return self._buffer.push(event)
        except BufferOverloadError:
            # §10: P0 overload is a protective signal, not a crash.
            # The ingestion manager's monitoring loop observes buffer
            # state and is responsible for the DATA_OVERLOAD response —
            # this sink just ensures a raised overload never propagates
            # as an unhandled exception into the WebSocket receive loop.
            logger.critical("Buffer P0 overload — event dropped at sink boundary")
            return False
