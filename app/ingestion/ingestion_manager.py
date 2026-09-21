"""
Ingestion Manager (§5, §43 layer 1, §44 concurrency).

Owns the exchange adapters and the Ring Buffer, and runs the "Main
Async Event Loop" consumer that drains normalized events. Phase 1's
consumer is intentionally minimal — it only counts/logs events and
protects buffer overload — because Phase 1 explicitly excludes the
Feature Engine, Signal Engine, Claude, and storage (§61 Phase 1 scope).
Later phases plug their own consumers in without touching this class's
adapter/buffer wiring.
"""

from __future__ import annotations

import asyncio
import logging

from app.buffer.ring_buffer import PriorityRingBuffer
from app.buffer.sink import RingBufferSink
from app.config.settings import Settings
from app.core.clock import ClockSyncChecker, NoOpClockSyncChecker
from app.domain.enums import IngestionState
from app.exchanges.base import ExchangeAdapter
from app.exchanges.binance.adapter import BinanceAdapter
from app.exchanges.bybit.adapter import BybitAdapter

logger = logging.getLogger(__name__)


class IngestionManager:
    def __init__(
        self,
        settings: Settings,
        clock_checker: ClockSyncChecker | None = None,
        consume_internally: bool = True,
    ) -> None:
        """`consume_internally=False` is used when an external consumer
        (e.g. SignalPipeline) already drains `self.buffer` — running
        BOTH IngestionManager's own placeholder consumer AND an
        external one on the same buffer would race them against each
        other for events (§44: exactly one Main Async Event Loop
        consumer per buffer, not two). Metrics logging still works
        either way since it reads buffer.metrics()/adapter metrics
        directly, not the internal consumption counter."""
        self._settings = settings
        self._buffer = PriorityRingBuffer(
            capacity=settings.buffer.capacity,
            pressure_high_watermark=settings.buffer.pressure_high_watermark,
            p0_overload_watermark=settings.buffer.p0_overload_watermark,
            recovery_watermark=settings.buffer.recovery_watermark,
        )
        self._sink = RingBufferSink(self._buffer)
        self._clock_checker = clock_checker or NoOpClockSyncChecker()

        self.binance = BinanceAdapter(settings.binance)
        self.bybit = BybitAdapter(settings.bybit)
        self._adapters: list[ExchangeAdapter] = [self.binance, self.bybit]
        self._consume_internally = consume_internally

        self._consumer_task: asyncio.Task | None = None
        self._monitor_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

        self._events_consumed_total = 0
        self._events_consumed_last_interval = 0

    @property
    def buffer(self) -> PriorityRingBuffer:
        return self._buffer

    async def start(self) -> None:
        symbols = list(self._settings.universe.static_symbols)
        clock_health = self._clock_checker.check()
        if not clock_health.healthy:
            logger.error("Clock sync check failed: %s", clock_health.detail)

        await self.binance.start(symbols, self._sink)
        await self.bybit.start(symbols, self._sink)

        self._stop_event.clear()
        if self._consume_internally:
            self._consumer_task = asyncio.create_task(self._consume_loop(), name="ingestion-consumer")
        self._monitor_task = asyncio.create_task(self._monitor_loop(), name="ingestion-monitor")
        logger.info("Ingestion started for %d symbols across Binance + Bybit", len(symbols))

    async def stop(self) -> None:
        self._stop_event.set()
        for task in (self._consumer_task, self._monitor_task):
            if task:
                task.cancel()
        for adapter in self._adapters:
            await adapter.stop()
        logger.info("Ingestion stopped")

    async def _consume_loop(self) -> None:
        """Drains the Ring Buffer. Phase 1: count + (optionally) forward
        to a downstream callback. Never performs feature/strategy work
        here — that's explicitly out of scope for Phase 1 (§61)."""
        try:
            while not self._stop_event.is_set():
                batch = self._buffer.pop_batch(max_items=500)
                if not batch:
                    await asyncio.sleep(0.05)
                    continue
                self._events_consumed_total += len(batch)
                self._events_consumed_last_interval += len(batch)
        except asyncio.CancelledError:
            return

    async def _monitor_loop(self) -> None:
        """Lightweight periodic terminal/log metrics output (§46)."""
        interval = self._settings.metrics_log_interval_seconds
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(interval)
                self._log_metrics(interval)
                self._maybe_recover_buffer()
        except asyncio.CancelledError:
            return

    def _maybe_recover_buffer(self) -> None:
        if self._buffer.state in (IngestionState.DATA_OVERLOAD, IngestionState.LOSSY_MODE):
            self._buffer.acknowledge_recovery()

    def _log_metrics(self, interval_seconds: float) -> None:
        buf = self._buffer.metrics()
        binance_metrics = self.binance.get_metrics()
        bybit_metrics = self.bybit.get_metrics()
        events_per_sec = self._events_consumed_last_interval / interval_seconds
        self._events_consumed_last_interval = 0

        logger.info(
            "ingestion | state=%s buffer=%d/%d (%.1f%%) dropped_total=%d "
            "p0_overflow=%d events/s=%.1f | binance: connections=%d msgs=%d "
            "reconnects=%d healthy=%s | bybit: connections=%d msgs=%d "
            "reconnects=%d healthy=%s",
            buf.state.value, buf.size, buf.capacity, buf.utilization * 100,
            buf.dropped_total, buf.p0_protected_overflow_events, events_per_sec,
            len(binance_metrics.connections), binance_metrics.total_messages_received,
            binance_metrics.total_reconnects, binance_metrics.is_healthy,
            len(bybit_metrics.connections), bybit_metrics.total_messages_received,
            bybit_metrics.total_reconnects, bybit_metrics.is_healthy,
        )

    def get_snapshot(self) -> dict:
        """Machine-readable snapshot for tests / a future API endpoint."""
        buf = self._buffer.metrics()
        return {
            "buffer": {
                "state": buf.state.value,
                "size": buf.size,
                "capacity": buf.capacity,
                "utilization": buf.utilization,
                "dropped_total": buf.dropped_total,
                "dropped_by_priority": {p.name: v for p, v in buf.dropped_by_priority.items()},
                "p0_protected_overflow_events": buf.p0_protected_overflow_events,
                "counts_by_priority": {p.name: v for p, v in buf.counts_by_priority.items()},
            },
            "binance": _adapter_snapshot(self.binance.get_metrics()),
            "bybit": _adapter_snapshot(self.bybit.get_metrics()),
            "events_consumed_total": self._events_consumed_total,
        }


def _adapter_snapshot(metrics) -> dict:
    return {
        "exchange": metrics.exchange,
        "total_messages_received": metrics.total_messages_received,
        "total_reconnects": metrics.total_reconnects,
        "is_healthy": metrics.is_healthy,
        "connections": [
            {
                "connection_id": c.connection_id,
                "state": c.state.value,
                "subscribed_symbols": c.subscribed_symbols,
                "messages_received": c.messages_received,
                "reconnect_count": c.reconnect_count,
                "last_message_age_ms": c.last_message_age_ms,
                "last_error": c.last_error,
            }
            for c in metrics.connections
        ],
    }
