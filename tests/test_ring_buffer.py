import pytest

from app.buffer.ring_buffer import BufferOverloadError, PriorityRingBuffer
from app.domain.enums import Exchange, EventPriority, EventType, IngestionState
from app.domain.events import DomainEvent


def make_event(priority: EventPriority, symbol: str = "BTCUSDT", exchange_timestamp: int = 1000) -> DomainEvent:
    return DomainEvent(
        exchange=Exchange.BINANCE,
        symbol=symbol,
        event_type=EventType.SYSTEM,
        exchange_timestamp=exchange_timestamp,
        local_ingest_timestamp=exchange_timestamp + 5,
        priority=priority,
    )


def test_push_and_pop_respects_capacity():
    buf = PriorityRingBuffer(capacity=3)
    for i in range(3):
        assert buf.push(make_event(EventPriority.P2, exchange_timestamp=i)) is True
    assert buf.size == 3


def test_pop_batch_drains_highest_priority_first():
    buf = PriorityRingBuffer(capacity=10)
    buf.push(make_event(EventPriority.P3, symbol="A"))
    buf.push(make_event(EventPriority.P1, symbol="B"))
    buf.push(make_event(EventPriority.P0, symbol="C"))
    buf.push(make_event(EventPriority.P2, symbol="D"))

    batch = buf.pop_batch(max_items=10)
    symbols_in_order = [e.symbol for e in batch]
    assert symbols_in_order == ["C", "B", "D", "A"]


def test_p3_dropped_first_under_pressure():
    buf = PriorityRingBuffer(capacity=4, pressure_high_watermark=0.5)
    buf.push(make_event(EventPriority.P3, symbol="p3-1"))
    buf.push(make_event(EventPriority.P2, symbol="p2-1"))
    buf.push(make_event(EventPriority.P1, symbol="p1-1"))
    buf.push(make_event(EventPriority.P0, symbol="p0-1"))
    # buffer full — pushing a new P2 event must evict the P3 first
    accepted = buf.push(make_event(EventPriority.P2, symbol="p2-2"))
    assert accepted is True

    remaining_symbols = {e.symbol for e in buf.pop_batch(max_items=10)}
    assert "p3-1" not in remaining_symbols
    assert "p2-2" in remaining_symbols
    metrics = buf.metrics()
    assert metrics.dropped_by_priority[EventPriority.P3] == 1


def test_p2_dropped_after_p3_exhausted():
    buf = PriorityRingBuffer(capacity=3)
    buf.push(make_event(EventPriority.P2, symbol="p2-1"))
    buf.push(make_event(EventPriority.P1, symbol="p1-1"))
    buf.push(make_event(EventPriority.P0, symbol="p0-1"))
    # No P3 present — a new P1 event should evict the P2.
    accepted = buf.push(make_event(EventPriority.P1, symbol="p1-2"))
    assert accepted is True
    metrics = buf.metrics()
    assert metrics.dropped_by_priority[EventPriority.P2] == 1
    assert metrics.dropped_by_priority[EventPriority.P3] == 0


def test_p0_never_silently_dropped_raises_overload():
    buf = PriorityRingBuffer(capacity=2)
    buf.push(make_event(EventPriority.P0, symbol="p0-1"))
    buf.push(make_event(EventPriority.P0, symbol="p0-2"))
    # Buffer full of P0 only, nothing lower-priority to evict.
    with pytest.raises(BufferOverloadError):
        buf.push(make_event(EventPriority.P0, symbol="p0-3"))
    metrics = buf.metrics()
    assert metrics.p0_protected_overflow_events == 1
    assert buf.state == IngestionState.DATA_OVERLOAD


def test_non_p0_dropped_not_raised_when_buffer_full_of_p0_p1():
    buf = PriorityRingBuffer(capacity=2)
    buf.push(make_event(EventPriority.P0, symbol="p0-1"))
    buf.push(make_event(EventPriority.P1, symbol="p1-1"))
    # A P3 event with nothing lower to evict (P1 is protected from P3's
    # perspective — only P0 triggers the emergency P1 sacrifice) is
    # simply dropped, not raised.
    accepted = buf.push(make_event(EventPriority.P3, symbol="p3-1"))
    assert accepted is False
    metrics = buf.metrics()
    assert metrics.dropped_by_priority[EventPriority.P3] == 1


def test_recovery_after_overload_is_explicit_not_automatic():
    buf = PriorityRingBuffer(capacity=2, recovery_watermark=0.4)
    buf.push(make_event(EventPriority.P0, symbol="p0-1"))
    buf.push(make_event(EventPriority.P0, symbol="p0-2"))
    with pytest.raises(BufferOverloadError):
        buf.push(make_event(EventPriority.P0, symbol="p0-3"))
    assert buf.state == IngestionState.DATA_OVERLOAD

    buf.pop_batch(max_items=10)  # drain everything
    # State should NOT silently flip back without an explicit ack call.
    assert buf.state == IngestionState.DATA_OVERLOAD
    buf.acknowledge_recovery()
    assert buf.state == IngestionState.NORMAL


def test_utilization_and_lossy_mode_transition():
    buf = PriorityRingBuffer(capacity=10, pressure_high_watermark=0.5)
    for i in range(6):
        buf.push(make_event(EventPriority.P3, symbol=f"s{i}"))
    assert buf.state == IngestionState.LOSSY_MODE
