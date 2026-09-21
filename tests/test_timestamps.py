from app.domain.enums import Exchange, EventPriority
from app.domain.events import KlineEvent


def test_ingest_latency_computed_correctly():
    event = KlineEvent(
        exchange=Exchange.BINANCE,
        symbol="BTCUSDT",
        exchange_timestamp=1_000_000,
        local_ingest_timestamp=1_000_120,
        priority=EventPriority.P0,
        open_time=1_000_000,
        close_time=1_000_060,
        open=100.0, high=101.0, low=99.0, close=100.5,
        base_volume=10.0, quote_volume=1000.0,
        taker_buy_base_volume=6.0, taker_buy_quote_volume=600.0,
        is_closed=True,
    )
    assert event.ingest_latency_ms == 120


def test_exchange_timestamp_is_never_mutated_by_model():
    """Events are frozen — nothing downstream can silently overwrite
    exchange_timestamp (§7)."""
    event = KlineEvent(
        exchange=Exchange.BINANCE,
        symbol="BTCUSDT",
        exchange_timestamp=1_000_000,
        local_ingest_timestamp=1_000_050,
        priority=EventPriority.P0,
        open_time=1_000_000,
        close_time=1_000_060,
        open=100.0, high=101.0, low=99.0, close=100.5,
        base_volume=10.0, quote_volume=1000.0,
        taker_buy_base_volume=6.0, taker_buy_quote_volume=600.0,
        is_closed=True,
    )
    try:
        event.exchange_timestamp = 999  # type: ignore[misc]
        mutated = True
    except Exception:
        mutated = False
    assert mutated is False, "DomainEvent must be immutable — exchange_timestamp must never be overwritten"


def test_is_stale_flags_old_events():
    stale_event = KlineEvent(
        exchange=Exchange.BINANCE,
        symbol="BTCUSDT",
        exchange_timestamp=1_000_000,
        local_ingest_timestamp=1_000_000 + 5000,  # 5s ingest latency
        priority=EventPriority.P0,
        open_time=1_000_000,
        close_time=1_000_060,
        open=100.0, high=101.0, low=99.0, close=100.5,
        base_volume=10.0, quote_volume=1000.0,
        taker_buy_base_volume=6.0, taker_buy_quote_volume=600.0,
        is_closed=True,
    )
    assert stale_event.is_stale is True


def test_net_taker_delta_formula():
    """§21: (2 * V_buy - V_total) / V_total"""
    event = KlineEvent(
        exchange=Exchange.BINANCE,
        symbol="BTCUSDT",
        exchange_timestamp=1_000_000,
        local_ingest_timestamp=1_000_010,
        priority=EventPriority.P0,
        open_time=1_000_000,
        close_time=1_000_060,
        open=100.0, high=101.0, low=99.0, close=100.5,
        base_volume=100.0, quote_volume=10000.0,
        taker_buy_base_volume=80.0, taker_buy_quote_volume=8000.0,
        is_closed=True,
    )
    # (2*80 - 100) / 100 = 0.6
    assert abs(event.net_taker_delta - 0.6) < 1e-9


def test_net_taker_delta_guards_zero_denominator():
    event = KlineEvent(
        exchange=Exchange.BINANCE,
        symbol="BTCUSDT",
        exchange_timestamp=1_000_000,
        local_ingest_timestamp=1_000_010,
        priority=EventPriority.P0,
        open_time=1_000_000,
        close_time=1_000_060,
        open=100.0, high=101.0, low=99.0, close=100.5,
        base_volume=0.0, quote_volume=0.0,
        taker_buy_base_volume=0.0, taker_buy_quote_volume=0.0,
        is_closed=True,
    )
    assert event.net_taker_delta == 0.0
