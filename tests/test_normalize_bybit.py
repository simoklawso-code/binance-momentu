from app.domain.enums import EventPriority
from app.domain.events import BookTickerEvent, KlineEvent, MarketTickerEvent
from app.exchanges.bybit.normalize import normalize_message, parse_kline, parse_ticker


def test_parse_closed_kline_uses_confirm_field_not_x():
    raw = {
        "topic": "kline.1.BTCUSDT", "type": "snapshot", "ts": 123456789,
        "data": [{
            "start": 123456000, "end": 123456059, "interval": "1",
            "open": "50000", "close": "50100", "high": "50150", "low": "49950",
            "volume": "12.5", "turnover": "625000", "confirm": True, "timestamp": 123456789,
        }],
    }
    event = parse_kline(raw, is_candidate=False)
    assert isinstance(event, KlineEvent)
    assert event.is_closed is True
    assert event.priority == EventPriority.P0
    assert event.close == 50100.0


def test_bybit_kline_has_no_taker_split_documented_as_zero_not_missing():
    raw = {
        "topic": "kline.1.BTCUSDT", "type": "snapshot", "ts": 1,
        "data": [{
            "start": 1, "end": 60, "interval": "1",
            "open": "1", "close": "1", "high": "1", "low": "1",
            "volume": "1", "turnover": "1", "confirm": False,
        }],
    }
    event = parse_kline(raw, is_candidate=False)
    assert event.taker_buy_base_volume == 0.0
    assert event.taker_buy_quote_volume == 0.0
    # net_taker_delta should not be misinterpreted as "all sell" (-1.0);
    # documented limitation means callers must not trust this value for Bybit.
    assert event.net_taker_delta == -1.0  # explicit: formula result, NOT claimed to be meaningful


def test_parse_ticker():
    raw = {
        "topic": "tickers.ETHUSDT", "type": "snapshot", "ts": 123,
        "data": {"symbol": "ETHUSDT", "lastPrice": "3000.5", "volume24h": "10000", "turnover24h": "30000000", "price24hPcnt": "0.015"},
    }
    event = parse_ticker(raw, is_candidate=False)
    assert isinstance(event, MarketTickerEvent)
    assert event.last_price == 3000.5
    assert round(event.price_change_percent, 3) == 1.5


def test_topic_symbol_extraction_for_multi_segment_topics():
    raw = {
        "topic": "orderbook.1.BTCUSDT", "type": "snapshot", "ts": 1,
        "data": {"s": "BTCUSDT", "b": [["49999.5", "1.0"]], "a": [["50000.5", "1.0"]]},
    }
    event = normalize_message(raw, is_candidate_fn=lambda s: True)
    assert isinstance(event, BookTickerEvent)
    assert event.symbol == "BTCUSDT"
    assert event.priority == EventPriority.P0


def test_ping_pong_control_messages_produce_no_event():
    raw = {"op": "pong", "ts": 1}
    event = normalize_message(raw, is_candidate_fn=lambda s: False)
    assert event is None
