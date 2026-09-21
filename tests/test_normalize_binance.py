from app.domain.enums import EventPriority
from app.domain.events import BookTickerEvent, KlineEvent, MarketTickerEvent
from app.exchanges.binance.normalize import normalize_message, parse_kline, parse_mini_ticker


def test_parse_mini_ticker():
    raw = {
        "e": "24hrMiniTicker", "E": 123456789, "s": "BTCUSDT",
        "c": "50000.10", "o": "49000.00", "h": "51000.00", "l": "48500.00",
        "v": "1000.5", "q": "50000000.0",
    }
    event = parse_mini_ticker(raw, is_candidate=False)
    assert isinstance(event, MarketTickerEvent)
    assert event.symbol == "BTCUSDT"
    assert event.last_price == 50000.10
    assert event.priority == EventPriority.P3


def test_parse_closed_kline_is_p0():
    raw = {
        "e": "kline", "E": 123456789, "s": "ETHUSDT",
        "k": {
            "t": 123456000, "T": 123456059, "i": "1m",
            "o": "3000.0", "c": "3010.0", "h": "3015.0", "l": "2995.0",
            "v": "500.0", "q": "1505000.0",
            "V": "300.0", "Q": "903000.0",
            "n": 120, "x": True,
        },
    }
    event = parse_kline(raw, is_candidate=False)
    assert isinstance(event, KlineEvent)
    assert event.is_closed is True
    assert event.priority == EventPriority.P0
    assert event.close == 3010.0


def test_parse_intermediate_kline_is_not_p0():
    raw = {
        "e": "kline", "E": 123456789, "s": "ETHUSDT",
        "k": {
            "t": 123456000, "T": 123456059, "i": "1m",
            "o": "3000.0", "c": "3005.0", "h": "3006.0", "l": "2999.0",
            "v": "100.0", "q": "300500.0",
            "V": "60.0", "Q": "180300.0",
            "n": 30, "x": False,
        },
    }
    event = parse_kline(raw, is_candidate=False)
    assert event.is_closed is False
    assert event.priority == EventPriority.P2


def test_combined_stream_envelope_unwrapped():
    envelope = {
        "stream": "btcusdt@miniTicker",
        "data": {
            "e": "24hrMiniTicker", "E": 1, "s": "BTCUSDT",
            "c": "1.0", "o": "1.0", "h": "1.0", "l": "1.0", "v": "1.0", "q": "1.0",
        },
    }
    event = normalize_message(envelope, is_candidate_fn=lambda s: False)
    assert isinstance(event, MarketTickerEvent)
    assert event.symbol == "BTCUSDT"


def test_subscription_ack_is_ignored():
    ack = {"result": None, "id": 1}
    event = normalize_message(ack, is_candidate_fn=lambda s: False)
    assert event is None


def test_book_ticker_parsed_and_always_p0():
    raw = {"e": "bookTicker", "s": "BTCUSDT", "b": "49999.5", "B": "2.0", "a": "50000.5", "A": "1.5", "T": 111, "E": 112}
    event = normalize_message(raw, is_candidate_fn=lambda s: True)
    assert isinstance(event, BookTickerEvent)
    assert event.priority == EventPriority.P0
    assert round(event.spread_percent, 4) == round((50000.5 - 49999.5) / ((50000.5 + 49999.5) / 2) * 100, 4)
