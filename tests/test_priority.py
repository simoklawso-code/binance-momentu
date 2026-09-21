from app.domain.enums import EventPriority, EventType
from app.ingestion.priority import classify_priority


def test_closed_kline_is_always_p0():
    assert classify_priority(EventType.KLINE, is_candidate=False, is_closed_kline=True) == EventPriority.P0
    assert classify_priority(EventType.KLINE, is_candidate=True, is_closed_kline=True) == EventPriority.P0


def test_intermediate_kline_depends_on_candidate_status():
    assert classify_priority(EventType.KLINE, is_candidate=True, is_closed_kline=False) == EventPriority.P1
    assert classify_priority(EventType.KLINE, is_candidate=False, is_closed_kline=False) == EventPriority.P2


def test_book_ticker_is_always_p0():
    assert classify_priority(EventType.BOOK_TICKER, is_candidate=True) == EventPriority.P0
    assert classify_priority(EventType.BOOK_TICKER, is_candidate=False) == EventPriority.P0


def test_mini_ticker_is_p3_unless_candidate():
    assert classify_priority(EventType.MARKET_TICKER, is_candidate=False) == EventPriority.P3
    assert classify_priority(EventType.MARKET_TICKER, is_candidate=True) == EventPriority.P1
