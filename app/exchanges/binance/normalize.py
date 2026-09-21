"""
Binance Futures combined-stream message normalization.

Pure functions: raw dict (already orjson-decoded) -> DomainEvent. No I/O,
no state, no priority-classification side effects beyond what's passed
in — easy to unit test without a live connection.
"""

from __future__ import annotations

from typing import Any, Optional

from app.domain.enums import Exchange, EventType
from app.domain.events import BookTickerEvent, KlineEvent, MarketTickerEvent, now_ms
from app.ingestion.priority import classify_priority


def unwrap_combined_stream(message: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Combined-stream payloads look like {"stream": "...", "data": {...}}.
    Direct /ws subscriptions deliver the raw payload directly. Handle both."""
    if "data" in message and "stream" in message:
        return message["data"]
    return message


def parse_mini_ticker(data: dict[str, Any], *, is_candidate: bool = False) -> Optional[MarketTickerEvent]:
    if data.get("e") not in ("24hrMiniTicker", "24hrTicker"):
        return None
    symbol = data["s"]
    exchange_ts = int(data.get("E", now_ms()))
    return MarketTickerEvent(
        exchange=Exchange.BINANCE,
        symbol=symbol,
        exchange_timestamp=exchange_ts,
        priority=classify_priority(EventType.MARKET_TICKER, is_candidate=is_candidate),
        last_price=float(data["c"]),
        price_change_percent=float(data["P"]) if "P" in data else None,
        volume_24h_base=float(data["v"]) if "v" in data else None,
        volume_24h_quote=float(data["q"]) if "q" in data else None,
    )


def parse_kline(data: dict[str, Any], *, is_candidate: bool = False) -> Optional[KlineEvent]:
    if data.get("e") != "kline":
        return None
    k = data["k"]
    is_closed = bool(k.get("x", False))
    exchange_ts = int(data.get("E", now_ms()))
    return KlineEvent(
        exchange=Exchange.BINANCE,
        symbol=data["s"],
        exchange_timestamp=exchange_ts,
        priority=classify_priority(EventType.KLINE, is_candidate=is_candidate, is_closed_kline=is_closed),
        interval=k.get("i", "1m"),
        open_time=int(k["t"]),
        close_time=int(k["T"]),
        open=float(k["o"]),
        high=float(k["h"]),
        low=float(k["l"]),
        close=float(k["c"]),
        base_volume=float(k["v"]),
        quote_volume=float(k["q"]),
        taker_buy_base_volume=float(k["V"]),
        taker_buy_quote_volume=float(k["Q"]),
        number_of_trades=int(k["n"]) if "n" in k else None,
        is_closed=is_closed,
    )


def parse_book_ticker(data: dict[str, Any], *, is_candidate: bool = True) -> Optional[BookTickerEvent]:
    if "b" not in data or "a" not in data or "s" not in data:
        return None
    exchange_ts = int(data.get("E") or data.get("T") or now_ms())
    return BookTickerEvent(
        exchange=Exchange.BINANCE,
        symbol=data["s"],
        exchange_timestamp=exchange_ts,
        priority=classify_priority(EventType.BOOK_TICKER, is_candidate=is_candidate),
        bid_price=float(data["b"]),
        bid_qty=float(data["B"]),
        ask_price=float(data["a"]),
        ask_qty=float(data["A"]),
    )


def normalize_message(raw: dict[str, Any], *, is_candidate_fn) -> Optional[Any]:
    """Route a decoded combined-stream message to the correct parser.
    `is_candidate_fn(symbol) -> bool` lets the caller inject current
    candidate status without this module depending on the Candidate
    Stream Manager."""
    data = unwrap_combined_stream(raw)
    if data is None or "e" not in data:
        return None  # e.g. subscription ack {"result": null, "id": 1}

    event_kind = data["e"]
    symbol = data.get("s", "")
    is_candidate = is_candidate_fn(symbol) if symbol else False

    if event_kind in ("24hrMiniTicker", "24hrTicker"):
        return parse_mini_ticker(data, is_candidate=is_candidate)
    if event_kind == "kline":
        return parse_kline(data, is_candidate=is_candidate)
    if event_kind == "bookTicker":
        return parse_book_ticker(data, is_candidate=is_candidate)
    return None
