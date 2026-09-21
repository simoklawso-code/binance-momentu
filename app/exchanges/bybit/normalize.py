"""
Bybit V5 Linear Perpetual public-stream message normalization.

Bybit's topic/payload structure is genuinely different from Binance's
(§6: "Do not assume Binance stream semantics apply to Bybit") — kline
"closed" is signaled by `confirm`, not `x`; tickers and orderbook come
as separate topics with their own envelope ({"topic", "type", "ts",
"data"}), and volumes are strings, not always floats-as-strings in the
same positions as Binance. Handled explicitly here rather than sharing
code with the Binance normalizer.
"""

from __future__ import annotations

from typing import Any, Optional

from app.domain.enums import Exchange, EventType
from app.domain.events import BookTickerEvent, KlineEvent, MarketTickerEvent, now_ms
from app.ingestion.priority import classify_priority


def _topic_symbol(topic: str) -> str:
    # "kline.1.BTCUSDT" -> "BTCUSDT" ; "tickers.BTCUSDT" -> "BTCUSDT"
    return topic.split(".")[-1]


def parse_kline(message: dict[str, Any], *, is_candidate: bool = False) -> Optional[KlineEvent]:
    topic = message.get("topic", "")
    if not topic.startswith("kline."):
        return None
    data_list = message.get("data") or []
    if not data_list:
        return None
    d = data_list[0]
    is_closed = bool(d.get("confirm", False))
    exchange_ts = int(message.get("ts", now_ms()))
    return KlineEvent(
        exchange=Exchange.BYBIT,
        symbol=_topic_symbol(topic),
        exchange_timestamp=exchange_ts,
        priority=classify_priority(EventType.KLINE, is_candidate=is_candidate, is_closed_kline=is_closed),
        interval="1m",
        open_time=int(d["start"]),
        close_time=int(d["end"]),
        open=float(d["open"]),
        high=float(d["high"]),
        low=float(d["low"]),
        close=float(d["close"]),
        base_volume=float(d["volume"]),
        quote_volume=float(d.get("turnover", 0.0)),
        # Bybit's public kline stream does not provide a taker buy/sell
        # split (unlike Binance). Net Taker Delta for Bybit-sourced
        # klines therefore cannot be computed from this stream alone —
        # documented limitation, never invented. Feature Engine (Phase 2)
        # must treat Bybit taker_buy_* as unavailable, not as zero.
        taker_buy_base_volume=0.0,
        taker_buy_quote_volume=0.0,
        number_of_trades=None,
        is_closed=is_closed,
    )


def parse_ticker(message: dict[str, Any], *, is_candidate: bool = False) -> Optional[MarketTickerEvent]:
    topic = message.get("topic", "")
    if not topic.startswith("tickers."):
        return None
    d = message.get("data")
    if not d:
        return None
    exchange_ts = int(message.get("ts", now_ms()))
    last_price = d.get("lastPrice")
    if last_price is None:
        return None
    return MarketTickerEvent(
        exchange=Exchange.BYBIT,
        symbol=_topic_symbol(topic),
        exchange_timestamp=exchange_ts,
        priority=classify_priority(EventType.MARKET_TICKER, is_candidate=is_candidate),
        last_price=float(last_price),
        price_change_percent=float(d["price24hPcnt"]) * 100 if d.get("price24hPcnt") is not None else None,
        volume_24h_base=float(d["volume24h"]) if d.get("volume24h") is not None else None,
        volume_24h_quote=float(d["turnover24h"]) if d.get("turnover24h") is not None else None,
    )


def parse_orderbook_l1(message: dict[str, Any], *, is_candidate: bool = True) -> Optional[BookTickerEvent]:
    topic = message.get("topic", "")
    if not topic.startswith("orderbook.1."):
        return None
    d = message.get("data")
    if not d or not d.get("b") or not d.get("a"):
        return None
    exchange_ts = int(message.get("ts", now_ms()))
    best_bid = d["b"][0]
    best_ask = d["a"][0]
    return BookTickerEvent(
        exchange=Exchange.BYBIT,
        symbol=d.get("s", _topic_symbol(topic)),
        exchange_timestamp=exchange_ts,
        priority=classify_priority(EventType.BOOK_TICKER, is_candidate=is_candidate),
        bid_price=float(best_bid[0]),
        bid_qty=float(best_bid[1]),
        ask_price=float(best_ask[0]),
        ask_qty=float(best_ask[1]),
    )


def normalize_message(raw: dict[str, Any], *, is_candidate_fn) -> Optional[Any]:
    topic = raw.get("topic")
    if not topic:
        return None  # op responses (subscribe ack, pong) have no "topic"

    symbol = _topic_symbol(topic)
    is_candidate = is_candidate_fn(symbol) if symbol else False

    if topic.startswith("kline."):
        return parse_kline(raw, is_candidate=is_candidate)
    if topic.startswith("tickers."):
        return parse_ticker(raw, is_candidate=is_candidate)
    if topic.startswith("orderbook.1."):
        return parse_orderbook_l1(raw, is_candidate=is_candidate)
    return None
