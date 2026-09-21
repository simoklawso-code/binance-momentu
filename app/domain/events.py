"""
Domain Event models (§8).

Every event that crosses the ingestion boundary is one of these typed
models — never a raw unstructured dict. Each event carries both
exchange_timestamp (from the source, never overwritten) and
local_ingest_timestamp (set once, at the moment of ingestion) so that
ingest_latency_ms is always computable (§7).

These models are intentionally "dumb data" — no strategy logic, no
feature calculation lives here. That belongs to the Feature Engine in
a later phase.
"""

from __future__ import annotations

import time
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

from app.domain.enums import Exchange, EventPriority, EventType, SignalState


def now_ms() -> int:
    """Monotonic-ish wall clock in milliseconds, used for
    local_ingest_timestamp. A dedicated function so NTP/clock-sync
    concerns (§7) have a single call site to intercept later."""
    return int(time.time() * 1000)


class DomainEvent(BaseModel):
    """Base class for every event on the Ring Buffer.

    Fields present on every event, per §7/§8:
      - exchange, symbol, event_type: identity
      - exchange_timestamp: as reported by the exchange — NEVER overwritten
      - local_ingest_timestamp: set once, at ingestion
      - priority: assigned by the adapter/normalizer per §9 rules
      - sequence: optional per-symbol/per-stream ordering hint, when the
        exchange provides one (e.g. Binance kline "E" / update id)
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    exchange: Exchange
    symbol: str
    event_type: EventType
    exchange_timestamp: int = Field(..., description="ms epoch, from source, never overwritten")
    local_ingest_timestamp: int = Field(default_factory=now_ms, description="ms epoch, set at ingestion")
    priority: EventPriority
    sequence: Optional[int] = None

    @property
    def ingest_latency_ms(self) -> int:
        """§7: local_ingest_timestamp - exchange_timestamp."""
        return self.local_ingest_timestamp - self.exchange_timestamp

    @property
    def is_stale(self) -> bool:
        """Cheap default staleness check (2s). Feature/OI layers in later
        phases may apply stricter, metric-specific staleness rules —
        this is only a generic ingestion-level signal."""
        return self.ingest_latency_ms > 2000


class MarketTickerEvent(DomainEvent):
    """miniTicker / ticker stream normalization."""

    event_type: EventType = EventType.MARKET_TICKER
    last_price: float
    price_change_percent: Optional[float] = None
    volume_24h_base: Optional[float] = None
    volume_24h_quote: Optional[float] = None


class KlineEvent(DomainEvent):
    """1m Kline normalization. `is_closed` mirrors Binance's `x` field —
    closed klines are P0 and are protected from intentional drop (§11)."""

    event_type: EventType = EventType.KLINE
    interval: str = "1m"
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    base_volume: float
    quote_volume: float
    taker_buy_base_volume: float
    taker_buy_quote_volume: float
    number_of_trades: Optional[int] = None
    is_closed: bool

    @property
    def net_taker_delta(self) -> float:
        """§21: (2 * V_buy - V_total) / V_total. Guards zero denominator."""
        if self.base_volume == 0:
            return 0.0
        return (2 * self.taker_buy_base_volume - self.base_volume) / self.base_volume


class BookTickerEvent(DomainEvent):
    """Best bid/ask — candidate-only, Stage 3 metric source for spread (§17)."""

    event_type: EventType = EventType.BOOK_TICKER
    bid_price: float
    bid_qty: float
    ask_price: float
    ask_qty: float

    @property
    def spread_percent(self) -> Optional[float]:
        if self.bid_price <= 0 or self.ask_price <= 0:
            return None
        mid = (self.bid_price + self.ask_price) / 2
        if mid == 0:
            return None
        return (self.ask_price - self.bid_price) / mid * 100


class TradeEvent(DomainEvent):
    """Individual trade / aggTrade (optional stream, off by default)."""

    event_type: EventType = EventType.TRADE
    trade_id: Optional[int] = None
    price: float
    quantity: float
    is_buyer_maker: Optional[bool] = None


class OIEvent(DomainEvent):
    """Open Interest snapshot (§19). data_age_ms / is_stale rely on the
    base class's ingest_latency, but OI freshness rules are provider-
    specific and enforced by the OI adapter layer, not here."""

    event_type: EventType = EventType.OPEN_INTEREST
    open_interest: float
    open_interest_value_usd: Optional[float] = None
    provider_timestamp: Optional[int] = None


class EconomicEvent(DomainEvent):
    """Macro calendar event (§56). Not wired into Phase 1 — modeled now
    so Phase 2's Macro Context Engine doesn't require a schema change."""

    event_type: EventType = EventType.ECONOMIC
    event_id: str
    event_name: str
    currency: Optional[str] = None
    scheduled_timestamp: int
    actual_timestamp: Optional[int] = None
    importance: Optional[str] = None
    forecast: Optional[str] = None
    previous: Optional[str] = None
    actual: Optional[str] = None
    data_source: Optional[str] = None
    status: str = "upcoming"


class SignalEvent(DomainEvent):
    """Signal state transition (§34). Not emitted until Phase 3 — modeled
    now to keep the event model schema-stable across phases."""

    event_type: EventType = EventType.SIGNAL
    signal_id: str
    state: SignalState
    quant_score: Optional[float] = None
    payload: dict[str, Any] = Field(default_factory=dict)


class ExecutionEvent(DomainEvent):
    """Paper execution state change (§29, §34). Not emitted until Phase 3."""

    event_type: EventType = EventType.EXECUTION
    signal_id: str
    action: str
    price: Optional[float] = None
    payload: dict[str, Any] = Field(default_factory=dict)


class SystemEvent(DomainEvent):
    """Internal system/observability event — connection health, buffer
    pressure transitions, degraded-mode notices (§10, §46)."""

    event_type: EventType = EventType.SYSTEM
    level: str = "info"  # info | warning | error | critical
    component: str
    message: str
    metric_name: Optional[str] = None
    metric_value: Optional[float] = None
