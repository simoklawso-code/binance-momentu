"""
Shared enumerations for the domain event model and signal state machine.

Kept in one place so Phase 1 (events, buffer, ingestion) and later phases
(features, signals, risk) share a single vocabulary — never redefined
locally in another module (§8, §34).
"""

from __future__ import annotations

from enum import Enum


class Exchange(str, Enum):
    """Logical exchange identity. A logical exchange may be backed by
    multiple physical connections (see Binance sharding, §6) — that is
    an adapter-internal detail and never leaks into the domain model."""

    BINANCE = "binance"
    BYBIT = "bybit"


class EventType(str, Enum):
    """Domain event kinds (§8). Every event on the Ring Buffer carries
    exactly one of these — raw unstructured dicts are never passed
    through the ingestion → buffer → consumer path."""

    MARKET_TICKER = "market_ticker"
    KLINE = "kline"
    BOOK_TICKER = "book_ticker"
    TRADE = "trade"
    OPEN_INTEREST = "open_interest"
    ECONOMIC = "economic"
    SIGNAL = "signal"
    EXECUTION = "execution"
    SYSTEM = "system"


class EventPriority(int, Enum):
    """Ring Buffer priority classes (§9). Lower numeric value = higher
    priority = dropped last. Ordered so that `sorted()` / comparisons
    behave intuitively (P0 < P1 < P2 < P3)."""

    P0 = 0  # closed 1m klines, candidate-critical events, never silently dropped
    P1 = 1  # candidate-related intermediate events
    P2 = 2  # non-critical intermediate events
    P3 = 3  # low-value ticker updates, redundant frames


class ConnectionState(str, Enum):
    """Per-connection (per-shard) WebSocket lifecycle state, exposed via
    observability metrics (§46) and used by reconnect logic (§6, §47)."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    SUBSCRIBING = "subscribing"
    HEALTHY = "healthy"
    RECONNECTING = "reconnecting"
    SOFT_RECONNECTING = "soft_reconnecting"
    FAILED = "failed"


class IngestionState(str, Enum):
    """System-wide ingestion health state (§10). DATA_OVERLOAD /
    INGESTION_DEGRADED must map to a protective NO_TRADE state upstream
    in later phases — Phase 1 only needs to raise and expose it."""

    NORMAL = "normal"
    LOSSY_MODE = "lossy_mode"
    DATA_OVERLOAD = "data_overload"
    INGESTION_DEGRADED = "ingestion_degraded"


class SignalState(str, Enum):
    """Full signal state machine (§34). Phase 1 does not emit these, but
    the enum is defined now so later phases and the SignalEvent model
    do not require a schema migration (§65 step 2 principle applied to
    the domain layer too)."""

    NO_TRADE = "no_trade"
    WATCH = "watch"
    CANDIDATE = "candidate"
    PUMP_DETECTED = "pump_detected"
    ENTRY_PENDING = "entry_pending"
    ENTRY_APPROVED = "entry_approved"
    PAPER_POSITION_OPEN = "paper_position_open"
    TAKE_PROFIT = "take_profit"
    STOP_LOSS = "stop_loss"
    INVALIDATED = "invalidated"
    EXPIRED = "expired"
    REJECTED = "rejected"


class MacroRiskState(str, Enum):
    """Macro Context Engine states (§56). Not active in Phase 1; defined
    for forward compatibility only."""

    NORMAL = "normal"
    EVENT_APPROACHING = "event_approaching"
    EVENT_ACTIVE = "event_active"
    POST_EVENT_VOLATILITY = "post_event_volatility"
    MACRO_DATA_UNAVAILABLE = "macro_data_unavailable"
