"""
Feature Store (§13, §43 layer 2).

Holds the minimal rolling state the Feature Engine needs per
(exchange, symbol): a bounded window of CLOSED 1m klines (for RVOL,
ATR, breakout lookback, momentum), plus the latest ticker/book-ticker/
OI snapshots. This is intentionally NOT a database — it's in-memory,
process-local state that the Feature Engine reads synchronously. The
Storage layer (Phase 1's §42 SQLite worker) persists a copy separately
for audit/backtest; this store is only the live working set.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from app.domain.enums import Exchange
from app.domain.events import BookTickerEvent, KlineEvent, MarketTickerEvent, OIEvent

DEFAULT_KLINE_WINDOW = 60  # enough for a 20-period lookback + headroom


@dataclass
class SymbolFeatureState:
    exchange: Exchange
    symbol: str
    closed_klines: deque[KlineEvent] = field(default_factory=lambda: deque(maxlen=DEFAULT_KLINE_WINDOW))
    latest_ticker: MarketTickerEvent | None = None
    latest_book_ticker: BookTickerEvent | None = None
    latest_oi: OIEvent | None = None

    def net_taker_delta_available(self) -> bool:
        """§21: Binance provides a real taker buy/sell split; Bybit's
        public kline stream does not (documented limitation, see
        app/exchanges/bybit/normalize.py). Never silently treat Bybit's
        placeholder 0.0 taker volume as a meaningful delta."""
        return self.exchange == Exchange.BINANCE


class FeatureStore:
    def __init__(self, kline_window: int = DEFAULT_KLINE_WINDOW) -> None:
        self._kline_window = kline_window
        self._states: dict[tuple[Exchange, str], SymbolFeatureState] = {}

    def _get_or_create(self, exchange: Exchange, symbol: str) -> SymbolFeatureState:
        key = (exchange, symbol)
        state = self._states.get(key)
        if state is None:
            state = SymbolFeatureState(
                exchange=exchange, symbol=symbol,
                closed_klines=deque(maxlen=self._kline_window),
            )
            self._states[key] = state
        return state

    def ingest(self, event) -> None:  # DomainEvent, kept loosely typed to avoid import cycles
        if isinstance(event, KlineEvent):
            state = self._get_or_create(event.exchange, event.symbol)
            if event.is_closed:
                state.closed_klines.append(event)
        elif isinstance(event, MarketTickerEvent):
            state = self._get_or_create(event.exchange, event.symbol)
            state.latest_ticker = event
        elif isinstance(event, BookTickerEvent):
            state = self._get_or_create(event.exchange, event.symbol)
            state.latest_book_ticker = event
        elif isinstance(event, OIEvent):
            state = self._get_or_create(event.exchange, event.symbol)
            state.latest_oi = event

    def get(self, exchange: Exchange, symbol: str) -> SymbolFeatureState | None:
        return self._states.get((exchange, symbol))

    def all_states(self) -> list[SymbolFeatureState]:
        return list(self._states.values())
