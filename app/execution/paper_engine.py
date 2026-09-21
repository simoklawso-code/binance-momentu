"""
Paper Trading Execution (§29 PAPER TRADING branch, §34 state machine).

Live paper trading uses actual chronological event order — a single
live price tick can only be on one side of SL or TP at a time, so the
SL_FIRST ambiguity that the Backtest engine must assume (§28, OHLC-bar
ambiguity) simply doesn't arise here. This module NEVER applies
SL_FIRST — that would be an accidental strategy difference forbidden
by §58 (Backtest/Paper parity: differences must be limited to
execution-data-availability, never a hidden strategy change).
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from app.domain.enums import SignalState


@dataclass
class PaperPosition:
    signal_id: str
    symbol: str
    exchange: str
    entry_price: float
    stop_loss: float
    take_profit: float
    size_usd: float
    opened_at_ms: int
    state: SignalState = SignalState.PAPER_POSITION_OPEN
    closed_at_ms: int | None = None
    exit_price: float | None = None
    realized_pnl_usd: float | None = None
    realized_pnl_percent_of_size: float | None = None


class PaperTradingEngine:
    def __init__(self) -> None:
        self._open: dict[str, PaperPosition] = {}
        self._closed: list[PaperPosition] = []

    def open_position(
        self, signal_id: str, symbol: str, exchange: str,
        entry_price: float, stop_loss: float, take_profit: float, size_usd: float,
        opened_at_ms: int | None = None,
    ) -> PaperPosition:
        position = PaperPosition(
            signal_id=signal_id, symbol=symbol, exchange=exchange,
            entry_price=entry_price, stop_loss=stop_loss, take_profit=take_profit,
            size_usd=size_usd, opened_at_ms=opened_at_ms or int(time.time() * 1000),
        )
        self._open[signal_id] = position
        return position

    def on_price_update(self, symbol: str, exchange: str, price: float, now_ms: int | None = None) -> list[PaperPosition]:
        """Checks every open position for this symbol/exchange against a
        single live price tick. LONG only (§33G): TP hit if price >=
        take_profit, SL hit if price <= stop_loss. Since this is one
        live tick (not an OHLC bar), there is no ordering ambiguity —
        both can't be true unless stop_loss >= take_profit, which
        §33C/§33D already prevent by construction."""
        now_ms = now_ms or int(time.time() * 1000)
        closed_now: list[PaperPosition] = []
        for signal_id, position in list(self._open.items()):
            if position.symbol != symbol or position.exchange != exchange:
                continue
            if price >= position.take_profit:
                self._close(position, price, SignalState.TAKE_PROFIT, now_ms)
                closed_now.append(position)
            elif price <= position.stop_loss:
                self._close(position, price, SignalState.STOP_LOSS, now_ms)
                closed_now.append(position)
        return closed_now

    def _close(self, position: PaperPosition, exit_price: float, state: SignalState, now_ms: int) -> None:
        pnl_percent_of_size = (exit_price - position.entry_price) / position.entry_price
        position.exit_price = exit_price
        position.state = state
        position.closed_at_ms = now_ms
        position.realized_pnl_percent_of_size = pnl_percent_of_size
        position.realized_pnl_usd = position.size_usd * pnl_percent_of_size
        self._open.pop(position.signal_id, None)
        self._closed.append(position)

    def force_close(self, signal_id: str, exit_price: float, state: SignalState, now_ms: int | None = None) -> PaperPosition | None:
        """Used for INVALIDATED/EXPIRED exits (§34) that aren't a TP/SL
        price cross — e.g. a signal manually invalidated by an
        operator, or a position expired per a max-hold-time rule."""
        position = self._open.get(signal_id)
        if position is None:
            return None
        self._close(position, exit_price, state, now_ms or int(time.time() * 1000))
        return position

    @property
    def open_positions(self) -> list[PaperPosition]:
        return list(self._open.values())

    @property
    def closed_positions(self) -> list[PaperPosition]:
        return list(self._closed)
