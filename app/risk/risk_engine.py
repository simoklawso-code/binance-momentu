"""
Risk Engine (§30, §31, §52 fixed safety rules).

Tracks paper-trading risk state: open position count, portfolio heat
(sum of risk % committed across open positions), and the daily loss
limit freeze. Deliberately simple and auditable — no "smart" recovery
logic, no increasing risk after a loss (§30: "No automatic recovery by
increasing risk").
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

from app.config.settings import StrategyConfig


@dataclass
class OpenPositionRisk:
    signal_id: str
    symbol: str
    risk_percent: float


@dataclass
class RiskCheckResult:
    allowed: bool
    reasons_blocked: list[str] = field(default_factory=list)


class RiskEngine:
    def __init__(self, strategy: StrategyConfig) -> None:
        self._strategy = strategy
        self._open_positions: dict[str, OpenPositionRisk] = {}
        self._daily_pnl_percent: float = 0.0
        self._daily_reset_date: dt.date = dt.datetime.now(dt.timezone.utc).date()
        self._frozen_for_today: bool = False

    def _maybe_reset_day(self, now: dt.datetime | None = None) -> None:
        now = now or dt.datetime.now(dt.timezone.utc)
        today = now.date()
        if today != self._daily_reset_date:
            self._daily_reset_date = today
            self._daily_pnl_percent = 0.0
            self._frozen_for_today = False

    @property
    def portfolio_heat_percent(self) -> float:
        return sum(p.risk_percent for p in self._open_positions.values())

    @property
    def open_position_count(self) -> int:
        return len(self._open_positions)

    def check_trade_allowed(self, btc_pause_active: bool, now: dt.datetime | None = None) -> RiskCheckResult:
        """§30/§31: evaluated as part of §33E step 18. Does NOT know
        about friction/quant score — those gates are evaluated
        separately, earlier in the pipeline (§33E)."""
        self._maybe_reset_day(now)
        reasons: list[str] = []

        if self._frozen_for_today:
            reasons.append("daily_loss_limit_freeze_active")
        if btc_pause_active:
            reasons.append("btc_volatility_pause_active")
        if self.open_position_count >= self._strategy.max_open_positions:
            reasons.append("max_open_positions_reached")
        if self.portfolio_heat_percent + self._strategy.risk_per_trade_percent > self._strategy.max_portfolio_heat:
            reasons.append("max_portfolio_heat_would_be_exceeded")

        return RiskCheckResult(allowed=not reasons, reasons_blocked=reasons)

    def register_open_position(self, signal_id: str, symbol: str) -> None:
        self._open_positions[signal_id] = OpenPositionRisk(
            signal_id=signal_id, symbol=symbol, risk_percent=self._strategy.risk_per_trade_percent,
        )

    def register_closed_position(self, signal_id: str, realized_pnl_percent_of_equity: float, now: dt.datetime | None = None) -> None:
        """§30: realized P&L, expressed as a percent of account equity
        (not of the trade's own size), accumulates against the daily
        loss limit. A losing trade NEVER triggers an automatic
        risk-per-trade increase — this engine has no such mechanism."""
        self._maybe_reset_day(now)
        self._open_positions.pop(signal_id, None)
        self._daily_pnl_percent += realized_pnl_percent_of_equity
        if self._daily_pnl_percent <= -abs(self._strategy.daily_loss_limit_percent):
            self._frozen_for_today = True

    @property
    def is_frozen_for_today(self) -> bool:
        return self._frozen_for_today

    @property
    def daily_pnl_percent(self) -> float:
        return self._daily_pnl_percent
