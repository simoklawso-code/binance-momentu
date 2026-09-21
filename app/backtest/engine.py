"""
Backtest Engine (§27-29, §49, §58).

Shares the EXACT SAME FeatureStore, Feature Engine, Quant Score,
Signal Engine, and Risk Engine code the live pipeline uses — this is
what §58 (Backtest/Paper parity) requires: the two must never
accidentally diverge in strategy logic, only in execution-data-
availability and fill-simulation methodology.

ADAPTATION NOTE (documented, not silently assumed): §27's "Limit Order
Backtest Fill Model" is written for a passive RESTING limit order.
Strategy A (§33) enters via a BREAKOUT/stop-style trigger (a market
order fires once price crosses Breakout_Level), which is structurally
different. This engine applies the SAME PESSIMISM PRINCIPLE §27
requires (prefer NO FILL over an optimistic FILL) to that different
order type: a breakout only "fills" in a given historical bar if the
bar's High penetates Breakout_Level by a configured buffer
(`backtest_penetration_buffer_percent`), and the assumed fill price is
the penetration point itself (Breakout_Level + buffer) — never the
bar's close, and never the exact breakout level (both would be more
optimistic than what a real stop order could realistically achieve
intrabar with OHLC-only data).

§28 SL_FIRST: if a single bar's Low <= Stop_Loss AND High >= Take_Profit,
this engine ALWAYS assumes Stop_Loss executed first (pessimistic
baseline) — it never has finer-than-1m data to resolve the real
ordering, and never fabricates one.

NOT IMPLEMENTED in this MVP backtest (documented limitation, not
faked): Purged Walk-Forward Cross Validation and 1,000-run Monte Carlo
Simulation (§49) — both are substantial additional statistical
machinery beyond a single deterministic pass over one OHLCV series.
This engine produces the single-pass metrics §49 also requires
(trade count, win rate, expectancy, profit factor, average R, maximum
drawdown) as a foundation to build Walk-Forward/Monte Carlo on top of
later.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from app.config.settings import StrategyConfig
from app.domain.enums import Exchange, SignalState
from app.domain.events import KlineEvent
from app.features.store import FeatureStore, SymbolFeatureState
from app.risk.risk_engine import RiskEngine
from app.signals.signal_engine import evaluate_signal

DEFAULT_PENETRATION_BUFFER_PERCENT = 0.02  # of price; re-tune per instrument/liquidity


@dataclass
class BacktestTrade:
    entry_bar_index: int
    exit_bar_index: int
    symbol: str
    entry_price: float
    exit_price: float
    stop_loss: float
    take_profit: float
    exit_state: SignalState
    pnl_percent: float
    r_multiple: float  # pnl_percent / risk_percent_of_entry (stop distance)


@dataclass
class BacktestReport:
    symbol: str
    total_bars: int
    trades: list[BacktestTrade] = field(default_factory=list)
    no_trade_count: int = 0
    rejected_count: int = 0

    @property
    def trade_count(self) -> int:
        return len(self.trades)

    @property
    def win_rate(self) -> float | None:
        if not self.trades:
            return None
        wins = sum(1 for t in self.trades if t.pnl_percent > 0)
        return wins / len(self.trades)

    @property
    def expectancy_r(self) -> float | None:
        if not self.trades:
            return None
        return sum(t.r_multiple for t in self.trades) / len(self.trades)

    @property
    def profit_factor(self) -> float | None:
        gains = sum(t.pnl_percent for t in self.trades if t.pnl_percent > 0)
        losses = sum(-t.pnl_percent for t in self.trades if t.pnl_percent < 0)
        if losses <= 0:
            return None
        return gains / losses

    @property
    def max_drawdown_percent(self) -> float | None:
        if not self.trades:
            return None
        equity = 100.0
        peak = equity
        max_dd = 0.0
        for t in self.trades:
            equity *= (1 + t.pnl_percent / 100.0)
            peak = max(peak, equity)
            drawdown = (peak - equity) / peak * 100.0
            max_dd = max(max_dd, drawdown)
        return max_dd


class BacktestEngine:
    def __init__(
        self,
        strategy: StrategyConfig,
        symbol: str,
        exchange: Exchange = Exchange.BINANCE,
        account_equity_usd: float | None = None,
        penetration_buffer_percent: float = DEFAULT_PENETRATION_BUFFER_PERCENT,
    ) -> None:
        self._strategy = strategy
        self._symbol = symbol
        self._exchange = exchange
        self._account_equity_usd = account_equity_usd or strategy.paper_starting_equity_usd
        self._penetration_buffer_percent = penetration_buffer_percent

    def run(self, bars: list[KlineEvent]) -> BacktestReport:
        store = FeatureStore()
        risk_engine = RiskEngine(self._strategy)
        report = BacktestReport(symbol=self._symbol, total_bars=len(bars))

        open_trade: dict | None = None  # {"signal_id","entry_bar","entry_price","stop_loss","take_profit"}

        for i, bar in enumerate(bars):
            store.ingest(bar)

            if open_trade is not None:
                exit_result = self._resolve_intrabar_exit(bar, open_trade)
                if exit_result is not None:
                    exit_price, exit_state = exit_result
                    pnl_percent = (exit_price - open_trade["entry_price"]) / open_trade["entry_price"] * 100.0
                    risk_percent = abs(open_trade["entry_price"] - open_trade["stop_loss"]) / open_trade["entry_price"] * 100.0
                    r_multiple = pnl_percent / risk_percent if risk_percent > 0 else 0.0
                    report.trades.append(BacktestTrade(
                        entry_bar_index=open_trade["entry_bar"], exit_bar_index=i, symbol=self._symbol,
                        entry_price=open_trade["entry_price"], exit_price=exit_price,
                        stop_loss=open_trade["stop_loss"], take_profit=open_trade["take_profit"],
                        exit_state=exit_state, pnl_percent=pnl_percent, r_multiple=r_multiple,
                    ))
                    risk_engine.register_closed_position(open_trade["signal_id"], pnl_percent * (open_trade["size_pct_of_equity"]))
                    open_trade = None
                continue  # one position at a time in this simple engine

            state = store.get(self._exchange, self._symbol)
            if state is None:
                report.no_trade_count += 1
                continue

            result = evaluate_signal(
                state, bar.close, self._strategy, risk_engine,
                self._account_equity_usd, btc_pause_active=False,
                decision_timestamp_ms=bar.close_time,
            )

            if result.state == SignalState.REJECTED:
                report.rejected_count += 1
                continue
            if result.state != SignalState.ENTRY_PENDING:
                report.no_trade_count += 1
                continue

            fill = self._apply_pessimistic_breakout_fill(bar, result.breakout_result.breakout_level)
            if fill is None:
                report.no_trade_count += 1
                continue  # triggered on a bar-close basis but NOT pessimistically fillable this bar -> NO FILL

            # Re-evaluate at the assumed pessimistic fill price so Entry/
            # SL/TP/friction/position-size are all consistent with what a
            # real stop order could have achieved (never reuse the
            # bar.close-based numbers from the first pass above).
            filled_result = evaluate_signal(
                state, fill, self._strategy, risk_engine,
                self._account_equity_usd, btc_pause_active=False,
                decision_timestamp_ms=bar.close_time,
            )
            if filled_result.state != SignalState.ENTRY_PENDING:
                report.no_trade_count += 1
                continue

            risk_engine.register_open_position(filled_result.signal_id, self._symbol)
            open_trade = {
                "signal_id": filled_result.signal_id,
                "entry_bar": i,
                "entry_price": filled_result.entry_price,
                "stop_loss": filled_result.stop_loss_result.stop_loss,
                "take_profit": filled_result.take_profit_result.take_profit,
                "size_pct_of_equity": (filled_result.final_position_size_usd or 0.0) / self._account_equity_usd,
            }

        return report

    def _apply_pessimistic_breakout_fill(self, bar: KlineEvent, breakout_level: float | None) -> float | None:
        if breakout_level is None:
            return None
        buffer = breakout_level * (self._penetration_buffer_percent / 100.0)
        fill_price = breakout_level + buffer
        if bar.high >= fill_price:
            return fill_price
        return None  # §27 principle: prefer NO FILL over an optimistic one

    def _resolve_intrabar_exit(self, bar: KlineEvent, open_trade: dict) -> tuple[float, SignalState] | None:
        sl = open_trade["stop_loss"]
        tp = open_trade["take_profit"]
        hit_sl = bar.low <= sl
        hit_tp = bar.high >= tp
        if hit_sl and hit_tp:
            return sl, SignalState.STOP_LOSS  # §28 LOCKED: SL_FIRST when both true in one bar
        if hit_sl:
            return sl, SignalState.STOP_LOSS
        if hit_tp:
            return tp, SignalState.TAKE_PROFIT
        return None
