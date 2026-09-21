"""
Feature Engine (§13-§23, §33A.1).

Pure, deterministic, unit-testable calculation functions. Every
formula here is documented against the spec section it implements.
Nothing here calls Claude, touches storage, or does network I/O — it
only reads a SymbolFeatureState (§13: "No heavy order-book analysis
across all 300 symbols. No Claude calls.").

All functions return None (never an invented 0.0) when a required
input is missing or insufficient — the Quant Score layer is
responsible for turning "missing component" into NO_TRADE (§33A.1.8:
"Never substitute an arbitrary zero merely to produce a score").
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass

from app.config.settings import StrategyConfig
from app.domain.events import KlineEvent
from app.features.store import SymbolFeatureState

EPSILON = 1e-9


def true_range(current: KlineEvent, prev_close: float) -> float:
    return max(
        current.high - current.low,
        abs(current.high - prev_close),
        abs(current.low - prev_close),
    )


def atr_1m(state: SymbolFeatureState, period: int = 14) -> float | None:
    """Simple-average True Range over the last `period` closed 1m
    candles. Requires period+1 candles (needs a previous close for the
    first True Range in the window)."""
    klines = list(state.closed_klines)
    if len(klines) < period + 1:
        return None
    window = klines[-(period + 1):]
    trs = [true_range(window[i], window[i - 1].close) for i in range(1, len(window))]
    if not trs:
        return None
    return sum(trs) / len(trs)


def rvol(state: SymbolFeatureState, baseline_period: int = 20) -> float | None:
    """§14: RVOL = latest closed candle's volume / (baseline_period)-
    candle moving MEDIAN of volume (robust to outliers, per spec —
    avoids assuming normally distributed volume). Requires
    baseline_period+1 candles: 1 "current" + baseline_period prior."""
    klines = list(state.closed_klines)
    if len(klines) < baseline_period + 1:
        return None
    current = klines[-1]
    baseline_window = klines[-(baseline_period + 1):-1]
    volumes = [k.base_volume for k in baseline_window]
    median_volume = statistics.median(volumes)
    if median_volume <= EPSILON:
        return None  # guard zero/near-zero denominator (§14)
    return current.base_volume / median_volume


def trade_acceleration(
    state: SymbolFeatureState,
    recent_window: int = 3,
    baseline_period: int = 20,
) -> float | None:
    """§15: "a documented, deterministic formula ... never an
    undefined/invented formula." Definition used here (documented,
    configurable window sizes):

        Trade_Acceleration =
            mean(base_volume over the last `recent_window` closed candles)
            / median(base_volume over the `baseline_period` candles
                     immediately BEFORE that recent window)

    This captures "is the very recent rate of activity accelerating
    relative to the recent past" — distinct from RVOL, which compares
    ONE candle against a longer median. Requires
    recent_window + baseline_period candles.
    """
    klines = list(state.closed_klines)
    total_needed = recent_window + baseline_period
    if len(klines) < total_needed:
        return None
    recent = klines[-recent_window:]
    baseline = klines[-total_needed:-recent_window]
    recent_mean = sum(k.base_volume for k in recent) / len(recent)
    baseline_median = statistics.median(k.base_volume for k in baseline)
    if baseline_median <= EPSILON:
        return None
    return recent_mean / baseline_median


def price_change_percent(kline: KlineEvent) -> float:
    """§23.1: Price Change = abs(Close_t - Open_t) / Open_t"""
    if kline.open <= EPSILON:
        return 0.0
    return abs(kline.close - kline.open) / kline.open


def net_taker_delta(state: SymbolFeatureState) -> float | None:
    """§21. Returns None (not 0.0) when the exchange doesn't provide a
    real taker split (Bybit, §21/§33A.1.8) — a missing component must
    never be silently treated as "balanced" (0.0 has a real meaning:
    exactly balanced buy/sell flow)."""
    if not state.net_taker_delta_available():
        return None
    if not state.closed_klines:
        return None
    return state.closed_klines[-1].net_taker_delta


def pde(state: SymbolFeatureState) -> float | None:
    """§23.2: ranking/logging metric only — never compared directly
    against a threshold. PDE = Price_Change / max(|delta|, epsilon)."""
    if not state.closed_klines:
        return None
    delta = net_taker_delta(state)
    if delta is None:
        return None
    latest = state.closed_klines[-1]
    change = price_change_percent(latest)
    return change / max(abs(delta), EPSILON)


def absorption_gate(state: SymbolFeatureState, atr_period: int = 14) -> bool | None:
    """§23.3: hard rejection condition.
        Low_Impact_Threshold = 0.25 * ATR_1m / Open
        ABSORPTION = (Net_Taker_Delta >= +0.30) AND (Price_Change < Low_Impact_Threshold)
    Returns None if inputs are unavailable (caller must treat that as
    "cannot evaluate" -> NO_TRADE, per §33A.1.8, not as False/safe)."""
    if not state.closed_klines:
        return None
    latest = state.closed_klines[-1]
    delta = net_taker_delta(state)
    atr = atr_1m(state, period=atr_period)
    if delta is None or atr is None or latest.open <= EPSILON:
        return None
    low_impact_threshold = 0.25 * atr / latest.open
    change = price_change_percent(latest)
    return bool(delta >= 0.30 and change < low_impact_threshold)


def momentum_ratio(state: SymbolFeatureState, window_minutes: int = 3, atr_period: int = 14) -> float | None:
    """§33A.1.4:
        Momentum_Move = |Current_Price - Price_N_Mins_Ago| / Price_N_Mins_Ago
        Momentum_Volatility_Baseline = ATR_1m / Current_Price
        Momentum_Ratio = Momentum_Move / Momentum_Volatility_Baseline
    "Current_Price" here is the latest closed candle's close (Phase 2 has
    no separate live-tick feed wired into this function; the Signal
    Engine in Phase 3 may substitute a fresher live price at decision
    time — this function documents which price it used via its return
    contract: always the last CLOSED candle's close)."""
    klines = list(state.closed_klines)
    if len(klines) < window_minutes + 1:
        return None
    current_price = klines[-1].close
    price_n_ago = klines[-(window_minutes + 1)].close
    if price_n_ago <= EPSILON or current_price <= EPSILON:
        return None
    momentum_move = abs(current_price - price_n_ago) / price_n_ago
    atr = atr_1m(state, period=atr_period)
    if atr is None:
        return None
    baseline = atr / current_price
    if baseline <= EPSILON:
        return None
    return momentum_move / baseline


@dataclass
class BreakoutInputs:
    prior_high: float
    breakout_buffer: float
    breakout_level: float
    breakout_distance_atr: float  # (Current_Price - Prior_High) / ATR_1m


def breakout_inputs(
    state: SymbolFeatureState,
    current_price: float,
    strategy: StrategyConfig,
    lookback_period: int = 20,
    atr_period: int = 14,
) -> BreakoutInputs | None:
    """§33A.1.5 / §33B. Prior_High is the highest High over the
    `lookback_period` candles STRICTLY BEFORE the current decision
    point (never includes the still-forming candle, never uses future
    High/Low)."""
    klines = list(state.closed_klines)
    if len(klines) < lookback_period:
        return None
    window = klines[-lookback_period:]
    prior_high = max(k.high for k in window)
    atr = atr_1m(state, period=atr_period)
    if atr is None:
        return None
    breakout_buffer = strategy.breakout_atr_multiplier * atr
    breakout_level = prior_high + breakout_buffer
    breakout_distance_atr = (current_price - prior_high) / atr if atr > EPSILON else 0.0
    return BreakoutInputs(
        prior_high=prior_high,
        breakout_buffer=breakout_buffer,
        breakout_level=breakout_level,
        breakout_distance_atr=breakout_distance_atr,
    )


def volume_1m_usd(state: SymbolFeatureState) -> float | None:
    """Quote-denominated volume of the latest closed 1m candle — used
    as the immediate-liquidity denominator in the Slippage Model (§25),
    NEVER 24h volume / 1440 (explicitly forbidden by §25)."""
    if not state.closed_klines:
        return None
    return state.closed_klines[-1].quote_volume


def range_1m(state: SymbolFeatureState) -> float | None:
    if not state.closed_klines:
        return None
    k = state.closed_klines[-1]
    return k.high - k.low


def dollar_volume_24h(state: SymbolFeatureState) -> float | None:
    """§16 eligibility input, sourced from the ticker stream (never
    the OI or kline streams)."""
    if state.latest_ticker is None:
        return None
    return state.latest_ticker.volume_24h_quote


def spread_percent(state: SymbolFeatureState) -> float | None:
    """§17: spread is a Stage-3, candidate-only metric sourced from
    bookTicker — never derived from Kline OHLC or miniTicker."""
    if state.latest_book_ticker is None:
        return None
    return state.latest_book_ticker.spread_percent
