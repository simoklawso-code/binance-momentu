"""Breakout Entry Trigger (§33B, LOCKED). LONG only. Every condition
must hold simultaneously — this function evaluates and reports exactly
which ones failed, for the audit trail (§57)."""

from __future__ import annotations

from dataclasses import dataclass, field

from app.config.settings import StrategyConfig
from app.features import feature_engine as fe
from app.features.store import SymbolFeatureState
from app.signals.quant_score import QuantScoreResult


@dataclass
class BreakoutTriggerResult:
    triggered: bool
    breakout_level: float | None
    prior_high: float | None
    breakout_buffer: float | None
    absorption: bool | None
    reasons_failed: list[str] = field(default_factory=list)


def evaluate_breakout_trigger(
    state: SymbolFeatureState,
    current_price: float,
    quant_result: QuantScoreResult,
    strategy: StrategyConfig,
    btc_pause_active: bool,
) -> BreakoutTriggerResult:
    reasons: list[str] = []

    if not quant_result.computable or quant_result.quant_score is None:
        return BreakoutTriggerResult(
            triggered=False, breakout_level=None, prior_high=None, breakout_buffer=None,
            absorption=None, reasons_failed=["required_market_data_not_fresh_or_incomplete"],
        )

    breakout = quant_result.breakout
    if breakout is None:
        reasons.append("breakout_inputs_unavailable")

    if breakout is not None and current_price < breakout.breakout_level:
        reasons.append("price_below_breakout_level")

    if quant_result.rvol is None or quant_result.rvol < strategy.rvol_threshold:
        reasons.append("rvol_below_threshold")

    if quant_result.trade_acceleration is None or quant_result.trade_acceleration < strategy.acceleration_threshold:
        reasons.append("acceleration_below_threshold")

    if quant_result.net_taker_delta is None or quant_result.net_taker_delta < 0.30:
        reasons.append("taker_delta_below_threshold")

    if quant_result.dollar_volume_24h is None or quant_result.dollar_volume_24h < strategy.min_24h_volume_usd:
        reasons.append("volume_below_threshold")

    spread = fe.spread_percent(state)
    if spread is None or spread > strategy.max_spread_percent:
        reasons.append("spread_above_max_or_unavailable")

    if btc_pause_active:
        reasons.append("btc_volatility_pause_active")

    absorption = fe.absorption_gate(state)
    if absorption is None:
        reasons.append("absorption_state_unavailable")
    elif absorption is True:
        reasons.append("absorption_detected")

    if quant_result.quant_score < strategy.claude_score_threshold:
        reasons.append("quant_score_below_threshold")

    triggered = not reasons
    return BreakoutTriggerResult(
        triggered=triggered,
        breakout_level=breakout.breakout_level if breakout else None,
        prior_high=breakout.prior_high if breakout else None,
        breakout_buffer=breakout.breakout_buffer if breakout else None,
        absorption=absorption,
        reasons_failed=reasons,
    )
