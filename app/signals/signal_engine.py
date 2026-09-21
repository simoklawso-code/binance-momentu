"""
Signal Engine (§33E LOCKED calculation order, steps 1-19).

No step depends on a later step. This module implements steps 8-19:
Quant Score (already covered by quant_score.py, called here) through
the Risk Engine gate. Steps 20-23 (Claude Auditor) live in
app/ai/claude_auditor.py and are invoked by the caller AFTER this
module returns an ENTRY_PENDING result — the Signal Engine itself
never calls Claude (§36: Claude is a veto gate, not part of the
quantitative pipeline).

No later stage may modify a previously calculated Entry, Stop Loss, or
Take Profit to force a trade through a failed gate (§33E). No step may
shrink Raw Position Size merely to pass the Friction Coverage Gate
(§32 step 5, §33E step 17).
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field

from app.config.settings import StrategyConfig
from app.domain.enums import SignalState
from app.features import feature_engine as fe
from app.features.btc_filter import btc_volatility_pause_active
from app.features.store import FeatureStore, SymbolFeatureState
from app.risk.position_sizing import apply_position_constraints, compute_raw_position_size
from app.risk.risk_engine import RiskEngine
from app.signals import friction as friction_mod
from app.signals.breakout import BreakoutTriggerResult, evaluate_breakout_trigger
from app.signals.exit_levels import StopLossResult, TakeProfitResult, compute_stop_loss, compute_take_profit
from app.signals.quant_score import QuantScoreResult, compute_quant_score


@dataclass
class SignalResult:
    signal_id: str
    symbol: str
    exchange: str
    state: SignalState
    reasons: list[str] = field(default_factory=list)

    quant_result: QuantScoreResult | None = None
    breakout_result: BreakoutTriggerResult | None = None
    entry_price: float | None = None
    stop_loss_result: StopLossResult | None = None
    take_profit_result: TakeProfitResult | None = None
    raw_position_size_usd: float | None = None
    final_position_size_usd: float | None = None
    real_total_friction_percent: float | None = None
    real_friction_coverage: float | None = None

    audit_trail: dict = field(default_factory=dict)


def evaluate_signal(
    state: SymbolFeatureState,
    current_price: float,
    strategy: StrategyConfig,
    risk_engine: RiskEngine,
    account_equity_usd: float,
    btc_pause_active: bool,
    decision_timestamp_ms: int | None = None,
) -> SignalResult:
    signal_id = str(uuid.uuid4())
    decision_ts = decision_timestamp_ms if decision_timestamp_ms is not None else int(time.time() * 1000)

    def finalize(sig_state: SignalState, reasons: list[str], **extra) -> SignalResult:
        result = SignalResult(signal_id=signal_id, symbol=state.symbol, exchange=state.exchange.value,
                               state=sig_state, reasons=reasons, **extra)
        result.audit_trail = build_audit_trail(state, decision_ts, result)
        return result

    # Step 8-9: Quant Score (includes reference friction chain).
    quant_result = compute_quant_score(state, current_price, strategy)
    if not quant_result.computable:
        return finalize(SignalState.NO_TRADE, [f"missing_component:{c}" for c in quant_result.missing_components],
                         quant_result=quant_result)

    if quant_result.quant_score < strategy.claude_score_threshold:
        return finalize(SignalState.NO_TRADE, ["quant_score_below_threshold"], quant_result=quant_result)

    # Step 10: Breakout Entry Trigger (§33B).
    breakout_result = evaluate_breakout_trigger(state, current_price, quant_result, strategy, btc_pause_active)
    if not breakout_result.triggered:
        return finalize(SignalState.NO_TRADE, breakout_result.reasons_failed,
                         quant_result=quant_result, breakout_result=breakout_result)

    # Step 11: real Entry Price = first valid observed market price (paper trading).
    entry_price = current_price

    # Step 12: real Stop Loss (§33C).
    if quant_result.atr_1m is None:
        return finalize(SignalState.NO_TRADE, ["atr_unavailable_for_stop_loss"],
                         quant_result=quant_result, breakout_result=breakout_result, entry_price=entry_price)
    stop_loss_result = compute_stop_loss(entry_price, quant_result.atr_1m, strategy.sl_atr_multiplier)
    if not stop_loss_result.valid:
        return finalize(SignalState.NO_TRADE, ["stop_loss_invalid_gte_entry"],
                         quant_result=quant_result, breakout_result=breakout_result,
                         entry_price=entry_price, stop_loss_result=stop_loss_result)

    # Step 13: real Take Profit / Expected Move (§33D).
    take_profit_result = compute_take_profit(entry_price, stop_loss_result.stop_loss, strategy.take_profit_rr)

    # Step 15: Raw Position Size (§32 steps 1-2).
    risk_amount, raw_position_size = compute_raw_position_size(
        account_equity_usd, strategy.risk_per_trade_percent, stop_loss_result.stop_distance_percent,
    )
    if raw_position_size <= 0:
        return finalize(SignalState.NO_TRADE, ["raw_position_size_invalid"],
                         quant_result=quant_result, breakout_result=breakout_result,
                         entry_price=entry_price, stop_loss_result=stop_loss_result,
                         take_profit_result=take_profit_result)

    # Step 16: real, size-dependent friction at Raw Position Size (§25, §32 step 3).
    range_1m_value = fe.range_1m(state)
    volume_1m = fe.volume_1m_usd(state)
    spread = fe.spread_percent(state)
    if range_1m_value is None or volume_1m is None or spread is None:
        return finalize(SignalState.NO_TRADE, ["real_friction_inputs_unavailable"],
                         quant_result=quant_result, breakout_result=breakout_result,
                         entry_price=entry_price, stop_loss_result=stop_loss_result,
                         take_profit_result=take_profit_result, raw_position_size_usd=raw_position_size)

    real_slippage = friction_mod.estimated_slippage_percent(
        order_size_usd=raw_position_size, range_1m=range_1m_value, atr_1m=quant_result.atr_1m,
        volume_1m_usd=volume_1m, base_slippage_percent=strategy.base_slippage_percent, gamma=strategy.slippage_gamma,
    )
    real_total_friction = friction_mod.total_friction_percent(strategy.taker_fee_percent, spread, real_slippage)
    real_coverage = friction_mod.friction_coverage(take_profit_result.net_expected_profit, real_total_friction)

    # Step 17: AUTHORITATIVE Friction Coverage Gate (§26, §33D, §32 step 4).
    # Rejected here even if Quant_Score >= 80 and Reference_Friction_Coverage passed.
    if not friction_mod.friction_gate_passes(take_profit_result.net_expected_profit, real_total_friction, strategy.friction_coverage_ratio):
        return finalize(SignalState.REJECTED, ["friction_coverage_gate_failed"],
                         quant_result=quant_result, breakout_result=breakout_result,
                         entry_price=entry_price, stop_loss_result=stop_loss_result,
                         take_profit_result=take_profit_result, raw_position_size_usd=raw_position_size,
                         real_total_friction_percent=real_total_friction, real_friction_coverage=real_coverage)

    # Step 6: min/max position constraints -> Final Position Size (§32 step 6).
    position_constraints = apply_position_constraints(raw_position_size, strategy.min_position_size_usd, strategy.max_position_size_usd)

    # Step 18: Risk Engine (§30, §31).
    risk_check = risk_engine.check_trade_allowed(btc_pause_active)
    if not risk_check.allowed:
        return finalize(SignalState.REJECTED, risk_check.reasons_blocked,
                         quant_result=quant_result, breakout_result=breakout_result,
                         entry_price=entry_price, stop_loss_result=stop_loss_result,
                         take_profit_result=take_profit_result, raw_position_size_usd=raw_position_size,
                         final_position_size_usd=position_constraints.final_position_size_usd,
                         real_total_friction_percent=real_total_friction, real_friction_coverage=real_coverage)

    # Step 19: all quantitative gates passed -> ENTRY_PENDING, awaiting Claude (§20-23, separate module).
    return finalize(SignalState.ENTRY_PENDING, [],
                     quant_result=quant_result, breakout_result=breakout_result,
                     entry_price=entry_price, stop_loss_result=stop_loss_result,
                     take_profit_result=take_profit_result, raw_position_size_usd=raw_position_size,
                     final_position_size_usd=position_constraints.final_position_size_usd,
                     real_total_friction_percent=real_total_friction, real_friction_coverage=real_coverage)


def build_audit_trail(state: SymbolFeatureState, decision_timestamp_ms: int, result: SignalResult) -> dict:
    """§57: signal_id, symbol, exchange, decision_timestamp,
    feature_snapshot, threshold_snapshot, market_state, risk_state,
    friction_state, Claude_state (filled in later by the caller once
    the Claude Auditor has run), final_signal_state."""
    feature_snapshot = {}
    if result.quant_result is not None:
        feature_snapshot = {
            c.name: {
                "value": c.component_value, "score": c.normalized_score,
                "floor": c.normalization_floor, "ceiling": c.normalization_ceiling,
            }
            for c in result.quant_result.components
        }
    return {
        "signal_id": result.signal_id,
        "symbol": state.symbol,
        "exchange": state.exchange.value,
        "decision_timestamp": decision_timestamp_ms,
        "feature_snapshot": feature_snapshot,
        "market_state": {
            "quant_score": result.quant_result.quant_score if result.quant_result else None,
            "entry_price": result.entry_price,
            "breakout_level": result.breakout_result.breakout_level if result.breakout_result else None,
            "absorption": result.breakout_result.absorption if result.breakout_result else None,
        },
        "risk_state": {
            "stop_loss": result.stop_loss_result.stop_loss if result.stop_loss_result else None,
            "take_profit": result.take_profit_result.take_profit if result.take_profit_result else None,
            "raw_position_size_usd": result.raw_position_size_usd,
            "final_position_size_usd": result.final_position_size_usd,
        },
        "friction_state": {
            "real_total_friction_percent": result.real_total_friction_percent,
            "real_friction_coverage": result.real_friction_coverage,
        },
        "claude_state": None,
        "final_signal_state": result.state.value,
        "reasons": result.reasons,
    }


def compute_btc_pause(store: FeatureStore, strategy: StrategyConfig) -> bool:
    return btc_volatility_pause_active(store, strategy.btc_volatility_pause_threshold_percent)
