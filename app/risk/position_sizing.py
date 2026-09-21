"""Position Sizing (§32, LOCKED order). Never calculate size from a
fixed dollar amount alone; never shrink size just to force the
Friction Coverage Gate to pass (§32 step 5)."""

from __future__ import annotations

from dataclasses import dataclass

EPSILON = 1e-9


@dataclass
class PositionSizeResult:
    risk_amount_usd: float
    raw_position_size_usd: float
    final_position_size_usd: float
    clamped_by_min: bool
    clamped_by_max: bool


def compute_raw_position_size(account_equity_usd: float, risk_per_trade_percent: float, stop_distance_percent: float) -> tuple[float, float]:
    """§32 steps 1-2:
        Risk Amount = Account Equity * Risk Per Trade %
        Raw Position Size = Risk Amount / Stop Loss Distance (%)
    Returns (risk_amount_usd, raw_position_size_usd)."""
    risk_amount = account_equity_usd * (risk_per_trade_percent / 100.0)
    if stop_distance_percent <= EPSILON:
        return risk_amount, 0.0
    raw_size = risk_amount / stop_distance_percent
    return risk_amount, raw_size


def apply_position_constraints(raw_position_size_usd: float, min_usd: float, max_usd: float) -> PositionSizeResult:
    """§32 step 6: apply min/max AFTER the Friction Coverage Gate has
    already passed at Raw Position Size (step 4) — this function only
    clamps the final tradable size, it must never be used to decide
    whether the gate passed."""
    final = raw_position_size_usd
    clamped_by_min = False
    clamped_by_max = False
    if final < min_usd:
        final = min_usd
        clamped_by_min = True
    if final > max_usd:
        final = max_usd
        clamped_by_max = True
    return PositionSizeResult(
        risk_amount_usd=0.0,  # filled in by caller (compute_raw_position_size call site)
        raw_position_size_usd=raw_position_size_usd,
        final_position_size_usd=final,
        clamped_by_min=clamped_by_min,
        clamped_by_max=clamped_by_max,
    )
