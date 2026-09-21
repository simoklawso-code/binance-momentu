"""Stop Loss (§33C) and Take Profit / Expected Move (§33D, LOCKED
unit-consistent form). LONG only (§33G)."""

from __future__ import annotations

from dataclasses import dataclass

EPSILON = 1e-9


@dataclass
class StopLossResult:
    stop_loss: float
    stop_distance_absolute: float
    stop_distance_percent: float
    valid: bool  # False if Stop_Loss >= Entry_Price -> §33C: NO_TRADE


def compute_stop_loss(entry_price: float, atr_1m: float, sl_atr_multiplier: float) -> StopLossResult:
    """§33C:
        Stop_Distance = SL_ATR_Multiplier * ATR_1m
        Stop_Loss (LONG) = Entry_Price - Stop_Distance
    If Stop_Loss >= Entry_Price: NO_TRADE (guards a degenerate/zero ATR)."""
    stop_distance = sl_atr_multiplier * atr_1m
    stop_loss = entry_price - stop_distance
    valid = stop_loss < entry_price
    stop_distance_percent = stop_distance / entry_price if entry_price > EPSILON else 0.0
    return StopLossResult(
        stop_loss=stop_loss,
        stop_distance_absolute=stop_distance,
        stop_distance_percent=stop_distance_percent,
        valid=valid,
    )


@dataclass
class TakeProfitResult:
    take_profit: float
    expected_move_absolute: float
    expected_move_percent: float
    net_expected_profit: float  # == expected_move_percent, the AUTHORITATIVE gate input (§33D)
    reward_risk_ratio: float


def compute_take_profit(entry_price: float, stop_loss: float, take_profit_rr: float) -> TakeProfitResult:
    """§33D LOCKED, unit-consistent form:
        Risk_Per_Unit = |Entry_Price - Stop_Loss|
        Expected_Move = Take_Profit_RR * Risk_Per_Unit          [absolute price]
        Take_Profit (LONG) = Entry_Price + Expected_Move
        Expected_Move_Percent = Expected_Move / Entry_Price     [dimensionless]
        Net_Expected_Profit = Expected_Move_Percent             [AUTHORITATIVE]
    Never divide Expected_Move (absolute) directly by Total_Friction
    (percent) — always use Net_Expected_Profit / expected_move_percent
    for the Friction Coverage Gate (§26)."""
    risk_per_unit = abs(entry_price - stop_loss)
    expected_move = take_profit_rr * risk_per_unit
    take_profit = entry_price + expected_move
    # Expressed in PERCENT POINTS (e.g. 3.44 means 3.44%), matching the
    # convention used throughout app/signals/friction.py (taker fee,
    # spread, slippage are all percent-point numbers there too) — see
    # the STRICT UNIT SEPARATION rule (§33D): both sides of the
    # Friction Coverage Gate must share one consistent unit, and this
    # module's contract is "percent points" end to end.
    expected_move_percent = (expected_move / entry_price) * 100 if entry_price > EPSILON else 0.0
    return TakeProfitResult(
        take_profit=take_profit,
        expected_move_absolute=expected_move,
        expected_move_percent=expected_move_percent,
        net_expected_profit=expected_move_percent,
        reward_risk_ratio=take_profit_rr,
    )
