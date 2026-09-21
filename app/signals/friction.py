"""
Friction Model (§24), Slippage Model (§25), Friction Coverage Gate
(§26). All percentages here are expressed as plain percent numbers
(e.g. 0.05 means 0.05%, NOT 0.0005) for readability, and are converted
consistently — see the STRICT UNIT SEPARATION rule in §33D: as long as
Net_Expected_Profit and Total_Friction both use the SAME convention,
the ratio is correct. This module keeps everything in "percent points"
throughout.
"""

from __future__ import annotations

EPSILON = 1e-9


def estimated_slippage_percent(
    order_size_usd: float,
    range_1m: float,
    atr_1m: float,
    volume_1m_usd: float,
    base_slippage_percent: float,
    gamma: float,
    minimum_liquidity_floor: float = 1.0,
) -> float:
    """§25:
        Estimated_Slippage% = Base_Slippage%
            + Gamma * (Range_1m / max(ATR_1m, eps))
              * sqrt(Order_Size_USD / max(Volume_1m_USD, Minimum_Liquidity_Floor))
    Never substitutes 24h volume for Volume_1m_USD (caller's responsibility
    to pass the correct 1-minute figure — see feature_engine.volume_1m_usd).
    """
    import math

    atr_safe = max(atr_1m, EPSILON)
    volume_floor = max(volume_1m_usd, minimum_liquidity_floor)
    slippage = base_slippage_percent + gamma * (range_1m / atr_safe) * math.sqrt(order_size_usd / volume_floor)
    return max(0.0, slippage)


def total_friction_percent(taker_fee_percent: float, spread_percent: float, slippage_percent: float) -> float:
    """§24: Total Friction = 2 * Taker Fee + Spread + Estimated Slippage
    (2x taker fee because a scalp both enters and exits with a taker
    order, per the "round trip" cost the spec's Friction Model implies)."""
    return 2 * taker_fee_percent + spread_percent + slippage_percent


def friction_coverage(net_expected_profit_percent: float, total_friction_percent_value: float) -> float:
    """§26: Friction_Coverage = Net_Expected_Profit / max(Total_Friction, eps).
    Both inputs MUST already be the same dimensionless unit (§33D STRICT
    UNIT SEPARATION) — this function does no conversion."""
    return net_expected_profit_percent / max(total_friction_percent_value, EPSILON)


def friction_gate_passes(net_expected_profit_percent: float, total_friction_percent_value: float, ratio_threshold: float) -> bool:
    """§26 gate: Net_Expected_Profit >= ratio_threshold * Total_Friction."""
    return net_expected_profit_percent >= ratio_threshold * total_friction_percent_value
