from app.risk.position_sizing import apply_position_constraints, compute_raw_position_size


def test_compute_raw_position_size_basic():
    risk_amount, raw_size = compute_raw_position_size(account_equity_usd=10_000, risk_per_trade_percent=0.5, stop_distance_percent=0.02)
    assert risk_amount == 50.0
    assert raw_size == 2500.0  # 50 / 0.02


def test_compute_raw_position_size_guards_zero_stop_distance():
    risk_amount, raw_size = compute_raw_position_size(account_equity_usd=10_000, risk_per_trade_percent=0.5, stop_distance_percent=0.0)
    assert raw_size == 0.0


def test_apply_position_constraints_clamps_min_and_max():
    below_min = apply_position_constraints(5.0, min_usd=20.0, max_usd=5000.0)
    assert below_min.final_position_size_usd == 20.0
    assert below_min.clamped_by_min is True

    above_max = apply_position_constraints(10_000.0, min_usd=20.0, max_usd=5000.0)
    assert above_max.final_position_size_usd == 5000.0
    assert above_max.clamped_by_max is True

    within_range = apply_position_constraints(1000.0, min_usd=20.0, max_usd=5000.0)
    assert within_range.final_position_size_usd == 1000.0
    assert within_range.clamped_by_min is False
    assert within_range.clamped_by_max is False
