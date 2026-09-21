import datetime as dt

from app.config.settings import StrategyConfig
from app.risk.risk_engine import RiskEngine


def test_risk_engine_blocks_when_max_positions_reached():
    strategy = StrategyConfig(max_open_positions=2)
    engine = RiskEngine(strategy)
    engine.register_open_position("s1", "AUSDT")
    engine.register_open_position("s2", "BUSDT")
    result = engine.check_trade_allowed(btc_pause_active=False)
    assert result.allowed is False
    assert "max_open_positions_reached" in result.reasons_blocked


def test_risk_engine_blocks_on_btc_pause():
    strategy = StrategyConfig()
    engine = RiskEngine(strategy)
    result = engine.check_trade_allowed(btc_pause_active=True)
    assert result.allowed is False
    assert "btc_volatility_pause_active" in result.reasons_blocked


def test_risk_engine_blocks_when_portfolio_heat_would_be_exceeded():
    strategy = StrategyConfig(risk_per_trade_percent=1.0, max_portfolio_heat=1.5, max_open_positions=10)
    engine = RiskEngine(strategy)
    engine.register_open_position("s1", "AUSDT")  # heat = 1.0%
    result = engine.check_trade_allowed(btc_pause_active=False)
    # 1.0 (existing) + 1.0 (new) = 2.0 > 1.5 max -> blocked
    assert result.allowed is False
    assert "max_portfolio_heat_would_be_exceeded" in result.reasons_blocked


def test_risk_engine_daily_loss_limit_freezes_trading():
    strategy = StrategyConfig(daily_loss_limit_percent=3.0)
    engine = RiskEngine(strategy)
    engine.register_open_position("s1", "AUSDT")
    engine.register_closed_position("s1", realized_pnl_percent_of_equity=-3.5)
    assert engine.is_frozen_for_today is True
    result = engine.check_trade_allowed(btc_pause_active=False)
    assert result.allowed is False
    assert "daily_loss_limit_freeze_active" in result.reasons_blocked


def test_risk_engine_no_automatic_recovery_by_increasing_risk():
    """§30: a loss never changes risk_per_trade_percent itself —
    verify the config value the engine reads is untouched after a loss."""
    strategy = StrategyConfig(risk_per_trade_percent=0.5)
    engine = RiskEngine(strategy)
    engine.register_open_position("s1", "AUSDT")
    engine.register_closed_position("s1", realized_pnl_percent_of_equity=-1.0)
    assert strategy.risk_per_trade_percent == 0.5  # unchanged


def test_risk_engine_daily_reset_clears_freeze():
    strategy = StrategyConfig(daily_loss_limit_percent=3.0)
    engine = RiskEngine(strategy)
    engine.register_open_position("s1", "AUSDT")
    yesterday = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)
    engine.register_closed_position("s1", realized_pnl_percent_of_equity=-5.0, now=yesterday)
    assert engine.is_frozen_for_today is True

    today = dt.datetime.now(dt.timezone.utc)
    result = engine.check_trade_allowed(btc_pause_active=False, now=today)
    assert result.allowed is True
    assert engine.is_frozen_for_today is False
