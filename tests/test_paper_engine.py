from app.domain.enums import SignalState
from app.execution.paper_engine import PaperTradingEngine


def test_take_profit_resolves_correctly():
    engine = PaperTradingEngine()
    engine.open_position("sig1", "BTCUSDT", "binance", entry_price=100.0, stop_loss=95.0, take_profit=110.0, size_usd=1000.0)
    closed = engine.on_price_update("BTCUSDT", "binance", price=111.0)
    assert len(closed) == 1
    assert closed[0].state == SignalState.TAKE_PROFIT
    assert closed[0].realized_pnl_usd > 0
    assert engine.open_positions == []


def test_stop_loss_resolves_correctly():
    engine = PaperTradingEngine()
    engine.open_position("sig1", "BTCUSDT", "binance", entry_price=100.0, stop_loss=95.0, take_profit=110.0, size_usd=1000.0)
    closed = engine.on_price_update("BTCUSDT", "binance", price=94.0)
    assert len(closed) == 1
    assert closed[0].state == SignalState.STOP_LOSS
    assert closed[0].realized_pnl_usd < 0


def test_price_between_sl_and_tp_does_not_close_position():
    engine = PaperTradingEngine()
    engine.open_position("sig1", "BTCUSDT", "binance", entry_price=100.0, stop_loss=95.0, take_profit=110.0, size_usd=1000.0)
    closed = engine.on_price_update("BTCUSDT", "binance", price=103.0)
    assert closed == []
    assert len(engine.open_positions) == 1


def test_only_matching_symbol_exchange_positions_are_checked():
    engine = PaperTradingEngine()
    engine.open_position("sig1", "BTCUSDT", "binance", entry_price=100.0, stop_loss=95.0, take_profit=110.0, size_usd=1000.0)
    engine.open_position("sig2", "BTCUSDT", "bybit", entry_price=100.0, stop_loss=95.0, take_profit=110.0, size_usd=1000.0)
    closed = engine.on_price_update("BTCUSDT", "binance", price=111.0)
    assert len(closed) == 1
    assert closed[0].signal_id == "sig1"
    assert len(engine.open_positions) == 1
    assert engine.open_positions[0].signal_id == "sig2"


def test_force_close_for_invalidated_state():
    engine = PaperTradingEngine()
    engine.open_position("sig1", "BTCUSDT", "binance", entry_price=100.0, stop_loss=95.0, take_profit=110.0, size_usd=1000.0)
    closed = engine.force_close("sig1", exit_price=101.0, state=SignalState.INVALIDATED)
    assert closed.state == SignalState.INVALIDATED
    assert engine.open_positions == []
