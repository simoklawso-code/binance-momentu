from app.config.settings import StrategyConfig
from app.domain.enums import Exchange, EventPriority, SignalState
from app.domain.events import BookTickerEvent, MarketTickerEvent
from app.features.store import SymbolFeatureState
from app.risk.risk_engine import RiskEngine
from app.signals.signal_engine import evaluate_signal
from tests.test_feature_engine import make_kline


def build_breakout_ready_state(symbol="PUMPUSDT", price=115.0) -> SymbolFeatureState:
    state = SymbolFeatureState(exchange=Exchange.BINANCE, symbol=symbol)
    for i in range(40):
        state.closed_klines.append(make_kline(100, 100.3, 99.7, 100, 10.0, taker_buy_base=5.0, t=i))
    for i in range(40, 43):
        state.closed_klines.append(
            make_kline(100 + (i - 40), 103 + (i - 40) * 2, 99.5, 102 + (i - 40) * 2, 60.0, taker_buy_base=50.0, t=i)
        )
    state.latest_ticker = MarketTickerEvent(
        exchange=Exchange.BINANCE, symbol=symbol, exchange_timestamp=1, priority=EventPriority.P1,
        last_price=price, volume_24h_quote=40_000_000.0,
    )
    state.latest_book_ticker = BookTickerEvent(
        exchange=Exchange.BINANCE, symbol=symbol, exchange_timestamp=1, priority=EventPriority.P0,
        bid_price=price - 0.01, bid_qty=10, ask_price=price + 0.01, ask_qty=10,
    )
    return state


def test_signal_engine_full_pass_reaches_entry_pending():
    state = build_breakout_ready_state()
    strategy = StrategyConfig()
    risk_engine = RiskEngine(strategy)
    result = evaluate_signal(
        state, current_price=115.0, strategy=strategy, risk_engine=risk_engine,
        account_equity_usd=strategy.paper_starting_equity_usd, btc_pause_active=False,
    )
    assert result.state == SignalState.ENTRY_PENDING, result.reasons
    assert result.entry_price == 115.0
    assert result.stop_loss_result is not None and result.stop_loss_result.valid
    assert result.take_profit_result is not None
    assert result.take_profit_result.take_profit > result.entry_price
    assert result.stop_loss_result.stop_loss < result.entry_price
    assert result.final_position_size_usd is not None
    assert result.real_friction_coverage is not None
    assert result.audit_trail["final_signal_state"] == "entry_pending"


def test_signal_engine_no_trade_when_price_has_not_broken_out():
    """A price that hasn't meaningfully broken out also naturally fails
    to reach the Quant Score threshold in this synthetic scenario
    (momentum/breakout components score low too) — the important
    behavior is that the pipeline correctly refuses to trade, not
    which specific upstream gate reports it first. See
    tests/test_breakout.py for the breakout gate tested in isolation."""
    state = build_breakout_ready_state(price=101.0)  # price hasn't actually broken out
    strategy = StrategyConfig()
    risk_engine = RiskEngine(strategy)
    result = evaluate_signal(
        state, current_price=101.0, strategy=strategy, risk_engine=risk_engine,
        account_equity_usd=strategy.paper_starting_equity_usd, btc_pause_active=False,
    )
    assert result.state == SignalState.NO_TRADE
    assert result.reasons  # some documented reason was recorded, never a silent NO_TRADE


def test_signal_engine_no_trade_on_btc_pause():
    state = build_breakout_ready_state()
    strategy = StrategyConfig()
    risk_engine = RiskEngine(strategy)
    result = evaluate_signal(
        state, current_price=115.0, strategy=strategy, risk_engine=risk_engine,
        account_equity_usd=strategy.paper_starting_equity_usd, btc_pause_active=True,
    )
    assert result.state == SignalState.NO_TRADE
    assert "btc_volatility_pause_active" in result.reasons


def test_signal_engine_no_trade_on_insufficient_data():
    state = SymbolFeatureState(exchange=Exchange.BINANCE, symbol="EMPTYUSDT")
    strategy = StrategyConfig()
    risk_engine = RiskEngine(strategy)
    result = evaluate_signal(
        state, current_price=100.0, strategy=strategy, risk_engine=risk_engine,
        account_equity_usd=strategy.paper_starting_equity_usd, btc_pause_active=False,
    )
    assert result.state == SignalState.NO_TRADE
    assert any(r.startswith("missing_component") for r in result.reasons)


def test_signal_engine_rejected_when_max_open_positions_reached():
    state = build_breakout_ready_state()
    strategy = StrategyConfig(max_open_positions=1)
    risk_engine = RiskEngine(strategy)
    risk_engine.register_open_position("existing-signal", "OTHERUSDT")

    result = evaluate_signal(
        state, current_price=115.0, strategy=strategy, risk_engine=risk_engine,
        account_equity_usd=strategy.paper_starting_equity_usd, btc_pause_active=False,
    )
    assert result.state == SignalState.REJECTED
    assert "max_open_positions_reached" in result.reasons


def test_signal_engine_rejected_when_friction_coverage_fails():
    state = build_breakout_ready_state()
    # An absurdly high friction ratio requirement forces the authoritative gate to fail.
    strategy = StrategyConfig(friction_coverage_ratio=1000.0)
    risk_engine = RiskEngine(strategy)
    result = evaluate_signal(
        state, current_price=115.0, strategy=strategy, risk_engine=risk_engine,
        account_equity_usd=strategy.paper_starting_equity_usd, btc_pause_active=False,
    )
    assert result.state == SignalState.REJECTED
    assert "friction_coverage_gate_failed" in result.reasons
