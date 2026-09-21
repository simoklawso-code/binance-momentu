from app.config.settings import StrategyConfig
from app.domain.enums import Exchange, EventPriority
from app.domain.events import BookTickerEvent, MarketTickerEvent
from app.features import feature_engine as fe
from app.features.store import SymbolFeatureState
from app.signals.breakout import evaluate_breakout_trigger
from app.signals.quant_score import ComponentAudit, QuantScoreResult
from tests.test_feature_engine import make_kline


def build_ready_state(symbol="TESTUSDT") -> SymbolFeatureState:
    state = SymbolFeatureState(exchange=Exchange.BINANCE, symbol=symbol)
    for i in range(40):
        state.closed_klines.append(make_kline(100, 100.3, 99.7, 100, 10.0, taker_buy_base=5.0, t=i))
    for i in range(40, 43):
        state.closed_klines.append(
            make_kline(100 + (i - 40), 103 + (i - 40) * 2, 99.5, 102 + (i - 40) * 2, 60.0, taker_buy_base=50.0, t=i)
        )
    state.latest_ticker = MarketTickerEvent(
        exchange=Exchange.BINANCE, symbol=symbol, exchange_timestamp=1, priority=EventPriority.P1,
        last_price=108.0, volume_24h_quote=40_000_000.0,
    )
    state.latest_book_ticker = BookTickerEvent(
        exchange=Exchange.BINANCE, symbol=symbol, exchange_timestamp=1, priority=EventPriority.P0,
        bid_price=107.99, bid_qty=10, ask_price=108.01, ask_qty=10,
    )
    return state


def fake_high_quant_result(state: SymbolFeatureState, strategy: StrategyConfig, current_price: float) -> QuantScoreResult:
    """A stubbed, artificially-high-scoring QuantScoreResult so the
    breakout gate's OWN conditions can be tested in isolation, without
    depending on the exact numeric interplay between the Quant Score
    and Breakout components (that interplay is covered at the
    integration level in test_signal_engine.py)."""
    breakout = fe.breakout_inputs(state, current_price, strategy)
    return QuantScoreResult(
        computable=True,
        quant_score=95.0,
        components=[ComponentAudit("stub", 1.0, 100.0, 0.0, 1.0)],
        rvol=fe.rvol(state),
        trade_acceleration=fe.trade_acceleration(state),
        net_taker_delta=fe.net_taker_delta(state),
        momentum_ratio=fe.momentum_ratio(state),
        breakout=breakout,
        dollar_volume_24h=fe.dollar_volume_24h(state),
        atr_1m=fe.atr_1m(state),
    )


def test_breakout_triggers_when_price_clears_level():
    state = build_ready_state()
    strategy = StrategyConfig()
    quant_result = fake_high_quant_result(state, strategy, current_price=108.0)
    result = evaluate_breakout_trigger(state, current_price=108.0, quant_result=quant_result, strategy=strategy, btc_pause_active=False)
    assert result.triggered is True, result.reasons_failed


def test_breakout_fails_when_price_below_level():
    state = build_ready_state()
    strategy = StrategyConfig()
    quant_result = fake_high_quant_result(state, strategy, current_price=101.0)
    result = evaluate_breakout_trigger(state, current_price=101.0, quant_result=quant_result, strategy=strategy, btc_pause_active=False)
    assert result.triggered is False
    assert "price_below_breakout_level" in result.reasons_failed


def test_breakout_fails_when_btc_pause_active():
    state = build_ready_state()
    strategy = StrategyConfig()
    quant_result = fake_high_quant_result(state, strategy, current_price=108.0)
    result = evaluate_breakout_trigger(state, current_price=108.0, quant_result=quant_result, strategy=strategy, btc_pause_active=True)
    assert result.triggered is False
    assert "btc_volatility_pause_active" in result.reasons_failed


def test_breakout_fails_when_spread_too_wide():
    state = build_ready_state()
    state.latest_book_ticker = BookTickerEvent(
        exchange=Exchange.BINANCE, symbol="TESTUSDT", exchange_timestamp=1, priority=EventPriority.P0,
        bid_price=107.0, bid_qty=10, ask_price=109.0, ask_qty=10,  # ~1.85% spread, way above 0.08% max
    )
    strategy = StrategyConfig()
    quant_result = fake_high_quant_result(state, strategy, current_price=108.0)
    result = evaluate_breakout_trigger(state, current_price=108.0, quant_result=quant_result, strategy=strategy, btc_pause_active=False)
    assert result.triggered is False
    assert "spread_above_max_or_unavailable" in result.reasons_failed


def test_breakout_fails_when_quant_score_not_computable():
    state = build_ready_state()
    strategy = StrategyConfig()
    broken_quant_result = QuantScoreResult(computable=False, missing_components=["rvol"])
    result = evaluate_breakout_trigger(state, current_price=108.0, quant_result=broken_quant_result, strategy=strategy, btc_pause_active=False)
    assert result.triggered is False
    assert "required_market_data_not_fresh_or_incomplete" in result.reasons_failed
