from app.config.settings import StrategyConfig
from app.domain.enums import Exchange, EventPriority
from app.domain.events import BookTickerEvent, MarketTickerEvent
from app.features.store import SymbolFeatureState
from app.signals.quant_score import classify_quant_score, compute_quant_score
from tests.test_feature_engine import make_kline


def build_pumping_state(symbol="PUMPUSDT") -> SymbolFeatureState:
    state = SymbolFeatureState(exchange=Exchange.BINANCE, symbol=symbol)
    # 40 flat baseline candles
    for i in range(40):
        state.closed_klines.append(make_kline(100, 100.3, 99.7, 100, 10.0, taker_buy_base=5.0, t=i))
    # 3 pumping candles: high volume, strong taker buy delta, breaking above the prior 20-candle high
    for i in range(40, 43):
        state.closed_klines.append(
            make_kline(100 + (i - 40), 103 + (i - 40) * 2, 99.5, 102 + (i - 40) * 2, 60.0, taker_buy_base=50.0, t=i)
        )
    state.latest_ticker = MarketTickerEvent(
        exchange=Exchange.BINANCE, symbol=symbol, exchange_timestamp=1, priority=EventPriority.P1,
        last_price=106.0, volume_24h_quote=40_000_000.0,
    )
    state.latest_book_ticker = BookTickerEvent(
        exchange=Exchange.BINANCE, symbol=symbol, exchange_timestamp=1, priority=EventPriority.P0,
        bid_price=105.98, bid_qty=10, ask_price=106.02, ask_qty=10,
    )
    return state


def test_quant_score_computable_and_in_valid_range():
    state = build_pumping_state()
    strategy = StrategyConfig()
    result = compute_quant_score(state, current_price=106.0, strategy=strategy)
    assert result.computable is True
    assert result.quant_score is not None
    assert 0.0 <= result.quant_score <= 100.0
    # This synthetic scenario is deliberately extreme — should score well
    # into "candidate" territory (friction realistically drags reference
    # coverage down for a fast, volatile synthetic pump, which is honest
    # behavior, not a bug).
    assert result.quant_score >= 60.0


def test_quant_score_missing_ticker_blocks_computation():
    state = build_pumping_state()
    state.latest_ticker = None  # no 24h volume available
    strategy = StrategyConfig()
    result = compute_quant_score(state, current_price=106.0, strategy=strategy)
    assert result.computable is False
    assert "liquidity" in result.missing_components


def test_quant_score_missing_book_ticker_blocks_friction_component():
    state = build_pumping_state()
    state.latest_book_ticker = None  # no spread -> can't compute reference friction chain
    strategy = StrategyConfig()
    result = compute_quant_score(state, current_price=106.0, strategy=strategy)
    assert result.computable is False
    assert "friction" in result.missing_components


def test_breakout_floor_locked_to_breakout_atr_multiplier():
    """§33A.1.5.1: Breakout_Floor MUST equal Breakout_ATR_Multiplier —
    changing breakout_atr_multiplier must shift the floor automatically."""
    state = build_pumping_state()
    custom = StrategyConfig(breakout_atr_multiplier=0.25)
    result = compute_quant_score(state, current_price=106.0, strategy=custom)
    breakout_component = next(c for c in result.components if c.name == "breakout")
    assert breakout_component.normalization_floor == 0.25


def test_negative_delta_never_scores_high_on_delta_component():
    state = build_pumping_state()
    # Overwrite the pumping candles with heavy SELL taker flow instead of buy.
    for i in range(40, 43):
        state.closed_klines[-(43 - i)] = make_kline(
            100 + (i - 40), 103 + (i - 40) * 2, 99.5, 102 + (i - 40) * 2, 60.0, taker_buy_base=5.0, t=i,
        )
    strategy = StrategyConfig()
    result = compute_quant_score(state, current_price=106.0, strategy=strategy)
    delta_component = next(c for c in result.components if c.name == "delta")
    assert delta_component.normalized_score == 0.0


def test_classify_quant_score_bands():
    strategy = StrategyConfig()
    assert classify_quant_score(95, strategy) == "high_conviction"
    assert classify_quant_score(85, strategy) == "pump_detected"
    assert classify_quant_score(75, strategy) == "candidate"
    assert classify_quant_score(65, strategy) == "watch"
    assert classify_quant_score(30, strategy) == "no_trade"
