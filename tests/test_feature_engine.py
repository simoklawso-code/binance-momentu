from app.config.settings import StrategyConfig
from app.domain.enums import Exchange, EventPriority
from app.domain.events import KlineEvent
from app.features import feature_engine as fe
from app.features.store import FeatureStore, SymbolFeatureState


def make_kline(open_p, high, low, close, base_vol, taker_buy_base=None, exchange=Exchange.BINANCE, t=0):
    if taker_buy_base is None:
        taker_buy_base = base_vol * 0.5
    return KlineEvent(
        exchange=exchange, symbol="TESTUSDT",
        exchange_timestamp=1_000_000 + t * 60_000,
        local_ingest_timestamp=1_000_010 + t * 60_000,
        priority=EventPriority.P0,
        open_time=1_000_000 + t * 60_000, close_time=1_000_060 + t * 60_000,
        open=open_p, high=high, low=low, close=close,
        base_volume=base_vol, quote_volume=base_vol * close,
        taker_buy_base_volume=taker_buy_base, taker_buy_quote_volume=taker_buy_base * close,
        is_closed=True,
    )


def state_with_klines(klines, exchange=Exchange.BINANCE):
    state = SymbolFeatureState(exchange=exchange, symbol="TESTUSDT")
    for k in klines:
        state.closed_klines.append(k)
    return state


def test_rvol_uses_median_baseline_and_current_separately():
    # 20 baseline candles with volume 10, then 1 current candle with volume 50
    baseline = [make_kline(100, 101, 99, 100, 10.0, t=i) for i in range(20)]
    current = make_kline(100, 101, 99, 100, 50.0, t=20)
    state = state_with_klines(baseline + [current])
    result = fe.rvol(state, baseline_period=20)
    assert result == 5.0  # 50 / median(10...) = 5.0


def test_rvol_returns_none_with_insufficient_history():
    state = state_with_klines([make_kline(100, 101, 99, 100, 10.0, t=0)])
    assert fe.rvol(state, baseline_period=20) is None


def test_atr_1m_basic_true_range_average():
    klines = [
        make_kline(100, 102, 98, 100, 10, t=0),  # TR = 4 (no prev)
        make_kline(100, 103, 99, 101, 10, t=1),  # TR = max(4, |103-100|=3, |99-100|=1) = 4
        make_kline(101, 105, 100, 104, 10, t=2),  # TR = max(5, |105-101|=4, |100-101|=1) = 5
    ]
    state = state_with_klines(klines)
    result = fe.atr_1m(state, period=2)
    # window = last 3 candles; TRs computed between consecutive pairs (2 TRs)
    assert result == 4.5  # (4 + 5) / 2


def test_trade_acceleration_detects_recent_spike():
    baseline = [make_kline(100, 101, 99, 100, 10.0, t=i) for i in range(20)]
    recent = [make_kline(100, 101, 99, 100, 40.0, t=20 + i) for i in range(3)]
    state = state_with_klines(baseline + recent)
    result = fe.trade_acceleration(state, recent_window=3, baseline_period=20)
    assert result == 4.0  # 40 / median(10s) = 4.0


def test_absorption_gate_triggers_on_high_delta_low_impact():
    # Build ATR baseline with small ranges, then a candle with high taker
    # buy delta but a tiny price change relative to ATR.
    baseline = [make_kline(100, 100.5, 99.5, 100, 10.0, t=i) for i in range(15)]
    # open=100, close=100.05 -> tiny price change; taker_buy_base=95/100=0.95 delta -> (2*95-100)/100=0.90
    absorbing = make_kline(100, 100.6, 99.9, 100.05, 100.0, taker_buy_base=95.0, t=15)
    state = state_with_klines(baseline + [absorbing])
    result = fe.absorption_gate(state, atr_period=14)
    assert result is True


def test_absorption_gate_false_when_price_moves_with_volume():
    baseline = [make_kline(100, 100.5, 99.5, 100, 10.0, t=i) for i in range(15)]
    # Large price change alongside high delta -> not absorption (real move, not absorption)
    moving = make_kline(100, 106, 99.5, 105.0, 100.0, taker_buy_base=95.0, t=15)
    state = state_with_klines(baseline + [moving])
    result = fe.absorption_gate(state, atr_period=14)
    assert result is False


def test_absorption_gate_none_for_bybit_missing_taker_split():
    baseline = [make_kline(100, 100.5, 99.5, 100, 10.0, exchange=Exchange.BYBIT, t=i) for i in range(15)]
    state = state_with_klines(baseline, exchange=Exchange.BYBIT)
    assert fe.absorption_gate(state) is None


def test_net_taker_delta_unavailable_for_bybit_is_none_not_zero():
    state = state_with_klines([make_kline(100, 101, 99, 100, 10.0, exchange=Exchange.BYBIT, t=0)], exchange=Exchange.BYBIT)
    assert fe.net_taker_delta(state) is None


def test_momentum_ratio_computation():
    baseline = [make_kline(100, 100.5, 99.5, 100, 10.0, t=i) for i in range(15)]  # ATR baseline, flat
    moved = [
        make_kline(100, 100.5, 99.5, 100, 10.0, t=15),
        make_kline(100, 100.5, 99.5, 100, 10.0, t=16),
        make_kline(100, 106, 99.5, 105, 10.0, t=17),  # big move on the 3rd candle
    ]
    state = state_with_klines(baseline + moved)
    result = fe.momentum_ratio(state, window_minutes=3, atr_period=14)
    assert result is not None
    assert result > 1.0  # a 5% move against a ~0.5-1 point ATR baseline should be a large ratio


def test_breakout_inputs_prior_high_and_level():
    klines = [make_kline(100, 100 + i * 0.1, 99, 100, 10.0, t=i) for i in range(20)]
    state = state_with_klines(klines)
    strategy = StrategyConfig(breakout_atr_multiplier=0.10)
    result = fe.breakout_inputs(state, current_price=103.0, strategy=strategy, lookback_period=20, atr_period=14)
    assert result is not None
    assert result.prior_high == max(k.high for k in klines)


def test_dollar_volume_and_spread_missing_return_none():
    state = SymbolFeatureState(exchange=Exchange.BINANCE, symbol="TESTUSDT")
    assert fe.dollar_volume_24h(state) is None
    assert fe.spread_percent(state) is None
