from app.domain.enums import Exchange
from app.features.btc_filter import btc_15m_price_change_percent, btc_volatility_pause_active
from app.features.store import FeatureStore
from tests.test_feature_engine import make_kline


def test_btc_filter_triggers_pause_above_threshold():
    store = FeatureStore()
    # 14 flat candles, then a 2% jump on the 15th (relative to 15-candles-ago close)
    for i in range(14):
        k = make_kline(100, 100.5, 99.5, 100, 10.0, t=i).model_copy(update={"symbol": "BTCUSDT"})
        store.ingest(k)
    jump = make_kline(100, 103, 99.5, 102.5, 10.0, t=14).model_copy(update={"symbol": "BTCUSDT"})
    store.ingest(jump)

    change = btc_15m_price_change_percent(store)
    assert change is not None
    assert round(change, 2) == 2.5

    assert btc_volatility_pause_active(store, threshold_percent=1.2) is True
    assert btc_volatility_pause_active(store, threshold_percent=5.0) is False


def test_btc_filter_not_paused_when_data_unavailable():
    store = FeatureStore()
    assert btc_volatility_pause_active(store, threshold_percent=1.2) is False
