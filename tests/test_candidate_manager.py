from app.config.settings import StrategyConfig
from app.domain.enums import Exchange, EventPriority
from app.domain.events import KlineEvent, MarketTickerEvent
from app.features.store import FeatureStore
from app.signals.candidate_manager import CandidateStreamManager, rank_and_promote, screen_stage1
from tests.test_feature_engine import make_kline


def seed_symbol(store: FeatureStore, symbol: str, rvol_boost: float, dollar_volume: float, exchange=Exchange.BINANCE):
    # 40 baseline candles at volume 10 (covers both RVOL's 20-baseline
    # window and Trade Acceleration's 20-baseline window preceding its
    # own 3-candle recent window), then 3 boosted "recent" candles.
    for i in range(40):
        k = make_kline(100, 101, 99, 100, 10.0, exchange=exchange, t=i)
        k = k.model_copy(update={"symbol": symbol})
        store.ingest(k)
    for i in range(40, 43):
        k = make_kline(100, 101, 99, 100, 10.0 * rvol_boost, exchange=exchange, t=i)
        k = k.model_copy(update={"symbol": symbol})
        store.ingest(k)
    ticker = MarketTickerEvent(
        exchange=exchange, symbol=symbol, exchange_timestamp=1, priority=EventPriority.P3,
        last_price=100.0, volume_24h_quote=dollar_volume,
    )
    store.ingest(ticker)


def test_stage1_screens_eligible_and_ineligible_symbols():
    store = FeatureStore()
    strategy = StrategyConfig()
    seed_symbol(store, "GOODUSDT", rvol_boost=5.0, dollar_volume=20_000_000)
    seed_symbol(store, "BADUSDT", rvol_boost=1.0, dollar_volume=1_000_000)

    results = screen_stage1(store, strategy)
    by_symbol = {r.symbol: r for r in results}
    assert by_symbol["GOODUSDT"].eligible is True
    assert by_symbol["BADUSDT"].eligible is False
    assert "volume_below_threshold_or_unavailable" in by_symbol["BADUSDT"].reasons_failed


def test_rank_and_promote_respects_candidate_max():
    store = FeatureStore()
    strategy = StrategyConfig(candidate_max=2)
    seed_symbol(store, "A", rvol_boost=10.0, dollar_volume=20_000_000)
    seed_symbol(store, "B", rvol_boost=8.0, dollar_volume=20_000_000)
    seed_symbol(store, "C", rvol_boost=6.0, dollar_volume=20_000_000)

    results = screen_stage1(store, strategy)
    promoted = rank_and_promote(results, strategy)
    assert len(promoted) == 2
    assert [r.symbol for r in promoted] == ["A", "B"]  # ranked by RVOL desc


def test_candidate_stream_manager_diffs_add_and_remove():
    manager = CandidateStreamManager()
    strategy = StrategyConfig()

    from app.signals.candidate_manager import Stage1Result
    round1 = [
        Stage1Result(Exchange.BINANCE, "A", 5.0, 3.0, 20_000_000, True),
        Stage1Result(Exchange.BINANCE, "B", 4.0, 3.0, 20_000_000, True),
    ]
    diffs = manager.update(round1)
    assert diffs[Exchange.BINANCE][0] == {"A", "B"}  # added
    assert diffs[Exchange.BINANCE][1] == set()  # removed

    round2 = [
        Stage1Result(Exchange.BINANCE, "A", 5.0, 3.0, 20_000_000, True),
        Stage1Result(Exchange.BINANCE, "C", 6.0, 3.0, 20_000_000, True),
    ]
    diffs2 = manager.update(round2)
    assert diffs2[Exchange.BINANCE][0] == {"C"}       # only C is newly added
    assert diffs2[Exchange.BINANCE][1] == {"B"}        # only B was dropped, A untouched
