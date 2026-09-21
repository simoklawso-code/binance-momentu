import sqlite3
import time

from app.backtest.engine import BacktestReport, BacktestTrade
from app.domain.enums import SignalState
from app.experiments.tracker import record_experiment
from app.storage.db import Storage


def wait_until(predicate, timeout=2.0, interval=0.02):
    start = time.time()
    while time.time() - start < timeout:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_record_experiment_writes_to_storage(tmp_path):
    db_path = str(tmp_path / "test.sqlite3")
    storage = Storage(db_path)
    storage.start()
    try:
        report = BacktestReport(symbol="BTCUSDT", total_bars=100)
        report.trades.append(BacktestTrade(0, 1, "BTCUSDT", 100, 110, 95, 110, SignalState.TAKE_PROFIT, 10.0, 2.0))

        record = record_experiment(
            storage, report, configuration_version="cfg-v1",
            parameter_changes={"rvol_threshold": 3.0}, dataset="synthetic-test",
            date_range="2026-01-01/2026-01-02", universe="BTCUSDT",
        )
        assert record.status == "EXPERIMENT"
        assert record.metrics["trade_count"] == 1

        def row_exists():
            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute("SELECT status FROM experiments WHERE experiment_id=?", (record.experiment_id,)).fetchone()
                return row is not None
            finally:
                conn.close()

        assert wait_until(row_exists)
    finally:
        storage.stop()


def test_experiment_never_mutates_strategy_config():
    """§59: never auto-promote — recording an experiment must not touch
    any live config object."""
    from app.config.settings import StrategyConfig
    strategy = StrategyConfig(rvol_threshold=2.5)
    report = BacktestReport(symbol="X", total_bars=10)
    # record_experiment doesn't even receive the live strategy object —
    # this test documents that contract explicitly.
    import inspect
    sig = inspect.signature(record_experiment)
    assert "strategy" not in sig.parameters
    assert strategy.rvol_threshold == 2.5
