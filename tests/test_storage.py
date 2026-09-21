import sqlite3
import time

from app.storage.db import Storage


def wait_until(predicate, timeout=2.0, interval=0.02):
    start = time.time()
    while time.time() - start < timeout:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_storage_writes_signal_audit_trail(tmp_path):
    db_path = str(tmp_path / "test.sqlite3")
    storage = Storage(db_path)
    storage.start()
    try:
        audit_trail = {
            "signal_id": "sig-1", "symbol": "BTCUSDT", "exchange": "binance",
            "decision_timestamp": 123456789, "final_signal_state": "entry_pending",
            "market_state": {"quant_score": 88.5, "entry_price": 100.0},
            "risk_state": {"stop_loss": 95.0, "take_profit": 110.0, "final_position_size_usd": 500.0},
            "friction_state": {"real_friction_coverage": 3.2},
            "reasons": [],
        }
        storage.enqueue_signal(audit_trail)

        def row_exists():
            conn = sqlite3.connect(db_path)
            try:
                cur = conn.execute("SELECT signal_id, quant_score FROM signals WHERE signal_id=?", ("sig-1",))
                return cur.fetchone() is not None
            finally:
                conn.close()

        assert wait_until(row_exists), "signal row was not written within timeout"

        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT symbol, final_signal_state, quant_score FROM signals WHERE signal_id=?", ("sig-1",)).fetchone()
        conn.close()
        assert row == ("BTCUSDT", "entry_pending", 88.5)
    finally:
        storage.stop()


def test_storage_wal_mode_enabled(tmp_path):
    db_path = str(tmp_path / "test.sqlite3")
    storage = Storage(db_path)
    storage.start()
    try:
        time.sleep(0.1)
        conn = sqlite3.connect(db_path)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        conn.close()
        assert mode.lower() == "wal"
    finally:
        storage.stop()


def test_storage_purge_older_than(tmp_path):
    db_path = str(tmp_path / "test.sqlite3")
    storage = Storage(db_path)
    storage.start()
    try:
        old_trail = {
            "signal_id": "old-sig", "symbol": "AUSDT", "exchange": "binance",
            "decision_timestamp": 1, "final_signal_state": "no_trade",
            "market_state": {}, "risk_state": {}, "friction_state": {}, "reasons": [],
        }
        storage.enqueue_signal(old_trail)
        assert wait_until(lambda: _count_rows(db_path) == 1)

        # purge everything created before "now + 1 day" -> should remove it
        future_cutoff = int(time.time() * 1000) + 86_400_000
        deleted = storage.purge_older_than("signals", future_cutoff)
        assert deleted == 1
        assert _count_rows(db_path) == 0
    finally:
        storage.stop()


def _count_rows(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    finally:
        conn.close()
