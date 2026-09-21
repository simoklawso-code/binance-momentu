"""
Storage layer (§42). SQLite with WAL, a bounded Persistence Queue, and
a dedicated background writer thread — the Main Event Loop and Signal
Engine never perform a direct DB write; they call `enqueue()` (cheap,
non-blocking) and the worker thread batches actual writes.

Tables: signals (one row per SignalResult, including NO_TRADE/REJECTED
— the audit trail, §57), paper_trades (opened/closed paper positions),
claude_decisions (§37: approved / rejected-judgment / rejected-failure
kept distinguishable), system_health (periodic ingestion snapshots).

Retention (§42: "do not store unlimited raw high-frequency data
indefinitely") is implemented as an explicit `purge_older_than()` call
— not automatic, so the caller decides when/how often to run it (e.g.
a daily maintenance task), rather than this module silently deleting
data on its own schedule.
"""

from __future__ import annotations

import json
import logging
import queue
import sqlite3
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    signal_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    decision_timestamp INTEGER NOT NULL,
    final_signal_state TEXT NOT NULL,
    quant_score REAL,
    entry_price REAL,
    stop_loss REAL,
    take_profit REAL,
    final_position_size_usd REAL,
    real_friction_coverage REAL,
    reasons TEXT,
    audit_trail_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol, exchange);
CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at);

CREATE TABLE IF NOT EXISTS paper_trades (
    signal_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    entry_price REAL NOT NULL,
    stop_loss REAL NOT NULL,
    take_profit REAL NOT NULL,
    size_usd REAL NOT NULL,
    opened_at INTEGER NOT NULL,
    closed_at INTEGER,
    exit_price REAL,
    final_state TEXT,
    realized_pnl_usd REAL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS claude_decisions (
    signal_id TEXT NOT NULL,
    decision TEXT NOT NULL,             -- approved | rejected_judgment | rejected_failure
    reasoning_json TEXT,
    latency_ms REAL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_claude_decisions_signal ON claude_decisions(signal_id);

CREATE TABLE IF NOT EXISTS system_health (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS experiments (
    experiment_id TEXT PRIMARY KEY,
    parent_strategy_version TEXT,
    configuration_version TEXT,
    parameter_changes_json TEXT,
    dataset TEXT,
    date_range TEXT,
    universe TEXT,
    random_seed INTEGER,
    backtest_method TEXT,
    metrics_json TEXT,
    observations TEXT,
    status TEXT NOT NULL,          -- BASELINE | EXPERIMENT | VALIDATED | REJECTED
    decision TEXT,
    created_at INTEGER NOT NULL
);
"""


class Storage:
    def __init__(self, db_path: str, busy_timeout_ms: int = 5000, queue_max_size: int = 5000) -> None:
        self._db_path = db_path
        self._busy_timeout_ms = busy_timeout_ms
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._queue: "queue.Queue[tuple[str, tuple]]" = queue.Queue(maxsize=queue_max_size)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._init_schema()
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._worker_loop, name="storage-worker", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=self._busy_timeout_ms / 1000)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
        return conn

    def _init_schema(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    # -- enqueue helpers (cheap, non-blocking, called from the hot path) ---

    def enqueue_signal(self, audit_trail: dict) -> None:
        row = (
            audit_trail["signal_id"], audit_trail["symbol"], audit_trail["exchange"],
            audit_trail["decision_timestamp"], audit_trail["final_signal_state"],
            audit_trail.get("market_state", {}).get("quant_score"),
            audit_trail.get("market_state", {}).get("entry_price"),
            audit_trail.get("risk_state", {}).get("stop_loss"),
            audit_trail.get("risk_state", {}).get("take_profit"),
            audit_trail.get("risk_state", {}).get("final_position_size_usd"),
            audit_trail.get("friction_state", {}).get("real_friction_coverage"),
            json.dumps(audit_trail.get("reasons", [])),
            json.dumps(audit_trail),
            int(time.time() * 1000),
        )
        self._safe_put(("signal", row))

    def enqueue_paper_trade_open(self, position) -> None:
        row = (
            position.signal_id, position.symbol, position.exchange, position.entry_price,
            position.stop_loss, position.take_profit, position.size_usd, position.opened_at_ms,
            None, None, None, None, int(time.time() * 1000),
        )
        self._safe_put(("paper_trade_open", row))

    def enqueue_paper_trade_close(self, position) -> None:
        row = (position.closed_at_ms, position.exit_price, position.state.value, position.realized_pnl_usd, position.signal_id)
        self._safe_put(("paper_trade_close", row))

    def enqueue_claude_decision(self, signal_id: str, decision: str, reasoning: dict, latency_ms: float) -> None:
        row = (signal_id, decision, json.dumps(reasoning), latency_ms, int(time.time() * 1000))
        self._safe_put(("claude_decision", row))

    def enqueue_system_health(self, snapshot: dict) -> None:
        row = (json.dumps(snapshot), int(time.time() * 1000))
        self._safe_put(("system_health", row))

    def enqueue_experiment(self, record: dict) -> None:
        row = (
            record["experiment_id"], record.get("parent_strategy_version"), record.get("configuration_version"),
            json.dumps(record.get("parameter_changes", {})), record.get("dataset"), record.get("date_range"),
            record.get("universe"), record.get("random_seed"), record.get("backtest_method"),
            json.dumps(record.get("metrics", {})), record.get("observations"), record.get("status", "EXPERIMENT"),
            record.get("decision"), int(time.time() * 1000),
        )
        self._safe_put(("experiment", row))

    def _safe_put(self, item: tuple[str, tuple]) -> None:
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            logger.warning("Storage queue full — dropping a %s write (writer thread may be stalled)", item[0])

    # -- worker ------------------------------------------------------------

    def _worker_loop(self) -> None:
        conn = self._connect()
        try:
            while not self._stop_event.is_set() or not self._queue.empty():
                batch = self._drain_batch(max_items=200, timeout=0.5)
                if not batch:
                    continue
                self._write_batch(conn, batch)
        finally:
            conn.close()

    def _drain_batch(self, max_items: int, timeout: float) -> list[tuple[str, tuple]]:
        batch: list[tuple[str, tuple]] = []
        try:
            batch.append(self._queue.get(timeout=timeout))
        except queue.Empty:
            return batch
        while len(batch) < max_items:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return batch

    def _write_batch(self, conn: sqlite3.Connection, batch: list[tuple[str, tuple]]) -> None:
        try:
            for kind, row in batch:
                if kind == "signal":
                    conn.execute(
                        "INSERT OR REPLACE INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row,
                    )
                elif kind == "paper_trade_open":
                    conn.execute(
                        "INSERT OR REPLACE INTO paper_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", row,
                    )
                elif kind == "paper_trade_close":
                    conn.execute(
                        "UPDATE paper_trades SET closed_at=?, exit_price=?, final_state=?, realized_pnl_usd=? WHERE signal_id=?",
                        row,
                    )
                elif kind == "claude_decision":
                    conn.execute("INSERT INTO claude_decisions VALUES (?,?,?,?,?)", row)
                elif kind == "system_health":
                    conn.execute("INSERT INTO system_health (snapshot_json, created_at) VALUES (?,?)", row)
                elif kind == "experiment":
                    conn.execute("INSERT OR REPLACE INTO experiments VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row)
            conn.commit()
        except sqlite3.Error as exc:
            logger.error("Storage write batch failed: %s", exc)
            conn.rollback()

    # -- retention (§42: explicit, never automatic/silent) ------------------

    def purge_older_than(self, table: str, cutoff_ms: int, timestamp_column: str = "created_at") -> int:
        if table not in ("signals", "paper_trades", "system_health"):
            raise ValueError(f"purge not supported for table {table!r}")
        conn = self._connect()
        try:
            cur = conn.execute(f"DELETE FROM {table} WHERE {timestamp_column} < ?", (cutoff_ms,))
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()
