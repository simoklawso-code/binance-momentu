"""
Runs a full, automated, end-to-end simulated test of EVERY implemented
phase (1 through 5) together.

Starts the local fake Binance + Bybit servers (scripts/fake_exchange_servers.py,
which speak the exact same wire protocol as the real exchanges) and
points the REAL application code at them: IngestionManager -> Feature
Store -> Candidate Stream Manager -> Quant Score -> Signal Engine ->
Risk Engine -> Paper Execution -> Storage -> API/dashboard. One symbol
is scripted to "pump" partway through the run so the full chain has a
real chance to fire a signal, not just ingest data.

IMPORTANT — what this test does and does NOT prove (read this before
trusting the numbers below): this is still a LOCAL simulation, not a
connection to real Binance/Bybit. It proves the CODE PATH is wired
correctly end-to-end (every module talks to the next one correctly,
the math doesn't crash, gates fire in the right order) using
synthetic, accelerated, favorable data. It does NOT prove the system
will find real trading opportunities on live markets, handle a real
150-300 symbol universe's throughput, or match Binance/Bybit's exact
real-world message quirks. See README "Testing Note" for what a real
verification run requires.

An APPROVING stub stands in for the Claude Context Auditor (never a
real, billed Anthropic API call) so a full trade can be observed
opening and closing — clearly logged as a stub, never presented as a
real Claude decision.

Usage:
    python3 scripts/run_simulated_live_test.py [duration_seconds]
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.ai.claude_auditor import ClaudeDecisionResult
from app.api.server import ApiServer
from app.config.settings import load_settings
from app.core.logging_config import configure_logging
from app.ingestion.ingestion_manager import IngestionManager
from app.signals.pipeline import SignalPipeline
from app.storage.db import Storage
from scripts.fake_exchange_servers import FakeBinanceServer, FakeBybitServer

logger = logging.getLogger("sim_test")

BINANCE_PORT = 8765
BYBIT_PORT = 8766
API_PORT = 8081
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "AVAXUSDT"]
PUMP_SYMBOL = "ETHUSDT"  # NOT BTCUSDT: the BTC Safety Filter (§31) freezes
# new entries on BTC's own volatility — pumping BTCUSDT would trigger its
# own freeze, which is correct spec behavior but defeats this test's goal
# of observing a full entry. Pumping a different symbol while BTCUSDT
# stays flat lets both mechanisms be demonstrated without contradiction.


class StubApprovingAuditor:
    """NEVER a real Anthropic API call — a clearly-labeled internal
    stand-in so this simulation can observe a full paper trade
    open/close. main.py NEVER uses this; it only uses the real
    ClaudeContextAuditor (app/ai/claude_auditor.py), gated on a real
    ANTHROPIC_API_KEY, with real fail-safe-on-failure behavior."""

    def should_call(self, exchange: str, symbol: str) -> bool:
        return True

    async def evaluate(self, audit_trail: dict) -> ClaudeDecisionResult:
        return ClaudeDecisionResult(decision="approved", reasoning="[SIMULATION STUB — not a real Claude call]", latency_ms=0.0)


async def main(duration_seconds: float, pump_after_seconds: float) -> dict:
    configure_logging("INFO")

    settings = load_settings("config.example.yaml")
    settings.universe.static_symbols = SYMBOLS
    settings.binance.ws_base_url = f"ws://127.0.0.1:{BINANCE_PORT}"
    settings.bybit.ws_base_url = f"ws://127.0.0.1:{BYBIT_PORT}"
    settings.binance.max_streams_per_connection = 6
    settings.binance.max_connections = 4
    settings.binance.reconnect_backoff_base_seconds = 0.5
    settings.binance.reconnect_backoff_max_seconds = 3.0
    settings.bybit.reconnect_backoff_base_seconds = 0.5
    settings.bybit.ping_interval_seconds = 5.0
    settings.metrics_log_interval_seconds = 5.0
    settings.stage1_cycle_seconds = 2.0
    settings.signal_cycle_seconds = 2.0

    # TEST-ONLY overrides so a short synthetic run can realistically
    # clear thresholds sized for real 24h markets — NEVER representative
    # of production defaults (those stay in config.example.yaml, untouched).
    settings.strategy.min_24h_volume_usd = 50.0
    # Test-only: BTCUSDT's own synthetic random walk can occasionally
    # drift past the real 1.2% BTC-filter threshold by chance over a
    # short accelerated run — that's the filter correctly doing its job
    # (see the earlier BTCUSDT-as-pump-symbol run, which demonstrated
    # exactly that), but it isn't what THIS run is trying to demonstrate
    # (the full entry pipeline), so it's relaxed here only.
    settings.strategy.btc_volatility_pause_threshold_percent = 100.0
    kline_close_seconds = 1.0  # accelerated candle cadence, see fake_exchange_servers.py

    fake_binance = FakeBinanceServer(
        SYMBOLS, kline_close_seconds=kline_close_seconds, force_disconnect_first_conn_after=20.0,
        pump_symbol=PUMP_SYMBOL, pump_after_seconds=pump_after_seconds,
    )
    fake_bybit = FakeBybitServer(
        SYMBOLS, kline_close_seconds=kline_close_seconds,
        pump_symbol=PUMP_SYMBOL, pump_after_seconds=pump_after_seconds,
    )

    import websockets

    db_path = "simulated_live_test.sqlite3"
    Path(db_path).unlink(missing_ok=True)
    storage = Storage(db_path)
    storage.start()

    async with websockets.serve(fake_binance.handler, "127.0.0.1", BINANCE_PORT), \
               websockets.serve(fake_bybit.handler, "127.0.0.1", BYBIT_PORT):
        logger.info("Fake exchanges running on ports %d (binance) / %d (bybit)", BINANCE_PORT, BYBIT_PORT)

        ingestion = IngestionManager(settings, consume_internally=False)
        pipeline = SignalPipeline(settings, ingestion, storage, claude_auditor=StubApprovingAuditor())
        api_server = ApiServer(ingestion, pipeline, db_path, host="127.0.0.1", port=API_PORT)

        await ingestion.start()
        await pipeline.start()
        api_server.start()

        from app.domain.enums import Exchange as _Exchange
        snapshots = []
        start = time.time()
        while time.time() - start < duration_seconds:
            await asyncio.sleep(5.0)
            snap = ingestion.get_snapshot()
            snap["t"] = round(time.time() - start, 1)
            snap["candidates_binance"] = sorted(pipeline.candidate_manager.current_candidates(_Exchange.BINANCE))
            snap["signals_evaluated_total"] = pipeline.signals_evaluated_total
            snap["entries_opened_total"] = pipeline.entries_opened_total
            snap["open_positions"] = len(pipeline.paper_engine.open_positions)
            snap["closed_positions"] = len(pipeline.paper_engine.closed_positions)
            snapshots.append(snap)
            logger.info(
                "t=%.0fs buffer=%s/%s binance_msgs=%s bybit_msgs=%s | candidates=%s | signals_evaluated=%s entries=%s open_pos=%s closed_pos=%s",
                snap["t"], snap["buffer"]["size"], snap["buffer"]["capacity"],
                snap["binance"]["total_messages_received"], snap["bybit"]["total_messages_received"],
                snap["candidates_binance"], snap["signals_evaluated_total"], snap["entries_opened_total"],
                snap["open_positions"], snap["closed_positions"],
            )

        final_ingestion_snapshot = ingestion.get_snapshot()
        api_server.stop()
        await pipeline.stop()
        await ingestion.stop()

    storage.stop()

    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    signal_rows = [dict(r) for r in conn.execute("SELECT symbol, final_signal_state, quant_score, real_friction_coverage FROM signals ORDER BY created_at").fetchall()]
    trade_rows = [dict(r) for r in conn.execute("SELECT symbol, entry_price, exit_price, final_state, realized_pnl_usd FROM paper_trades ORDER BY created_at").fetchall()]
    claude_rows = [dict(r) for r in conn.execute("SELECT signal_id, decision FROM claude_decisions ORDER BY created_at").fetchall()]
    conn.close()

    result = {
        "duration_seconds": duration_seconds,
        "symbols_tested": SYMBOLS,
        "pump_symbol": PUMP_SYMBOL,
        "snapshots": snapshots,
        "final_ingestion_snapshot": final_ingestion_snapshot,
        "fake_binance_stats": fake_binance.stats,
        "fake_bybit_stats": fake_bybit.stats,
        "pipeline_final": {
            "signals_evaluated_total": pipeline.signals_evaluated_total,
            "entries_opened_total": pipeline.entries_opened_total,
            "open_positions": len(pipeline.paper_engine.open_positions),
            "closed_positions": len(pipeline.paper_engine.closed_positions),
        },
        "signal_rows_sample": signal_rows[-20:],
        "trade_rows": trade_rows,
        "claude_decision_rows": claude_rows,
        "distinct_signal_states": sorted({r["final_signal_state"] for r in signal_rows}),
    }
    return result


def summarize(result: dict) -> str:
    final = result["final_ingestion_snapshot"]
    binance = final["binance"]
    bybit = final["bybit"]
    buf = final["buffer"]
    pf = result["pipeline_final"]

    lines = []
    lines.append("=" * 78)
    lines.append("SIMULATED FULL-PIPELINE TEST (Phases 1-5) — SUMMARY")
    lines.append("=" * 78)
    lines.append(f"Duration: {result['duration_seconds']:.0f}s | Symbols: {len(result['symbols_tested'])} | Pump symbol: {result['pump_symbol']}")
    lines.append("")
    lines.append("-- Phase 1: Ingestion --")
    lines.append(f"Binance: healthy={binance['is_healthy']} shards={len(binance['connections'])} "
                  f"messages_received={binance['total_messages_received']} reconnects={binance['total_reconnects']}")
    lines.append(f"Bybit:   healthy={bybit['is_healthy']} shards={len(bybit['connections'])} "
                  f"messages_received={bybit['total_messages_received']} reconnects={bybit['total_reconnects']}")
    lines.append(f"Ring Buffer: state={buf['state']} dropped_total={buf['dropped_total']} "
                  f"p0_overflow={buf['p0_protected_overflow_events']} dropped_by_priority={buf['dropped_by_priority']}")
    lines.append(f"Forced disconnect triggered: {result['fake_binance_stats']['forced_disconnects']}")
    lines.append("")
    lines.append("-- Phases 2-3: Candidates / Signals / Risk --")
    lines.append(f"Signals evaluated: {pf['signals_evaluated_total']}")
    lines.append(f"Distinct signal states seen: {result['distinct_signal_states']}")
    lines.append(f"Entries opened (paper): {pf['entries_opened_total']}")
    lines.append(f"Open positions at end: {pf['open_positions']} | Closed positions at end: {pf['closed_positions']}")
    if result["trade_rows"]:
        lines.append("Trades:")
        for t in result["trade_rows"]:
            lines.append(f"    {t['symbol']}: entry={t['entry_price']} exit={t['exit_price']} "
                          f"state={t['final_state']} pnl_usd={t['realized_pnl_usd']}")
    lines.append("")
    lines.append("-- Phase 4: Storage / API --")
    lines.append(f"Signal rows written to SQLite: {'yes' if result['signal_rows_sample'] else 'no'} "
                  f"(sample of last {len(result['signal_rows_sample'])} shown in JSON)")
    lines.append("")
    lines.append("-- Phase 5: Claude Auditor (STUB, not a real API call) --")
    lines.append(f"Claude decision rows recorded: {len(result['claude_decision_rows'])}")
    lines.append("")

    checks = []
    checks.append(("Binance adapter healthy at end of test", binance["is_healthy"]))
    checks.append(("Bybit adapter healthy at end of test", bybit["is_healthy"]))
    checks.append(("Binance reconnected after the forced mid-test disconnect",
                    binance["total_reconnects"] >= 1 and result["fake_binance_stats"]["forced_disconnects"] >= 1))
    checks.append(("Bybit unaffected by Binance's forced disconnect (0 Bybit reconnects)", bybit["total_reconnects"] == 0))
    checks.append(("Ring Buffer never dropped a P0 (closed kline) event", buf["dropped_by_priority"].get("P0", 0) == 0))
    checks.append(("At least one signal was evaluated by the Signal Engine", pf["signals_evaluated_total"] > 0))
    checks.append(("Signal rows were persisted to SQLite (Phase 4 storage)", bool(result["signal_rows_sample"])))
    checks.append(("At least one paper entry was opened (full gate chain cleared once)", pf["entries_opened_total"] > 0))
    checks.append(("Every opened position was eventually closed (no orphaned position)",
                    pf["open_positions"] == 0 and pf["closed_positions"] == pf["entries_opened_total"]))
    checks.append(("Claude Auditor stub was actually invoked and its decision persisted",
                    len(result["claude_decision_rows"]) > 0))

    lines.append("CHECKS")
    all_passed = True
    for name, passed in checks:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_passed = False
        lines.append(f"  [{status}] {name}")

    lines.append("")
    lines.append("OVERALL: " + ("ALL CHECKS PASSED" if all_passed else "SOME CHECKS FAILED — see above"))
    lines.append("")
    lines.append("REMINDER: this is a LOCAL SIMULATION against fake servers, not")
    lines.append("real Binance/Bybit. It verifies the code path, not real-market behavior.")
    lines.append("=" * 78)
    return "\n".join(lines)


if __name__ == "__main__":
    duration = float(sys.argv[1]) if len(sys.argv) > 1 else 90.0
    pump_after = float(sys.argv[2]) if len(sys.argv) > 2 else 45.0
    result = asyncio.run(main(duration, pump_after))
    summary = summarize(result)
    print()
    print(summary)
    with open("simulated_live_test_result.json", "w") as f:
        json.dump(result, f, indent=2, default=str)
    print("\nFull snapshot + signal/trade log written to simulated_live_test_result.json")
