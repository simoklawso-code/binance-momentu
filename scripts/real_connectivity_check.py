"""
REAL exchange connectivity test — no fake servers, no mocks. Connects
directly to fstream.binance.com and stream.bybit.com and ingests real
market data for `duration_seconds`, then prints a clear PASS/FAIL
summary and exits with a non-zero code if anything failed (so CI shows
red/green honestly).

This is the test that a sandboxed dev environment (like the one this
project was originally built in) cannot run — see README "Testing
Note". Run it anywhere with outbound internet access:

    python3 scripts/real_connectivity_check.py [duration_seconds]

Exit code 0 = all checks passed (real, live data flowed cleanly).
Exit code 1 = something failed — read the printed reasons.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config.settings import load_settings
from app.core.logging_config import configure_logging
from app.ingestion.ingestion_manager import IngestionManager

# Small, cheap default universe for a quick CI check — not the full
# 150-300 symbol production universe (see config.example.yaml to change it).
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]


async def main(duration_seconds: float) -> bool:
    configure_logging("INFO")
    settings = load_settings("config.example.yaml")
    settings.universe.static_symbols = SYMBOLS
    settings.metrics_log_interval_seconds = 10.0
    # NOTE: settings.binance.ws_base_url / settings.bybit.ws_base_url are
    # left at their config.example.yaml defaults — the REAL exchange
    # endpoints (wss://fstream.binance.com, wss://stream.bybit.com).

    print(f"Connecting to REAL Binance + Bybit for {duration_seconds:.0f}s, symbols: {SYMBOLS}")
    print(f"Binance endpoint: {settings.binance.ws_base_url}")
    print(f"Bybit endpoint:   {settings.bybit.ws_base_url}")
    print()

    ingestion = IngestionManager(settings)
    await ingestion.start()

    start = time.time()
    while time.time() - start < duration_seconds:
        await asyncio.sleep(10.0)
        snap = ingestion.get_snapshot()
        elapsed = time.time() - start
        print(
            f"t={elapsed:.0f}s buffer={snap['buffer']['size']}/{snap['buffer']['capacity']} "
            f"binance_msgs={snap['binance']['total_messages_received']} "
            f"(healthy={snap['binance']['is_healthy']}, reconnects={snap['binance']['total_reconnects']}) "
            f"bybit_msgs={snap['bybit']['total_messages_received']} "
            f"(healthy={snap['bybit']['is_healthy']}, reconnects={snap['bybit']['total_reconnects']})"
        )

    final = ingestion.get_snapshot()
    await ingestion.stop()

    print()
    print("=" * 70)
    print("REAL EXCHANGE CONNECTIVITY TEST — SUMMARY")
    print("=" * 70)

    checks = [
        ("Binance connection became healthy", final["binance"]["is_healthy"]),
        ("Bybit connection became healthy", final["bybit"]["is_healthy"]),
        ("Binance received real market data messages", final["binance"]["total_messages_received"] > 0),
        ("Bybit received real market data messages", final["bybit"]["total_messages_received"] > 0),
        ("Ring Buffer never dropped a P0 (closed kline) event", final["buffer"]["dropped_by_priority"].get("P0", 0) == 0),
        ("Ring Buffer stayed in a normal/healthy state", final["buffer"]["p0_protected_overflow_events"] == 0),
    ]

    all_passed = True
    for name, passed in checks:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_passed = False
        print(f"  [{status}] {name}")

    print()
    print(f"Binance: {final['binance']['total_messages_received']} messages, "
          f"{len(final['binance']['connections'])} connection(s), "
          f"{final['binance']['total_reconnects']} reconnect(s)")
    print(f"Bybit:   {final['bybit']['total_messages_received']} messages, "
          f"{len(final['bybit']['connections'])} connection(s), "
          f"{final['bybit']['total_reconnects']} reconnect(s)")
    print()
    print("OVERALL: " + ("ALL CHECKS PASSED — this ran against the REAL exchanges."
                          if all_passed else "SOME CHECKS FAILED — see above."))
    print("=" * 70)

    return all_passed


if __name__ == "__main__":
    duration = float(sys.argv[1]) if len(sys.argv) > 1 else 90.0
    passed = asyncio.run(main(duration))
    sys.exit(0 if passed else 1)
