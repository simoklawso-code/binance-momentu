"""
Crypto Momentum & Scalping Hunter — local entrypoint.

Wires ALL implemented phases together behind a single start command
(§41):
  Phase 1 — sharded Binance/Bybit WebSocket ingestion -> Ring Buffer
  Phase 2 — Feature Engine, Dynamic Funnel, Candidate Stream Manager, OI polling
  Phase 3 — Quant Score, Signal Engine, Risk Engine, Paper Execution
  Phase 4 — SQLite storage worker, lightweight JSON API + dashboard
  Phase 5 — Claude Context Auditor (only if ANTHROPIC_API_KEY is set;
            otherwise every ENTRY_PENDING signal is logged and stored
            but no paper position is opened — fail-safe, §48)

Backtesting (also Phase 5) is a separate, explicit workflow — see
scripts/run_backtest_example.py — not part of the live loop.

Usage:
    python main.py
    python main.py --config config.yaml
    python main.py --no-api          # skip the dashboard/API server
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal

from app.ai.claude_auditor import ClaudeContextAuditor
from app.api.server import ApiServer
from app.config.settings import load_settings
from app.core.logging_config import configure_logging
from app.ingestion.ingestion_manager import IngestionManager
from app.signals.pipeline import SignalPipeline
from app.storage.db import Storage

logger = logging.getLogger(__name__)


async def run(config_path: str | None, run_api: bool) -> None:
    settings = load_settings(config_path)
    configure_logging(settings.log_level)

    logger.info("Starting Crypto Momentum & Scalping Hunter")
    logger.info(
        "Universe: %d symbols | Binance shards up to %d | Bybit shards up to %d",
        len(settings.universe.static_symbols),
        settings.binance.max_connections,
        settings.bybit.max_connections,
    )

    storage = Storage(
        settings.database.path,
        busy_timeout_ms=settings.database.busy_timeout_ms,
        queue_max_size=settings.database.write_queue_max_size,
    )
    storage.start()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    claude_auditor = None
    if api_key:
        claude_auditor = ClaudeContextAuditor(api_key=api_key, cooldown_seconds=settings.strategy.claude_cooldown_seconds)
        logger.info("Claude Context Auditor ENABLED (cooldown=%ds)", settings.strategy.claude_cooldown_seconds)
    else:
        logger.warning(
            "ANTHROPIC_API_KEY not set - Claude Context Auditor DISABLED. "
            "Signals will reach ENTRY_PENDING and be logged/stored, but no "
            "paper position will be opened (fail-safe, section 48)."
        )

    ingestion = IngestionManager(settings, consume_internally=False)
    pipeline = SignalPipeline(settings, ingestion, storage, claude_auditor=claude_auditor)

    api_server = None
    if run_api:
        api_server = ApiServer(ingestion, pipeline, settings.database.path, host="127.0.0.1", port=8080)

    await ingestion.start()
    await pipeline.start()
    if api_server:
        api_server.start()
        logger.info("Dashboard: http://127.0.0.1:8080")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down...")
        if api_server:
            api_server.stop()
        await pipeline.stop()
        await ingestion.stop()
        storage.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Crypto Momentum & Scalping Hunter")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--no-api", action="store_true", help="Skip starting the API/dashboard server")
    args = parser.parse_args()

    try:
        asyncio.run(run(args.config, run_api=not args.no_api))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
