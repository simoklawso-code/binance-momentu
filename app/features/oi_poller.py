"""OI Poller (§19). Never polls the full universe — only whatever the
CandidateStreamManager currently considers a candidate, on independent
per-exchange intervals (binance_oi_interval_seconds /
bybit_oi_interval_seconds — explicitly NOT a shared 30s rule)."""

from __future__ import annotations

import asyncio
import logging

from app.config.settings import Settings
from app.domain.enums import Exchange
from app.exchanges.binance import oi as binance_oi
from app.exchanges.bybit import oi as bybit_oi
from app.features.store import FeatureStore
from app.signals.candidate_manager import CandidateStreamManager

logger = logging.getLogger(__name__)


class OIPoller:
    def __init__(self, settings: Settings, store: FeatureStore, candidates: CandidateStreamManager) -> None:
        self._settings = settings
        self._store = store
        self._candidates = candidates
        self._tasks: dict[tuple[Exchange, str], asyncio.Task] = {}
        self._stop_event = asyncio.Event()

    async def sync_tasks(self) -> None:
        """Call after every candidate promotion cycle — starts polling
        for newly-promoted candidates, cancels polling for demoted
        ones. Idempotent."""
        for exchange in (Exchange.BINANCE, Exchange.BYBIT):
            wanted = self._candidates.current_candidates(exchange)
            existing = {sym for (ex, sym) in self._tasks if ex == exchange}

            for symbol in wanted - existing:
                key = (exchange, symbol)
                self._tasks[key] = asyncio.create_task(
                    self._poll_loop(exchange, symbol), name=f"oi-poll-{exchange.value}-{symbol}"
                )
            for symbol in existing - wanted:
                key = (exchange, symbol)
                task = self._tasks.pop(key, None)
                if task:
                    task.cancel()

    async def stop(self) -> None:
        self._stop_event.set()
        for task in self._tasks.values():
            task.cancel()
        self._tasks.clear()

    async def _poll_loop(self, exchange: Exchange, symbol: str) -> None:
        interval = (
            self._settings.strategy.binance_oi_interval_seconds
            if exchange == Exchange.BINANCE
            else self._settings.strategy.bybit_oi_interval_seconds
        )
        fetch_fn = binance_oi.fetch_open_interest if exchange == Exchange.BINANCE else bybit_oi.fetch_open_interest
        base_url = self._settings.binance.rest_base_url if exchange == Exchange.BINANCE else self._settings.bybit.rest_base_url
        try:
            while not self._stop_event.is_set():
                event = await fetch_fn(base_url, symbol)
                if event is not None:
                    self._store.ingest(event)
                else:
                    logger.debug("OI unavailable for %s:%s this cycle", exchange.value, symbol)
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return
