"""Bybit V5 Linear Perpetual exchange adapter. Mirrors the Binance
adapter's public contract (ExchangeAdapter) but the sharding math and
subscription protocol are Bybit-specific — see shard.py docstring for
why the two exchanges are NOT treated as interchangeable internally."""

from __future__ import annotations

import logging
import math

from app.config.settings import BybitConfig
from app.domain.enums import Exchange
from app.exchanges.base import AdapterMetrics, EventSink, ExchangeAdapter
from app.exchanges.bybit.shard import DEFAULT_TOPIC_TEMPLATES, BybitShard

logger = logging.getLogger(__name__)


class BybitAdapter(ExchangeAdapter):
    exchange_name = Exchange.BYBIT.value

    def __init__(
        self,
        config: BybitConfig,
        topic_templates: tuple[str, ...] = DEFAULT_TOPIC_TEMPLATES,
    ) -> None:
        self._config = config
        self._topic_templates = topic_templates
        self._shards: dict[str, BybitShard] = {}
        self._sink: EventSink | None = None
        self._candidate_symbols: set[str] = set()
        self._candidate_book_ticker_shard: BybitShard | None = None

    async def start(self, symbols: list[str], sink: EventSink) -> None:
        self._sink = sink
        shards_needed = self._compute_shard_count(len(symbols))
        buckets = self._shard_symbols(symbols, shards_needed)

        for i, bucket in enumerate(buckets):
            shard_id = f"bybit-shard-{i}"
            shard = BybitShard(
                shard_id=shard_id,
                config=self._config,
                sink=sink,
                is_candidate_fn=lambda s: s in self._candidate_symbols,
                topic_templates=self._topic_templates,
            )
            self._shards[shard_id] = shard
            await shard.start(set(bucket))

        logger.info(
            "Bybit adapter started: %d symbols across %d shard(s)",
            len(symbols), len(buckets),
        )

    async def stop(self) -> None:
        for shard in self._shards.values():
            await shard.stop()
        self._shards.clear()
        if self._candidate_book_ticker_shard is not None:
            await self._candidate_book_ticker_shard.stop()
            self._candidate_book_ticker_shard = None

    async def update_subscriptions(self, add: list[str], remove: list[str]) -> None:
        add_set, remove_set = set(add), set(remove)

        for shard in self._shards.values():
            owned_remove = shard.symbols & remove_set
            if owned_remove:
                await shard.update_subscriptions(add=set(), remove=owned_remove)

        max_symbols_per_shard = max(1, self._config.max_streams_per_connection // max(1, len(self._topic_templates)))
        for symbol in add_set:
            target = self._least_loaded_shard(max_symbols_per_shard)
            if target is None:
                logger.warning(
                    "Bybit adapter: no shard capacity for new candidate %s — "
                    "consider raising bybit.max_connections", symbol,
                )
                continue
            await target.update_subscriptions(add={symbol}, remove=set())

    def get_metrics(self) -> AdapterMetrics:
        connections = [shard.get_metrics() for shard in self._shards.values()]
        return AdapterMetrics(
            exchange=self.exchange_name,
            connections=connections,
            total_messages_received=sum(c.messages_received for c in connections),
            total_reconnects=sum(c.reconnect_count for c in connections),
        )

    def set_candidate_symbols(self, symbols: set[str]) -> None:
        self._candidate_symbols = set(symbols)

    # -- candidate-only orderbook.1 stream (§17: spread is Stage-3-only) ---

    async def sync_candidate_book_ticker_stream(self, symbols: set[str]) -> None:
        if self._sink is None:
            return
        if self._candidate_book_ticker_shard is None:
            self._candidate_book_ticker_shard = BybitShard(
                shard_id="bybit-candidates-orderbook",
                config=self._config, sink=self._sink,
                is_candidate_fn=lambda s: s in self._candidate_symbols,
                topic_templates=("orderbook.1.{symbol}",),
            )
            await self._candidate_book_ticker_shard.start(set(symbols))
            return
        current = self._candidate_book_ticker_shard.symbols
        add = symbols - current
        remove = current - symbols
        if add or remove:
            await self._candidate_book_ticker_shard.update_subscriptions(add=add, remove=remove)

    # -- sharding helpers -----------------------------------------------------

    def _compute_shard_count(self, symbol_count: int) -> int:
        topics_per_symbol = len(self._topic_templates)
        total_topics = symbol_count * topics_per_symbol
        needed = max(1, math.ceil(total_topics / max(1, self._config.max_streams_per_connection)))
        return min(needed, self._config.max_connections) if self._config.max_connections > 0 else needed

    @staticmethod
    def _shard_symbols(symbols: list[str], shard_count: int) -> list[list[str]]:
        buckets: list[list[str]] = [[] for _ in range(shard_count)]
        for i, symbol in enumerate(symbols):
            buckets[i % shard_count].append(symbol)
        return [b for b in buckets if b]

    def _least_loaded_shard(self, max_symbols_per_shard: int) -> BybitShard | None:
        candidates = [s for s in self._shards.values() if len(s.symbols) < max_symbols_per_shard]
        if not candidates:
            return None
        return min(candidates, key=lambda s: len(s.symbols))
