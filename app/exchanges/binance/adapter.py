"""
Binance Futures exchange adapter (§6 — LOCKED sharding requirement).

Does NOT assume the full 150-300+ symbol universe fits on one
WebSocket connection. Subscriptions are sharded across multiple
connections per Binance's documented stream/message-rate limits, with
each shard maintaining independent health, reconnecting without
affecting the others, and restoring only its own subscriptions.

The number of shards is configuration-driven
(`binance.max_streams_per_connection`, `binance.max_connections`) —
never hardcoded. This adapter is the only place in the codebase that
knows Binance uses multiple sockets; the Quant Engine above it only
ever sees the logical "Binance Futures" exchange.
"""

from __future__ import annotations

import logging
import math

from app.config.settings import BinanceConfig
from app.domain.enums import Exchange
from app.exchanges.base import AdapterMetrics, EventSink, ExchangeAdapter
from app.exchanges.binance.shard import DEFAULT_STREAM_TYPES, BinanceShard

logger = logging.getLogger(__name__)


class BinanceAdapter(ExchangeAdapter):
    exchange_name = Exchange.BINANCE.value

    def __init__(
        self,
        config: BinanceConfig,
        stream_types: tuple[str, ...] = DEFAULT_STREAM_TYPES,
    ) -> None:
        self._config = config
        self._stream_types = stream_types
        self._shards: dict[str, BinanceShard] = {}
        self._sink: EventSink | None = None
        self._candidate_symbols: set[str] = set()
        self._candidate_book_ticker_shard: BinanceShard | None = None

    # -- ExchangeAdapter interface ------------------------------------------

    async def start(self, symbols: list[str], sink: EventSink) -> None:
        self._sink = sink
        shards_needed = self._compute_shard_count(len(symbols))
        buckets = self._shard_symbols(symbols, shards_needed)

        for i, bucket in enumerate(buckets):
            shard_id = f"binance-shard-{i}"
            shard = BinanceShard(
                shard_id=shard_id,
                config=self._config,
                sink=sink,
                is_candidate_fn=lambda s: s in self._candidate_symbols,
                stream_types=self._stream_types,
            )
            self._shards[shard_id] = shard
            await shard.start(set(bucket))

        logger.info(
            "Binance adapter started: %d symbols across %d shard(s) (max %d streams/connection)",
            len(symbols), len(buckets), self._config.max_streams_per_connection,
        )

    async def stop(self) -> None:
        for shard in self._shards.values():
            await shard.stop()
        self._shards.clear()
        if self._candidate_book_ticker_shard is not None:
            await self._candidate_book_ticker_shard.stop()
            self._candidate_book_ticker_shard = None

    async def update_subscriptions(self, add: list[str], remove: list[str]) -> None:
        """Adds go to the least-loaded shard (simple balancing); removes
        are routed to whichever shard currently owns each symbol. This
        avoids full unsubscribe/resubscribe churn (§18) for symbols that
        are unaffected by the change."""
        add_set, remove_set = set(add), set(remove)

        # Route removals to their owning shard.
        for shard in self._shards.values():
            owned_remove = shard.symbols & remove_set
            if owned_remove:
                await shard.update_subscriptions(add=set(), remove=owned_remove)

        # Route additions to the least-loaded shard that has capacity.
        max_symbols_per_shard = max(1, self._config.max_streams_per_connection // max(1, len(self._stream_types)))
        for symbol in add_set:
            target = self._least_loaded_shard(max_symbols_per_shard)
            if target is None:
                logger.warning(
                    "Binance adapter: no shard capacity for new candidate %s — "
                    "consider raising binance.max_connections", symbol,
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

    # -- candidate awareness (feeds priority classification, §9) ------------

    def set_candidate_symbols(self, symbols: set[str]) -> None:
        """Called by the (future) Candidate Stream Manager / Feature
        Engine to mark which symbols currently deserve P0/P1 treatment
        and candidate-only streams like bookTicker. Phase 1 exposes the
        hook; nothing calls it yet."""
        self._candidate_symbols = set(symbols)

    # -- candidate-only bookTicker stream (§17: spread is a Stage-3, ------
    # candidate-only metric — never subscribed for the full universe) ----

    async def sync_candidate_book_ticker_stream(self, symbols: set[str]) -> None:
        """Maintains ONE extra dedicated shard subscribed to bookTicker
        only for the current candidate set. Kept separate from the main
        miniTicker+kline shards (which cover the full universe) so the
        Stage-3-only nature of spread data (§17) is enforced structurally,
        not just by convention."""
        if self._sink is None:
            return
        if self._candidate_book_ticker_shard is None:
            self._candidate_book_ticker_shard = BinanceShard(
                shard_id="binance-candidates-bookticker",
                config=self._config, sink=self._sink,
                is_candidate_fn=lambda s: s in self._candidate_symbols,
                stream_types=("bookTicker",),
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
        streams_per_symbol = len(self._stream_types)
        total_streams = symbol_count * streams_per_symbol
        needed = max(1, math.ceil(total_streams / max(1, self._config.max_streams_per_connection)))
        return min(needed, self._config.max_connections) if self._config.max_connections > 0 else needed

    @staticmethod
    def _shard_symbols(symbols: list[str], shard_count: int) -> list[list[str]]:
        buckets: list[list[str]] = [[] for _ in range(shard_count)]
        for i, symbol in enumerate(symbols):
            buckets[i % shard_count].append(symbol)
        return [b for b in buckets if b]

    def _least_loaded_shard(self, max_symbols_per_shard: int) -> BinanceShard | None:
        candidates = [s for s in self._shards.values() if len(s.symbols) < max_symbols_per_shard]
        if not candidates:
            return None
        return min(candidates, key=lambda s: len(s.symbols))
