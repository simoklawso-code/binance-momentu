"""
Exchange Adapter interface (§4, §6, §44).

Every exchange adapter (Binance, Bybit, and any future exchange) must
implement this interface. The Quant Engine and Ring Buffer never know
which concrete exchange they're talking to — they only depend on this
contract, so a new exchange can be added without rewriting anything
above the adapter layer.

An adapter may internally use multiple physical WebSocket connections
(sharding, §6) — that is entirely hidden behind this interface. The
logical `Exchange` identity is what the rest of the system sees.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field

from app.domain.enums import ConnectionState
from app.domain.events import DomainEvent


@dataclass
class ConnectionMetrics:
    """Per-connection (per-shard) health snapshot (§46)."""

    connection_id: str
    state: ConnectionState
    subscribed_symbols: int
    messages_received: int = 0
    reconnect_count: int = 0
    last_message_age_ms: float | None = None
    last_error: str | None = None


@dataclass
class AdapterMetrics:
    exchange: str
    connections: list[ConnectionMetrics] = field(default_factory=list)
    total_messages_received: int = 0
    total_reconnects: int = 0

    @property
    def is_healthy(self) -> bool:
        return any(c.state in (ConnectionState.HEALTHY, ConnectionState.CONNECTED) for c in self.connections)


class EventSink(abc.ABC):
    """Anything an adapter can push normalized DomainEvents into. In
    production this is the PriorityRingBuffer; in tests it's a simple
    list-backed fake — the adapter never imports the buffer directly,
    keeping ingestion decoupled from buffering (§5)."""

    @abc.abstractmethod
    def push(self, event: DomainEvent) -> bool:
        ...


class ExchangeAdapter(abc.ABC):
    """Common lifecycle every exchange adapter must implement."""

    exchange_name: str

    @abc.abstractmethod
    async def start(self, symbols: list[str], sink: EventSink) -> None:
        """Begin streaming for the given symbol universe, normalizing
        every message into a DomainEvent and pushing it into `sink`.
        Must not block indefinitely — runs as a background task."""

    @abc.abstractmethod
    async def stop(self) -> None:
        """Gracefully close all connections owned by this adapter."""

    @abc.abstractmethod
    async def update_subscriptions(self, add: list[str], remove: list[str]) -> None:
        """Diff-based subscription update (used by the future Candidate
        Stream Manager, §18) — must not force a full unsubscribe/
        resubscribe churn for symbols that are unaffected."""

    @abc.abstractmethod
    def get_metrics(self) -> AdapterMetrics:
        """Current connection-level health/metrics snapshot (§46)."""
