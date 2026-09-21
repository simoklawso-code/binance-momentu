import asyncio

import pytest

from app.config.settings import BinanceConfig
from app.domain.events import DomainEvent
from app.exchanges.base import EventSink
from app.exchanges.binance import shard as binance_shard_module
from app.exchanges.binance.adapter import BinanceAdapter
from tests.fakes import FakeConnectFactory, FakeWebSocket


class FakeSink(EventSink):
    def push(self, event: DomainEvent) -> bool:
        return True


async def _wait_until(predicate, timeout: float = 2.0, interval: float = 0.01) -> None:
    elapsed = 0.0
    while not predicate():
        await asyncio.sleep(interval)
        elapsed += interval
        if elapsed > timeout:
            raise AssertionError("condition not met within timeout")


@pytest.mark.asyncio
async def test_symbol_universe_sharded_across_multiple_connections(monkeypatch):
    # 5 symbols, 2 stream types => 10 streams; cap at 4 streams/connection
    # => ceil(10/4) = 3 shards needed.
    sockets = [FakeWebSocket() for _ in range(5)]
    for s in sockets:
        s.hold_open = True
    factory = FakeConnectFactory(sockets)
    monkeypatch.setattr(binance_shard_module.websockets, "connect", factory)

    config = BinanceConfig(
        max_streams_per_connection=4,
        max_connections=5,
        reconnect_backoff_base_seconds=0.01,
        soft_reconnect_interval_hours=1000,
    )
    adapter = BinanceAdapter(config)
    symbols = ["A", "B", "C", "D", "E"]
    await adapter.start(symbols, FakeSink())

    metrics = adapter.get_metrics()
    assert len(metrics.connections) == 3

    total_subscribed = sum(c.subscribed_symbols for c in metrics.connections)
    assert total_subscribed == len(symbols)

    await adapter.stop()


@pytest.mark.asyncio
async def test_candidate_book_ticker_stream_is_separate_and_subscribes_bookticker(monkeypatch):
    main_sockets = [FakeWebSocket() for _ in range(1)]
    candidate_socket = FakeWebSocket()
    for s in main_sockets + [candidate_socket]:
        s.hold_open = True
    factory = FakeConnectFactory(main_sockets + [candidate_socket])
    monkeypatch.setattr(binance_shard_module.websockets, "connect", factory)

    config = BinanceConfig(
        max_streams_per_connection=200, max_connections=4,
        reconnect_backoff_base_seconds=0.01, soft_reconnect_interval_hours=1000,
    )
    adapter = BinanceAdapter(config)
    await adapter.start(["A", "B", "C"], FakeSink())

    await adapter.sync_candidate_book_ticker_stream({"A"})
    await _wait_until(lambda: any("bookTicker" in m for m in candidate_socket.sent))

    assert any("bookTicker" in m and '"a@bookTicker"'.lower() in m.lower() for m in candidate_socket.sent)
    # The main universe shard must NEVER have been asked to subscribe bookTicker.
    assert not any("bookTicker" in m for m in main_sockets[0].sent)

    await adapter.stop()


@pytest.mark.asyncio
async def test_update_subscriptions_does_not_churn_unaffected_shards(monkeypatch):
    sockets = [FakeWebSocket() for _ in range(2)]
    for s in sockets:
        s.hold_open = True
    factory = FakeConnectFactory(sockets)
    monkeypatch.setattr(binance_shard_module.websockets, "connect", factory)

    config = BinanceConfig(
        max_streams_per_connection=2,  # 1 symbol per shard (2 stream types)
        max_connections=2,
        reconnect_backoff_base_seconds=0.01,
        soft_reconnect_interval_hours=1000,
    )
    adapter = BinanceAdapter(config)
    await adapter.start(["A", "B"], FakeSink())
    await _wait_until(lambda: all(len(s.sent) >= 1 for s in sockets))

    sent_counts_before = [len(s.sent) for s in sockets]

    # Remove "A" only — the shard NOT owning "A" must receive zero new
    # messages (no unsubscribe/resubscribe churn for symbols it never
    # touched, §18).
    await adapter.update_subscriptions(add=[], remove=["A"])
    await asyncio.sleep(0.05)

    sent_counts_after = [len(s.sent) for s in sockets]
    # Exactly one socket should have grown (the one that owned "A" and
    # received the UNSUBSCRIBE); the other must be untouched.
    grown = [after - before for before, after in zip(sent_counts_before, sent_counts_after)]
    assert grown.count(0) == 1
    assert sum(grown) == 1

    await adapter.stop()
