import asyncio

import pytest

from app.config.settings import BinanceConfig, BybitConfig
from app.domain.events import DomainEvent
from app.exchanges.base import EventSink
from app.exchanges.binance import shard as binance_shard_module
from app.exchanges.bybit import shard as bybit_shard_module
from tests.fakes import FakeConnectFactory, FakeWebSocket


class FakeSink(EventSink):
    def __init__(self):
        self.events: list[DomainEvent] = []

    def push(self, event: DomainEvent) -> bool:
        self.events.append(event)
        return True


async def _wait_until(predicate, timeout: float = 2.0, interval: float = 0.01) -> None:
    elapsed = 0.0
    while not predicate():
        await asyncio.sleep(interval)
        elapsed += interval
        if elapsed > timeout:
            raise AssertionError("condition not met within timeout")


@pytest.mark.asyncio
async def test_binance_shard_reconnects_and_restores_only_its_own_subscriptions(monkeypatch):
    ws1 = FakeWebSocket()          # drops immediately (no messages, not held open)
    ws2 = FakeWebSocket()
    ws2.hold_open = True           # stays open once reconnected, for inspection
    factory = FakeConnectFactory([ws1, ws2])
    monkeypatch.setattr(binance_shard_module.websockets, "connect", factory)

    config = BinanceConfig(
        reconnect_backoff_base_seconds=0.01,
        reconnect_backoff_max_seconds=0.02,
        reconnect_backoff_jitter_seconds=0.0,
        soft_reconnect_interval_hours=1000,
    )
    sink = FakeSink()
    shard = binance_shard_module.BinanceShard("test-shard-0", config, sink, is_candidate_fn=lambda s: False)

    await shard.start({"BTCUSDT", "ETHUSDT"})
    await _wait_until(lambda: factory.call_count >= 2)

    # First connection subscribed to both symbols.
    assert any("SUBSCRIBE" in msg and "btcusdt" in msg for msg in ws1.sent)
    assert any("SUBSCRIBE" in msg and "ethusdt" in msg for msg in ws1.sent)

    # After reconnect, the SAME shard restores its OWN subscriptions —
    # nothing added, nothing lost.
    assert any("SUBSCRIBE" in msg and "btcusdt" in msg for msg in ws2.sent)
    assert any("SUBSCRIBE" in msg and "ethusdt" in msg for msg in ws2.sent)
    assert shard.symbols == {"BTCUSDT", "ETHUSDT"}

    metrics = shard.get_metrics()
    assert metrics.reconnect_count >= 1

    await shard.stop()


@pytest.mark.asyncio
async def test_bybit_shard_sends_client_side_ping(monkeypatch):
    ws = FakeWebSocket()
    ws.hold_open = True
    factory = FakeConnectFactory([ws])
    monkeypatch.setattr(bybit_shard_module.websockets, "connect", factory)

    config = BybitConfig(
        ping_interval_seconds=0.02,
        reconnect_backoff_base_seconds=0.01,
        soft_reconnect_interval_hours=1000,
    )
    sink = FakeSink()
    shard = bybit_shard_module.BybitShard("test-shard-0", config, sink, is_candidate_fn=lambda s: False)
    await shard.start({"BTCUSDT"})

    await _wait_until(lambda: any('"op": "ping"' in m or '"op":"ping"' in m for m in ws.sent), timeout=1.0)

    await shard.stop()
