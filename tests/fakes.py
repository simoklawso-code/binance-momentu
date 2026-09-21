"""Fake websockets.connect() replacement for testing shard reconnect /
resubscription logic without a real network connection (this sandbox
has no route to fstream.binance.com / stream.bybit.com — see README
"Testing Note")."""

from __future__ import annotations

import asyncio


class FakeWebSocket:
    def __init__(self, incoming_messages: list[str] | None = None):
        self.sent: list[str] = []
        self.closed = False
        self._incoming = list(incoming_messages or [])
        self._closed_event = asyncio.Event()

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def close(self) -> None:
        self.closed = True
        self._closed_event.set()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._incoming:
            return self._incoming.pop(0)
        # Simulate the connection dropping once messages are exhausted,
        # unless explicitly held open by the test via `hold_open=True`.
        if getattr(self, "hold_open", False):
            await self._closed_event.wait()
        raise StopAsyncIteration


class FakeConnectContextManager:
    def __init__(self, ws: FakeWebSocket):
        self._ws = ws

    async def __aenter__(self) -> FakeWebSocket:
        return self._ws

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class FakeConnectFactory:
    """Callable replacement for websockets.connect. Hand it a list of
    FakeWebSocket instances to be returned on successive calls (1st
    call -> connections[0], 2nd call (reconnect) -> connections[1], ...).
    """

    def __init__(self, connections: list[FakeWebSocket]):
        self._connections = list(connections)
        self.call_count = 0

    def __call__(self, url: str, **kwargs):
        idx = min(self.call_count, len(self._connections) - 1)
        self.call_count += 1
        return FakeConnectContextManager(self._connections[idx])
