"""
A single Binance Futures WebSocket connection ("shard", §6).

The Binance adapter owns several of these. Each shard:
  - connects independently and maintains its own health,
  - reconnects on failure WITHOUT affecting other shards,
  - restores only the subscriptions it owns on reconnect,
  - performs a scheduled soft reconnect before Binance's ~24h forced
    disconnect, never waiting for the forced disconnect,
  - exposes connection-level metrics.

Uses Binance's dynamic SUBSCRIBE/UNSUBSCRIBE protocol over the plain
`/ws` endpoint (rather than baking the stream list into the URL) so
`update_subscriptions` can add/remove symbols without a full
reconnect — required for the future Candidate Stream Manager (§18) to
avoid subscription churn.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Optional

try:
    import orjson as _json

    def _loads(b: bytes | str) -> Any:
        return _json.loads(b)

except ImportError:  # pragma: no cover - fallback path
    import json as _json

    def _loads(b: bytes | str) -> Any:
        return _json.loads(b)

import websockets
from websockets.exceptions import ConnectionClosed

from app.config.settings import BinanceConfig
from app.core.backoff import BackoffPolicy
from app.domain.enums import ConnectionState
from app.exchanges.base import ConnectionMetrics, EventSink
from app.exchanges.binance.normalize import normalize_message

logger = logging.getLogger(__name__)

DEFAULT_STREAM_TYPES = ("miniTicker", "kline_1m")
MAX_PARAMS_PER_SUBSCRIBE_MSG = 200


class BinanceShard:
    def __init__(
        self,
        shard_id: str,
        config: BinanceConfig,
        sink: EventSink,
        is_candidate_fn: Callable[[str], bool],
        stream_types: tuple[str, ...] = DEFAULT_STREAM_TYPES,
    ) -> None:
        self.shard_id = shard_id
        self._config = config
        self._sink = sink
        self._is_candidate_fn = is_candidate_fn
        self._stream_types = stream_types

        self._symbols: set[str] = set()
        self._state = ConnectionState.DISCONNECTED
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._backoff = BackoffPolicy(
            base_seconds=config.reconnect_backoff_base_seconds,
            max_seconds=config.reconnect_backoff_max_seconds,
            jitter_seconds=config.reconnect_backoff_jitter_seconds,
        )
        self._reconnect_attempt = 0
        self._reconnect_count = 0
        self._messages_received = 0
        self._last_message_at: Optional[float] = None
        self._last_error: Optional[str] = None
        self._sub_id_counter = 1

        self._stop_event = asyncio.Event()
        self._run_task: Optional[asyncio.Task] = None
        self._soft_reconnect_task: Optional[asyncio.Task] = None
        self._connected_at: Optional[float] = None
        # Pending add/remove requested while disconnected/connecting are
        # applied once the connection is (re-)established.
        self._pending_add: set[str] = set()
        self._pending_remove: set[str] = set()

    # -- lifecycle --------------------------------------------------------

    async def start(self, initial_symbols: set[str]) -> None:
        self._symbols = set(initial_symbols)
        self._stop_event.clear()
        self._run_task = asyncio.create_task(self._run_forever(), name=f"binance-shard-{self.shard_id}")
        self._soft_reconnect_task = asyncio.create_task(
            self._soft_reconnect_loop(), name=f"binance-shard-{self.shard_id}-soft-reconnect"
        )

    async def stop(self) -> None:
        self._stop_event.set()
        for task in (self._run_task, self._soft_reconnect_task):
            if task:
                task.cancel()
        if self._ws is not None:
            await self._ws.close()
        self._state = ConnectionState.DISCONNECTED

    async def update_subscriptions(self, add: set[str], remove: set[str]) -> None:
        add = add - self._symbols
        remove = remove & self._symbols
        self._symbols |= add
        self._symbols -= remove
        if self._ws is None or self._state not in (ConnectionState.HEALTHY, ConnectionState.CONNECTED):
            # Not connected yet — will be applied via full resubscribe on connect.
            self._pending_add |= add
            self._pending_remove |= remove
            return
        if add:
            await self._send_subscribe(add)
        if remove:
            await self._send_unsubscribe(remove)

    # -- connection loop ----------------------------------------------------

    async def _run_forever(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reconnect on anything
                self._last_error = str(exc)
                logger.warning("Binance shard %s connection error: %s", self.shard_id, exc)

            if self._stop_event.is_set():
                break

            self._state = ConnectionState.RECONNECTING
            self._reconnect_attempt += 1
            self._reconnect_count += 1
            delay = self._backoff.delay_for_attempt(self._reconnect_attempt)
            logger.info("Binance shard %s reconnecting in %.1fs (attempt %d)", self.shard_id, delay, self._reconnect_attempt)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass  # normal path: delay elapsed, retry connect

    async def _connect_and_listen(self) -> None:
        url = f"{self._config.ws_base_url}/ws"
        self._state = ConnectionState.CONNECTING
        async with websockets.connect(url, ping_interval=None, close_timeout=5) as ws:
            self._ws = ws
            self._state = ConnectionState.SUBSCRIBING
            self._connected_at = time.monotonic()

            # Apply anything queued while disconnected, plus the full
            # current symbol set — restoring ONLY this shard's subscriptions.
            self._symbols |= self._pending_add
            self._symbols -= self._pending_remove
            self._pending_add.clear()
            self._pending_remove.clear()
            if self._symbols:
                await self._send_subscribe(self._symbols)

            self._state = ConnectionState.HEALTHY
            self._reconnect_attempt = 0  # reset backoff after a clean connect
            logger.info("Binance shard %s connected, %d symbols subscribed", self.shard_id, len(self._symbols))

            async for raw_message in ws:
                self._messages_received += 1
                self._last_message_at = time.monotonic()
                self._handle_raw_message(raw_message)

                if time.monotonic() - self._connected_at > self._config.connection_health_timeout_seconds and self._messages_received == 0:
                    # No data at all shortly after connecting — treat as unhealthy.
                    raise ConnectionClosed(None, None)

        self._ws = None
        if not self._stop_event.is_set():
            raise ConnectionClosed(None, None)

    def _handle_raw_message(self, raw_message: bytes | str) -> None:
        try:
            decoded = _loads(raw_message)
        except Exception:  # noqa: BLE001
            logger.debug("Binance shard %s: failed to decode message", self.shard_id)
            return

        if isinstance(decoded, dict) and "result" in decoded and "id" in decoded and "e" not in decoded:
            return  # subscribe/unsubscribe ack

        event = normalize_message(decoded, is_candidate_fn=self._is_candidate_fn)
        if event is not None:
            self._sink.push(event)

    # -- subscribe helpers ------------------------------------------------

    async def _send_subscribe(self, symbols: set[str]) -> None:
        await self._send_sub_message("SUBSCRIBE", symbols)

    async def _send_unsubscribe(self, symbols: set[str]) -> None:
        await self._send_sub_message("UNSUBSCRIBE", symbols)

    async def _send_sub_message(self, method: str, symbols: set[str]) -> None:
        if self._ws is None:
            return
        params = [
            f"{symbol.lower()}@{stream_type}"
            for symbol in symbols
            for stream_type in self._stream_types
        ]
        for i in range(0, len(params), MAX_PARAMS_PER_SUBSCRIBE_MSG):
            chunk = params[i : i + MAX_PARAMS_PER_SUBSCRIBE_MSG]
            payload = {"method": method, "params": chunk, "id": self._sub_id_counter}
            self._sub_id_counter += 1
            await self._ws.send(_dumps(payload))

    # -- scheduled soft reconnect (§6) -------------------------------------

    async def _soft_reconnect_loop(self) -> None:
        interval_seconds = self._config.soft_reconnect_interval_hours * 3600
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(interval_seconds)
                if self._stop_event.is_set():
                    return
                logger.info("Binance shard %s: scheduled soft reconnect before forced disconnect", self.shard_id)
                self._state = ConnectionState.SOFT_RECONNECTING
                if self._ws is not None:
                    await self._ws.close()
        except asyncio.CancelledError:
            return

    # -- metrics ------------------------------------------------------------

    def get_metrics(self) -> ConnectionMetrics:
        age_ms = None
        if self._last_message_at is not None:
            age_ms = (time.monotonic() - self._last_message_at) * 1000
        return ConnectionMetrics(
            connection_id=self.shard_id,
            state=self._state,
            subscribed_symbols=len(self._symbols),
            messages_received=self._messages_received,
            reconnect_count=self._reconnect_count,
            last_message_age_ms=age_ms,
            last_error=self._last_error,
        )

    @property
    def symbols(self) -> set[str]:
        return set(self._symbols)


def _dumps(payload: dict) -> str:
    try:
        import orjson

        return orjson.dumps(payload).decode()
    except ImportError:  # pragma: no cover
        import json

        return json.dumps(payload)
