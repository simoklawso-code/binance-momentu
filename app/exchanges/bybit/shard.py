"""
A single Bybit V5 Linear Perpetual public WebSocket connection ("shard").

Deliberately NOT a copy-paste of the Binance shard: Bybit requires the
CLIENT to send periodic {"op": "ping"} frames (Binance is the reverse —
the server pings and the client just needs to respond, which the
`websockets` library does automatically). Topic subscription also uses
a flat `args` list of topic strings rather than Binance's
`{symbol}@{stream}` convention.

NOTE ON RATE LIMITS: Bybit's documented max topics-per-subscribe-request
value should be re-verified against the live V5 docs before production
use (same discipline as §19 requires for OI) — the constant below
(`BYBIT_MAX_ARGS_PER_SUBSCRIBE_MSG`) is a conservative default, not an
assumption to trust blindly.
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

except ImportError:  # pragma: no cover
    import json as _json

    def _loads(b: bytes | str) -> Any:
        return _json.loads(b)

import websockets
from websockets.exceptions import ConnectionClosed

from app.config.settings import BybitConfig
from app.core.backoff import BackoffPolicy
from app.domain.enums import ConnectionState
from app.exchanges.base import ConnectionMetrics, EventSink
from app.exchanges.bybit.normalize import normalize_message

logger = logging.getLogger(__name__)

DEFAULT_TOPIC_TEMPLATES = ("kline.1.{symbol}", "tickers.{symbol}")
BYBIT_MAX_ARGS_PER_SUBSCRIBE_MSG = 10  # conservative default — re-verify against live V5 docs (§19 discipline)


class BybitShard:
    def __init__(
        self,
        shard_id: str,
        config: BybitConfig,
        sink: EventSink,
        is_candidate_fn: Callable[[str], bool],
        topic_templates: tuple[str, ...] = DEFAULT_TOPIC_TEMPLATES,
    ) -> None:
        self.shard_id = shard_id
        self._config = config
        self._sink = sink
        self._is_candidate_fn = is_candidate_fn
        self._topic_templates = topic_templates

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

        self._stop_event = asyncio.Event()
        self._run_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._soft_reconnect_task: Optional[asyncio.Task] = None
        self._pending_add: set[str] = set()
        self._pending_remove: set[str] = set()

    # -- lifecycle --------------------------------------------------------

    async def start(self, initial_symbols: set[str]) -> None:
        self._symbols = set(initial_symbols)
        self._stop_event.clear()
        self._run_task = asyncio.create_task(self._run_forever(), name=f"bybit-shard-{self.shard_id}")
        self._soft_reconnect_task = asyncio.create_task(
            self._soft_reconnect_loop(), name=f"bybit-shard-{self.shard_id}-soft-reconnect"
        )

    async def stop(self) -> None:
        self._stop_event.set()
        for task in (self._run_task, self._ping_task, self._soft_reconnect_task):
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
            self._pending_add |= add
            self._pending_remove |= remove
            return
        if add:
            await self._send_op("subscribe", add)
        if remove:
            await self._send_op("unsubscribe", remove)

    # -- connection loop ----------------------------------------------------

    async def _run_forever(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                logger.warning("Bybit shard %s connection error: %s", self.shard_id, exc)

            if self._stop_event.is_set():
                break

            self._state = ConnectionState.RECONNECTING
            self._reconnect_attempt += 1
            self._reconnect_count += 1
            delay = self._backoff.delay_for_attempt(self._reconnect_attempt)
            logger.info("Bybit shard %s reconnecting in %.1fs (attempt %d)", self.shard_id, delay, self._reconnect_attempt)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    async def _connect_and_listen(self) -> None:
        self._state = ConnectionState.CONNECTING
        async with websockets.connect(self._config.ws_base_url, ping_interval=None, close_timeout=5) as ws:
            self._ws = ws
            self._state = ConnectionState.SUBSCRIBING

            self._symbols |= self._pending_add
            self._symbols -= self._pending_remove
            self._pending_add.clear()
            self._pending_remove.clear()
            if self._symbols:
                await self._send_op("subscribe", self._symbols)

            self._state = ConnectionState.HEALTHY
            self._reconnect_attempt = 0
            logger.info("Bybit shard %s connected, %d symbols subscribed", self.shard_id, len(self._symbols))

            self._ping_task = asyncio.create_task(self._ping_loop(ws), name=f"bybit-shard-{self.shard_id}-ping")
            try:
                async for raw_message in ws:
                    self._messages_received += 1
                    self._last_message_at = time.monotonic()
                    self._handle_raw_message(raw_message)
            finally:
                self._ping_task.cancel()

        self._ws = None
        if not self._stop_event.is_set():
            raise ConnectionClosed(None, None)

    async def _ping_loop(self, ws: "websockets.WebSocketClientProtocol") -> None:
        """Bybit requires the CLIENT to ping (opposite of Binance) — see
        module docstring. Idle connections are dropped by Bybit if this
        is skipped."""
        try:
            while True:
                await asyncio.sleep(self._config.ping_interval_seconds)
                await ws.send(_dumps({"op": "ping"}))
        except asyncio.CancelledError:
            return
        except Exception as exc:  # noqa: BLE001
            logger.debug("Bybit shard %s ping loop ended: %s", self.shard_id, exc)

    def _handle_raw_message(self, raw_message: bytes | str) -> None:
        try:
            decoded = _loads(raw_message)
        except Exception:  # noqa: BLE001
            logger.debug("Bybit shard %s: failed to decode message", self.shard_id)
            return

        if not isinstance(decoded, dict):
            return
        if decoded.get("op") in ("pong", "ping", "subscribe", "unsubscribe"):
            return  # control-channel ack, not market data

        event = normalize_message(decoded, is_candidate_fn=self._is_candidate_fn)
        if event is not None:
            self._sink.push(event)

    # -- subscribe helpers ------------------------------------------------

    async def _send_op(self, op: str, symbols: set[str]) -> None:
        if self._ws is None:
            return
        args = [tpl.format(symbol=symbol) for symbol in symbols for tpl in self._topic_templates]
        for i in range(0, len(args), BYBIT_MAX_ARGS_PER_SUBSCRIBE_MSG):
            chunk = args[i : i + BYBIT_MAX_ARGS_PER_SUBSCRIBE_MSG]
            await self._ws.send(_dumps({"op": op, "args": chunk}))

    # -- scheduled soft reconnect ------------------------------------------

    async def _soft_reconnect_loop(self) -> None:
        interval_seconds = self._config.soft_reconnect_interval_hours * 3600
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(interval_seconds)
                if self._stop_event.is_set():
                    return
                logger.info("Bybit shard %s: scheduled soft reconnect", self.shard_id)
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
