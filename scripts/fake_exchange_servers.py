"""
Local simulated Binance + Bybit WebSocket servers.

This sandbox has no outbound access to fstream.binance.com /
stream.bybit.com, so a genuine "live ingestion test" against the real
exchanges cannot run here (see README "Testing Note"). This script
stands up two LOCAL servers that speak the same wire protocol our real
adapters expect (SUBSCRIBE/UNSUBSCRIBE for Binance, op:subscribe +
client ping for Bybit) and push continuously-generated synthetic
ticker/kline data — so the full adapter -> normalizer -> priority
classifier -> Ring Buffer -> consumer pipeline runs EXACTLY as it would
against the real exchanges, over a real TCP/WebSocket connection, for
several minutes, including a deliberate mid-test disconnect to prove
per-shard reconnect works end-to-end (not just in the mocked unit
tests).

This is a testing aid only — it is never imported by app/ code and
must not be mistaken for real exchange behavior (message pacing, kline
close cadence, and price dynamics are all synthetic/accelerated).
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time

import websockets

logger = logging.getLogger("fake_exchange")


class SymbolState:
    def __init__(self, symbol: str, start_price: float, pump_after_seconds: float | None = None):
        self.symbol = symbol
        self.price = start_price
        self.open_price = start_price
        self.high = start_price
        self.low = start_price
        self.base_volume = 0.0
        self.taker_buy_base_volume = 0.0
        self.open_time = int(time.time() * 1000)
        self._pump_after_seconds = pump_after_seconds
        self._start_time = time.time()

    def _is_pumping(self) -> bool:
        if self._pump_after_seconds is None:
            return False
        return (time.time() - self._start_time) >= self._pump_after_seconds

    def tick(self) -> None:
        if self._is_pumping():
            # Progressively accelerating move: both size and volume grow
            # with elapsed pump time so RVOL/Trade-Acceleration keep
            # climbing instead of plateauing once the pump itself
            # becomes "the new baseline" in its own rolling window --
            # deliberately exaggerated so a short synthetic test has a
            # real chance to clear every LOCKED gate at least once.
            elapsed = time.time() - self._start_time - (self._pump_after_seconds or 0.0)
            growth = min(1.0 + elapsed / 8.0, 6.0)
            move = random.uniform(0.004, 0.012) * growth
            vol = random.uniform(20.0, 40.0) * growth
            taker_share = random.uniform(0.75, 0.92)
        else:
            move = random.uniform(-0.002, 0.0022)
            vol = random.uniform(0.5, 5.0)
            taker_share = random.uniform(0.4, 0.6)

        self.price = max(0.01, self.price * (1 + move))
        self.high = max(self.high, self.price)
        self.low = min(self.low, self.price)
        self.base_volume += vol
        self.taker_buy_base_volume += vol * taker_share

    def close_and_reset(self) -> dict:
        closed = {
            "open": self.open_price, "high": self.high, "low": self.low,
            "close": self.price, "base_volume": self.base_volume,
            "taker_buy_base_volume": self.taker_buy_base_volume,
            "open_time": self.open_time, "close_time": int(time.time() * 1000),
        }
        self.open_price = self.price
        self.high = self.price
        self.low = self.price
        self.base_volume = 0.0
        self.taker_buy_base_volume = 0.0
        self.open_time = int(time.time() * 1000)
        return closed


class FakeBinanceServer:
    """Mimics enough of Binance Futures' combined /ws endpoint:
    SUBSCRIBE/UNSUBSCRIBE control messages, then periodic miniTicker +
    kline pushes for subscribed streams. Kline "close" cadence is
    accelerated (every `kline_close_seconds`) so a short test still
    exercises the P0/closed-kline path many times.
    """

    def __init__(self, symbols: list[str], kline_close_seconds: float = 5.0,
                 force_disconnect_first_conn_after: float | None = 15.0,
                 pump_symbol: str | None = None, pump_after_seconds: float | None = None):
        self._symbols_config = symbols
        self._kline_close_seconds = kline_close_seconds
        self._force_disconnect_first_conn_after = force_disconnect_first_conn_after
        self._pump_symbol = pump_symbol
        self._pump_after_seconds = pump_after_seconds
        self._connection_count = 0
        self.stats = {"connections": 0, "messages_sent": 0, "forced_disconnects": 0}

    async def handler(self, ws):
        self._connection_count += 1
        conn_id = self._connection_count
        self.stats["connections"] += 1
        subscribed: dict[str, set[str]] = {}  # symbol -> set of stream types actually subscribed
        states: dict[str, SymbolState] = {
            s: SymbolState(s, random.uniform(10, 500),
                            pump_after_seconds=self._pump_after_seconds if s == self._pump_symbol else None)
            for s in self._symbols_config
        }
        # Fixed, realistic best bid/ask half-spread as a fraction of price.
        half_spread_fraction = 0.00015
        logger.info("[fake-binance] connection #%d opened", conn_id)

        async def reader():
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                method = msg.get("method")
                params = msg.get("params", [])
                if method == "SUBSCRIBE":
                    for p in params:
                        sym, _, stream_type = p.partition("@")
                        subscribed.setdefault(sym.upper(), set()).add(stream_type)
                elif method == "UNSUBSCRIBE":
                    for p in params:
                        sym, _, stream_type = p.partition("@")
                        subscribed.get(sym.upper(), set()).discard(stream_type)
                await ws.send(json.dumps({"result": None, "id": msg.get("id", 1)}))

        async def writer():
            last_close = time.time()
            forced = False
            while True:
                await asyncio.sleep(0.5)
                now = time.time()
                if (
                    conn_id == 1 and not forced
                    and self._force_disconnect_first_conn_after is not None
                    and now - start_time > self._force_disconnect_first_conn_after
                ):
                    logger.info("[fake-binance] intentionally dropping connection #%d to test reconnect", conn_id)
                    self.stats["forced_disconnects"] += 1
                    forced = True
                    await ws.close(code=1006, reason="simulated network drop")
                    return

                should_close_kline = (now - last_close) >= self._kline_close_seconds
                for sym, stream_types in list(subscribed.items()):
                    if not stream_types:
                        continue
                    st = states[sym]
                    st.tick()

                    if "miniTicker" in stream_types:
                        mini = {
                            "e": "24hrMiniTicker", "E": int(now * 1000), "s": sym,
                            "c": f"{st.price:.4f}", "o": f"{st.open_price:.4f}",
                            "h": f"{st.high:.4f}", "l": f"{st.low:.4f}",
                            "v": f"{st.base_volume:.4f}", "q": f"{st.base_volume * st.price:.4f}",
                        }
                        await ws.send(json.dumps({"stream": f"{sym.lower()}@miniTicker", "data": mini}))
                        self.stats["messages_sent"] += 1

                    if "bookTicker" in stream_types:
                        half_spread = st.price * half_spread_fraction
                        book = {
                            "e": "bookTicker", "s": sym, "T": int(now * 1000), "E": int(now * 1000),
                            "b": f"{st.price - half_spread:.6f}", "B": "5.0",
                            "a": f"{st.price + half_spread:.6f}", "A": "5.0",
                        }
                        await ws.send(json.dumps({"stream": f"{sym.lower()}@bookTicker", "data": book}))
                        self.stats["messages_sent"] += 1

                    if "kline_1m" in stream_types and should_close_kline:
                        snap = st.close_and_reset()
                        kline = {
                            "e": "kline", "E": int(now * 1000), "s": sym,
                            "k": {
                                "t": snap["open_time"], "T": snap["close_time"], "i": "1m",
                                "o": f"{snap['open']:.4f}", "c": f"{snap['close']:.4f}",
                                "h": f"{snap['high']:.4f}", "l": f"{snap['low']:.4f}",
                                "v": f"{snap['base_volume']:.4f}",
                                "q": f"{snap['base_volume'] * snap['close']:.4f}",
                                "V": f"{snap['taker_buy_base_volume']:.4f}",
                                "Q": f"{snap['taker_buy_base_volume'] * snap['close']:.4f}",
                                "n": random.randint(5, 50), "x": True,
                            },
                        }
                        await ws.send(json.dumps({"stream": f"{sym.lower()}@kline_1m", "data": kline}))
                        self.stats["messages_sent"] += 1
                if should_close_kline:
                    last_close = now

        start_time = time.time()
        reader_task = asyncio.create_task(reader())
        writer_task = asyncio.create_task(writer())
        try:
            await asyncio.wait([reader_task, writer_task], return_when=asyncio.FIRST_COMPLETED)
        finally:
            reader_task.cancel()
            writer_task.cancel()
            logger.info("[fake-binance] connection #%d closed", conn_id)


class FakeBybitServer:
    """Mimics Bybit V5's public linear endpoint: op:subscribe / op:ping,
    then periodic kline.1.{symbol} + tickers.{symbol} pushes."""

    def __init__(self, symbols: list[str], kline_close_seconds: float = 5.0,
                 pump_symbol: str | None = None, pump_after_seconds: float | None = None):
        self._symbols_config = symbols
        self._kline_close_seconds = kline_close_seconds
        self._pump_symbol = pump_symbol
        self._pump_after_seconds = pump_after_seconds
        self._connection_count = 0
        self.stats = {"connections": 0, "messages_sent": 0, "pings_received": 0}

    async def handler(self, ws):
        self._connection_count += 1
        conn_id = self._connection_count
        self.stats["connections"] += 1
        subscribed: dict[str, set[str]] = {}  # symbol -> set of {"kline","tickers","orderbook"}
        states: dict[str, SymbolState] = {
            s: SymbolState(s, random.uniform(10, 500),
                            pump_after_seconds=self._pump_after_seconds if s == self._pump_symbol else None)
            for s in self._symbols_config
        }
        half_spread_fraction = 0.00015
        logger.info("[fake-bybit] connection #%d opened", conn_id)

        def _topic_kind_and_symbol(topic: str) -> tuple[str, str]:
            parts = topic.split(".")
            symbol = parts[-1]
            kind = parts[0]  # "kline" | "tickers" | "orderbook"
            return kind, symbol

        async def reader():
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                op = msg.get("op")
                if op == "subscribe":
                    for arg in msg.get("args", []):
                        kind, sym = _topic_kind_and_symbol(arg)
                        subscribed.setdefault(sym, set()).add(kind)
                elif op == "unsubscribe":
                    for arg in msg.get("args", []):
                        kind, sym = _topic_kind_and_symbol(arg)
                        subscribed.get(sym, set()).discard(kind)
                elif op == "ping":
                    self.stats["pings_received"] += 1
                    await ws.send(json.dumps({"op": "pong", "ts": int(time.time() * 1000)}))

        async def writer():
            last_close = time.time()
            while True:
                await asyncio.sleep(0.5)
                now = time.time()
                should_close_kline = (now - last_close) >= self._kline_close_seconds
                for sym, kinds in list(subscribed.items()):
                    if not kinds:
                        continue
                    st = states[sym]
                    st.tick()

                    if "tickers" in kinds:
                        ticker = {
                            "topic": f"tickers.{sym}", "type": "snapshot", "ts": int(now * 1000),
                            "data": {
                                "symbol": sym, "lastPrice": f"{st.price:.4f}",
                                "volume24h": f"{st.base_volume:.4f}",
                                "turnover24h": f"{st.base_volume * st.price:.4f}",
                                "price24hPcnt": f"{(st.price - st.open_price) / st.open_price:.5f}",
                            },
                        }
                        await ws.send(json.dumps(ticker))
                        self.stats["messages_sent"] += 1

                    if "orderbook" in kinds:
                        half_spread = st.price * half_spread_fraction
                        book = {
                            "topic": f"orderbook.1.{sym}", "type": "snapshot", "ts": int(now * 1000),
                            "data": {
                                "s": sym,
                                "b": [[f"{st.price - half_spread:.6f}", "5.0"]],
                                "a": [[f"{st.price + half_spread:.6f}", "5.0"]],
                            },
                        }
                        await ws.send(json.dumps(book))
                        self.stats["messages_sent"] += 1

                    if "kline" in kinds and should_close_kline:
                        snap = st.close_and_reset()
                        kline = {
                            "topic": f"kline.1.{sym}", "type": "snapshot", "ts": int(now * 1000),
                            "data": [{
                                "start": snap["open_time"], "end": snap["close_time"], "interval": "1",
                                "open": f"{snap['open']:.4f}", "close": f"{snap['close']:.4f}",
                                "high": f"{snap['high']:.4f}", "low": f"{snap['low']:.4f}",
                                "volume": f"{snap['base_volume']:.4f}",
                                "turnover": f"{snap['base_volume'] * snap['close']:.4f}",
                                "confirm": True,
                            }],
                        }
                        await ws.send(json.dumps(kline))
                        self.stats["messages_sent"] += 1
                if should_close_kline:
                    last_close = now

        reader_task = asyncio.create_task(reader())
        writer_task = asyncio.create_task(writer())
        try:
            await asyncio.wait([reader_task, writer_task], return_when=asyncio.FIRST_COMPLETED)
        finally:
            reader_task.cancel()
            writer_task.cancel()
            logger.info("[fake-bybit] connection #%d closed", conn_id)


async def serve_forever(binance: FakeBinanceServer, bybit: FakeBybitServer, binance_port: int, bybit_port: int):
    async with websockets.serve(binance.handler, "127.0.0.1", binance_port), \
               websockets.serve(bybit.handler, "127.0.0.1", bybit_port):
        logger.info("Fake Binance server on ws://127.0.0.1:%d", binance_port)
        logger.info("Fake Bybit server on ws://127.0.0.1:%d", bybit_port)
        await asyncio.Future()  # run forever
