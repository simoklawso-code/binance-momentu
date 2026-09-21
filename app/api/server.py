"""
Lightweight local Web API (§39, "Keep the UI lightweight, not
enterprise-complex"). Built on Python's stdlib `http.server` rather
than a framework — no extra dependency, runs in a background thread
so it never competes with the asyncio ingestion/signal loops for the
event loop itself.

Endpoints are read-only snapshots of in-memory pipeline state (thread
reads of plain dicts/lists are safe enough under the GIL for a
dashboard — occasional staleness is fine here, this is not a
transactional system) plus a couple of SQLite read queries (safe for
concurrent readers under WAL).
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)

DASHBOARD_HTML_PATH = Path(__file__).resolve().parent.parent.parent / "app" / "web" / "dashboard.html"


class ApiContext:
    """Everything the HTTP handler needs, assembled once and shared
    across requests. Kept as a plain object (not a global) so tests can
    construct an isolated context per server instance."""

    def __init__(self, ingestion, pipeline, db_path: str) -> None:
        self.ingestion = ingestion
        self.pipeline = pipeline
        self.db_path = db_path


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload) -> None:
    body = json.dumps(payload, default=str).encode()
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.end_headers()
    handler.wfile.write(body)


def _query_recent_signals(db_path: str, limit: int) -> list[dict]:
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT signal_id, symbol, exchange, decision_timestamp, final_signal_state, "
            "quant_score, entry_price, stop_loss, take_profit, final_position_size_usd, "
            "real_friction_coverage, reasons FROM signals ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _query_recent_trades(db_path: str, limit: int) -> list[dict]:
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM paper_trades ORDER BY created_at DESC LIMIT ?", (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def make_handler(ctx: ApiContext):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # silence default stderr access logging
            logger.debug(fmt, *args)

        def do_GET(self) -> None:  # noqa: N802 (stdlib method name)
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)

            try:
                if path in ("/", "/dashboard", "/dashboard.html"):
                    self._serve_dashboard()
                elif path == "/api/health":
                    _json_response(self, 200, self._health())
                elif path == "/api/candidates":
                    _json_response(self, 200, self._candidates())
                elif path == "/api/positions":
                    _json_response(self, 200, self._positions())
                elif path == "/api/trades":
                    limit = int(query.get("limit", ["50"])[0])
                    _json_response(self, 200, _query_recent_trades(ctx.db_path, limit))
                elif path == "/api/signals":
                    limit = int(query.get("limit", ["50"])[0])
                    _json_response(self, 200, _query_recent_signals(ctx.db_path, limit))
                elif path == "/api/notifications":
                    limit = int(query.get("limit", ["50"])[0])
                    notes = ctx.pipeline.notifications.recent(limit) if ctx.pipeline else []
                    _json_response(self, 200, [n.__dict__ for n in notes])
                else:
                    _json_response(self, 404, {"error": "not found"})
            except Exception as exc:  # noqa: BLE001 - never let the HTTP thread die
                logger.exception("API handler error for %s", path)
                _json_response(self, 500, {"error": str(exc)})

        def _serve_dashboard(self) -> None:
            if DASHBOARD_HTML_PATH.exists():
                body = DASHBOARD_HTML_PATH.read_bytes()
            else:
                body = b"<html><body><h1>Dashboard file not found</h1></body></html>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _health(self) -> dict:
            snapshot = ctx.ingestion.get_snapshot() if ctx.ingestion else {}
            pipeline_stats = {}
            if ctx.pipeline is not None:
                pipeline_stats = {
                    "signals_evaluated_total": ctx.pipeline.signals_evaluated_total,
                    "entries_opened_total": ctx.pipeline.entries_opened_total,
                    "open_positions": len(ctx.pipeline.paper_engine.open_positions),
                    "candidates_binance": len(ctx.pipeline.candidate_manager.current_candidates(_binance_key())),
                }
            return {"ingestion": snapshot, "pipeline": pipeline_stats}

        def _candidates(self) -> dict:
            if ctx.pipeline is None:
                return {}
            from app.domain.enums import Exchange
            result = {}
            for exchange in (Exchange.BINANCE, Exchange.BYBIT):
                symbols = ctx.pipeline.candidate_manager.current_candidates(exchange)
                rows = []
                for symbol in symbols:
                    state = ctx.pipeline.feature_store.get(exchange, symbol)
                    if state is None:
                        continue
                    from app.features import feature_engine as fe
                    rows.append({
                        "symbol": symbol,
                        "rvol": fe.rvol(state),
                        "trade_acceleration": fe.trade_acceleration(state),
                        "dollar_volume_24h": fe.dollar_volume_24h(state),
                        "spread_percent": fe.spread_percent(state),
                        "last_price": state.latest_ticker.last_price if state.latest_ticker else None,
                    })
                result[exchange.value] = rows
            return result

        def _positions(self) -> list[dict]:
            if ctx.pipeline is None:
                return []
            return [p.__dict__ for p in ctx.pipeline.paper_engine.open_positions]

    return Handler


def _binance_key():
    from app.domain.enums import Exchange
    return Exchange.BINANCE


class ApiServer:
    def __init__(self, ingestion, pipeline, db_path: str, host: str = "127.0.0.1", port: int = 8080) -> None:
        self._ctx = ApiContext(ingestion, pipeline, db_path)
        self._host = host
        self._port = port
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        handler_cls = make_handler(self._ctx)
        self._httpd = ThreadingHTTPServer((self._host, self._port), handler_cls)
        self._thread = threading.Thread(target=self._httpd.serve_forever, name="api-server", daemon=True)
        self._thread.start()
        logger.info("API + dashboard serving on http://%s:%d", self._host, self._port)

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
