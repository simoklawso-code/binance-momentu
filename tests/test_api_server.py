import json
import time
import urllib.request

import pytest

from app.api.server import ApiServer
from app.domain.enums import Exchange
from app.signals.candidate_manager import Stage1Result
from app.storage.db import Storage
from tests.test_pipeline import FakeIngestion
from tests.test_signal_engine import build_breakout_ready_state


@pytest.fixture
def running_server(tmp_path):
    from app.config.settings import Settings
    from app.signals.pipeline import SignalPipeline

    storage = Storage(str(tmp_path / "test.sqlite3"))
    storage.start()
    settings = Settings()
    ingestion = FakeIngestion()
    pipeline = SignalPipeline(settings, ingestion, storage, claude_auditor=None)

    state = build_breakout_ready_state()
    pipeline.feature_store._states[(Exchange.BINANCE, "PUMPUSDT")] = state
    pipeline.candidate_manager.update([
        Stage1Result(Exchange.BINANCE, "PUMPUSDT", 5.0, 3.0, 40_000_000.0, True),
    ])
    pipeline.notifications.notify("PUMP_DETECTED", "PUMPUSDT", "binance", "test notification")

    server = ApiServer(ingestion, pipeline, str(tmp_path / "test.sqlite3"), host="127.0.0.1", port=8099)
    server.start()
    time.sleep(0.2)
    try:
        yield server
    finally:
        server.stop()
        storage.stop()


def get_json(path):
    with urllib.request.urlopen(f"http://127.0.0.1:8099{path}", timeout=5) as resp:
        return json.loads(resp.read().decode())


def test_dashboard_serves_html(running_server):
    with urllib.request.urlopen("http://127.0.0.1:8099/", timeout=5) as resp:
        assert resp.status == 200
        body = resp.read().decode()
        assert "<html" in body.lower()


def test_health_endpoint(running_server):
    data = get_json("/api/health")
    assert "ingestion" in data
    assert "pipeline" in data


def test_candidates_endpoint_returns_promoted_symbol(running_server):
    data = get_json("/api/candidates")
    binance_symbols = [r["symbol"] for r in data.get("binance", [])]
    assert "PUMPUSDT" in binance_symbols


def test_notifications_endpoint(running_server):
    data = get_json("/api/notifications")
    assert any(n["kind"] == "PUMP_DETECTED" for n in data)


def test_unknown_route_returns_404(running_server):
    try:
        urllib.request.urlopen("http://127.0.0.1:8099/api/does-not-exist", timeout=5)
        assert False, "expected HTTPError"
    except urllib.error.HTTPError as e:
        assert e.code == 404
