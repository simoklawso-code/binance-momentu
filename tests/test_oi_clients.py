import json

import pytest

from app.exchanges.binance import oi as binance_oi
from app.exchanges.bybit import oi as bybit_oi


@pytest.mark.asyncio
async def test_binance_oi_parses_valid_response(monkeypatch):
    def fake_fetch_sync(url, timeout=5.0):
        assert "BTCUSDT" in url
        return {"symbol": "BTCUSDT", "openInterest": "12345.678", "time": 1700000000000}

    monkeypatch.setattr(binance_oi, "_fetch_sync", fake_fetch_sync)
    event = await binance_oi.fetch_open_interest("https://fapi.binance.com", "BTCUSDT")
    assert event is not None
    assert event.open_interest == 12345.678
    assert event.symbol == "BTCUSDT"


@pytest.mark.asyncio
async def test_binance_oi_returns_none_on_malformed_response(monkeypatch):
    monkeypatch.setattr(binance_oi, "_fetch_sync", lambda url, timeout=5.0: {"unexpected": "shape"})
    event = await binance_oi.fetch_open_interest("https://fapi.binance.com", "BTCUSDT")
    assert event is None


@pytest.mark.asyncio
async def test_binance_oi_returns_none_on_network_error(monkeypatch):
    def raise_error(url, timeout=5.0):
        raise OSError("network unreachable")

    monkeypatch.setattr(binance_oi, "_fetch_sync", raise_error)
    event = await binance_oi.fetch_open_interest("https://fapi.binance.com", "BTCUSDT")
    assert event is None


@pytest.mark.asyncio
async def test_bybit_oi_parses_valid_response(monkeypatch):
    def fake_fetch_sync(url, timeout=5.0):
        assert "BTCUSDT" in url
        return {"result": {"list": [{"openInterest": "999.5", "timestamp": "1700000000000"}]}}

    monkeypatch.setattr(bybit_oi, "_fetch_sync", fake_fetch_sync)
    event = await bybit_oi.fetch_open_interest("https://api.bybit.com", "BTCUSDT")
    assert event is not None
    assert event.open_interest == 999.5


@pytest.mark.asyncio
async def test_bybit_oi_returns_none_on_empty_list(monkeypatch):
    monkeypatch.setattr(bybit_oi, "_fetch_sync", lambda url, timeout=5.0: {"result": {"list": []}})
    event = await bybit_oi.fetch_open_interest("https://api.bybit.com", "BTCUSDT")
    assert event is None
