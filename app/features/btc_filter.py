"""BTC Market Safety Filter (§31). No 24h Beta model in MVP — a single
absolute 15m price-change threshold on BTC freezes new entries
system-wide. Existing paper positions keep being monitored under their
own exit rules; only NEW entries are blocked."""

from __future__ import annotations

from app.domain.enums import Exchange
from app.features.store import FeatureStore


def btc_15m_price_change_percent(store: FeatureStore, btc_symbol: str = "BTCUSDT") -> float | None:
    state = store.get(Exchange.BINANCE, btc_symbol)
    if state is None or len(state.closed_klines) < 15:
        return None
    klines = list(state.closed_klines)
    current = klines[-1].close
    price_15m_ago = klines[-15].close
    if price_15m_ago <= 0:
        return None
    return (current - price_15m_ago) / price_15m_ago * 100


def btc_volatility_pause_active(store: FeatureStore, threshold_percent: float, btc_symbol: str = "BTCUSDT") -> bool:
    """§31: freeze on |15m change| > threshold. Returns False (not
    paused) when BTC data isn't available yet — but the caller's own
    data-integrity gate should independently refuse to trade on missing
    required data (§48: FAIL-SAFE = NO TRADE covers that, not this
    function's job to also encode it)."""
    change = btc_15m_price_change_percent(store, btc_symbol)
    if change is None:
        return False
    return abs(change) > threshold_percent
