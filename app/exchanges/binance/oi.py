"""
Binance Futures Open Interest (§19, LOCKED from V1.7).

There is no bulk OI endpoint on Binance Futures REST — this client is
deliberately single-symbol, and the caller (OIPoller) is responsible
for restricting calls to the current Top 5-15 candidate universe only,
never the full 150-300+ symbol universe, with proper rate-limit
accounting (handled by OIPoller's per-symbol interval spacing).
"""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.error
import urllib.request

from app.domain.enums import Exchange
from app.domain.events import OIEvent, now_ms
from app.ingestion.priority import classify_priority
from app.domain.enums import EventType

logger = logging.getLogger(__name__)

ENDPOINT_TEMPLATE = "{base_url}/fapi/v1/openInterest?symbol={symbol}"


def _fetch_sync(url: str, timeout: float = 5.0) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "crypto-momentum-hunter/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


async def fetch_open_interest(base_url: str, symbol: str, timeout: float = 5.0) -> OIEvent | None:
    """Returns None on any failure — callers must treat missing OI as
    "stale/unavailable", never as zero (§19, §48 FAIL-SAFE = NO TRADE)."""
    url = ENDPOINT_TEMPLATE.format(base_url=base_url, symbol=symbol)
    try:
        data = await asyncio.to_thread(_fetch_sync, url, timeout)
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        logger.warning("Binance OI fetch failed for %s: %s", symbol, exc)
        return None

    try:
        open_interest = float(data["openInterest"])
        provider_ts = int(data.get("time", now_ms()))
    except (KeyError, TypeError, ValueError) as exc:
        logger.warning("Binance OI response malformed for %s: %s", symbol, exc)
        return None

    return OIEvent(
        exchange=Exchange.BINANCE,
        symbol=symbol,
        exchange_timestamp=provider_ts,
        priority=classify_priority(EventType.OPEN_INTEREST, is_candidate=True),
        open_interest=open_interest,
        provider_timestamp=provider_ts,
    )
