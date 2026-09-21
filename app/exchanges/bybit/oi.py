"""
Bybit V5 Open Interest (§19, LOCKED from V1.7: "verify real docs,
don't assume").

The V5 `/v5/market/open-interest` endpoint requires an `intervalTime`
parameter (Bybit reports OI as a time series, not a single instantaneous
value like Binance). The value used below (`5min`) is a reasonable
default for a scalping timeframe, but per the spec's explicit
instruction, this must be re-verified against Bybit's live V5
documentation before production use — it is called out here rather
than silently assumed correct.
"""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.error
import urllib.request

from app.domain.enums import Exchange, EventType
from app.domain.events import OIEvent, now_ms
from app.ingestion.priority import classify_priority

logger = logging.getLogger(__name__)

# NOTE: re-verify `intervalTime` values and response shape against the
# live Bybit V5 docs (§19 discipline) before production use.
ENDPOINT_TEMPLATE = "{base_url}/v5/market/open-interest?category=linear&symbol={symbol}&intervalTime=5min"


def _fetch_sync(url: str, timeout: float = 5.0) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "crypto-momentum-hunter/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


async def fetch_open_interest(base_url: str, symbol: str, timeout: float = 5.0) -> OIEvent | None:
    url = ENDPOINT_TEMPLATE.format(base_url=base_url, symbol=symbol)
    try:
        data = await asyncio.to_thread(_fetch_sync, url, timeout)
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        logger.warning("Bybit OI fetch failed for %s: %s", symbol, exc)
        return None

    try:
        result_list = data["result"]["list"]
        if not result_list:
            return None
        latest = result_list[0]
        open_interest = float(latest["openInterest"])
        provider_ts = int(latest.get("timestamp", now_ms()))
    except (KeyError, TypeError, ValueError, IndexError) as exc:
        logger.warning("Bybit OI response malformed for %s: %s", symbol, exc)
        return None

    return OIEvent(
        exchange=Exchange.BYBIT,
        symbol=symbol,
        exchange_timestamp=provider_ts,
        priority=classify_priority(EventType.OPEN_INTEREST, is_candidate=True),
        open_interest=open_interest,
        provider_timestamp=provider_ts,
    )
