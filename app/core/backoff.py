"""
Exponential backoff with jitter for WebSocket reconnects (§6, §47).

Deliberately exchange-agnostic: both the Binance and Bybit adapters use
the same policy object, configured with their own base/max/jitter from
settings, so reconnect behavior is consistent and never duplicated with
subtly different logic per exchange.

No aggressive retry storms during outages — uncertain API conditions
must not translate into hammering the exchange (§47).
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass
class BackoffPolicy:
    base_seconds: float = 1.0
    max_seconds: float = 60.0
    jitter_seconds: float = 1.0
    multiplier: float = 2.0

    def delay_for_attempt(self, attempt: int) -> float:
        """attempt is 1-indexed (first retry = attempt 1)."""
        if attempt < 1:
            attempt = 1
        raw = self.base_seconds * (self.multiplier ** (attempt - 1))
        capped = min(raw, self.max_seconds)
        jitter = random.uniform(0, self.jitter_seconds) if self.jitter_seconds > 0 else 0.0
        return capped + jitter
