"""
Notification Bus (§35 dedup/state-transition alerts, §40 triggers).

LIMITATION (documented, not silently approximated): §40 describes the
full browser Push API (permission flow, VAPID subscription handling).
Implementing that correctly needs a service worker + VAPID keypair +
per-browser subscription storage, which is real infrastructure beyond
a lightweight Phase 4 MVP. This module implements the same TRIGGER
logic (meaningful state transitions only, never every raw market
event) as a bounded in-memory bus the dashboard polls via
`GET /api/notifications` — the "cooldown, deduplication, severity"
requirements from §40 are covered; actual OS-level push delivery is
not. Swap in a real Push API layer later without changing callers —
they only ever call `bus.notify(...)`.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field


@dataclass
class Notification:
    kind: str  # PUMP_DETECTED | ENTRY_APPROVED | TAKE_PROFIT | STOP_LOSS | INVALIDATED
    symbol: str
    exchange: str
    message: str
    severity: str = "info"  # info | warning | critical
    created_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    data: dict = field(default_factory=dict)


class NotificationBus:
    def __init__(self, max_size: int = 200, dedup_window_seconds: float = 30.0) -> None:
        self._items: deque[Notification] = deque(maxlen=max_size)
        self._dedup_window_seconds = dedup_window_seconds
        self._last_sent: dict[str, float] = {}

    def notify(self, kind: str, symbol: str, exchange: str, message: str, severity: str = "info", data: dict | None = None) -> bool:
        """Returns True if the notification was actually recorded (not
        deduplicated). §35: no new notification every scan cycle —
        the same (kind, symbol, exchange) is suppressed within the
        dedup window."""
        key = f"{kind}:{exchange}:{symbol}"
        now = time.monotonic()
        last = self._last_sent.get(key)
        if last is not None and (now - last) < self._dedup_window_seconds:
            return False
        self._last_sent[key] = now
        self._items.append(Notification(kind=kind, symbol=symbol, exchange=exchange, message=message, severity=severity, data=data or {}))
        return True

    def recent(self, limit: int = 50) -> list[Notification]:
        return list(self._items)[-limit:]
