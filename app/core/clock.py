"""
Clock / NTP sync interface (§7).

Timestamp integrity depends on the local machine's clock being
reasonably accurate — exchange_timestamp comparisons and
ingest_latency_ms are meaningless if the local clock has drifted. This
module doesn't perform NTP sync itself (that's an OS-level operational
concern — e.g. `chrony`/`ntpd` running on the host, called out in the
README), but it exposes a pluggable health check so the system can
detect and report gross clock drift rather than silently trusting it.
"""

from __future__ import annotations

import abc
import time
from dataclasses import dataclass


@dataclass
class ClockHealth:
    checked_at_ms: int
    estimated_drift_ms: float | None
    healthy: bool
    detail: str = ""


class ClockSyncChecker(abc.ABC):
    """Pluggable clock-drift check. Default implementation is a no-op
    that always reports healthy (assumes OS-level NTP is configured, per
    the operational requirement in §7) — swap in a real NTP round-trip
    check in production if stricter guarantees are needed."""

    @abc.abstractmethod
    def check(self) -> ClockHealth:
        ...


class NoOpClockSyncChecker(ClockSyncChecker):
    """Assumes the host's own NTP daemon is correctly configured (an
    explicit operational requirement — see README "Operational
    Requirements"). Always reports healthy; exists so the interface has
    exactly one call site to upgrade later without touching callers."""

    def check(self) -> ClockHealth:
        return ClockHealth(
            checked_at_ms=int(time.time() * 1000),
            estimated_drift_ms=None,
            healthy=True,
            detail="OS-level NTP assumed configured; no active drift check performed",
        )
