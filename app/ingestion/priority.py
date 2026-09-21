"""
Priority classification (§9).

The ingestion layer classifies event priority before pushing onto the
Ring Buffer — this is a normalizer-time decision, not something the
buffer itself decides. Kept as pure functions so both exchange adapters
apply exactly the same rule, and so Stage-1/Stage-3 "candidate" status
(from later phases) can be injected as a simple predicate without the
adapters needing to know anything about the Candidate Stream Manager.
"""

from __future__ import annotations

from app.domain.enums import EventPriority, EventType


def classify_priority(
    event_type: EventType,
    *,
    is_candidate: bool = False,
    is_closed_kline: bool = False,
) -> EventPriority:
    """§9 priority classes:

    P0: closed 1m Klines, candidate-critical market events, candidate
        BookTicker events, critical execution-state events.
    P1: candidate-related intermediate market events, important
        candidate updates.
    P2: non-critical intermediate events.
    P3: low-value ticker updates, redundant intermediate frames,
        non-candidate miniTicker updates.
    """
    if event_type == EventType.KLINE:
        if is_closed_kline:
            return EventPriority.P0
        return EventPriority.P1 if is_candidate else EventPriority.P2

    if event_type == EventType.BOOK_TICKER:
        # BookTicker is only ever subscribed for candidates (§17) —
        # candidate-critical by definition.
        return EventPriority.P0

    if event_type == EventType.MARKET_TICKER:
        return EventPriority.P1 if is_candidate else EventPriority.P3

    if event_type == EventType.OPEN_INTEREST:
        return EventPriority.P1 if is_candidate else EventPriority.P2

    if event_type == EventType.TRADE:
        return EventPriority.P1 if is_candidate else EventPriority.P2

    if event_type == EventType.EXECUTION:
        return EventPriority.P0

    if event_type == EventType.SIGNAL:
        return EventPriority.P0

    if event_type == EventType.SYSTEM:
        return EventPriority.P1

    if event_type == EventType.ECONOMIC:
        return EventPriority.P2

    return EventPriority.P2
