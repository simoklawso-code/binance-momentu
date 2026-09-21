"""
Dynamic Funnel (§12) and Candidate Stream Manager (§18).

Stage 1 is cheap: RVOL, Trade Acceleration, 24h Dollar Volume only —
no order-book analysis, no Claude, no heavy per-symbol REST calls
(§13). The funnel narrows 150-300+ symbols down to the configured
Top 5-15 candidates, and the Candidate Stream Manager turns that into
a minimal, diff-based subscription update so exchange adapters never
churn symbols that are still candidates.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from app.config.settings import StrategyConfig
from app.domain.enums import Exchange
from app.features.feature_engine import dollar_volume_24h, rvol, trade_acceleration
from app.features.store import FeatureStore


@dataclass
class Stage1Result:
    exchange: Exchange
    symbol: str
    rvol: float | None
    trade_acceleration: float | None
    dollar_volume_24h: float | None
    eligible: bool
    reasons_failed: list[str] = field(default_factory=list)


def screen_stage1(store: FeatureStore, strategy: StrategyConfig) -> list[Stage1Result]:
    results: list[Stage1Result] = []
    for state in store.all_states():
        rv = rvol(state)
        acc = trade_acceleration(state)
        vol = dollar_volume_24h(state)

        reasons: list[str] = []
        if rv is None or rv < strategy.rvol_threshold:
            reasons.append("rvol_below_threshold_or_unavailable")
        if acc is None or acc < strategy.acceleration_threshold:
            reasons.append("acceleration_below_threshold_or_unavailable")
        if vol is None or vol < strategy.min_24h_volume_usd:
            reasons.append("volume_below_threshold_or_unavailable")

        results.append(Stage1Result(
            exchange=state.exchange, symbol=state.symbol,
            rvol=rv, trade_acceleration=acc, dollar_volume_24h=vol,
            eligible=not reasons, reasons_failed=reasons,
        ))
    return results


def rank_and_promote(results: list[Stage1Result], strategy: StrategyConfig) -> list[Stage1Result]:
    """Ranks eligible candidates by RVOL descending (a documented,
    deterministic ranking choice — the spec requires ranking, §33E
    step 4, but doesn't mandate a specific metric) and promotes the
    configured Top candidate_min-candidate_max window."""
    eligible = [r for r in results if r.eligible]
    eligible.sort(key=lambda r: r.rvol or 0.0, reverse=True)
    return eligible[: strategy.candidate_max]


class CandidateStreamManager:
    """§18: diffs previous vs new candidate sets so adapters only
    subscribe/unsubscribe exactly what changed."""

    def __init__(self) -> None:
        self._current_by_exchange: dict[Exchange, set[str]] = {Exchange.BINANCE: set(), Exchange.BYBIT: set()}

    def update(self, promoted: list[Stage1Result]) -> dict[Exchange, tuple[set[str], set[str]]]:
        """Returns {exchange: (added, removed)} — apply directly to
        `ExchangeAdapter.update_subscriptions(add=list(added), remove=list(removed))`."""
        new_by_exchange: dict[Exchange, set[str]] = {Exchange.BINANCE: set(), Exchange.BYBIT: set()}
        for r in promoted:
            new_by_exchange.setdefault(r.exchange, set()).add(r.symbol)

        diffs: dict[Exchange, tuple[set[str], set[str]]] = {}
        for exchange in (Exchange.BINANCE, Exchange.BYBIT):
            old_set = self._current_by_exchange.get(exchange, set())
            new_set = new_by_exchange.get(exchange, set())
            added = new_set - old_set
            removed = old_set - new_set
            diffs[exchange] = (added, removed)
            self._current_by_exchange[exchange] = new_set
        return diffs

    def current_candidates(self, exchange: Exchange) -> set[str]:
        return set(self._current_by_exchange.get(exchange, set()))

    def is_candidate(self, exchange: Exchange, symbol: str) -> bool:
        return symbol in self._current_by_exchange.get(exchange, set())
