"""
Signal Pipeline — ties Phase 2 (Feature Engine, Dynamic Funnel,
Candidate Stream Manager) and Phase 3 (Quant Score, Signal Engine,
Risk Engine, Paper Execution) together into the live loop described
architecturally in §43 layers 2/3/5, plus §44's separate, non-blocking
workers (Claude latency and database latency must never block market
ingestion — both run via asyncio.to_thread / their own async calls,
never inline in the ingestion consumer).

This is the "Main Async Event Loop" consumer referenced in §44: it
drains the Ring Buffer (via IngestionManager), updates Feature Store
state, and on a slower cadence re-screens Stage 1, promotes candidates,
runs the Signal Engine on the current candidate set, and — only for a
signal that reaches ENTRY_PENDING — asks the Claude Auditor before
opening a paper position.
"""

from __future__ import annotations

import asyncio
import logging

from app.ai.claude_auditor import ClaudeContextAuditor
from app.config.settings import Settings
from app.domain.enums import Exchange, SignalState
from app.domain.events import MarketTickerEvent
from app.execution.paper_engine import PaperTradingEngine
from app.exchanges.binance.adapter import BinanceAdapter
from app.exchanges.bybit.adapter import BybitAdapter
from app.features.oi_poller import OIPoller
from app.features.store import FeatureStore
from app.ingestion.ingestion_manager import IngestionManager
from app.notifications.bus import NotificationBus
from app.risk.risk_engine import RiskEngine
from app.signals.candidate_manager import CandidateStreamManager, rank_and_promote, screen_stage1
from app.signals.signal_engine import compute_btc_pause, evaluate_signal
from app.storage.db import Storage

logger = logging.getLogger(__name__)


def current_price_for(state) -> float | None:
    """Prefer the live ticker price; fall back to the last closed
    candle's close if no ticker has arrived yet."""
    if state.latest_ticker is not None:
        return state.latest_ticker.last_price
    if state.closed_klines:
        return state.closed_klines[-1].close
    return None


class SignalPipeline:
    def __init__(
        self,
        settings: Settings,
        ingestion: IngestionManager,
        storage: Storage,
        claude_auditor: ClaudeContextAuditor | None = None,
    ) -> None:
        self._settings = settings
        self._ingestion = ingestion
        self._storage = storage
        self._claude_auditor = claude_auditor

        self.feature_store = FeatureStore()
        self.candidate_manager = CandidateStreamManager()
        self.risk_engine = RiskEngine(settings.strategy)
        self.paper_engine = PaperTradingEngine()
        self.oi_poller = OIPoller(settings, self.feature_store, self.candidate_manager)
        self.notifications = NotificationBus()

        self._consumer_task: asyncio.Task | None = None
        self._stage1_task: asyncio.Task | None = None
        self._signal_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

        self.signals_evaluated_total = 0
        self.entries_opened_total = 0

    async def start(self) -> None:
        self._stop_event.clear()
        self._consumer_task = asyncio.create_task(self._consume_loop(), name="pipeline-consumer")
        self._stage1_task = asyncio.create_task(self._stage1_loop(), name="pipeline-stage1")
        self._signal_task = asyncio.create_task(self._signal_loop(), name="pipeline-signal")

    async def stop(self) -> None:
        self._stop_event.set()
        for task in (self._consumer_task, self._stage1_task, self._signal_task):
            if task:
                task.cancel()
        await self.oi_poller.stop()

    # -- consumer: drains the Ring Buffer, feeds Feature Store + Paper Engine ---

    async def _consume_loop(self) -> None:
        buffer = self._ingestion.buffer
        try:
            while not self._stop_event.is_set():
                batch = buffer.pop_batch(max_items=500)
                if not batch:
                    await asyncio.sleep(0.05)
                    continue
                for event in batch:
                    self.feature_store.ingest(event)
                    if isinstance(event, MarketTickerEvent):
                        self._check_exits(event)
        except asyncio.CancelledError:
            return

    def _check_exits(self, ticker: MarketTickerEvent) -> None:
        closed = self.paper_engine.on_price_update(ticker.symbol, ticker.exchange.value, ticker.last_price)
        for position in closed:
            self._storage.enqueue_paper_trade_close(position)
            pnl_percent_of_equity = (position.realized_pnl_usd / self._settings.strategy.paper_starting_equity_usd) * 100
            self.risk_engine.register_closed_position(position.signal_id, pnl_percent_of_equity)
            self.notifications.notify(
                kind=position.state.value.upper(), symbol=position.symbol, exchange=position.exchange,
                message=f"{position.symbol} closed {position.state.value} pnl=${position.realized_pnl_usd:.2f}",
                severity="info" if position.state == SignalState.TAKE_PROFIT else "warning",
                data={"pnl_usd": position.realized_pnl_usd},
            )
            logger.info(
                "Paper position closed: %s %s state=%s pnl_usd=%.2f",
                position.exchange, position.symbol, position.state.value, position.realized_pnl_usd,
            )

    # -- Stage 1 / Dynamic Funnel / Candidate Stream Manager loop --------------

    async def _stage1_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(self._settings.stage1_cycle_seconds)
                self._run_stage1_cycle()
                await self.oi_poller.sync_tasks()
        except asyncio.CancelledError:
            return

    def _run_stage1_cycle(self) -> None:
        results = screen_stage1(self.feature_store, self._settings.strategy)
        promoted = rank_and_promote(results, self._settings.strategy)
        diffs = self.candidate_manager.update(promoted)

        binance_adapter = self._ingestion.binance
        bybit_adapter = self._ingestion.bybit
        for exchange, (added, removed) in diffs.items():
            adapter = binance_adapter if exchange == Exchange.BINANCE else bybit_adapter
            if added or removed:
                asyncio.create_task(adapter.update_subscriptions(add=list(added), remove=list(removed)))
            if isinstance(adapter, (BinanceAdapter, BybitAdapter)):
                adapter.set_candidate_symbols(self.candidate_manager.current_candidates(exchange))
                # §17: spread is sourced from a candidate-only bookTicker/
                # orderbook stream — sync that dedicated stream here too.
                asyncio.create_task(adapter.sync_candidate_book_ticker_stream(self.candidate_manager.current_candidates(exchange)))

    # -- Signal Engine / Claude Auditor / Paper Execution loop -----------------

    async def _signal_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(self._settings.signal_cycle_seconds)
                await self._run_signal_cycle()
        except asyncio.CancelledError:
            return

    async def _run_signal_cycle(self) -> None:
        for exchange in (Exchange.BINANCE, Exchange.BYBIT):
            for symbol in self.candidate_manager.current_candidates(exchange):
                state = self.feature_store.get(exchange, symbol)
                if state is None:
                    continue
                price = current_price_for(state)
                if price is None:
                    continue

                btc_pause = compute_btc_pause(self.feature_store, self._settings.strategy)
                result = evaluate_signal(
                    state, price, self._settings.strategy, self.risk_engine,
                    self._settings.strategy.paper_starting_equity_usd, btc_pause,
                )
                self.signals_evaluated_total += 1
                self._storage.enqueue_signal(result.audit_trail)

                if result.state != SignalState.ENTRY_PENDING:
                    continue

                await self._handle_entry_pending(result)

    async def _handle_entry_pending(self, result) -> None:
        if self._claude_auditor is None:
            logger.info("Claude Auditor not wired — leaving %s as ENTRY_PENDING without opening a position", result.symbol)
            return
        if not self._claude_auditor.should_call(result.exchange, result.symbol):
            return  # cooldown active — do not spam Claude (§36)

        decision = await self._claude_auditor.evaluate(result.audit_trail)
        self._storage.enqueue_claude_decision(result.signal_id, decision.decision, {"reasoning": decision.reasoning}, decision.latency_ms)

        if decision.decision != "approved":
            logger.info("Claude Auditor declined %s: %s (%s)", result.symbol, decision.decision, decision.reasoning)
            return

        position = self.paper_engine.open_position(
            signal_id=result.signal_id, symbol=result.symbol, exchange=result.exchange,
            entry_price=result.entry_price, stop_loss=result.stop_loss_result.stop_loss,
            take_profit=result.take_profit_result.take_profit, size_usd=result.final_position_size_usd,
        )
        self.risk_engine.register_open_position(result.signal_id, result.symbol)
        self._storage.enqueue_paper_trade_open(position)
        self.entries_opened_total += 1
        self.notifications.notify(
            kind="ENTRY_APPROVED", symbol=result.symbol, exchange=result.exchange,
            message=f"{result.symbol} entry approved @ {position.entry_price:.4f} (score={result.quant_result.quant_score:.1f})",
            severity="info",
            data={"entry": position.entry_price, "stop_loss": position.stop_loss, "take_profit": position.take_profit},
        )
        logger.info("PAPER ENTRY OPENED: %s %s entry=%.4f sl=%.4f tp=%.4f size=$%.2f",
                    result.exchange, result.symbol, position.entry_price, position.stop_loss,
                    position.take_profit, position.size_usd)
