import pytest

from app.config.settings import Settings
from app.domain.enums import Exchange
from app.execution.paper_engine import PaperTradingEngine
from app.signals.candidate_manager import Stage1Result
from app.signals.pipeline import SignalPipeline
from app.storage.db import Storage
from tests.test_signal_engine import build_breakout_ready_state


class FakeAdapter:
    async def update_subscriptions(self, add, remove):
        pass


class FakeIngestion:
    def __init__(self):
        from app.buffer.ring_buffer import PriorityRingBuffer
        self.buffer = PriorityRingBuffer(capacity=1000)
        self.binance = FakeAdapter()
        self.bybit = FakeAdapter()

    def get_snapshot(self):
        return {"buffer": {"size": 0, "capacity": 1000, "state": "normal"},
                "binance": {"is_healthy": True, "total_messages_received": 0, "total_reconnects": 0},
                "bybit": {"is_healthy": True, "total_messages_received": 0, "total_reconnects": 0}}


class ApprovingAuditor:
    """Duck-typed stand-in for ClaudeContextAuditor — always approves,
    never makes a network call."""

    def should_call(self, exchange, symbol):
        return True

    async def evaluate(self, audit_trail):
        from app.ai.claude_auditor import ClaudeDecisionResult
        return ClaudeDecisionResult(decision="approved", reasoning="test stub", latency_ms=0.0)


class DecliningAuditor:
    def should_call(self, exchange, symbol):
        return True

    async def evaluate(self, audit_trail):
        from app.ai.claude_auditor import ClaudeDecisionResult
        return ClaudeDecisionResult(decision="rejected_judgment", reasoning="test stub decline", latency_ms=0.0)


@pytest.mark.asyncio
async def test_signal_cycle_opens_paper_position_when_claude_approves(tmp_path):
    settings = Settings()
    storage = Storage(str(tmp_path / "test.sqlite3"))
    storage.start()
    try:
        pipeline = SignalPipeline(settings, FakeIngestion(), storage, claude_auditor=ApprovingAuditor())

        state = build_breakout_ready_state()
        pipeline.feature_store._states[(Exchange.BINANCE, "PUMPUSDT")] = state
        pipeline.candidate_manager.update([
            Stage1Result(Exchange.BINANCE, "PUMPUSDT", 5.0, 3.0, 40_000_000.0, True),
        ])

        await pipeline._run_signal_cycle()

        assert pipeline.entries_opened_total == 1
        assert len(pipeline.paper_engine.open_positions) == 1
        position = pipeline.paper_engine.open_positions[0]
        assert position.symbol == "PUMPUSDT"
        assert pipeline.risk_engine.open_position_count == 1
    finally:
        storage.stop()


@pytest.mark.asyncio
async def test_signal_cycle_does_not_open_position_when_claude_declines(tmp_path):
    settings = Settings()
    storage = Storage(str(tmp_path / "test.sqlite3"))
    storage.start()
    try:
        pipeline = SignalPipeline(settings, FakeIngestion(), storage, claude_auditor=DecliningAuditor())
        state = build_breakout_ready_state()
        pipeline.feature_store._states[(Exchange.BINANCE, "PUMPUSDT")] = state
        pipeline.candidate_manager.update([
            Stage1Result(Exchange.BINANCE, "PUMPUSDT", 5.0, 3.0, 40_000_000.0, True),
        ])

        await pipeline._run_signal_cycle()

        assert pipeline.entries_opened_total == 0
        assert pipeline.paper_engine.open_positions == []
    finally:
        storage.stop()


@pytest.mark.asyncio
async def test_signal_cycle_without_claude_auditor_stays_entry_pending(tmp_path):
    """No Claude Auditor wired (e.g. no API key) -> never opens a
    position, never crashes."""
    settings = Settings()
    storage = Storage(str(tmp_path / "test.sqlite3"))
    storage.start()
    try:
        pipeline = SignalPipeline(settings, FakeIngestion(), storage, claude_auditor=None)
        state = build_breakout_ready_state()
        pipeline.feature_store._states[(Exchange.BINANCE, "PUMPUSDT")] = state
        pipeline.candidate_manager.update([
            Stage1Result(Exchange.BINANCE, "PUMPUSDT", 5.0, 3.0, 40_000_000.0, True),
        ])

        await pipeline._run_signal_cycle()

        assert pipeline.entries_opened_total == 0
        assert pipeline.signals_evaluated_total == 1
    finally:
        storage.stop()


def test_check_exits_closes_position_and_updates_risk_engine(tmp_path):
    settings = Settings()
    storage = Storage(str(tmp_path / "test.sqlite3"))
    storage.start()
    try:
        pipeline = SignalPipeline(settings, FakeIngestion(), storage, claude_auditor=None)
        pipeline.paper_engine.open_position(
            "sig1", "BTCUSDT", "binance", entry_price=100.0, stop_loss=95.0, take_profit=110.0, size_usd=1000.0,
        )
        pipeline.risk_engine.register_open_position("sig1", "BTCUSDT")

        from app.domain.enums import EventPriority
        from app.domain.events import MarketTickerEvent
        ticker = MarketTickerEvent(
            exchange=Exchange.BINANCE, symbol="BTCUSDT", exchange_timestamp=1, priority=EventPriority.P1,
            last_price=111.0,
        )
        pipeline._check_exits(ticker)

        assert pipeline.paper_engine.open_positions == []
        assert pipeline.risk_engine.open_position_count == 0
    finally:
        storage.stop()
