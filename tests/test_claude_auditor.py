import pytest

from app.ai import claude_auditor as ca_module
from app.ai.claude_auditor import ClaudeContextAuditor

SAMPLE_AUDIT_TRAIL = {
    "symbol": "BTCUSDT", "exchange": "binance",
    "market_state": {"quant_score": 85.0, "entry_price": 100.0, "breakout_level": 99.5, "absorption": False},
    "risk_state": {"stop_loss": 95.0, "take_profit": 110.0, "final_position_size_usd": 500.0},
    "friction_state": {"real_friction_coverage": 3.5},
    "feature_snapshot": {},
}


@pytest.mark.asyncio
async def test_no_api_key_returns_rejected_failure_not_judgment():
    auditor = ClaudeContextAuditor(api_key=None)
    result = await auditor.evaluate(SAMPLE_AUDIT_TRAIL)
    assert result.decision == "rejected_failure"


@pytest.mark.asyncio
async def test_valid_approved_response_parsed_correctly(monkeypatch):
    def fake_post_sync(api_key, model, summary, timeout):
        return {"content": [{"type": "text", "text": '{"decision": "APPROVED", "reasoning": "Looks clean."}'}]}

    monkeypatch.setattr(ca_module, "_post_sync", fake_post_sync)
    auditor = ClaudeContextAuditor(api_key="fake-key")
    result = await auditor.evaluate(SAMPLE_AUDIT_TRAIL)
    assert result.decision == "approved"
    assert result.reasoning == "Looks clean."


@pytest.mark.asyncio
async def test_valid_rejected_response_is_judgment_not_failure(monkeypatch):
    def fake_post_sync(api_key, model, summary, timeout):
        return {"content": [{"type": "text", "text": '{"decision": "REJECTED", "reasoning": "Feature snapshot looks inconsistent."}'}]}

    monkeypatch.setattr(ca_module, "_post_sync", fake_post_sync)
    auditor = ClaudeContextAuditor(api_key="fake-key")
    result = await auditor.evaluate(SAMPLE_AUDIT_TRAIL)
    assert result.decision == "rejected_judgment"


@pytest.mark.asyncio
async def test_invalid_json_is_rejected_failure_not_judgment(monkeypatch):
    def fake_post_sync(api_key, model, summary, timeout):
        return {"content": [{"type": "text", "text": "not valid json at all"}]}

    monkeypatch.setattr(ca_module, "_post_sync", fake_post_sync)
    auditor = ClaudeContextAuditor(api_key="fake-key")
    result = await auditor.evaluate(SAMPLE_AUDIT_TRAIL)
    assert result.decision == "rejected_failure"


@pytest.mark.asyncio
async def test_network_error_is_rejected_failure(monkeypatch):
    def raise_error(api_key, model, summary, timeout):
        raise OSError("connection refused")

    monkeypatch.setattr(ca_module, "_post_sync", raise_error)
    auditor = ClaudeContextAuditor(api_key="fake-key")
    result = await auditor.evaluate(SAMPLE_AUDIT_TRAIL)
    assert result.decision == "rejected_failure"


def test_cooldown_blocks_repeated_calls_within_window():
    auditor = ClaudeContextAuditor(api_key="fake-key", cooldown_seconds=300)
    assert auditor.should_call("binance", "BTCUSDT", now=1000.0) is True
    auditor._record_call("binance", "BTCUSDT", now=1000.0)
    assert auditor.should_call("binance", "BTCUSDT", now=1100.0) is False  # only 100s elapsed
    assert auditor.should_call("binance", "BTCUSDT", now=1301.0) is True   # 301s elapsed


def test_cooldown_is_independent_per_symbol():
    auditor = ClaudeContextAuditor(api_key="fake-key", cooldown_seconds=300)
    auditor._record_call("binance", "BTCUSDT", now=1000.0)
    assert auditor.should_call("binance", "ETHUSDT", now=1000.0) is True
