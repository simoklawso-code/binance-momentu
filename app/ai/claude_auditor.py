"""
Claude AI Auditor (§36-38).

Claude is NEVER the quantitative engine — this module receives an
ALREADY-COMPLETE signal audit trail (Quant Score >= 80, Friction Gate
already passed, per §36's trigger condition) and asks Claude for a
single APPROVED/REJECTED verdict plus reasoning. Claude cannot
calculate position size, indicators, SL/TP, override risk limits, or
modify price/volume — this module only ever reads Claude's verdict
field; it never lets Claude's response numerically alter anything
already computed upstream.

Cooldown (§36: "Not called on every candidate every scan — use a
state-transition trigger with a configurable cooldown") is tracked per
(exchange, symbol) so a symbol that keeps re-triggering doesn't spam
Claude calls.

§37: on API failure / timeout / invalid JSON / schema violation, the
result is REJECTED for safety, but recorded as `rejected_failure`
(Claude_Rejected_Failure) — NEVER conflated with `rejected_judgment`
(a real veto) in statistics (§38).
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Literal

logger = logging.getLogger(__name__)

ANTHROPIC_MESSAGES_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
DEFAULT_MODEL = "claude-sonnet-4-6"

Decision = Literal["approved", "rejected_judgment", "rejected_failure"]

SYSTEM_PROMPT = """You are a context auditor for an automated crypto scalping system. \
You do NOT calculate any numbers — entry, stop loss, take profit, position size, and every \
quantitative gate have ALREADY been computed and have ALREADY passed. Your only job is to sanity-check \
the situation for context the quantitative engine cannot see (e.g. an obviously degenerate or \
self-contradictory feature snapshot, or a market_state that looks like a data error) and return a verdict.

Respond with ONLY a single JSON object, no other text, no markdown fences, matching exactly:
{"decision": "APPROVED" or "REJECTED", "reasoning": "<one or two sentences>"}
"""


@dataclass
class ClaudeDecisionResult:
    decision: Decision
    reasoning: str
    latency_ms: float
    raw_response: dict | None = field(default=None, repr=False)


def _post_sync(api_key: str, model: str, audit_trail_summary: dict, timeout: float) -> dict:
    payload = {
        "model": model,
        "max_tokens": 300,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": json.dumps(audit_trail_summary)}],
    }
    req = urllib.request.Request(
        ANTHROPIC_MESSAGES_URL,
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": ANTHROPIC_VERSION,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _summarize_for_claude(audit_trail: dict) -> dict:
    """Never send raw internal config or unrelated system state — only
    the fields relevant to a context sanity check (§36: Claude is not
    the quant engine, so it doesn't need the full feature dump either)."""
    return {
        "symbol": audit_trail.get("symbol"),
        "exchange": audit_trail.get("exchange"),
        "quant_score": audit_trail.get("market_state", {}).get("quant_score"),
        "entry_price": audit_trail.get("market_state", {}).get("entry_price"),
        "breakout_level": audit_trail.get("market_state", {}).get("breakout_level"),
        "absorption": audit_trail.get("market_state", {}).get("absorption"),
        "stop_loss": audit_trail.get("risk_state", {}).get("stop_loss"),
        "take_profit": audit_trail.get("risk_state", {}).get("take_profit"),
        "position_size_usd": audit_trail.get("risk_state", {}).get("final_position_size_usd"),
        "friction_coverage": audit_trail.get("friction_state", {}).get("real_friction_coverage"),
        "feature_snapshot": audit_trail.get("feature_snapshot", {}),
    }


class ClaudeContextAuditor:
    def __init__(self, api_key: str | None, model: str = DEFAULT_MODEL, cooldown_seconds: int = 300, timeout: float = 15.0) -> None:
        self._api_key = api_key
        self._model = model
        self._cooldown_seconds = cooldown_seconds
        self._timeout = timeout
        self._last_call_at: dict[str, float] = {}

    def should_call(self, exchange: str, symbol: str, now: float | None = None) -> bool:
        now = now if now is not None else time.monotonic()
        key = f"{exchange}:{symbol}"
        last = self._last_call_at.get(key)
        if last is None:
            return True
        return (now - last) >= self._cooldown_seconds

    def _record_call(self, exchange: str, symbol: str, now: float | None = None) -> None:
        now = now if now is not None else time.monotonic()
        self._last_call_at[f"{exchange}:{symbol}"] = now

    async def evaluate(self, audit_trail: dict) -> ClaudeDecisionResult:
        import asyncio

        exchange = audit_trail.get("exchange", "")
        symbol = audit_trail.get("symbol", "")
        self._record_call(exchange, symbol)

        if not self._api_key:
            logger.info("Claude Auditor not configured (no API key) — treating as NO TRADE (rejected_failure)")
            return ClaudeDecisionResult(decision="rejected_failure", reasoning="Claude Auditor not configured (no API key).", latency_ms=0.0)

        start = time.monotonic()
        summary = _summarize_for_claude(audit_trail)
        try:
            response = await asyncio.to_thread(_post_sync, self._api_key, self._model, summary, self._timeout)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            latency_ms = (time.monotonic() - start) * 1000
            logger.warning("Claude Auditor API call failed: %s", exc)
            return ClaudeDecisionResult(decision="rejected_failure", reasoning=f"API call failed: {exc}", latency_ms=latency_ms)

        latency_ms = (time.monotonic() - start) * 1000
        parsed = self._parse_response(response)
        if parsed is None:
            return ClaudeDecisionResult(decision="rejected_failure", reasoning="Invalid or unparseable Claude response.",
                                         latency_ms=latency_ms, raw_response=response)

        decision_text, reasoning_text = parsed
        decision: Decision = "approved" if decision_text == "APPROVED" else "rejected_judgment"
        return ClaudeDecisionResult(decision=decision, reasoning=reasoning_text, latency_ms=latency_ms, raw_response=response)

    @staticmethod
    def _parse_response(response: dict) -> tuple[str, str] | None:
        try:
            content_blocks = response["content"]
            text = "".join(b["text"] for b in content_blocks if b.get("type") == "text")
            parsed = json.loads(text.strip())
            decision = parsed["decision"]
            reasoning = parsed.get("reasoning", "")
            if decision not in ("APPROVED", "REJECTED"):
                return None
            return decision, reasoning
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None
