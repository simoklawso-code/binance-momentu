# Crypto Momentum & Scalping Hunter

Built against `CRYPTO_MOMENTUM_SCALPING_MASTER_SPEC_V2.1.md`. **All five
phases (§61) have been implemented**: ingestion, feature/signal
engines, storage/API, and backtest/Claude-auditor. Read "What's solid
vs. what's simplified" below before trusting any of this with real
money — this is a serious, tested implementation, but it has only ever
run against local simulations, never real Binance/Bybit markets.

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp config.example.yaml config.yaml   # edit the symbol universe / limits
cp .env.example .env                 # add ANTHROPIC_API_KEY to enable the Claude Auditor

python main.py
# Dashboard: http://127.0.0.1:8080
```

Ctrl+C stops everything cleanly. Without `ANTHROPIC_API_KEY` set, the
system still ingests data, screens candidates, and evaluates signals —
it just never opens a paper position past `ENTRY_PENDING` (fail-safe,
§48). This is intentional, not a bug: Claude is a veto gate, and no
veto gate means no trade.

## What's implemented, by phase

- **Phase 1 — Ingestion**: sharded Binance/Bybit WebSocket adapters
  (config-driven shard count, independent per-shard reconnect/backoff/
  soft-reconnect), typed Domain Events, priority Ring Buffer (P3→P2→
  emergency-P1 drop order, P0 never silently dropped), OI REST clients.
- **Phase 2 — Feature/Detection**: RVOL, Trade Acceleration, ATR,
  Price Impact/PDE, Absorption Gate, Momentum, Breakout inputs: all as
  pure, unit-tested functions that return `None` (never a fabricated
  0) on insufficient data. Dynamic Funnel + Candidate Stream Manager
  (diff-based, no subscription churn) + a dedicated candidate-only
  bookTicker/orderbook stream (§17).
- **Phase 3 — Signal Engine**: full Quant Score (§33A, all 7 weighted
  components + the pre-entry Friction_Score reference chain), Breakout
  Entry Trigger (§33B), Stop Loss/Take Profit (§33C/§33D, unit-
  consistent), Friction/Slippage Model (§24-25), the AUTHORITATIVE
  Friction Coverage Gate (§26, evaluated separately from the reference
  one, exactly as locked), Position Sizing (§32), Risk Engine (§30-31:
  max positions, portfolio heat, daily loss freeze), Paper Execution
  (TP/SL resolution, no artificial SL_FIRST in live mode).
- **Phase 4 — Storage/API**: SQLite (WAL, queued single-writer thread),
  a lightweight JSON API + single-page dashboard (`app/web/dashboard.html`),
  a notification bus (polling-based — see limitations).
- **Phase 5 — Backtest/Claude/Experiments**: a backtest engine sharing
  the exact same Feature/Signal/Risk Engine code as live trading
  (§58 parity), with a pessimistic breakout-fill model and the LOCKED
  SL_FIRST intrabar rule (§27-28); a real Claude Context Auditor
  (§36-38, calls the actual Anthropic API when a key is present, with
  `rejected_failure` vs `rejected_judgment` kept statistically
  separate per §37); an experiment tracker (§59-60) that only records,
  never auto-promotes a config change.

## What's solid vs. what's simplified (read this before trusting it)

**Solid, thoroughly tested (112 automated tests, all passing):**
every formula in §21-§33D (Net Taker Delta, RVOL, Absorption Gate,
Quant Score normalization incl. the Breakout-Floor-locked-to-ATR-
multiplier rule, SL/TP, Friction Coverage unit consistency — a real
percent-vs-fraction bug was caught and fixed here during testing),
the Ring Buffer's priority/overflow behavior, per-shard reconnect
isolation, the Risk Engine's daily-freeze/portfolio-heat math, the
Claude Auditor's fail-safe-on-failure behavior, and the full signal
pipeline wiring (verified via a local end-to-end simulation — see
below — that took a synthetic pump all the way from raw WebSocket
bytes to an opened AND closed paper position, correctly recorded in
SQLite).

**Simplified or documented-as-limitation, not silently faked:**
- **No real Binance/Bybit connection ever, from this environment**
  (network-sandboxed — see "Testing Note"). Everything above was
  proven against local fake servers speaking the real protocol, not
  the real exchanges.
- **Walk-Forward Cross Validation and 1,000-run Monte Carlo Simulation
  (§49) are NOT implemented** — the backtest engine produces the
  single-pass metrics (win rate, expectancy, profit factor, max
  drawdown) as a foundation for those, but not the statistical
  machinery itself.
- **Browser push notifications (§40) are a polling API, not real OS
  push** — no VAPID/service-worker infrastructure. The dashboard polls
  `/api/notifications` every 3s instead.
- **Bybit's public kline stream has no taker buy/sell split** — Net
  Taker Delta is architecturally `None` for Bybit-sourced candidates,
  not approximated.
- **`taker_fee_percent`, `min_position_size_usd`, `max_position_size_usd`**
  were added to config because the LOCKED Friction/Position-Sizing
  formulas need them but §51's explicit list doesn't enumerate them —
  called out in `app/config/settings.py`, not silently invented deep
  in the code.
- **No historical-data fetcher** — the backtest engine takes OHLCV
  bars you supply (e.g. loaded from your own CSV/exchange export);
  it doesn't download history itself (no network path to do so here).
- **Bybit OI endpoint params (`intervalTime`) and
  `BYBIT_MAX_ARGS_PER_SUBSCRIBE_MSG`** are reasonable defaults flagged
  for re-verification against live Bybit V5 docs (§19's explicit
  "verify real docs, don't assume" discipline) — never blindly trusted.

## Real verification via GitHub Actions (free, no local setup needed)

This repo includes `.github/workflows/verify-real-exchanges.yml`. Once
you push/upload this project to a GitHub repository, GitHub
automatically runs `scripts/real_connectivity_check.py` on its own
servers — which DO have real internet access to Binance and Bybit,
unlike the sandbox this project was built in. No payment, no local
Python install, no terminal needed on your side.

**What it does:** connects to the REAL `wss://fstream.binance.com` and
`wss://stream.bybit.com`, ingests real live market data for 3 minutes,
then prints a clear PASS/FAIL summary and turns the GitHub Actions run
green or red accordingly. It also runs the full 112-test suite.

**How to see the result:** open your repository on github.com → the
**"Actions"** tab → click the latest run → open the
"Run REAL Binance + Bybit connectivity test" step to read the log.

## Testing Note — network sandbox (this build environment)

This project was built inside a sandbox with **no outbound access to
`binance.com` / `bybit.com`** (confirmed with a direct request: the
egress proxy returns `403 host_not_allowed` — a sandbox restriction,
not the exchanges refusing the connection). Two layers of testing
compensate for this, and neither is a substitute for the real thing:

1. **112 unit/integration tests** (`pytest -q`) mock the WebSocket
   transport and the Anthropic API — fast, precise, cover every
   formula and edge case individually.
2. **`scripts/run_simulated_live_test.py`** — local fake Binance/Bybit
   servers (`scripts/fake_exchange_servers.py`) speaking the exact
   real wire protocol, driving the REAL application code (not test
   doubles) end-to-end: ingestion → features → candidates → signals →
   risk → paper execution → storage → API. A Claude Auditor STUB
   (never a real, billed API call) stands in so a full trade can be
   observed opening and closing.

Latest simulated run (150s, 8 symbols, one scripted to "pump"):
**all 10 checks passed** — Binance survived a forced mid-test
disconnect (reconnected in ~1.3s, 0 impact on Bybit or other Binance
shards), 3,150+ Binance / 3,277+ Bybit messages processed with zero
buffer drops, a candidate was promoted, a Quant Score of ~90 was
reached, a full breakout→SL/TP→friction→risk gate chain cleared, a
paper position opened (ETHUSDT @ 280.11) and later closed on a real
stop-loss cross (exit 223.57, -$188.09), and every step was persisted
to SQLite. Full JSON: `simulated_live_test_result.json`. Re-run it:

```bash
python3 scripts/run_simulated_live_test.py 150 25   # 150s run, pump starts at t=25s
```

**What this proves:** the code path is wired correctly end-to-end and
the math doesn't crash or silently misbehave. **What it does NOT
prove:** that the system will find real opportunities on live markets,
handle a real 150-300 symbol universe's throughput, or match
Binance/Bybit's exact real-world message quirks. Run it against the
real exchanges yourself once you have network access:

```bash
python main.py
# watch http://127.0.0.1:8080 and the logs for several minutes; confirm
# both exchanges stay healthy, candidates get promoted from REAL market
# conditions, and (if ANTHROPIC_API_KEY is set) Claude Auditor calls
# succeed with real latency.
```

## Project layout

```
/app
  /core        clock/NTP interface, backoff policy, logging setup
  /domain      typed Domain Event models + shared enums (§8, §34)
  /config      Settings schema (§51) — all thresholds, Phase 3 constants included
  /exchanges   base.py interface; /binance and /bybit: sharded adapters,
               per-shard connections, normalization, OI REST clients
  /ingestion   priority classification (§9) + IngestionManager (wiring)
  /buffer      PriorityRingBuffer (§9-§11) + EventSink adapter
  /features    Feature Engine (§13-23), FeatureStore, BTC filter, OI poller
  /signals     Candidate Manager, Quant Score, Breakout, Friction,
               Exit Levels (SL/TP), Signal Engine, live pipeline orchestrator
  /risk        Position Sizing (§32), Risk Engine (§30-31)
  /execution   Paper Trading Engine (§29, §34)
  /storage     SQLite worker (§42)
  /ai          Claude Context Auditor (§36-38)
  /api         Lightweight JSON API server (§39)
  /web         dashboard.html
  /notifications  Notification bus (§35, §40)
  /backtest    Backtest Engine (§27-29, §49, §58)
  /experiments Experiment tracker (§59-60)
  /macro       reserved for the Macro Event Awareness module (§56) — not implemented
/tests         112 tests across every module above
/scripts       fake_exchange_servers.py, run_simulated_live_test.py (testing aids only)
main.py        single start command — wires every phase together
```

## Running tests

```bash
pytest -q
```

112 tests. Highlights beyond what Phase 1's own section already
covered: Feature Engine formula correctness (RVOL median baseline,
ATR, Trade Acceleration, Absorption Gate trigger/non-trigger,
Momentum Ratio, Breakout inputs); Quant Score computability rules
(missing component blocks the whole score, never defaults to 0;
Breakout-Floor-locked-to-ATR-multiplier; negative delta never scores
high); the Signal Engine's full LOCKED calculation order including the
authoritative-vs-reference Friction Coverage Gate distinction; Risk
Engine (max positions, portfolio heat, daily-loss freeze, no
automatic risk increase after a loss); Paper Execution TP/SL
resolution; Storage WAL mode + retention purge; Claude Auditor
cooldown + failure/judgment separation (mocked HTTP, never a real
billed call in tests); Backtest pessimistic fill + SL_FIRST; API
server endpoints; and the full pipeline integration (Stage 1 →
candidate → signal → Claude stub → paper position, and price-driven
exits).

## Known limitations (see "What's solid vs. what's simplified" above for the full list)

Quick index: no real exchange connection ever tested; no Walk-Forward/
Monte Carlo; notifications are polling, not real push; Bybit has no
taker split; no historical-data fetcher; a few implementation-level
config keys were added beyond §51's explicit list (documented at
their definition site).

## What to do next

1. Run `python main.py` from a machine with real network access and
   watch it against live Binance/Bybit data for at least a few
   minutes (see "Testing Note").
2. Set `ANTHROPIC_API_KEY` and confirm a real Claude Auditor call
   succeeds (cost is small — one call per qualifying signal, cooldown-
   limited).
3. Point `universe.static_symbols` at your real 150-300+ symbol list.
4. If you want statistically rigorous backtesting, that's where
   Walk-Forward/Monte Carlo would need to be added on top of the
   existing `BacktestEngine`/`ExperimentTracker` foundation.
