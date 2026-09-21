"""
Configuration system (§51, §65 step 2).

Every important numerical threshold lives here — never hardcoded deeper
in the codebase. Per §65 step 2, the Strategy A / Quant Score constants
(used only from Phase 3 onward) are defined now too, so Phase 3 never
requires a config schema migration.

Loaded from config.yaml (or config.example.yaml as a fallback) with
environment variables able to override individual keys for
secrets/deployment overrides (never commit real secrets — see .env.example).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Phase 1 — ingestion / buffer / adapter configuration
# ---------------------------------------------------------------------------

class BinanceConfig(BaseModel):
    ws_base_url: str = "wss://fstream.binance.com"
    rest_base_url: str = "https://fapi.binance.com"
    # Sharding (§6) — configuration-driven, never hardcoded to "one socket".
    max_streams_per_connection: int = 200
    max_connections: int = 4
    soft_reconnect_interval_hours: float = 20.0  # before Binance's ~24h forced disconnect
    reconnect_backoff_base_seconds: float = 1.0
    reconnect_backoff_max_seconds: float = 60.0
    reconnect_backoff_jitter_seconds: float = 1.0
    ping_interval_seconds: float = 180.0
    connection_health_timeout_seconds: float = 30.0


class BybitConfig(BaseModel):
    ws_base_url: str = "wss://stream.bybit.com/v5/public/linear"
    rest_base_url: str = "https://api.bybit.com"
    max_streams_per_connection: int = 200
    max_connections: int = 2
    soft_reconnect_interval_hours: float = 20.0
    reconnect_backoff_base_seconds: float = 1.0
    reconnect_backoff_max_seconds: float = 60.0
    reconnect_backoff_jitter_seconds: float = 1.0
    ping_interval_seconds: float = 20.0
    connection_health_timeout_seconds: float = 30.0


class BufferConfig(BaseModel):
    capacity: int = 10_000
    pressure_high_watermark: float = 0.80  # §10: activate LOSSY_MODE above this
    p0_overload_watermark: float = 0.98  # §10 step: DATA_OVERLOAD if P0 alone threatens overflow
    recovery_watermark: float = 0.60  # utilization must drop below this to leave LOSSY_MODE


class UniverseConfig(BaseModel):
    min_symbols: int = 150
    max_symbols: int = 300
    # Phase 1 default universe — replace with the full discovered
    # USDT-perpetual list at startup (see app/core/universe.py hook).
    static_symbols: list[str] = Field(default_factory=lambda: [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    ])


class RetentionConfig(BaseModel):
    raw_event_retention_days: int = 7
    feature_snapshot_retention_days: int = 30
    signal_audit_retention_days: int = 180


class DatabaseConfig(BaseModel):
    path: str = "./data/crypto_hunter.sqlite3"
    journal_mode: str = "WAL"
    synchronous: str = "NORMAL"
    busy_timeout_ms: int = 5000
    write_queue_max_size: int = 5000


# ---------------------------------------------------------------------------
# Phase 2/3 constants — defined now per §65 step 2, not used until then
# ---------------------------------------------------------------------------

class QuantScoreWeights(BaseModel):
    rvol: float = 0.20
    acceleration: float = 0.15
    delta: float = 0.15
    momentum: float = 0.15
    breakout: float = 0.15
    liquidity: float = 0.10
    friction: float = 0.10


class QuantScoreThresholds(BaseModel):
    watch: float = 60.0
    candidate: float = 70.0
    pump_detected: float = 80.0
    high_conviction: float = 90.0


class StrategyConfig(BaseModel):
    rvol_threshold: float = 2.5
    acceleration_threshold: float = 2.0
    min_24h_volume_usd: float = 15_000_000.0
    friction_coverage_ratio: float = 2.5
    risk_per_trade_percent: float = 0.5
    daily_loss_limit_percent: float = 3.0
    max_open_positions: int = 3
    max_portfolio_heat: float = 1.5
    max_spread_percent: float = 0.08
    base_slippage_percent: float = 0.02
    slippage_gamma: float = 0.10
    candidate_min: int = 5
    candidate_max: int = 15
    claude_score_threshold: float = 80.0
    claude_cooldown_seconds: int = 300
    binance_oi_interval_seconds: int = 60
    bybit_oi_interval_seconds: int = 60
    btc_volatility_pause_threshold_percent: float = 1.2
    venue_dedup_score_epsilon: float = 2.0
    cross_exchange_dedup_window_seconds: int = 10

    rvol_score_floor: float = 1.0
    rvol_score_ceiling: float = 5.0
    acceleration_score_floor: float = 1.0
    acceleration_score_ceiling: float = 4.0
    delta_score_floor: float = 0.0
    delta_score_ceiling: float = 0.60
    momentum_score_floor: float = 0.5
    momentum_score_ceiling: float = 2.0
    momentum_window_minutes: int = 3
    breakout_atr_multiplier: float = 0.10  # Breakout_Floor derives from this (§33A.1.5.1)
    breakout_score_ceiling: float = 0.50
    liquidity_score_ceiling_usd: float = 100_000_000.0
    friction_score_floor: float = 1.0
    friction_score_ceiling: float = 4.0
    reference_order_size_usd: float = 1000.0
    sl_atr_multiplier: float = 1.20
    take_profit_rr: float = 2.0

    # Not explicitly enumerated in §51's list, but required by formulas
    # that ARE locked (§24 Friction Model needs a taker fee; §32 step 6
    # needs min/max position constraints). Documented here as necessary
    # implementation-level config, values are reasonable USDT-perpetual
    # defaults — adjust to your actual exchange fee tier.
    taker_fee_percent: float = 0.05  # Binance/Bybit USDT-perp taker fee is commonly ~0.05%
    min_position_size_usd: float = 20.0
    max_position_size_usd: float = 5000.0
    paper_starting_equity_usd: float = 10_000.0

    quant_score_weights: QuantScoreWeights = Field(default_factory=QuantScoreWeights)
    quant_score_thresholds: QuantScoreThresholds = Field(default_factory=QuantScoreThresholds)


class MacroConfig(BaseModel):
    economic_calendar_enabled: bool = True
    macro_high_impact_enabled: bool = True
    macro_event_pre_window_minutes: int = 30
    macro_event_post_window_minutes: int = 30
    macro_entry_restriction_enabled: bool = True
    macro_required_for_entry: bool = False
    macro_data_max_age_seconds: int = 900


class MarketIntelligenceConfig(BaseModel):
    """Service 3 (§66) — not active until Phase 4."""

    enabled: bool = False
    refresh_mode: str = "hybrid"
    narrative_cooldown_seconds: int = 300
    narrative_cache_ttl_seconds: int = 300
    regime_thresholds: dict = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Root settings object
# ---------------------------------------------------------------------------

class Settings(BaseModel):
    environment: str = "development"
    log_level: str = "INFO"
    metrics_log_interval_seconds: float = 7.0
    stage1_cycle_seconds: float = 5.0
    signal_cycle_seconds: float = 3.0

    binance: BinanceConfig = Field(default_factory=BinanceConfig)
    bybit: BybitConfig = Field(default_factory=BybitConfig)
    buffer: BufferConfig = Field(default_factory=BufferConfig)
    universe: UniverseConfig = Field(default_factory=UniverseConfig)
    retention: RetentionConfig = Field(default_factory=RetentionConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    strategy: StrategyConfig = Field(default_factory=StrategyConfig)
    macro: MacroConfig = Field(default_factory=MacroConfig)
    market_intelligence: MarketIntelligenceConfig = Field(default_factory=MarketIntelligenceConfig)


_ENV_OVERRIDE_PREFIX = "CMH_"


def _apply_env_overrides(data: dict) -> dict:
    """Allow flat env-var overrides like CMH_BINANCE__MAX_CONNECTIONS=6
    without requiring a secrets manager for local/dev use. Never used to
    inject API keys into this file — those stay in .env only."""
    for key, value in os.environ.items():
        if not key.startswith(_ENV_OVERRIDE_PREFIX):
            continue
        path = key[len(_ENV_OVERRIDE_PREFIX):].lower().split("__")
        node = data
        for part in path[:-1]:
            node = node.setdefault(part, {})
        leaf = path[-1]
        # best-effort type coercion
        if value.lower() in ("true", "false"):
            node[leaf] = value.lower() == "true"
        else:
            try:
                node[leaf] = int(value)
            except ValueError:
                try:
                    node[leaf] = float(value)
                except ValueError:
                    node[leaf] = value
    return data


def load_settings(config_path: Optional[str] = None) -> Settings:
    """Load Settings from a YAML file, falling back to config.example.yaml,
    falling back to pure defaults if neither exists (fresh clone)."""
    candidates = []
    if config_path:
        candidates.append(Path(config_path))
    candidates += [Path("config.yaml"), Path("config.example.yaml")]

    data: dict = {}
    for candidate in candidates:
        if candidate.exists():
            with candidate.open("r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f) or {}
            data = loaded
            break

    data = _apply_env_overrides(data)
    return Settings(**data)
