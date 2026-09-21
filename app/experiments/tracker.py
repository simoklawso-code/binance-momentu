"""
Experiment Tracking (§59-60).

Records a backtest run as a structured experiment: what changed, what
dataset it ran against, and its resulting metrics. This module NEVER
writes back into live `StrategyConfig` or `config.yaml` — §59 is
explicit that promotion is a human decision ("never auto-promote,
require validation before changing production configuration"). It
only records history for a human (or a later, explicitly separate
promotion tool) to review.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from app.backtest.engine import BacktestReport
from app.storage.db import Storage

STRATEGY_VERSION = "V2.1"  # bump manually when the LOCKED spec's strategy logic changes


@dataclass
class ExperimentRecord:
    experiment_id: str
    parent_strategy_version: str
    configuration_version: str
    parameter_changes: dict
    dataset: str
    date_range: str
    universe: str
    random_seed: int | None
    backtest_method: str
    metrics: dict
    observations: str
    status: str = "EXPERIMENT"  # BASELINE | EXPERIMENT | VALIDATED | REJECTED
    decision: str | None = None


def metrics_from_report(report: BacktestReport) -> dict:
    return {
        "trade_count": report.trade_count,
        "win_rate": report.win_rate,
        "expectancy_r": report.expectancy_r,
        "profit_factor": report.profit_factor,
        "max_drawdown_percent": report.max_drawdown_percent,
        "no_trade_count": report.no_trade_count,
        "rejected_count": report.rejected_count,
        # NOT implemented in this MVP backtest engine (documented, not
        # fabricated) — see app/backtest/engine.py module docstring.
        "walk_forward": None,
        "monte_carlo": None,
    }


def record_experiment(
    storage: Storage,
    report: BacktestReport,
    configuration_version: str,
    parameter_changes: dict,
    dataset: str,
    date_range: str,
    universe: str,
    backtest_method: str = "single_pass_ohlc",
    random_seed: int | None = None,
    observations: str = "",
    status: str = "EXPERIMENT",
) -> ExperimentRecord:
    record = ExperimentRecord(
        experiment_id=str(uuid.uuid4()),
        parent_strategy_version=STRATEGY_VERSION,
        configuration_version=configuration_version,
        parameter_changes=parameter_changes,
        dataset=dataset,
        date_range=date_range,
        universe=universe,
        random_seed=random_seed,
        backtest_method=backtest_method,
        metrics=metrics_from_report(report),
        observations=observations,
        status=status,
    )
    storage.enqueue_experiment(record.__dict__)
    return record
