"""Backtest engine modules shared by agent-facing scripts."""

from __future__ import annotations

from typing import Any

__all__ = [
    "AllocationMetrics",
    "BacktestMetrics",
    "BacktestResult",
    "DrawdownEpisode",
    "EngineBacktestResult",
    "Rebalance",
    "RobustnessSignals",
    "TacticalMetrics",
    "TradingCostModel",
    "Trade",
    "calculate_summary_metrics",
    "count_rebalance_swaps",
    "equal_weight_portfolio",
    "from_dict",
    "portfolio_turnover",
    "prune_small_weights",
    "run_backtest",
    "run_cross_sectional_momentum_backtest",
    "to_dict",
]


def __getattr__(name: str) -> Any:
    if name == "run_cross_sectional_momentum_backtest":
        from agent_invest_scripts._lib.strategies import (
            run_cross_sectional_momentum_backtest,
        )

        return run_cross_sectional_momentum_backtest
    if name in {"TradingCostModel", "count_rebalance_swaps", "portfolio_turnover"}:
        from . import costs

        return getattr(costs, name)
    if name in {"EngineBacktestResult", "run_backtest"}:
        from . import engine

        if name == "EngineBacktestResult":
            return engine.BacktestResult
        return engine.run_backtest
    if name == "calculate_summary_metrics":
        from .metrics import calculate_summary_metrics

        return calculate_summary_metrics
    if name in {"equal_weight_portfolio", "prune_small_weights"}:
        from . import portfolio

        return getattr(portfolio, name)
    if name in {
        "AllocationMetrics",
        "BacktestMetrics",
        "BacktestResult",
        "DrawdownEpisode",
        "Rebalance",
        "RobustnessSignals",
        "TacticalMetrics",
        "Trade",
        "from_dict",
        "to_dict",
    }:
        from . import result

        return getattr(result, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
