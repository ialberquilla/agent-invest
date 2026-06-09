"""Backtest result contract and shared cost model for agent-facing scripts.

The polars simulation engine was retired in favour of ``bt`` (see
``bt_templates/``); the only survivors here are the JSON result contract
(``result.py``) and the standalone trading-cost model (``costs.py``) that the
benchmarks still use.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "AllocationMetrics",
    "BacktestMetrics",
    "BacktestResult",
    "DrawdownEpisode",
    "Rebalance",
    "RobustnessSignals",
    "TacticalMetrics",
    "TradingCostModel",
    "Trade",
    "count_rebalance_swaps",
    "from_dict",
    "portfolio_turnover",
    "to_dict",
]


def __getattr__(name: str) -> Any:
    if name in {"TradingCostModel", "count_rebalance_swaps", "portfolio_turnover"}:
        from . import costs

        return getattr(costs, name)
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
