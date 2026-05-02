"""Backtest engine modules shared by agent-facing scripts."""

from agent_invest_scripts._lib.strategies import run_cross_sectional_momentum_backtest

from .costs import TradingCostModel, count_rebalance_swaps, portfolio_turnover
from .engine import BacktestResult, run_backtest
from .metrics import calculate_summary_metrics
from .portfolio import equal_weight_portfolio, prune_small_weights

__all__ = [
    "BacktestResult",
    "TradingCostModel",
    "calculate_summary_metrics",
    "count_rebalance_swaps",
    "equal_weight_portfolio",
    "portfolio_turnover",
    "prune_small_weights",
    "run_backtest",
    "run_cross_sectional_momentum_backtest",
]
