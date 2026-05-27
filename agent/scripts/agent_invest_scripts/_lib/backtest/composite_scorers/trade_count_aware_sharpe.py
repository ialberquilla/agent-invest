from __future__ import annotations

from dataclasses import dataclass

from agent_invest_scripts._lib.backtest.result import BacktestMetrics

from .base import FactorWeight, finite_or_zero, normalize_weights


@dataclass(frozen=True, slots=True)
class TradeCountAwareSharpeScorer:
    ID: str = "trade_count_aware_sharpe"

    def score(
        self,
        metrics: BacktestMetrics,
        primary_factors: list[FactorWeight],
    ) -> float:
        normalize_weights(primary_factors)
        n_trades = getattr(metrics, "n_trades", None)
        if not n_trades:
            return 0.0
        trade_factor = min(1.0, float(n_trades) / 30.0)
        return finite_or_zero(float(metrics.sharpe) * trade_factor)


trade_count_aware_sharpe = TradeCountAwareSharpeScorer()
