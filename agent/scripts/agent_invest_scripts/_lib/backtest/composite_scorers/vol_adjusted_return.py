from __future__ import annotations

from dataclasses import dataclass

from agent_invest_scripts._lib.backtest.result import BacktestMetrics

from .base import FactorWeight, finite_or_zero, normalize_weights


@dataclass(frozen=True, slots=True)
class VolAdjustedReturnScorer:
    ID: str = "vol_adjusted_return"

    def score(
        self,
        metrics: BacktestMetrics,
        primary_factors: list[FactorWeight],
    ) -> float:
        normalize_weights(primary_factors)
        return finite_or_zero(
            float(metrics.cagr) / max(float(metrics.volatility), 0.01)
        )


vol_adjusted_return = VolAdjustedReturnScorer()
