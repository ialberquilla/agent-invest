from __future__ import annotations

from dataclasses import dataclass

from agent_invest_scripts._lib.backtest.result import BacktestMetrics

from .base import FactorWeight, finite_or_zero, normalize_weights


@dataclass(frozen=True, slots=True)
class LongHorizonCompositeScorer:
    ID: str = "long_horizon_composite"

    def score(
        self,
        metrics: BacktestMetrics,
        primary_factors: list[FactorWeight],
    ) -> float:
        weights = normalize_weights(primary_factors)
        if not weights:
            weights = {
                "return_365d": 1.0 / 3.0,
                "recovery_rate": 1.0 / 3.0,
                "pct_horizon_windows_positive": 1.0 / 3.0,
            }

        score = 0.0
        for factor, weight in weights.items():
            score += weight * _factor_value(metrics, factor)
        return finite_or_zero(score)


def _factor_value(metrics: BacktestMetrics, factor: str) -> float:
    if factor == "return_365d":
        return float(getattr(metrics, "return_365d", metrics.cagr))
    if factor in {"recovery_rate", "pct_horizon_windows_positive"}:
        return float(getattr(metrics, factor, 0.0) or 0.0)
    return float(getattr(metrics, factor, 0.0) or 0.0)


long_horizon_composite = LongHorizonCompositeScorer()
