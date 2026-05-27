from __future__ import annotations

import math
from typing import Literal, Protocol, TypedDict

from agent_invest_scripts._lib.backtest.result import BacktestMetrics


class FactorWeight(TypedDict):
    factor: str
    direction: Literal["high", "low"]
    weight: float


class CompositeScorer(Protocol):
    ID: str

    def score(
        self,
        metrics: BacktestMetrics,
        primary_factors: list[FactorWeight],
    ) -> float:
        """Return a single scalar; higher is better."""
        ...


def normalize_weights(primary_factors: list[FactorWeight]) -> dict[str, float]:
    weights = {
        item["factor"]: max(float(item.get("weight", 0.0)), 0.0)
        for item in primary_factors
    }
    total = sum(weights.values())
    if total <= 0.0:
        return {}
    return {factor: weight / total for factor, weight in weights.items()}


def finite_or_zero(value: float) -> float:
    return value if math.isfinite(value) else 0.0
