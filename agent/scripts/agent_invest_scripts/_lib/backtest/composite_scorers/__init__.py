from __future__ import annotations

from .base import CompositeScorer, FactorWeight
from .long_horizon import long_horizon_composite
from .trade_count_aware_sharpe import trade_count_aware_sharpe
from .vol_adjusted_return import vol_adjusted_return

SCORERS: dict[str, CompositeScorer] = {
    scorer.ID: scorer
    for scorer in (
        long_horizon_composite,
        trade_count_aware_sharpe,
        vol_adjusted_return,
    )
}


__all__ = [
    "CompositeScorer",
    "FactorWeight",
    "SCORERS",
    "long_horizon_composite",
    "trade_count_aware_sharpe",
    "vol_adjusted_return",
]
