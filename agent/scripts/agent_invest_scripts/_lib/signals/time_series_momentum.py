from __future__ import annotations

import polars as pl

from agent_invest_scripts._lib.signals.cross_sectional_momentum import (
    trailing_return_scores,
)


def positive_trend_signal(
    prices_wide: pl.DataFrame, lookback_days: int, skip_days: int = 0
) -> pl.DataFrame:
    scores = trailing_return_scores(
        prices_wide, lookback_days=lookback_days, skip_days=skip_days
    )
    expressions = [
        pl.when(pl.col(column) > 0).then(1).otherwise(0).alias(column)
        for column in scores.columns
        if column != "date"
    ]
    return scores.select("date", *expressions)
