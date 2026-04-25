from __future__ import annotations

import polars as pl


def minimum_history_mask(
    prices_wide: pl.DataFrame, min_history_days: int
) -> pl.DataFrame:
    expressions = []
    for column in prices_wide.columns:
        if column == "date":
            continue
        expression = (
            pl.col(column)
            .is_not_null()
            .cast(pl.Int64)
            .rolling_sum(window_size=min_history_days)
            .fill_null(0)
            .ge(min_history_days)
            .alias(column)
        )
        expressions.append(expression)

    return prices_wide.select("date", *expressions)


def apply_boolean_mask(
    values_wide: pl.DataFrame, mask_wide: pl.DataFrame
) -> pl.DataFrame:
    joined = values_wide.join(mask_wide, on="date", suffix="_eligible")
    expressions = ["date"]
    for column in values_wide.columns:
        if column == "date":
            continue
        expressions.append(
            pl.when(pl.col(f"{column}_eligible"))
            .then(pl.col(column))
            .otherwise(None)
            .alias(column)
        )
    return joined.select(expressions)
