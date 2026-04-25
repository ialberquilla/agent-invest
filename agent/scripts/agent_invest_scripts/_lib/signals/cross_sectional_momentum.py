from __future__ import annotations

from collections.abc import Mapping

import polars as pl


def trailing_return_scores(
    prices_wide: pl.DataFrame, lookback_days: int, skip_days: int = 0
) -> pl.DataFrame:
    value_columns = [column for column in prices_wide.columns if column != "date"]
    expressions = []

    for column in value_columns:
        if skip_days:
            expression = (
                pl.col(column).shift(skip_days)
                / pl.col(column).shift(skip_days + lookback_days)
                - 1.0
            ).alias(column)
        else:
            expression = (
                pl.col(column) / pl.col(column).shift(lookback_days) - 1.0
            ).alias(column)
        expressions.append(expression)

    return prices_wide.select("date", *expressions)


def select_top_k(
    scores_row: Mapping[str, object], top_k: int, positive_only: bool = True
) -> list[tuple[str, float]]:
    ranked: list[tuple[str, float]] = []
    for coin_id, value in scores_row.items():
        if coin_id == "date" or value is None:
            continue
        score = float(value)
        if positive_only and score <= 0:
            continue
        ranked.append((coin_id, score))

    ranked.sort(key=lambda item: item[1], reverse=True)
    return ranked[:top_k]
