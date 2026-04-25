from __future__ import annotations

import polars as pl


def btc_above_moving_average(
    prices_wide: pl.DataFrame, coin_id: str = "bitcoin", window_days: int = 200
) -> pl.DataFrame:
    if coin_id not in prices_wide.columns:
        raise KeyError(f"{coin_id} not found in price matrix")

    return prices_wide.select(
        "date",
        (pl.col(coin_id) > pl.col(coin_id).rolling_mean(window_size=window_days))
        .fill_null(False)
        .alias("regime_on"),
    )
