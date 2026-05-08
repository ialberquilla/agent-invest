"""Compute the asset-universe screening features as of an arbitrary date.

This module mirrors the column shape of the ``agent_asset_universe_features``
materialized view but anchors every per-coin window to a caller-supplied
``as_of`` date instead of the current ``last_price_date``.  That fixes the
forward-peeking problem where the agent screened on present-day momentum
before backtesting over the same window.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import polars as pl

from .data import daily_prices
from .db import read_sql_frame

_LOOKBACKS_DAYS: tuple[int, ...] = (30, 90, 180, 365)
_VOLATILITY_WINDOWS_DAYS: tuple[int, ...] = (30, 90, 180)
_DRAWDOWN_WINDOW_DAYS = 180
_SHARPE_WINDOW_DAYS = 180
_SMA_WINDOW_DAYS = 200
_TRADING_DAYS_PER_YEAR = 365

_OUTPUT_COLUMNS: tuple[str, ...] = (
    "asset_id",
    "coin_id",
    "symbol",
    "name",
    "market_cap_rank",
    "market_cap",
    "latest_price",
    "first_price_date",
    "last_price_date",
    "data_days_365d",
    "return_30d",
    "return_90d",
    "return_180d",
    "return_365d",
    "volatility_30d",
    "volatility_90d",
    "volatility_180d",
    "max_drawdown_180d",
    "sharpe_180d",
    "price_above_sma_200d",
)


def compute_universe_features_as_of(
    *,
    as_of: date,
    prices: pl.DataFrame | None = None,
    universe: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Return universe features anchored at ``as_of`` with the view's column shape.

    Parameters
    ----------
    as_of:
        Anchor date.  Only price observations on or before this date are used,
        and per-coin lookback / volatility / drawdown windows are taken
        relative to each coin's most recent observation on or before this date.
    prices:
        Optional polars frame with columns ``date``, ``coin_id``, ``price``.
        When omitted, prices are loaded from Postgres via ``daily_prices()``.
    universe:
        Optional pandas frame with the metadata columns ``asset_id``,
        ``coin_id``, ``symbol``, ``name``, ``market_cap_rank``, ``market_cap``.
        When omitted, the metadata is loaded from ``agent_asset_universe``.

    Returns
    -------
    pandas.DataFrame
        Same columns as ``agent_asset_universe_features``, ordered by
        ``market_cap_rank`` (NULLs last) then ``coin_id``.
    """
    prices_frame = _normalize_prices(prices)
    universe_frame = _normalize_universe(universe)
    sliced = prices_frame.filter(pl.col("date") <= as_of)
    if sliced.is_empty():
        empty = pd.DataFrame(columns=_OUTPUT_COLUMNS)
        return _attach_universe_metadata(empty, universe_frame)

    features = _compute_per_coin_features(sliced)
    universe_coin_ids = set(universe_frame["coin_id"].dropna().tolist())
    features = features.filter(pl.col("coin_id").is_in(list(universe_coin_ids)))
    pdf = features.to_pandas()
    return _attach_universe_metadata(pdf, universe_frame)


def _compute_per_coin_features(prices: pl.DataFrame) -> pl.DataFrame:
    sorted_prices = prices.sort(["coin_id", "date"])
    daily_returns = sorted_prices.with_columns(
        (pl.col("price") / pl.col("price").shift(1).over("coin_id") - 1.0).alias(
            "daily_return"
        )
    )
    last_dates = (
        sorted_prices.group_by("coin_id")
        .agg(pl.col("date").max().alias("last_price_date"))
    )
    daily_returns = daily_returns.join(last_dates, on="coin_id", how="inner")
    daily_returns = daily_returns.with_columns(
        (pl.col("last_price_date") - pl.col("date")).dt.total_days().alias(
            "days_from_last"
        )
    )

    base = daily_returns.group_by("coin_id").agg(
        [
            pl.col("date").min().alias("first_price_date"),
            pl.col("last_price_date").first(),
            pl.col("price").sort_by("date").last().alias("latest_price"),
            pl.col("date")
            .filter(pl.col("days_from_last") <= 365)
            .count()
            .alias("data_days_365d"),
            pl.col("price")
            .filter(pl.col("days_from_last") <= _SMA_WINDOW_DAYS)
            .mean()
            .alias("sma_200d"),
            pl.col("daily_return")
            .filter(pl.col("days_from_last") < _SHARPE_WINDOW_DAYS)
            .mean()
            .alias("mean_daily_return_180d"),
            *(
                pl.col("daily_return")
                .filter(pl.col("days_from_last") < window)
                .std()
                .alias(f"std_daily_return_{window}d")
                for window in _VOLATILITY_WINDOWS_DAYS
            ),
        ]
    )

    base = base.with_columns(
        [
            *(
                (pl.col(f"std_daily_return_{window}d") * pl.lit(_TRADING_DAYS_PER_YEAR) ** 0.5)
                .alias(f"volatility_{window}d")
                for window in _VOLATILITY_WINDOWS_DAYS
            ),
        ]
    )
    base = base.with_columns(
        [
            (pl.col("mean_daily_return_180d") * pl.lit(_TRADING_DAYS_PER_YEAR)).alias(
                "annualized_return_180d"
            ),
            pl.when(pl.col("sma_200d").is_not_null())
            .then(pl.col("latest_price") > pl.col("sma_200d"))
            .otherwise(None)
            .alias("price_above_sma_200d"),
        ]
    )
    base = base.with_columns(
        pl.when(
            pl.col("volatility_180d").is_not_null() & (pl.col("volatility_180d") != 0)
        )
        .then(pl.col("annualized_return_180d") / pl.col("volatility_180d"))
        .otherwise(None)
        .alias("sharpe_180d")
    )

    base = _attach_lookback_returns(base, prices=sorted_prices)
    base = _attach_max_drawdown(base, daily_returns=daily_returns)

    return base.select(
        [
            "coin_id",
            "latest_price",
            "first_price_date",
            "last_price_date",
            "data_days_365d",
            *(f"return_{window}d" for window in _LOOKBACKS_DAYS),
            *(f"volatility_{window}d" for window in _VOLATILITY_WINDOWS_DAYS),
            "max_drawdown_180d",
            "sharpe_180d",
            "price_above_sma_200d",
        ]
    )


def _attach_lookback_returns(
    base: pl.DataFrame, *, prices: pl.DataFrame
) -> pl.DataFrame:
    prices_for_join = prices.select("coin_id", "date", "price").sort("date")
    enriched = base
    for window in _LOOKBACKS_DAYS:
        target_column = f"target_{window}d"
        price_column = f"price_{window}d"
        targets = enriched.select(
            "coin_id",
            (
                pl.col("last_price_date") - pl.duration(days=window)
            ).alias(target_column),
        ).sort(target_column)
        joined = targets.join_asof(
            prices_for_join,
            left_on=target_column,
            right_on="date",
            by="coin_id",
            strategy="backward",
        ).select("coin_id", pl.col("price").alias(price_column))
        enriched = enriched.join(joined, on="coin_id", how="left").with_columns(
            pl.when(pl.col(price_column).is_null() | (pl.col(price_column) == 0))
            .then(None)
            .otherwise(pl.col("latest_price") / pl.col(price_column) - 1.0)
            .alias(f"return_{window}d")
        ).drop(price_column)
    return enriched


def _attach_max_drawdown(
    base: pl.DataFrame, *, daily_returns: pl.DataFrame
) -> pl.DataFrame:
    window = daily_returns.filter(
        pl.col("days_from_last") < _DRAWDOWN_WINDOW_DAYS
    ).sort(["coin_id", "date"])
    if window.is_empty():
        return base.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("max_drawdown_180d")
        )
    window = window.with_columns(
        (pl.col("price") / pl.col("price").cum_max().over("coin_id") - 1.0).alias(
            "drawdown"
        )
    )
    drawdowns = window.group_by("coin_id").agg(
        pl.col("drawdown").min().alias("max_drawdown_180d")
    )
    return base.join(drawdowns, on="coin_id", how="left")


def _normalize_prices(prices: pl.DataFrame | None) -> pl.DataFrame:
    if prices is None:
        prices = pl.from_pandas(daily_prices())
    missing = {"date", "coin_id", "price"} - set(prices.columns)
    if missing:
        formatted = ", ".join(sorted(missing))
        raise ValueError(f"prices is missing required column(s): {formatted}")
    return (
        prices.select("date", "coin_id", "price")
        .with_columns(
            pl.col("date").cast(pl.Date),
            pl.col("coin_id").cast(pl.String),
            pl.col("price").cast(pl.Float64),
        )
        .filter(pl.col("price").is_not_null())
    )


def _normalize_universe(universe: pd.DataFrame | None) -> pd.DataFrame:
    if universe is None:
        universe = _load_universe_metadata()
    required = {"asset_id", "coin_id", "symbol", "name", "market_cap_rank", "market_cap"}
    missing = required - set(universe.columns)
    if missing:
        formatted = ", ".join(sorted(missing))
        raise ValueError(f"universe is missing required column(s): {formatted}")
    return universe[
        ["asset_id", "coin_id", "symbol", "name", "market_cap_rank", "market_cap"]
    ].copy()


def _load_universe_metadata() -> pd.DataFrame:
    return read_sql_frame(
        """
        SELECT
          "asset_id",
          "coin_id",
          "symbol",
          "name",
          "market_cap_rank",
          "market_cap"
        FROM "agent_asset_universe"
        ORDER BY "market_cap_rank" NULLS LAST, "coin_id"
        """
    )


def _attach_universe_metadata(
    features: pd.DataFrame, universe: pd.DataFrame
) -> pd.DataFrame:
    merged = universe.merge(features, on="coin_id", how="left")
    for column in _OUTPUT_COLUMNS:
        if column not in merged.columns:
            merged[column] = pd.NA
    merged = merged[list(_OUTPUT_COLUMNS)]
    return merged.sort_values(
        by=["market_cap_rank", "coin_id"], na_position="last", kind="mergesort"
    ).reset_index(drop=True)


__all__ = ["compute_universe_features_as_of"]
