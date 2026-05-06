"""Agent-facing dataset readers."""

from __future__ import annotations

import pandas as pd

from .db import read_sql_frame


def daily_prices() -> pd.DataFrame:
    """Return daily close prices from Postgres."""
    return read_sql_frame(
        """
        SELECT
          "date",
          "coin_id",
          "price"
        FROM "agent_daily_close_prices"
        ORDER BY "date", "coin_id"
        """
    )


def asset_universe() -> pd.DataFrame:
    """Return the current asset universe from Postgres."""
    return read_sql_frame(
        """
        SELECT
          "coin_id",
          "symbol",
          "name",
          "market_cap",
          "market_cap_rank",
          "price"
        FROM "agent_asset_universe"
        ORDER BY "market_cap_rank" NULLS LAST, "coin_id"
        """
    )


def asset_universe_features() -> pd.DataFrame:
    """Return current asset universe features from Postgres."""
    return read_sql_frame(
        """
        SELECT
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
          "price_above_sma_200d"
        FROM "agent_asset_universe_features"
        ORDER BY "market_cap_rank" NULLS LAST, "coin_id"
        """
    )


__all__ = [
    "asset_universe",
    "asset_universe_features",
    "daily_prices",
]
