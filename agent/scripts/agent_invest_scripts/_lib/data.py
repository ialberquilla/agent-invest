"""Agent-facing dataset readers."""

from __future__ import annotations

from pathlib import Path
from typing import Final, Literal, TypeAlias

import pandas as pd

from .db import read_sql_frame
from .storage import dataset_key, dataset_path

DatasetName: TypeAlias = Literal[
    "universe_history",
    "daily_prices",
    "daily_market_caps",
    "daily_volumes",
    "coin_metadata",
]

_DATASET_FILES: Final[dict[DatasetName, str]] = {
    "universe_history": "universe_history.parquet",
    "daily_prices": "daily_prices.parquet",
    "daily_market_caps": "daily_market_caps.parquet",
    "daily_volumes": "daily_volumes.parquet",
    "coin_metadata": "coin_metadata.parquet",
}


class DatasetNotFoundError(FileNotFoundError):
    """Raised when a legacy parquet compatibility dataset is missing."""

    def __init__(self, dataset: DatasetName, *, key: str, path: Path) -> None:
        super().__init__(f'Dataset "{dataset}" not found at {path}')
        self.dataset = dataset
        self.key = key
        self.path = str(path)


def read_dataset(dataset: DatasetName) -> pd.DataFrame:
    """Temporary parquet compatibility helper used by tests and old imports only."""
    filename = _DATASET_FILES[dataset]
    key = dataset_key(filename)
    path = dataset_path(filename)

    if not path.exists():
        raise DatasetNotFoundError(dataset, key=key, path=path)

    return pd.read_parquet(path)


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
    "DatasetName",
    "DatasetNotFoundError",
    "asset_universe",
    "asset_universe_features",
    "daily_prices",
    "read_dataset",
]
