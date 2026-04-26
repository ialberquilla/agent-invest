"""Dataset readers backed by local parquet files."""

from __future__ import annotations

from pathlib import Path
from typing import Final, Literal, TypeAlias

import pandas as pd

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
    """Raised when a requested dataset is missing from local storage."""

    def __init__(self, dataset: DatasetName, *, key: str, path: Path) -> None:
        super().__init__(f'Dataset "{dataset}" not found at {path}')
        self.dataset = dataset
        self.key = key
        self.path = str(path)


def read_dataset(dataset: DatasetName) -> pd.DataFrame:
    """Load a parquet dataset from STORAGE_ROOT/datasets."""
    filename = _DATASET_FILES[dataset]
    key = dataset_key(filename)
    path = dataset_path(filename)

    if not path.exists():
        raise DatasetNotFoundError(dataset, key=key, path=path)

    return pd.read_parquet(path)


def universe_history() -> pd.DataFrame:
    """Return the `universe_history` dataset."""
    return read_dataset("universe_history")


def daily_prices() -> pd.DataFrame:
    """Return the `daily_prices` dataset."""
    return read_dataset("daily_prices")


def daily_market_caps() -> pd.DataFrame:
    """Return the `daily_market_caps` dataset."""
    return read_dataset("daily_market_caps")


def daily_volumes() -> pd.DataFrame:
    """Return the `daily_volumes` dataset."""
    return read_dataset("daily_volumes")


def coin_metadata() -> pd.DataFrame:
    """Return the `coin_metadata` dataset."""
    return read_dataset("coin_metadata")


__all__ = [
    "DatasetName",
    "DatasetNotFoundError",
    "coin_metadata",
    "daily_market_caps",
    "daily_prices",
    "daily_volumes",
    "read_dataset",
    "universe_history",
]
