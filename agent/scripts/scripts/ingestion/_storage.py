"""Filesystem-backed parquet dataset helpers for ingestion refresh jobs."""

from __future__ import annotations

import io
import os
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Literal, TypeAlias

import pandas as pd

from agent_invest_scripts._lib.storage import storage_root

DatasetName: TypeAlias = Literal[
    "universe_history",
    "daily_prices",
    "daily_market_caps",
    "daily_volumes",
    "coin_metadata",
]

_DATASET_FILES: dict[DatasetName, str] = {
    "universe_history": "universe_history.parquet",
    "daily_prices": "daily_prices.parquet",
    "daily_market_caps": "daily_market_caps.parquet",
    "daily_volumes": "daily_volumes.parquet",
    "coin_metadata": "coin_metadata.parquet",
}


def _write_dataset_file(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None

    try:
        with NamedTemporaryFile(dir=path.parent, delete=False) as handle:
            handle.write(payload)
            temp_path = Path(handle.name)

        os.replace(temp_path, path)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()


@dataclass(slots=True)
class DatasetStorage:
    root: Path = field(default_factory=storage_root)

    def dataset_key(self, dataset: DatasetName) -> str:
        return f"datasets/{_DATASET_FILES[dataset]}"

    def dataset_uri(self, dataset: DatasetName) -> str:
        return self.dataset_path(dataset).as_uri()

    def dataset_path(self, dataset: DatasetName) -> Path:
        return self.root / self.dataset_key(dataset)

    def read_dataset(self, dataset: DatasetName) -> pd.DataFrame | None:
        path = self.dataset_path(dataset)
        if not path.exists():
            return None
        return pd.read_parquet(path)

    def write_dataset(self, dataset: DatasetName, frame: pd.DataFrame) -> str:
        payload = self._serialize_parquet(frame)
        _write_dataset_file(self.dataset_path(dataset), payload)
        return self.dataset_key(dataset)

    def _serialize_parquet(self, frame: pd.DataFrame) -> bytes:
        buffer = io.BytesIO()
        frame.to_parquet(buffer, index=False)
        return buffer.getvalue()
