from __future__ import annotations

from pathlib import Path

import pandas as pd

from agent_invest_scripts._lib import data


def _fixture_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"date": "2024-01-01", "coin_id": "bitcoin", "market_cap": 1.0},
            {"date": "2024-01-02", "coin_id": "ethereum", "market_cap": 2.0},
        ]
    )


def test_reads_dataset_from_storage_root(tmp_path: Path, monkeypatch) -> None:
    storage_root = tmp_path / "storage"
    dataset_path = storage_root / "datasets" / "universe_history.parquet"
    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    expected = _fixture_frame()
    expected.to_parquet(dataset_path, index=False)
    monkeypatch.setenv("STORAGE_ROOT", str(storage_root))

    first = data.universe_history()
    second = data.universe_history()

    pd.testing.assert_frame_equal(first, expected)
    pd.testing.assert_frame_equal(second, expected)


def test_missing_dataset_raises_typed_error(tmp_path: Path, monkeypatch) -> None:
    storage_root = tmp_path / "storage"
    monkeypatch.setenv("STORAGE_ROOT", str(storage_root))

    try:
        data.daily_prices()
    except data.DatasetNotFoundError as error:
        assert error.dataset == "daily_prices"
        assert error.key == "datasets/daily_prices.parquet"
        assert error.path == str(storage_root / "datasets" / "daily_prices.parquet")
    else:
        raise AssertionError(
            "daily_prices should fail when the parquet file is missing"
        )
