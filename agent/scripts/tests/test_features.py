"""Smoke tests for the as-of feature compute."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import polars as pl
import pytest

from agent_invest_scripts._lib import features


def _build_prices(start: date, days: int, prices: dict[str, float]) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for offset in range(days):
        day = start + timedelta(days=offset)
        for coin_id, base_price in prices.items():
            rows.append(
                {
                    "date": day,
                    "coin_id": coin_id,
                    "price": base_price * (1.0 + 0.001 * offset),
                }
            )
    return pl.DataFrame(rows)


def _build_universe(coin_ids: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "asset_id": index,
                "coin_id": coin_id,
                "symbol": coin_id[:3],
                "name": coin_id.title(),
                "market_cap_rank": index,
                "market_cap": 1_000_000.0 * (1 + index),
            }
            for index, coin_id in enumerate(coin_ids, start=1)
        ]
    )


def test_compute_returns_match_expected_lookback_ratios() -> None:
    start = date(2023, 1, 1)
    prices = _build_prices(start, days=400, prices={"bitcoin": 100.0})
    universe = _build_universe(["bitcoin"])

    as_of = date(2023, 12, 31)
    out = features.compute_universe_features_as_of(
        as_of=as_of, prices=prices, universe=universe
    )

    row = out.iloc[0]
    last_price_date = pd.Timestamp(row["last_price_date"]).date()
    assert last_price_date == as_of

    days_offset = (as_of - start).days
    expected_latest = 100.0 * (1.0 + 0.001 * days_offset)
    assert row["latest_price"] == pytest.approx(expected_latest)

    expected_return_30d = expected_latest / (
        100.0 * (1.0 + 0.001 * (days_offset - 30))
    ) - 1.0
    assert row["return_30d"] == pytest.approx(expected_return_30d)


def test_compute_filters_prices_after_as_of() -> None:
    start = date(2023, 1, 1)
    prices = _build_prices(start, days=400, prices={"bitcoin": 100.0})
    universe = _build_universe(["bitcoin"])

    as_of = date(2023, 6, 30)
    out = features.compute_universe_features_as_of(
        as_of=as_of, prices=prices, universe=universe
    )

    row = out.iloc[0]
    assert pd.Timestamp(row["last_price_date"]).date() == as_of
    days_offset = (as_of - start).days
    expected_latest = 100.0 * (1.0 + 0.001 * days_offset)
    assert row["latest_price"] == pytest.approx(expected_latest)


def test_compute_returns_only_universe_coins() -> None:
    start = date(2023, 1, 1)
    prices = _build_prices(
        start, days=400, prices={"bitcoin": 100.0, "delisted-coin": 50.0}
    )
    universe = _build_universe(["bitcoin"])

    out = features.compute_universe_features_as_of(
        as_of=date(2023, 12, 31), prices=prices, universe=universe
    )

    assert list(out["coin_id"]) == ["bitcoin"]
    assert set(out.columns) == set(features._OUTPUT_COLUMNS)


def test_compute_short_history_returns_nulls_for_long_windows() -> None:
    start = date(2023, 11, 1)
    prices = _build_prices(start, days=20, prices={"new-coin": 10.0})
    universe = _build_universe(["new-coin"])

    out = features.compute_universe_features_as_of(
        as_of=date(2023, 11, 19), prices=prices, universe=universe
    )

    row = out.iloc[0]
    assert pd.isna(row["return_180d"])
    assert pd.isna(row["return_365d"])
    assert row["data_days_365d"] == 19
