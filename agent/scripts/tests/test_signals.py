from __future__ import annotations

from datetime import date

import polars as pl
from polars.testing import assert_frame_equal

from agent_invest_scripts._lib.signals import (
    cross_sectional_momentum,
    filters,
    regimes,
    time_series_momentum,
)


def test_signal_modules_import_cleanly() -> None:
    assert cross_sectional_momentum is not None
    assert filters is not None
    assert regimes is not None
    assert time_series_momentum is not None


def test_trailing_return_scores_and_positive_trend_signal() -> None:
    prices = pl.DataFrame(
        {
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "bitcoin": [100.0, 110.0, 121.0],
            "ethereum": [100.0, 90.0, 95.0],
        }
    )

    scores = cross_sectional_momentum.trailing_return_scores(prices, lookback_days=1)
    expected_scores = pl.DataFrame(
        {
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "bitcoin": [None, 0.1, 0.1],
            "ethereum": [None, -0.1, 0.05555555555555558],
        }
    )
    assert_frame_equal(scores, expected_scores, check_exact=False)

    signal = time_series_momentum.positive_trend_signal(prices, lookback_days=1)
    expected_signal = pl.DataFrame(
        {
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "bitcoin": pl.Series([0, 1, 1], dtype=pl.Int32),
            "ethereum": pl.Series([0, 0, 1], dtype=pl.Int32),
        }
    )
    assert_frame_equal(signal, expected_signal)


def test_select_top_k_filters_missing_and_non_positive_scores() -> None:
    ranked = cross_sectional_momentum.select_top_k(
        {
            "date": date(2024, 1, 3),
            "bitcoin": 0.3,
            "ethereum": None,
            "solana": -0.1,
            "dogecoin": 0.1,
        },
        top_k=2,
    )

    assert ranked == [("bitcoin", 0.3), ("dogecoin", 0.1)]


def test_minimum_history_mask_and_apply_boolean_mask() -> None:
    values = pl.DataFrame(
        {
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "bitcoin": [None, 10.0, 11.0],
            "ethereum": [20.0, None, 22.0],
        }
    )

    mask = filters.minimum_history_mask(values, min_history_days=2)
    expected_mask = pl.DataFrame(
        {
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "bitcoin": [False, False, True],
            "ethereum": [False, False, False],
        }
    )
    assert_frame_equal(mask, expected_mask)

    masked_values = filters.apply_boolean_mask(values, mask)
    expected_masked_values = pl.DataFrame(
        {
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "bitcoin": pl.Series([None, None, 11.0], dtype=pl.Float64),
            "ethereum": pl.Series([None, None, None], dtype=pl.Float64),
        }
    )
    assert_frame_equal(masked_values, expected_masked_values)


def test_btc_above_moving_average() -> None:
    prices = pl.DataFrame(
        {
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "bitcoin": [1.0, 2.0, 3.0],
        }
    )

    regime = regimes.btc_above_moving_average(prices, window_days=2)
    expected_regime = pl.DataFrame(
        {
            "date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "regime_on": [False, True, True],
        }
    )
    assert_frame_equal(regime, expected_regime)
