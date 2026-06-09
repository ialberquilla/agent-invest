from __future__ import annotations

import math

import pandas as pd
import pytest

from agent_invest_scripts._lib.backtest.signal_indicators import (
    SIGNAL_INDICATORS,
    compute_indicator,
)
from agent_invest_scripts._lib.registries import list_registry


def test_signal_indicator_registry_contains_closed_set() -> None:
    expected = {
        "sma_cross",
        "ema_cross",
        "macd",
        "rsi",
        "roc",
        "z_score",
        "bollinger_bands",
        "atr_channel",
        "donchian_channel",
        "adx",
    }

    assert set(SIGNAL_INDICATORS) == expected
    entries = list_registry("signal_indicators")
    assert {entry["id"] for entry in entries} == expected
    assert len(entries) == 10
    assert all(entry["params_schema"]["properties"] for entry in entries)


def test_rsi_matches_standard_simple_average_formula() -> None:
    prices = pd.DataFrame({"close": [44, 44.15, 43.9, 44.35, 44.6, 44.4]})

    result = compute_indicator(prices, "rsi", {"period": 3})

    expected = pd.Series(
        [
            math.nan,
            math.nan,
            math.nan,
            70.58823529411774,
            73.68421052631581,
            77.77777777777787,
        ]
    )
    pd.testing.assert_series_equal(result, expected, check_names=False)


def test_roc_matches_period_percent_change() -> None:
    prices = pd.DataFrame({"close": [100.0, 110.0, 121.0, 115.5]})

    result = compute_indicator(prices, "roc", {"period": 2})

    expected = pd.Series([math.nan, math.nan, 0.21, 0.05])
    pd.testing.assert_series_equal(result, expected, check_names=False)


def test_bollinger_bands_returns_normalized_distance_from_midline() -> None:
    prices = pd.DataFrame({"close": [1.0, 2.0, 3.0, 4.0]})

    result = compute_indicator(prices, "bollinger_bands", {"period": 3, "stddev": 2.0})

    expected = pd.Series([math.nan, math.nan, 0.6123724356957945, 0.6123724356957945])
    pd.testing.assert_series_equal(result, expected, check_names=False)


def test_sma_cross_returns_fast_minus_slow_average() -> None:
    prices = pd.DataFrame({"close": [1.0, 2.0, 3.0, 4.0, 5.0]})

    result = compute_indicator(
        prices, "sma_cross", {"fast_period": 2, "slow_period": 3}
    )

    expected = pd.Series([math.nan, math.nan, 0.5, 0.5, 0.5])
    pd.testing.assert_series_equal(result, expected, check_names=False)


def test_unknown_indicator_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Unknown signal indicator"):
        compute_indicator(pd.DataFrame({"close": [1.0]}), "missing", {})
