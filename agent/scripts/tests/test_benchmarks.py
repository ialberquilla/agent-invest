from datetime import date

import pandas as pd
import pytest

from agent_invest_scripts._lib.backtest.benchmarks import (
    BENCHMARKS,
    benchmark_coin_ids,
    benchmark_for,
)
from agent_invest_scripts._lib.backtest.benchmarks.balanced_5050 import (
    Balanced5050Benchmark,
)
from agent_invest_scripts._lib.backtest.benchmarks.base import price_series
from agent_invest_scripts._lib.backtest.costs import TradingCostModel


WINDOW = (date(2024, 1, 1), date(2024, 1, 5))


def test_all_benchmarks_are_registered() -> None:
    assert set(BENCHMARKS) == {
        "btc_hodl",
        "balanced_5050",
        "usdc",
        "staked_eth_proxy",
    }


def test_benchmark_for_preserve_capital_returns_usdc() -> None:
    assert benchmark_for("preserve_capital").ID == "usdc"


def test_usdc_equity_curve_is_flat() -> None:
    curve = benchmark_for("preserve_capital").equity_curve(WINDOW, {})

    assert curve.to_list() == [1.0] * 5
    assert list(curve.index) == list(pd.date_range(*WINDOW, freq="D"))


def test_each_benchmark_starts_at_one_and_matches_daily_window() -> None:
    prices = _prices()

    for benchmark in BENCHMARKS.values():
        curve = benchmark.equity_curve(WINDOW, prices)

        assert curve.iloc[0] == 1.0
        assert len(curve) == 5
        assert list(curve.index) == list(pd.date_range(*WINDOW, freq="D"))


def test_balanced_5050_uses_repo_cost_defaults() -> None:
    benchmark = benchmark_for("balanced")

    assert isinstance(benchmark, Balanced5050Benchmark)
    assert benchmark.cost_model == TradingCostModel(
        protocol_bps=2,
        widget_bps=70,
        slippage_bps=30,
        gas_usd_per_swap=1,
    )


def test_benchmark_coin_ids_reports_required_history() -> None:
    assert benchmark_coin_ids("high_growth") == ("bitcoin",)
    assert benchmark_coin_ids("balanced") == ("bitcoin", "usd-coin")
    assert benchmark_coin_ids("preserve_capital") == ()
    assert benchmark_coin_ids("income") == ("ethereum",)


def test_price_series_forward_fills_interior_gaps() -> None:
    # A missing interior day (2024-01-03) is carried forward, not rejected:
    # real daily crypto data has sporadic single-day gaps.
    index = pd.date_range(*WINDOW, freq="D")
    sparse = pd.Series(
        [100.0, 101.0, 102.0, 105.0],
        index=pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-04", "2024-01-05"]),
    )

    series = price_series({"bitcoin": sparse}, "bitcoin", index)

    assert list(series.index) == list(index)
    assert series.loc["2024-01-03"] == 101.0  # carried from 2024-01-02


def test_price_series_rejects_leading_gap() -> None:
    # ffill cannot fill before the first observation, so a window that starts
    # before the coin's history is still an error.
    index = pd.date_range(*WINDOW, freq="D")
    late = pd.Series(
        [102.0, 105.0],
        index=pd.to_datetime(["2024-01-04", "2024-01-05"]),
    )

    with pytest.raises(ValueError, match="at the start of the requested window"):
        price_series({"bitcoin": late}, "bitcoin", index)


def _prices() -> dict[str, pd.DataFrame]:
    dates = pd.date_range(*WINDOW, freq="D")
    return {
        "bitcoin": pd.DataFrame({"date": dates, "price": [100, 101, 103, 102, 105]}),
        "ethereum": pd.DataFrame({"date": dates, "price": [50, 51, 51, 52, 53]}),
        "usd-coin": pd.DataFrame({"date": dates, "price": [1, 1, 1, 1, 1]}),
    }
