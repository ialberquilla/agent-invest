from datetime import date

import pandas as pd

from agent_invest_scripts._lib.backtest.benchmarks import BENCHMARKS, benchmark_for
from agent_invest_scripts._lib.backtest.benchmarks.balanced_5050 import (
    Balanced5050Benchmark,
)
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


def _prices() -> dict[str, pd.DataFrame]:
    dates = pd.date_range(*WINDOW, freq="D")
    return {
        "bitcoin": pd.DataFrame({"date": dates, "price": [100, 101, 103, 102, 105]}),
        "ethereum": pd.DataFrame({"date": dates, "price": [50, 51, 51, 52, 53]}),
        "usd-coin": pd.DataFrame({"date": dates, "price": [1, 1, 1, 1, 1]}),
    }
