from __future__ import annotations

from datetime import date
from typing import Literal, Protocol

import pandas as pd


Objective = Literal["high_growth", "balanced", "preserve_capital", "income"]


class Benchmark(Protocol):
    ID: str
    OBJECTIVE: Objective

    def equity_curve(
        self,
        window: tuple[date, date],
        prices: dict[str, pd.DataFrame | pd.Series],
    ) -> pd.Series:
        """Return an equity curve over the window, normalized to 1.0 at start."""
        ...


def daily_index(window: tuple[date, date]) -> pd.DatetimeIndex:
    start, end = window
    if end < start:
        raise ValueError("benchmark window end must be on or after start")
    return pd.date_range(start=start, end=end, freq="D")


def price_series(
    prices: dict[str, pd.DataFrame | pd.Series], coin_id: str, index: pd.DatetimeIndex
) -> pd.Series:
    if coin_id not in prices:
        raise ValueError(f"missing benchmark price series: {coin_id}")

    source = prices[coin_id]
    if isinstance(source, pd.Series):
        series = source.copy()
    else:
        if "price" not in source.columns:
            raise ValueError(f"benchmark price frame for {coin_id} must include price")
        if "date" in source.columns:
            series = source.set_index("date")["price"]
        else:
            series = source["price"]

    series.index = pd.to_datetime(series.index)
    series = series.sort_index().reindex(index)
    if series.isna().any():
        raise ValueError(f"missing benchmark price for {coin_id} in requested window")
    return series.astype(float)


def normalize(series: pd.Series) -> pd.Series:
    if series.empty:
        return series.astype(float)
    start = float(series.iloc[0])
    if start <= 0.0:
        raise ValueError("benchmark price series must start above zero")
    return (series.astype(float) / start).rename("equity")
