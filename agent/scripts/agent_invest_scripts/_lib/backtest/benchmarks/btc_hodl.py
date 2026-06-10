from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from .base import Objective, daily_index, normalize, price_series


@dataclass(frozen=True, slots=True)
class BtcHodlBenchmark:
    ID: str = "btc_hodl"
    OBJECTIVE: Objective = "high_growth"
    # Coins whose price history equity_curve() reads. The window selector
    # uses these to keep the backtest window inside the benchmark's data.
    REQUIRED_COIN_IDS: tuple[str, ...] = ("bitcoin",)

    def equity_curve(
        self,
        window: tuple[date, date],
        prices: dict[str, pd.DataFrame | pd.Series],
    ) -> pd.Series:
        index = daily_index(window)
        return normalize(price_series(prices, "bitcoin", index))


BENCHMARK = BtcHodlBenchmark()
