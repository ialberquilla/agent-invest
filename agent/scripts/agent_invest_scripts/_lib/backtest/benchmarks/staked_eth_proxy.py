from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from .base import Objective, daily_index, normalize, price_series


DAILY_YIELD = (1.0 + 0.04) ** (1.0 / 365.0) - 1.0


@dataclass(frozen=True, slots=True)
class StakedEthProxyBenchmark:
    ID: str = "staked_eth_proxy"
    OBJECTIVE: Objective = "income"
    # Coins whose price history equity_curve() reads. The window selector
    # uses these to keep the backtest window inside the benchmark's data.
    REQUIRED_COIN_IDS: tuple[str, ...] = ("ethereum",)

    def equity_curve(
        self,
        window: tuple[date, date],
        prices: dict[str, pd.DataFrame | pd.Series],
    ) -> pd.Series:
        index = daily_index(window)
        eth = normalize(price_series(prices, "ethereum", index))
        yield_multiplier = pd.Series(
            [(1.0 + DAILY_YIELD) ** offset for offset in range(len(index))],
            index=index,
        )
        return (eth * yield_multiplier).rename("equity")


BENCHMARK = StakedEthProxyBenchmark()
