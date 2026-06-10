from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from .base import Objective, daily_index


@dataclass(frozen=True, slots=True)
class UsdcBenchmark:
    ID: str = "usdc"
    OBJECTIVE: Objective = "preserve_capital"
    # A flat 1.0 curve reads no price history, so no coins constrain the window.
    REQUIRED_COIN_IDS: tuple[str, ...] = ()

    def equity_curve(
        self,
        window: tuple[date, date],
        prices: dict[str, pd.DataFrame | pd.Series],
    ) -> pd.Series:
        return pd.Series(1.0, index=daily_index(window), name="equity")


BENCHMARK = UsdcBenchmark()
