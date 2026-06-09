from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .base import close_series, positive_int_param, schema


@dataclass(frozen=True)
class DonchianChannelIndicator:
    ID = "donchian_channel"
    params_schema = schema({"period": positive_int_param(20)}, ["period"])

    def compute(self, prices: pd.DataFrame, params: dict[str, Any]) -> pd.Series:
        close = close_series(prices)
        high = pd.to_numeric(prices.get("high", close), errors="coerce")
        low = pd.to_numeric(prices.get("low", close), errors="coerce")
        period = int(params.get("period", 20))
        upper = high.rolling(period, min_periods=period).max()
        lower = low.rolling(period, min_periods=period).min()
        midpoint = (upper + lower) / 2.0
        half_width = (upper - lower) / 2.0
        return (close - midpoint) / half_width


donchian_channel = DonchianChannelIndicator()
