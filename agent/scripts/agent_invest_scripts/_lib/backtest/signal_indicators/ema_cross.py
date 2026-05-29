from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .base import close_series, positive_int_param, schema


@dataclass(frozen=True)
class EmaCrossIndicator:
    ID = "ema_cross"
    params_schema = schema(
        {"fast_period": positive_int_param(12), "slow_period": positive_int_param(26)},
        ["fast_period", "slow_period"],
    )

    def compute(self, prices: pd.DataFrame, params: dict[str, Any]) -> pd.Series:
        close = close_series(prices)
        fast_period = int(params.get("fast_period", 12))
        slow_period = int(params.get("slow_period", 26))
        if fast_period >= slow_period:
            raise ValueError("fast_period must be less than slow_period")
        fast = close.ewm(span=fast_period, adjust=False, min_periods=fast_period).mean()
        slow = close.ewm(span=slow_period, adjust=False, min_periods=slow_period).mean()
        return fast - slow


ema_cross = EmaCrossIndicator()
