from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .base import close_series, positive_int_param, schema


@dataclass(frozen=True)
class AtrChannelIndicator:
    ID = "atr_channel"
    params_schema = schema(
        {
            "period": positive_int_param(14),
            "multiplier": {"type": "number", "exclusiveMinimum": 0, "default": 2.0},
        },
        ["period", "multiplier"],
    )

    def compute(self, prices: pd.DataFrame, params: dict[str, Any]) -> pd.Series:
        close = close_series(prices)
        high = pd.to_numeric(prices.get("high", close), errors="coerce")
        low = pd.to_numeric(prices.get("low", close), errors="coerce")
        period = int(params.get("period", 14))
        multiplier = float(params.get("multiplier", 2.0))
        previous_close = close.shift(1)
        true_range = pd.concat(
            [(high - low), (high - previous_close).abs(), (low - previous_close).abs()],
            axis=1,
        ).max(axis=1)
        atr = true_range.rolling(period, min_periods=period).mean()
        midline = close.rolling(period, min_periods=period).mean()
        return (close - midline) / (multiplier * atr)


atr_channel = AtrChannelIndicator()
