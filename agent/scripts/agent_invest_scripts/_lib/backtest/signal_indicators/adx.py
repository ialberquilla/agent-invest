from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .base import close_series, positive_int_param, schema


@dataclass(frozen=True)
class AdxIndicator:
    ID = "adx"
    params_schema = schema({"period": positive_int_param(14)}, ["period"])

    def compute(self, prices: pd.DataFrame, params: dict[str, Any]) -> pd.Series:
        close = close_series(prices)
        high = pd.to_numeric(prices.get("high", close), errors="coerce")
        low = pd.to_numeric(prices.get("low", close), errors="coerce")
        period = int(params.get("period", 14))

        up_move = high.diff()
        down_move = -low.diff()
        plus_dm = up_move.where((up_move > down_move) & (up_move > 0.0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0.0), 0.0)
        previous_close = close.shift(1)
        true_range = pd.concat(
            [(high - low), (high - previous_close).abs(), (low - previous_close).abs()],
            axis=1,
        ).max(axis=1)

        atr = true_range.rolling(period, min_periods=period).mean()
        plus_di = 100.0 * plus_dm.rolling(period, min_periods=period).mean() / atr
        minus_di = 100.0 * minus_dm.rolling(period, min_periods=period).mean() / atr
        dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di)
        return dx.rolling(period, min_periods=period).mean()


adx = AdxIndicator()
