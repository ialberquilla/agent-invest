from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .base import close_series, positive_int_param, schema


@dataclass(frozen=True)
class ZScoreIndicator:
    ID = "z_score"
    params_schema = schema({"period": positive_int_param(20)}, ["period"])

    def compute(self, prices: pd.DataFrame, params: dict[str, Any]) -> pd.Series:
        close = close_series(prices)
        period = int(params.get("period", 20))
        mean = close.rolling(period, min_periods=period).mean()
        std = close.rolling(period, min_periods=period).std(ddof=0)
        return (close - mean) / std


z_score = ZScoreIndicator()
