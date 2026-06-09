from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .base import close_series, positive_int_param, schema


@dataclass(frozen=True)
class BollingerBandsIndicator:
    ID = "bollinger_bands"
    params_schema = schema(
        {
            "period": positive_int_param(20),
            "stddev": {"type": "number", "exclusiveMinimum": 0, "default": 2.0},
        },
        ["period", "stddev"],
    )

    def compute(self, prices: pd.DataFrame, params: dict[str, Any]) -> pd.Series:
        close = close_series(prices)
        period = int(params.get("period", 20))
        stddev = float(params.get("stddev", 2.0))
        mean = close.rolling(period, min_periods=period).mean()
        std = close.rolling(period, min_periods=period).std(ddof=0)
        return (close - mean) / (stddev * std)


bollinger_bands = BollingerBandsIndicator()
