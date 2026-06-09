from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from .base import close_series, positive_int_param, schema


@dataclass(frozen=True)
class RocIndicator:
    ID = "roc"
    params_schema = schema({"period": positive_int_param(12)}, ["period"])

    def compute(self, prices: pd.DataFrame, params: dict[str, Any]) -> pd.Series:
        close = close_series(prices)
        period = int(params.get("period", 12))
        return close.pct_change(periods=period)


roc = RocIndicator()
