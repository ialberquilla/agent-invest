from __future__ import annotations

from typing import Any, Protocol

import pandas as pd


class SignalIndicator(Protocol):
    ID: str
    params_schema: dict[str, Any]

    def compute(self, prices: pd.DataFrame, params: dict[str, Any]) -> pd.Series:
        """Return the indicator's numeric series aligned to the input price index."""
        ...


def close_series(prices: pd.DataFrame) -> pd.Series:
    if "close" in prices.columns:
        series = prices["close"]
    else:
        numeric = prices.select_dtypes(include="number")
        if numeric.empty:
            raise ValueError(
                "prices must include a 'close' column or numeric price column"
            )
        series = numeric.iloc[:, 0]
    return pd.to_numeric(series, errors="coerce")


def schema(
    properties: dict[str, Any], required: list[str] | None = None
) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": required or [],
        "additionalProperties": False,
    }


def positive_int_param(default: int, minimum: int = 1) -> dict[str, Any]:
    return {"type": "integer", "minimum": minimum, "default": default}
