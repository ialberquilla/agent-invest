from __future__ import annotations

from typing import Any

import pandas as pd

from .adx import adx
from .atr_channel import atr_channel
from .base import SignalIndicator
from .bollinger_bands import bollinger_bands
from .donchian_channel import donchian_channel
from .ema_cross import ema_cross
from .macd import macd
from .roc import roc
from .rsi import rsi
from .sma_cross import sma_cross
from .z_score import z_score

SIGNAL_INDICATORS: dict[str, SignalIndicator] = {
    indicator.ID: indicator
    for indicator in (
        sma_cross,
        ema_cross,
        macd,
        rsi,
        roc,
        z_score,
        bollinger_bands,
        atr_channel,
        donchian_channel,
        adx,
    )
}


def compute_indicator(
    prices_df: pd.DataFrame, indicator_id: str, params: dict[str, Any] | None = None
) -> pd.Series:
    try:
        indicator = SIGNAL_INDICATORS[indicator_id]
    except KeyError as error:
        valid = ", ".join(sorted(SIGNAL_INDICATORS))
        raise ValueError(
            f"Unknown signal indicator '{indicator_id}'. Valid indicators: {valid}"
        ) from error
    return indicator.compute(prices_df, params or {})


__all__ = ["SIGNAL_INDICATORS", "SignalIndicator", "compute_indicator"]
