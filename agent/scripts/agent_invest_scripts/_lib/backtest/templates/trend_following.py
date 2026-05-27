from __future__ import annotations

from datetime import date

import pandas as pd

from ..signal_indicators import compute_indicator
from .base import SignalPlan, TemplateMetadata, validate_against_slot_schema

METADATA = TemplateMetadata(
    id="trend_following",
    category="tactical",
    preferred_factors=["adx_30d", "pct_above_sma_50d"],
    default_universe={
        "selector": "top_n_by_mcap",
        "n": 30,
        "market_cap_floor_usd": 1_000_000_000,
    },
    min_history_days=730,
    composite_formula="trade_count_aware_sharpe",
    slot_schema={
        "signal_indicator": {
            "type": "registry",
            "registry": "signal_indicators",
            "required": True,
        },
        "signal_indicator_params": {"type": "object", "required": True},
        "exit_rule": {"type": "registry", "registry": "exit_rules", "required": True},
        "position_size_pct": {
            "type": "float",
            "required": True,
            "min": 0.0,
            "max": 1.0,
        },
    },
)


class TrendFollowing:
    METADATA = METADATA

    def validate_config(self, config: dict) -> None:
        validate_against_slot_schema(config, self.METADATA.slot_schema)
        position_size_pct = float(config["position_size_pct"])
        if not 0.0 <= position_size_pct <= 1.0:
            raise ValueError(
                f"position_size_pct must be in [0, 1], got {position_size_pct}"
            )

    def build(
        self,
        universe: pd.DataFrame,
        prices: dict[str, pd.DataFrame],
        config: dict,
        window: tuple[date, date],
    ) -> SignalPlan:
        self.validate_config(config)
        signal_indicator = config["signal_indicator"]
        signal_indicator_params = config["signal_indicator_params"]

        signals = {}
        sizing = {}
        for coin_id in universe["coin_id"]:
            indicator = compute_indicator(
                prices[coin_id], signal_indicator, signal_indicator_params
            )
            signals[coin_id] = (indicator > 0).astype(int).diff().fillna(0)
            sizing[coin_id] = float(config["position_size_pct"])

        return {"signals": signals, "sizing": sizing, "exit_rule": config["exit_rule"]}
