from __future__ import annotations

from datetime import date

import pandas as pd

from .._weighting import weight_basket
from .base import AllocationPlan, TemplateMetadata, validate_against_slot_schema

METADATA = TemplateMetadata(
    id="buy_and_hold",
    category="allocation",
    preferred_factors=["return_365d", "recovery_rate", "pct_horizon_windows_positive"],
    default_universe={
        "selector": "top_n_by_mcap",
        "n": 50,
        "market_cap_floor_usd": 100_000_000,
    },
    min_history_days=0,
    composite_formula="long_horizon_composite",
    slot_schema={
        "select_top": {"type": "int", "required": True, "min": 1, "max": 50},
        "weighting": {
            "type": "registry",
            "registry": "weighting_schemes",
            "required": True,
        },
    },
)


class BuyAndHold:
    METADATA = METADATA

    def validate_config(self, config: dict) -> None:
        validate_against_slot_schema(config, self.METADATA.slot_schema)
        n = config["select_top"]
        if not 1 <= n <= 50:
            raise ValueError(f"select_top must be in [1, 50], got {n}")

    def build(
        self,
        universe: pd.DataFrame,
        prices: dict[str, pd.DataFrame],
        config: dict,
        window: tuple[date, date],
    ) -> AllocationPlan:
        self.validate_config(config)
        top = universe.head(config["select_top"])
        weights = weight_basket(
            top, scheme=config["weighting"], prices=prices, as_of=window[0]
        )
        return {"holdings": {window[0]: weights}, "rebalance_dates": [window[0]]}
