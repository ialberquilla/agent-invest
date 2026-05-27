from __future__ import annotations

from datetime import date

import pandas as pd

from .base import SignalPlan, TemplateMetadata, validate_against_slot_schema

METADATA = TemplateMetadata(
    id="breakout",
    category="tactical",
    preferred_factors=["volatility_180d", "n_day_high_proximity"],
    default_universe={
        "selector": "top_n_by_mcap",
        "n": 50,
        "market_cap_floor_usd": 500_000_000,
    },
    min_history_days=365,
    composite_formula="trade_count_aware_sharpe",
    slot_schema={
        "breakout_window_days": {"type": "int", "required": True, "min": 1},
        "confirmation_filter": {
            "type": "registry",
            "registry": "signal_indicators",
            "required": False,
        },
        "exit_rule": {"type": "registry", "registry": "exit_rules", "required": True},
        "position_size_pct": {
            "type": "float",
            "required": True,
            "min": 0.0,
            "max": 1.0,
        },
    },
)


class Breakout:
    METADATA = METADATA

    def validate_config(self, config: dict) -> None:
        validate_against_slot_schema(config, self.METADATA.slot_schema)
        breakout_window_days = int(config["breakout_window_days"])
        if breakout_window_days <= 0:
            raise ValueError(
                f"breakout_window_days must be positive, got {breakout_window_days}"
            )
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
        breakout_window_days = int(config["breakout_window_days"])

        signals = {}
        sizing = {}
        for coin_id in universe["coin_id"]:
            frame = prices[coin_id]
            close = pd.to_numeric(frame["close"], errors="coerce")
            high = pd.to_numeric(frame.get("high", close), errors="coerce")
            prior_window_high = (
                high.shift(1)
                .rolling(breakout_window_days, min_periods=breakout_window_days)
                .max()
            )
            entry = (close > prior_window_high) & (close.shift(1) <= prior_window_high)
            signals[coin_id] = entry.astype(int).diff().fillna(entry.astype(int))
            sizing[coin_id] = float(config["position_size_pct"])

        return {"signals": signals, "sizing": sizing, "exit_rule": config["exit_rule"]}
