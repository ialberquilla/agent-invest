from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from .._weighting import weight_basket
from .base import AllocationPlan, TemplateMetadata, validate_against_slot_schema

METADATA = TemplateMetadata(
    id="periodic_rebalance",
    category="allocation",
    preferred_factors=["sharpe_365d", "recovery_rate"],
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
        "rebalance_trigger": {
            "type": "registry",
            "registry": "rebalance_triggers",
            "default": "periodic_30d",
        },
    },
)


class PeriodicRebalance:
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
        effective_config = {"rebalance_trigger": "periodic_30d", **config}
        self.validate_config(effective_config)
        top = universe.head(effective_config["select_top"])
        weights = weight_basket(
            top, scheme=effective_config["weighting"], prices=prices, as_of=window[0]
        )
        rebalance_dates = _rebalance_dates(
            effective_config["rebalance_trigger"], weights, prices, window
        )
        return {
            "holdings": {rebalance_date: weights for rebalance_date in rebalance_dates},
            "rebalance_dates": rebalance_dates,
        }


def _rebalance_dates(
    trigger: str,
    target_weights: dict[str, float],
    prices: dict[str, pd.DataFrame],
    window: tuple[date, date],
) -> list[date]:
    if trigger == "none":
        return [window[0]]
    if trigger == "periodic_30d":
        return _periodic_dates(window, 30)
    if trigger == "periodic_90d":
        return _periodic_dates(window, 90)
    if trigger == "threshold_drift_10pct":
        return _threshold_drift_dates(target_weights, prices, window, threshold=0.10)
    raise ValueError(f"Unsupported rebalance trigger: {trigger}")


def _periodic_dates(window: tuple[date, date], days: int) -> list[date]:
    start, end = window
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=days)
    return dates


def _threshold_drift_dates(
    target_weights: dict[str, float],
    prices: dict[str, pd.DataFrame],
    window: tuple[date, date],
    *,
    threshold: float,
) -> list[date]:
    rebalance_dates = [window[0]]
    current_weights = dict(target_weights)
    daily_prices = _daily_prices(target_weights, prices, window)
    price_dates = list(daily_prices)

    for previous_day, day in zip(price_dates, price_dates[1:]):
        returns = {
            coin_id: daily_prices[day][coin_id] / daily_prices[previous_day][coin_id]
            - 1.0
            for coin_id in target_weights
            if daily_prices[previous_day][coin_id] > 0
        }
        portfolio_return = sum(
            current_weights[coin_id] * returns.get(coin_id, 0.0)
            for coin_id in current_weights
        )
        current_weights = {
            coin_id: weight
            * (1.0 + returns.get(coin_id, 0.0))
            / (1.0 + portfolio_return)
            for coin_id, weight in current_weights.items()
            if 1.0 + portfolio_return > 0
        }

        if any(
            abs(current_weights.get(coin_id, 0.0) - target_weight) > threshold
            for coin_id, target_weight in target_weights.items()
        ):
            rebalance_dates.append(day)
            current_weights = dict(target_weights)

    return rebalance_dates


def _daily_prices(
    target_weights: dict[str, float],
    prices: dict[str, pd.DataFrame],
    window: tuple[date, date],
) -> dict[date, dict[str, float]]:
    start, end = window
    series_by_coin = {
        coin_id: _price_series(prices[coin_id], start, end)
        for coin_id in target_weights
    }
    common_dates = set.intersection(
        *(set(series.index) for series in series_by_coin.values())
    )
    return {
        day: {
            coin_id: float(series.loc[day])
            for coin_id, series in series_by_coin.items()
        }
        for day in sorted(common_dates)
    }


def _price_series(frame: pd.DataFrame, start: date, end: date) -> pd.Series:
    price_column = "close" if "close" in frame.columns else "price"
    if price_column not in frame.columns:
        raise ValueError("price frames must include a close or price column")
    if "date" not in frame.columns:
        raise ValueError("price frames must include a date column")

    dates = pd.to_datetime(frame["date"], errors="coerce").dt.date
    values = pd.to_numeric(frame[price_column], errors="coerce")
    series = pd.Series(values.to_numpy(), index=dates).dropna()
    return series[(series.index >= start) & (series.index <= end)]
