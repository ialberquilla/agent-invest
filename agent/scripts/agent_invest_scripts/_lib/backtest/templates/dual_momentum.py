from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from .base import SignalPlan, TemplateMetadata, validate_against_slot_schema

DEFAULT_UNIVERSE = {
    "selector": "fixed",
    "coin_ids": ["bitcoin", "ethereum", "solana", "binancecoin", "usd-coin"],
}

METADATA = TemplateMetadata(
    id="dual_momentum",
    category="tactical",
    preferred_factors=["roc_30d", "roc_90d"],
    default_universe=DEFAULT_UNIVERSE,
    min_history_days=730,
    composite_formula="trade_count_aware_sharpe",
    slot_schema={
        "lookback_days": {"type": "int", "default": 90, "min": 1},
        "absolute_floor": {"type": "float", "default": 0.0},
        "reserve_asset": {"type": "string", "default": "usd-coin"},
        "rebalance_frequency_days": {"type": "int", "default": 30, "min": 1},
        "position_size_pct": {
            "type": "float",
            "default": 1.0,
            "min": 0.0,
            "max": 1.0,
        },
    },
)


class DualMomentum:
    METADATA = METADATA

    def validate_config(self, config: dict) -> None:
        validate_against_slot_schema(config, self.METADATA.slot_schema)

    def build(
        self,
        universe: pd.DataFrame,
        prices: dict[str, pd.DataFrame],
        config: dict,
        window: tuple[date, date],
    ) -> SignalPlan:
        resolved_config = _with_defaults(config, self.METADATA.slot_schema)
        self.validate_config(resolved_config)

        lookback_days = int(resolved_config["lookback_days"])
        absolute_floor = float(resolved_config["absolute_floor"])
        reserve_asset = resolved_config["reserve_asset"]
        rebalance_frequency_days = int(resolved_config["rebalance_frequency_days"])
        position_size_pct = float(resolved_config["position_size_pct"])

        coin_ids = list(dict.fromkeys(universe["coin_id"].tolist() + [reserve_asset]))
        investable_coin_ids = [
            coin_id for coin_id in coin_ids if coin_id != reserve_asset
        ]
        calendar = _calendar(prices, coin_ids, window)
        target_positions = pd.DataFrame(0, index=calendar, columns=coin_ids, dtype=int)

        current_target = reserve_asset
        for current_date in calendar:
            if _is_rebalance_date(
                current_date.date(), window[0], rebalance_frequency_days
            ):
                current_target = _select_target(
                    current_date,
                    investable_coin_ids,
                    prices,
                    lookback_days,
                    absolute_floor,
                    reserve_asset,
                )
            target_positions.loc[current_date, current_target] = 1

        signals = {
            coin_id: target_positions[coin_id].diff().fillna(target_positions[coin_id])
            for coin_id in coin_ids
        }
        sizing = {coin_id: position_size_pct for coin_id in coin_ids}

        return {"signals": signals, "sizing": sizing, "exit_rule": "rebalance"}


def _with_defaults(config: dict, slot_schema: dict) -> dict:
    return {
        key: schema["default"]
        for key, schema in slot_schema.items()
        if "default" in schema
    } | config


def _calendar(
    prices: dict[str, pd.DataFrame], coin_ids: list[str], window: tuple[date, date]
) -> pd.DatetimeIndex:
    indexes = []
    for coin_id in coin_ids:
        if coin_id in prices:
            indexes.append(pd.DatetimeIndex(pd.to_datetime(prices[coin_id]["date"])))

    if not indexes:
        return pd.date_range(window[0], window[1], freq="D")

    calendar = indexes[0]
    for index in indexes[1:]:
        calendar = calendar.union(index)
    return calendar[(calendar.date >= window[0]) & (calendar.date <= window[1])]


def _is_rebalance_date(current: date, start: date, frequency_days: int) -> bool:
    return (current - start).days % frequency_days == 0


def _select_target(
    current_date: pd.Timestamp,
    coin_ids: list[str],
    prices: dict[str, pd.DataFrame],
    lookback_days: int,
    absolute_floor: float,
    reserve_asset: str,
) -> str:
    momentum = {
        coin_id: value
        for coin_id in coin_ids
        if (value := _momentum_on(prices.get(coin_id), current_date, lookback_days))
        is not None
    }
    if not momentum:
        return reserve_asset

    leader, leader_momentum = max(momentum.items(), key=lambda item: item[1])
    if leader_momentum > absolute_floor:
        return leader
    return reserve_asset


def _momentum_on(
    price_frame: pd.DataFrame | None, current_date: pd.Timestamp, lookback_days: int
) -> float | None:
    if price_frame is None or price_frame.empty:
        return None

    history = price_frame.copy()
    history["date"] = pd.to_datetime(history["date"])
    history = history.sort_values("date")
    current_rows = history[history["date"] <= current_date]
    past_rows = history[history["date"] <= current_date - timedelta(days=lookback_days)]
    if current_rows.empty or past_rows.empty:
        return None

    past_close = float(past_rows.iloc[-1]["close"])
    if past_close == 0.0:
        return None
    current_close = float(current_rows.iloc[-1]["close"])
    return current_close / past_close - 1.0
