from __future__ import annotations

from datetime import date

import pandas as pd

from agent_invest_scripts._lib.registries import list_registry


def weight_basket(
    basket: pd.DataFrame,
    *,
    scheme: str,
    prices: dict[str, pd.DataFrame],
    as_of: date,
) -> dict[str, float]:
    valid_schemes = {entry["id"] for entry in list_registry("weighting_schemes")}
    if scheme not in valid_schemes:
        raise ValueError(f"Unknown weighting scheme: {scheme}")

    coin_ids = _coin_ids(basket)
    if not coin_ids:
        return {}

    if scheme == "equal":
        raw_weights = {coin_id: 1.0 for coin_id in coin_ids}
    elif scheme == "cap":
        raw_weights = _market_cap_weights(basket, coin_ids)
    elif scheme == "vol_inverse":
        raw_weights = _inverse_vol_weights(coin_ids, prices, as_of)
    elif scheme == "ranking_proportional":
        raw_weights = _ranking_proportional_weights(basket, coin_ids)
    else:  # Defensive: registry and implementation should stay in sync.
        raise ValueError(f"Unsupported weighting scheme: {scheme}")

    return _normalize(raw_weights)


def _coin_ids(basket: pd.DataFrame) -> list[str]:
    if "coin_id" not in basket.columns:
        raise ValueError("basket must include a coin_id column")
    return [str(coin_id) for coin_id in basket["coin_id"].dropna().tolist()]


def _market_cap_weights(basket: pd.DataFrame, coin_ids: list[str]) -> dict[str, float]:
    if "market_cap" not in basket.columns:
        return {coin_id: 1.0 for coin_id in coin_ids}

    weights: dict[str, float] = {}
    for row in basket[["coin_id", "market_cap"]].itertuples(index=False):
        value = pd.to_numeric(row.market_cap, errors="coerce")
        weights[str(row.coin_id)] = (
            float(value) if pd.notna(value) and value > 0 else 0.0
        )
    return weights


def _inverse_vol_weights(
    coin_ids: list[str], prices: dict[str, pd.DataFrame], as_of: date
) -> dict[str, float]:
    weights: dict[str, float] = {}
    for coin_id in coin_ids:
        frame = prices.get(coin_id)
        if frame is None or frame.empty:
            weights[coin_id] = 0.0
            continue
        series = _price_series(frame, as_of)
        volatility = series.pct_change().dropna().std()
        weights[coin_id] = (
            1.0 / float(volatility) if pd.notna(volatility) and volatility > 0 else 0.0
        )
    return weights


def _price_series(frame: pd.DataFrame, as_of: date) -> pd.Series:
    price_column = "close" if "close" in frame.columns else "price"
    if price_column not in frame.columns:
        raise ValueError("price frames must include a close or price column")

    filtered = frame
    if "date" in frame.columns:
        dates = pd.to_datetime(frame["date"], errors="coerce").dt.date
        filtered = frame[dates <= as_of]

    return pd.to_numeric(filtered[price_column], errors="coerce").dropna().tail(90)


def _ranking_proportional_weights(
    basket: pd.DataFrame, coin_ids: list[str]
) -> dict[str, float]:
    if "score" in basket.columns:
        scores = pd.to_numeric(basket["score"], errors="coerce").clip(lower=0)
        return dict(
            zip(
                coin_ids, [float(score) if pd.notna(score) else 0.0 for score in scores]
            )
        )

    if "rank" in basket.columns:
        ranks = pd.to_numeric(basket["rank"], errors="coerce")
        max_rank = ranks.max(skipna=True)
        if pd.notna(max_rank):
            return dict(
                zip(
                    coin_ids,
                    [
                        float(max_rank - rank + 1)
                        if pd.notna(rank) and rank > 0
                        else 0.0
                        for rank in ranks
                    ],
                )
            )

    return {
        coin_id: float(len(coin_ids) - index) for index, coin_id in enumerate(coin_ids)
    }


def _normalize(raw_weights: dict[str, float]) -> dict[str, float]:
    total = sum(weight for weight in raw_weights.values() if weight > 0)
    if total <= 0:
        return {coin_id: 1.0 / len(raw_weights) for coin_id in raw_weights}
    return {
        coin_id: weight / total for coin_id, weight in raw_weights.items() if weight > 0
    }
