from __future__ import annotations


def equal_weight_portfolio(coin_ids: list[str]) -> dict[str, float]:
    unique_coin_ids = list(dict.fromkeys(coin_ids))
    if not unique_coin_ids:
        return {}

    weight = 1.0 / len(unique_coin_ids)
    return {coin_id: weight for coin_id in unique_coin_ids}


def prune_small_weights(
    weights: dict[str, float], tolerance: float = 1e-12
) -> dict[str, float]:
    return {
        coin_id: weight
        for coin_id, weight in weights.items()
        if abs(weight) > tolerance
    }
