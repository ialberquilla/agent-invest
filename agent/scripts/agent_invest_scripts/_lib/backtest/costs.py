from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class TradingCostModel:
    """CoW Protocol style cost model with proportional fees plus flat gas."""

    protocol_bps: float = 2.0
    widget_bps: float = 70.0
    slippage_bps: float = 30.0
    gas_usd_per_swap: float = 1.0

    @property
    def proportional_bps(self) -> float:
        return self.protocol_bps + self.widget_bps + self.slippage_bps

    def trade_cost_usd(
        self,
        *,
        turnover: float,
        num_swaps: int,
        portfolio_value_usd: float,
    ) -> float:
        proportional = turnover * portfolio_value_usd * self.proportional_bps / 10_000.0
        fixed = num_swaps * self.gas_usd_per_swap
        return proportional + fixed


def portfolio_turnover(
    current_weights: dict[str, float], target_weights: dict[str, float]
) -> float:
    coin_ids = set(current_weights) | set(target_weights)
    total_change = sum(
        abs(target_weights.get(coin_id, 0.0) - current_weights.get(coin_id, 0.0))
        for coin_id in coin_ids
    )
    return 0.5 * total_change


def count_rebalance_swaps(
    current_weights: dict[str, float],
    target_weights: dict[str, float],
    tolerance: float = 1e-6,
) -> int:
    coin_ids = set(current_weights) | set(target_weights)
    return sum(
        1
        for coin_id in coin_ids
        if abs(target_weights.get(coin_id, 0.0) - current_weights.get(coin_id, 0.0))
        > tolerance
    )
