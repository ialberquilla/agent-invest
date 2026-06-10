from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from agent_invest_scripts._lib.backtest.costs import (
    TradingCostModel,
    count_rebalance_swaps,
    portfolio_turnover,
)

from .base import Objective, daily_index, price_series


INITIAL_CAPITAL_USD = 1000.0
TARGET_WEIGHTS = {"bitcoin": 0.5, "usd-coin": 0.5}


@dataclass(frozen=True, slots=True)
class Balanced5050Benchmark:
    ID: str = "balanced_5050"
    OBJECTIVE: Objective = "balanced"
    # Coins whose price history equity_curve() reads. The window selector
    # uses these to keep the backtest window inside the benchmark's data.
    REQUIRED_COIN_IDS: tuple[str, ...] = ("bitcoin", "usd-coin")
    cost_model: TradingCostModel = TradingCostModel()

    def equity_curve(
        self,
        window: tuple[date, date],
        prices: dict[str, pd.DataFrame | pd.Series],
    ) -> pd.Series:
        index = daily_index(window)
        btc_returns = price_series(prices, "bitcoin", index).pct_change().fillna(0.0)
        usdc_returns = price_series(prices, "usd-coin", index).pct_change().fillna(0.0)

        equity = 1.0
        current_weights: dict[str, float] = {}
        values: list[float] = []
        for day_offset in range(len(index)):
            turnover = portfolio_turnover(current_weights, TARGET_WEIGHTS)
            num_swaps = count_rebalance_swaps(current_weights, TARGET_WEIGHTS)
            trading_cost = self.cost_model.trade_cost_usd(
                turnover=turnover,
                num_swaps=num_swaps,
                portfolio_value_usd=equity * INITIAL_CAPITAL_USD,
            ) / (equity * INITIAL_CAPITAL_USD)
            current_weights = dict(TARGET_WEIGHTS)

            day_returns = {
                "bitcoin": float(btc_returns.iloc[day_offset]),
                "usd-coin": float(usdc_returns.iloc[day_offset]),
            }
            gross_return = sum(
                current_weights[coin_id] * asset_return
                for coin_id, asset_return in day_returns.items()
            )
            if day_offset == 0:
                values.append(1.0)
                continue

            equity *= 1.0 + gross_return - trading_cost
            values.append(equity)
            current_weights = _drift_weights(current_weights, gross_return, day_returns)

        return pd.Series(values, index=index, name="equity")


BENCHMARK = Balanced5050Benchmark()


def _drift_weights(
    weights: dict[str, float], gross_return: float, day_returns: dict[str, float]
) -> dict[str, float]:
    if gross_return <= -1.0:
        return weights
    denominator = 1.0 + gross_return
    return {
        coin_id: weight * (1.0 + day_returns[coin_id]) / denominator
        for coin_id, weight in weights.items()
    }
