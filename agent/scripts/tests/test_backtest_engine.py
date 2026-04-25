from datetime import date, timedelta

import polars as pl

from agent_invest_scripts._lib.backtest import (
    TradingCostModel,
    run_cross_sectional_momentum_backtest,
)

ZERO_COST_MODEL = TradingCostModel(
    protocol_bps=0.0,
    widget_bps=0.0,
    slippage_bps=0.0,
    gas_usd_per_swap=0.0,
)


def test_cross_sectional_backtest_selects_top_asset() -> None:
    start_date = date(2024, 1, 1)
    rows = []
    coin_a_price = 100.0
    coin_b_price = 100.0

    for offset in range(30):
        current_date = start_date + timedelta(days=offset)
        coin_a_price *= 1.01
        coin_b_price *= 1.001
        rows.append({"date": current_date, "coin_id": "coin-a", "price": coin_a_price})
        rows.append({"date": current_date, "coin_id": "coin-b", "price": coin_b_price})

    prices = pl.DataFrame(rows)
    result = run_cross_sectional_momentum_backtest(
        prices,
        universe=["coin-a", "coin-b"],
        lookback_days=5,
        top_k=1,
        rebalance_frequency="weekly",
        cost_model=ZERO_COST_MODEL,
    )

    selected_coin_ids = set(result.selections.get_column("coin_id").to_list())

    assert "coin-a" in selected_coin_ids
    assert result.performance.height > 0
    assert result.summary["average_holding_count"] <= 1.0
