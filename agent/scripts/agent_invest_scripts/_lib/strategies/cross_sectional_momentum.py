from __future__ import annotations

from datetime import date

import polars as pl

from agent_invest_scripts._lib.backtest.costs import TradingCostModel
from agent_invest_scripts._lib.backtest.engine import (
    BacktestResult,
    _frame_from_rows,
    _period_key,
    _wide_prices,
    run_backtest,
)
from agent_invest_scripts._lib.backtest.portfolio import equal_weight_portfolio
from agent_invest_scripts._lib.signals.cross_sectional_momentum import (
    select_top_k,
    trailing_return_scores,
)


def run_cross_sectional_momentum_backtest(
    prices_long: pl.DataFrame,
    *,
    universe: list[str] | None = None,
    lookback_days: int = 60,
    top_k: int = 10,
    rebalance_frequency: str = "weekly",
    cost_model: TradingCostModel | None = None,
    initial_capital_usd: float = 1000.0,
    skip_days: int = 0,
    regime_frame: pl.DataFrame | None = None,
) -> BacktestResult:
    if cost_model is None:
        cost_model = TradingCostModel()

    prices_wide = _wide_prices(prices_long, universe)
    if prices_wide.height < lookback_days + skip_days + 2:
        raise ValueError(
            "Not enough price history for the requested backtest configuration"
        )

    score_wide = trailing_return_scores(
        prices_wide, lookback_days=lookback_days, skip_days=skip_days
    )

    dates = prices_wide.get_column("date").to_list()
    date_to_index = {day: index for index, day in enumerate(dates)}
    signal_dates = _rebalance_signal_dates(dates, rebalance_frequency)
    regime_by_date = _regime_map(regime_frame)

    score_rows = {row["date"]: row for row in score_wide.to_dicts()}
    targets_by_effective_date: dict[date, dict[str, float]] = {}
    selection_rows: list[dict[str, object]] = []

    for signal_date in signal_dates:
        effective_index = date_to_index[signal_date] + 1
        if effective_index >= len(dates):
            continue

        effective_date = dates[effective_index]
        score_row = score_rows.get(signal_date, {})
        regime_on = regime_by_date.get(signal_date, True)
        ranked = (
            select_top_k(score_row, top_k=top_k, positive_only=True)
            if regime_on
            else []
        )
        target_weights = equal_weight_portfolio([coin_id for coin_id, _score in ranked])
        targets_by_effective_date[effective_date] = target_weights

        for coin_id, score in ranked:
            selection_rows.append(
                {
                    "signal_date": signal_date.isoformat(),
                    "effective_date": effective_date.isoformat(),
                    "coin_id": coin_id,
                    "score": score,
                }
            )

    result = run_backtest(
        prices_long,
        targets_by_effective_date,
        cost_model=cost_model,
        initial_capital_usd=initial_capital_usd,
        universe=universe,
    )
    selections = _frame_from_rows(
        selection_rows,
        {
            "signal_date": pl.Date,
            "effective_date": pl.Date,
            "coin_id": pl.String,
            "score": pl.Float64,
        },
        date_column_names=["signal_date", "effective_date"],
    )

    summary = dict(result.summary)
    summary.update(
        {
            "lookback_days": lookback_days,
            "top_k": top_k,
            "rebalance_frequency": rebalance_frequency,
            "protocol_bps": cost_model.protocol_bps,
            "widget_bps": cost_model.widget_bps,
            "slippage_bps": cost_model.slippage_bps,
            "gas_usd_per_swap": cost_model.gas_usd_per_swap,
            "skip_days": skip_days,
        }
    )
    return BacktestResult(
        summary=summary,
        performance=result.performance,
        weights=result.weights,
        selections=selections,
    )


def _rebalance_signal_dates(dates: list[date], rebalance_frequency: str) -> list[date]:
    normalized = rebalance_frequency.lower()
    if normalized not in {"weekly", "monthly"}:
        raise ValueError("rebalance_frequency must be weekly or monthly")

    signal_dates: list[date] = []
    for index, day in enumerate(dates):
        current_key = _period_key(day, normalized)
        next_key = (
            _period_key(dates[index + 1], normalized)
            if index + 1 < len(dates)
            else None
        )
        if current_key != next_key:
            signal_dates.append(day)
    return signal_dates


def _regime_map(regime_frame: pl.DataFrame | None) -> dict[date, bool]:
    if regime_frame is None or regime_frame.is_empty():
        return {}
    return {row["date"]: bool(row["regime_on"]) for row in regime_frame.to_dicts()}
