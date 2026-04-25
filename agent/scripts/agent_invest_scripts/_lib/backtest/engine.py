from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

import polars as pl

from .costs import TradingCostModel, count_rebalance_swaps, portfolio_turnover
from .metrics import calculate_summary_metrics
from .portfolio import equal_weight_portfolio, prune_small_weights


@dataclass(slots=True)
class BacktestResult:
    summary: dict[str, float | int | str]
    performance: pl.DataFrame
    weights: pl.DataFrame
    selections: pl.DataFrame


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

    asset_columns = [column for column in prices_wide.columns if column != "date"]
    returns_wide = prices_wide.select(
        "date", *[pl.col(column).pct_change().alias(column) for column in asset_columns]
    )
    score_wide = _trailing_return_scores(
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
            _select_top_k(score_row, top_k=top_k, positive_only=True)
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

    current_weights: dict[str, float] = {}
    equity_multiplier = 1.0
    equity_usd = float(initial_capital_usd)
    performance_rows: list[dict[str, object]] = []
    weight_rows: list[dict[str, object]] = []

    for row in returns_wide.to_dicts()[1:]:
        day = row["date"]
        turnover = 0.0
        num_swaps = 0
        trading_cost_usd = 0.0
        trading_cost_fraction = 0.0

        if day in targets_by_effective_date:
            target_weights = targets_by_effective_date[day]
            turnover = portfolio_turnover(current_weights, target_weights)
            num_swaps = count_rebalance_swaps(current_weights, target_weights)
            trading_cost_usd = cost_model.trade_cost_usd(
                turnover=turnover,
                num_swaps=num_swaps,
                portfolio_value_usd=equity_usd,
            )
            trading_cost_fraction = (
                trading_cost_usd / equity_usd if equity_usd > 0 else 0.0
            )
            current_weights = dict(target_weights)

        gross_return = sum(
            current_weights.get(column, 0.0) * float(row.get(column) or 0.0)
            for column in asset_columns
        )
        net_return = gross_return - trading_cost_fraction
        equity_multiplier *= 1.0 + net_return
        equity_usd *= 1.0 + net_return

        for coin_id, weight in current_weights.items():
            weight_rows.append(
                {
                    "date": day.isoformat(),
                    "coin_id": coin_id,
                    "weight": weight,
                }
            )

        performance_rows.append(
            {
                "date": day.isoformat(),
                "gross_return": gross_return,
                "net_return": net_return,
                "turnover": turnover,
                "num_swaps": num_swaps,
                "trading_cost": trading_cost_fraction,
                "trading_cost_usd": trading_cost_usd,
                "holdings_count": len(current_weights),
                "equity": equity_multiplier,
                "equity_usd": equity_usd,
            }
        )

        current_weights = _drift_weights(current_weights, row, gross_return)

    performance = _frame_from_rows(
        performance_rows,
        {
            "date": pl.Date,
            "gross_return": pl.Float64,
            "net_return": pl.Float64,
            "turnover": pl.Float64,
            "num_swaps": pl.Int64,
            "trading_cost": pl.Float64,
            "trading_cost_usd": pl.Float64,
            "holdings_count": pl.Int64,
            "equity": pl.Float64,
            "equity_usd": pl.Float64,
        },
    )
    weights = _frame_from_rows(
        weight_rows,
        {
            "date": pl.Date,
            "coin_id": pl.String,
            "weight": pl.Float64,
        },
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

    summary = calculate_summary_metrics(performance)
    total_trading_cost_usd = float(performance.get_column("trading_cost_usd").sum())
    total_num_swaps = int(performance.get_column("num_swaps").sum())
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
            "initial_capital_usd": float(initial_capital_usd),
            "final_equity_usd": equity_usd,
            "total_trading_cost_usd": total_trading_cost_usd,
            "total_num_swaps": total_num_swaps,
            "start_date": performance.get_column("date").min().isoformat(),
            "end_date": performance.get_column("date").max().isoformat(),
        }
    )
    return BacktestResult(
        summary=summary,
        performance=performance,
        weights=weights,
        selections=selections,
    )


def _wide_prices(prices_long: pl.DataFrame, universe: list[str] | None) -> pl.DataFrame:
    frame = prices_long.select("date", "coin_id", "price")
    if universe:
        frame = frame.filter(pl.col("coin_id").is_in(universe))
    return (
        frame.sort(["date", "coin_id"])
        .pivot(values="price", index="date", on="coin_id", aggregate_function="last")
        .sort("date")
    )


def _trailing_return_scores(
    prices_wide: pl.DataFrame, lookback_days: int, skip_days: int = 0
) -> pl.DataFrame:
    value_columns = [column for column in prices_wide.columns if column != "date"]
    expressions = []

    for column in value_columns:
        if skip_days:
            expression = (
                pl.col(column).shift(skip_days)
                / pl.col(column).shift(skip_days + lookback_days)
                - 1.0
            ).alias(column)
        else:
            expression = (
                pl.col(column) / pl.col(column).shift(lookback_days) - 1.0
            ).alias(column)
        expressions.append(expression)

    return prices_wide.select("date", *expressions)


def _select_top_k(
    scores_row: Mapping[str, object], top_k: int, positive_only: bool = True
) -> list[tuple[str, float]]:
    ranked: list[tuple[str, float]] = []
    for coin_id, value in scores_row.items():
        if coin_id == "date" or value is None:
            continue
        score = float(value)
        if positive_only and score <= 0:
            continue
        ranked.append((coin_id, score))

    ranked.sort(key=lambda item: item[1], reverse=True)
    return ranked[:top_k]


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


def _period_key(day: date, rebalance_frequency: str) -> tuple[int, int]:
    if rebalance_frequency == "weekly":
        iso_calendar = day.isocalendar()
        return (iso_calendar.year, iso_calendar.week)
    return (day.year, day.month)


def _regime_map(regime_frame: pl.DataFrame | None) -> dict[date, bool]:
    if regime_frame is None or regime_frame.is_empty():
        return {}
    return {row["date"]: bool(row["regime_on"]) for row in regime_frame.to_dicts()}


def _drift_weights(
    current_weights: dict[str, float],
    returns_row: dict[str, object],
    gross_return: float,
) -> dict[str, float]:
    if not current_weights or gross_return <= -1.0:
        return current_weights

    denominator = 1.0 + gross_return
    drifted_weights: dict[str, float] = {}
    for coin_id, weight in current_weights.items():
        asset_return = float(returns_row.get(coin_id) or 0.0)
        drifted_weights[coin_id] = weight * (1.0 + asset_return) / denominator
    return prune_small_weights(drifted_weights)


def _frame_from_rows(
    rows: list[dict[str, object]],
    schema: dict[str, pl.DataType],
    date_column_names: list[str] | None = None,
) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=schema)

    frame = pl.DataFrame(rows)
    date_columns = date_column_names or ["date"]
    existing_date_columns = [
        column for column in date_columns if column in frame.columns
    ]
    if existing_date_columns:
        frame = frame.with_columns(
            [pl.col(column).str.to_date() for column in existing_date_columns]
        )
    return frame
