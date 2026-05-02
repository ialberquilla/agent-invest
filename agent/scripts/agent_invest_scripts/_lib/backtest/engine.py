from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import polars as pl

from .costs import TradingCostModel, count_rebalance_swaps, portfolio_turnover
from .metrics import calculate_summary_metrics
from .portfolio import prune_small_weights

_PERFORMANCE_SCHEMA = {
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
}

_WEIGHTS_SCHEMA = {
    "date": pl.Date,
    "coin_id": pl.String,
    "weight": pl.Float64,
}


@dataclass(slots=True)
class BacktestResult:
    summary: dict[str, float | int | str]
    performance: pl.DataFrame
    weights: pl.DataFrame
    selections: pl.DataFrame


def run_backtest(
    prices_long: pl.DataFrame,
    targets: dict[date, dict[str, float]],
    *,
    cost_model: TradingCostModel | None = None,
    initial_capital_usd: float = 1000.0,
    universe: list[str] | None = None,
) -> BacktestResult:
    """Walk prices forward, snapping to supplied target weights on target dates."""
    if cost_model is None:
        cost_model = TradingCostModel()

    prices_wide = _wide_prices(prices_long, universe)
    if prices_wide.height < 2:
        raise ValueError("At least two price dates are required to run a backtest")

    asset_columns = [column for column in prices_wide.columns if column != "date"]
    returns_wide = prices_wide.select(
        "date", *[pl.col(column).pct_change().alias(column) for column in asset_columns]
    )
    normalized_targets = {
        target_date: prune_small_weights(dict(target_weights))
        for target_date, target_weights in targets.items()
    }

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

        if day in normalized_targets:
            target_weights = normalized_targets[day]
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
                {"date": day.isoformat(), "coin_id": coin_id, "weight": weight}
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

    performance = _frame_from_rows(performance_rows, _PERFORMANCE_SCHEMA)
    weights = _frame_from_rows(weight_rows, _WEIGHTS_SCHEMA)
    selections = _frame_from_rows(
        [],
        {
            "signal_date": pl.Date,
            "effective_date": pl.Date,
            "coin_id": pl.String,
            "score": pl.Float64,
        },
        date_column_names=["signal_date", "effective_date"],
    )
    summary = calculate_summary_metrics(performance)
    summary.update(
        {
            "initial_capital_usd": float(initial_capital_usd),
            "final_equity_usd": equity_usd,
            "total_trading_cost_usd": float(
                performance.get_column("trading_cost_usd").sum()
            ),
            "total_num_swaps": int(performance.get_column("num_swaps").sum()),
            "start_date": performance.get_column("date").min().isoformat(),
            "end_date": performance.get_column("date").max().isoformat(),
        }
    )
    return BacktestResult(
        summary=summary, performance=performance, weights=weights, selections=selections
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


def _period_key(day: date, rebalance_frequency: str) -> tuple[int, int]:
    if rebalance_frequency == "weekly":
        iso_calendar = day.isocalendar()
        return (iso_calendar.year, iso_calendar.week)
    return (day.year, day.month)


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
