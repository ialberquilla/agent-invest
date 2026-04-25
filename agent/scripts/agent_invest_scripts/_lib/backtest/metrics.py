from __future__ import annotations

import math
import statistics

import polars as pl

TRADING_DAYS_PER_YEAR = 365.0


def calculate_summary_metrics(performance_frame: pl.DataFrame) -> dict[str, float]:
    if performance_frame.is_empty():
        raise ValueError("Performance frame is empty")

    returns = [
        float(value)
        for value in performance_frame.get_column("net_return").fill_null(0.0).to_list()
    ]
    equity_curve = [
        float(value) for value in performance_frame.get_column("equity").to_list()
    ]
    turnover = [
        float(value)
        for value in performance_frame.get_column("turnover").fill_null(0.0).to_list()
    ]
    holdings_count = [
        int(value)
        for value in performance_frame.get_column("holdings_count")
        .fill_null(0)
        .to_list()
    ]

    periods = len(returns)
    years = max(periods / TRADING_DAYS_PER_YEAR, 1.0 / TRADING_DAYS_PER_YEAR)
    mean_daily_return = sum(returns) / periods
    annualized_volatility = (
        statistics.pstdev(returns) * math.sqrt(TRADING_DAYS_PER_YEAR)
        if periods > 1
        else 0.0
    )

    downside_returns = [min(value, 0.0) for value in returns]
    downside_deviation = (
        math.sqrt(sum(value * value for value in downside_returns) / periods)
        * math.sqrt(TRADING_DAYS_PER_YEAR)
        if periods
        else 0.0
    )

    cagr = equity_curve[-1] ** (1.0 / years) - 1.0 if equity_curve[-1] > 0 else -1.0
    sharpe_ratio = (
        (mean_daily_return * TRADING_DAYS_PER_YEAR / annualized_volatility)
        if annualized_volatility
        else 0.0
    )
    sortino_ratio = (
        (mean_daily_return * TRADING_DAYS_PER_YEAR / downside_deviation)
        if downside_deviation
        else 0.0
    )
    max_drawdown = _max_drawdown(equity_curve)
    calmar_ratio = (cagr / abs(max_drawdown)) if max_drawdown < 0 else 0.0
    monthly_returns = _monthly_returns(performance_frame)

    return {
        "cagr": cagr,
        "annualized_volatility": annualized_volatility,
        "sharpe_ratio": sharpe_ratio,
        "sortino_ratio": sortino_ratio,
        "max_drawdown": max_drawdown,
        "calmar_ratio": calmar_ratio,
        "monthly_hit_rate": _positive_fraction(monthly_returns),
        "average_daily_turnover": sum(turnover) / periods,
        "average_holding_count": sum(holdings_count) / periods,
        "worst_month": min(monthly_returns) if monthly_returns else 0.0,
        "best_month": max(monthly_returns) if monthly_returns else 0.0,
    }


def _max_drawdown(equity_curve: list[float]) -> float:
    peak = 0.0
    max_drawdown = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        if peak <= 0:
            continue
        drawdown = value / peak - 1.0
        max_drawdown = min(max_drawdown, drawdown)
    return max_drawdown


def _monthly_returns(performance_frame: pl.DataFrame) -> list[float]:
    monthly = (
        performance_frame.with_columns(
            pl.col("date").dt.strftime("%Y-%m").alias("month")
        )
        .group_by("month")
        .agg(((pl.col("net_return") + 1.0).product() - 1.0).alias("monthly_return"))
        .sort("month")
    )
    return [float(value) for value in monthly.get_column("monthly_return").to_list()]


def _positive_fraction(values: list[float]) -> float:
    if not values:
        return 0.0
    positives = len([value for value in values if value > 0])
    return positives / len(values)
