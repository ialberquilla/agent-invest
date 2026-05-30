"""Robustness / benchmark signals computed off a plain equity ``pd.Series``.

These functions used to live in ``run_candidate_batch.py`` and read the polars
``engine`` result. They now take a pandas equity curve (``bt`` gives us one via
``bk.strategy.values``) plus a benchmark curve, so the same logic survives the
retirement of the polars engine. Every function here is engine-agnostic: feed it
an equity Series and it produces the contract's ``RobustnessSignals`` /
``DrawdownEpisode`` / benchmark curve.
"""

from __future__ import annotations

import math
from datetime import date

import pandas as pd

from agent_invest_scripts._lib.backtest.benchmarks import benchmark_for
from agent_invest_scripts._lib.backtest.result import (
    DrawdownEpisode,
    RobustnessSignals,
)


def benchmark_curve(
    objective: str,
    window: tuple[date, date],
    prices: dict[str, pd.DataFrame],
    index: pd.Index,
) -> pd.Series:
    """Benchmark equity for ``objective`` reindexed onto the strategy's dates."""
    curve = benchmark_for(objective).equity_curve(window, prices)
    curve.index = pd.to_datetime(curve.index).date
    return curve.reindex(index).ffill().bfill().rename("value")


def compute_robustness(
    equity: pd.Series,
    benchmark: pd.Series,
    *,
    n_rebalances: int,
    survivorship_warning: bool = False,
) -> RobustnessSignals:
    returns = equity.pct_change().dropna()
    benchmark_returns = benchmark.pct_change().dropna().reindex(returns.index).dropna()
    aligned = returns.reindex(benchmark_returns.index).dropna()
    half_score = _half_consistency(returns)
    top_3_months = _top_3_months_pct(equity)
    worst_180d = _worst_rolling_return(equity, 180)
    worst_90d_dd = _worst_rolling_drawdown(equity, 90)
    corr = float(aligned.corr(benchmark_returns)) if len(aligned) > 1 else 0.0
    beta = (
        float(aligned.cov(benchmark_returns) / benchmark_returns.var())
        if len(aligned) > 1 and benchmark_returns.var()
        else 0.0
    )
    excess = aligned - benchmark_returns.reindex(aligned.index)
    t_stat = (
        float(excess.mean() / (excess.std(ddof=0) / math.sqrt(len(excess))))
        if len(excess) > 1 and excess.std(ddof=0)
        else 0.0
    )
    duration = (equity.index[-1] - equity.index[0]).days if len(equity) else 0
    return RobustnessSignals(
        n_trades=None,
        n_rebalances=n_rebalances,
        duration_days=duration,
        sample_size_warning=n_rebalances < 3 or duration < 180,
        half_consistency_score=half_score,
        half_consistency_warning=half_score < 0.4,
        top_3_trades_pct_of_pnl=None,
        top_3_months_pct_of_pnl=top_3_months,
        concentration_warning=top_3_months > 0.7,
        worst_180d_return=worst_180d,
        worst_90d_drawdown=worst_90d_dd,
        worst_window_warning=worst_180d < -0.40 or worst_90d_dd < -0.50,
        correlation_to_benchmark=0.0 if math.isnan(corr) else corr,
        beta_to_benchmark=beta,
        excess_return_t_stat=t_stat,
        benchmark_coupling_warning=corr > 0.95 if not math.isnan(corr) else False,
        significance_warning=t_stat < 1.5,
        survivorship_warning=survivorship_warning,
    )


def drawdown_episodes(equity: pd.Series) -> list[DrawdownEpisode]:
    drawdown = equity / equity.cummax() - 1.0
    if float(drawdown.min()) >= 0.0:
        return []
    trough = drawdown.idxmin()
    peak = equity.loc[:trough].idxmax()
    recovered = drawdown.loc[trough:][drawdown.loc[trough:] >= 0.0]
    recovery = recovered.index[0] if not recovered.empty else None
    return [
        DrawdownEpisode(
            peak,
            trough,
            float(drawdown.loc[trough]),
            recovery,
            (trough - peak).days,
            (recovery - trough).days if recovery else None,
        )
    ]


def _half_consistency(returns: pd.Series) -> float:
    if len(returns) < 4:
        return 0.0
    mid = len(returns) // 2
    s1 = _sharpe(returns.iloc[:mid])
    s2 = _sharpe(returns.iloc[mid:])
    return max(0.0, min(1.0, 1.0 - abs(s1 - s2) / max(abs(s1), abs(s2), 0.1)))


def _sharpe(returns: pd.Series) -> float:
    std = returns.std(ddof=0)
    return float(returns.mean() / std * math.sqrt(365.0)) if std else 0.0


def _top_3_months_pct(equity: pd.Series) -> float:
    series = equity.copy()
    series.index = pd.to_datetime(series.index)
    monthly = series.resample("ME").last().pct_change().dropna()
    positives = sorted(
        [float(value) for value in monthly if float(value) > 0], reverse=True
    )
    total = sum(positives)
    return sum(positives[:3]) / total if total > 0 else 0.0


def _worst_rolling_return(equity: pd.Series, days: int) -> float:
    if len(equity) < 2:
        return 0.0
    values = [
        float(equity.iloc[end] / equity.iloc[start] - 1.0)
        for start in range(len(equity))
        for end in [min(len(equity) - 1, start + days)]
        if end > start
    ]
    return min(values) if values else 0.0


def _worst_rolling_drawdown(equity: pd.Series, days: int) -> float:
    worst = 0.0
    for start in range(len(equity)):
        window = equity.iloc[start : start + days + 1]
        if len(window) > 1:
            worst = min(worst, float((window / window.cummax() - 1.0).min()))
    return worst
