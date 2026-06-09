"""Run a ``bt`` recipe and shape the result into the existing ``BacktestResult``
contract, so ``validate_against_thesis`` / ``run_and_validate`` / ``decide`` are
untouched -- only the producer of the contract changes.

Everything the workflow reads off a candidate comes from one ``bt`` run:
``max_drawdown`` from ffn stats, ``max_single_weight`` from the (gross) security
weights, ``select_top`` from config. The robustness / benchmark / scoring
signals are reconstructed off ``bt``'s equity curve via the shared
``robustness`` module.
"""

from __future__ import annotations

from datetime import date

import bt
import pandas as pd

from agent_invest_scripts._lib.backtest.composite_scorers import SCORERS
from agent_invest_scripts._lib.backtest.result import (
    AllocationMetrics,
    BacktestMetrics,
    BacktestResult,
    Rebalance,
)
from agent_invest_scripts._lib.backtest.robustness import (
    benchmark_curve,
    compute_robustness,
    drawdown_episodes,
)

# The polars engine modelled CoW-style costs (proportional bps + flat gas). bt
# wants a per-trade $ callback, so we approximate with the same proportional
# bps applied to each fill's notional plus the flat gas. The workflow doesn't
# gate on fees, so small divergence is acceptable (flagged in the PR).
_INITIAL_CAPITAL = 1_000_000.0
_PROPORTIONAL_BPS = 102.0  # protocol 2 + widget 70 + slippage 30
_GAS_USD_PER_SWAP = 1.0


def run_recipe(
    recipe,
    universe: pd.DataFrame,
    prices_wide: pd.DataFrame,
    config: dict,
    window: tuple[date, date],
    *,
    candidate: dict,
) -> BacktestResult:
    coins = [str(coin) for coin in universe["coin_id"].tolist()]
    strategy = recipe.build(universe, prices_wide, config, window)
    clean = _prepare(prices_wide, coins, window)

    def commissions(quantity: float, price: float) -> float:
        # Purely proportional: bt's order-sizing solver bisects on the
        # commission and requires it to be smooth in quantity, so the flat
        # per-swap gas can't live here -- it's added to the fee total below.
        return abs(quantity * price) * _PROPORTIONAL_BPS / 10_000.0

    backtest = bt.Backtest(
        strategy,
        clean,
        initial_capital=_INITIAL_CAPITAL,
        commissions=commissions,
        # Crypto trades fractionally; integer share rounding also makes bt's
        # commission-aware order sizer fail to converge under full investment.
        integer_positions=False,
    )
    result = bt.run(backtest)

    equity = _equity_series(backtest)
    security_weights = _security_weights(result)
    rebalances = _rebalances(result, equity)

    total_fees = sum(rebalance.cost_paid for rebalance in rebalances)
    metrics = _metrics(result, equity, total_fees=total_fees)
    allocation = _allocation_metrics(security_weights, rebalances)

    objective = candidate.get("thesis", {}).get("objective", "balanced")
    benchmark = benchmark_curve(
        objective, window, _price_map(prices_wide), equity.index
    )
    robustness = compute_robustness(equity, benchmark, n_rebalances=len(rebalances))
    scorer = SCORERS[recipe.METADATA.composite_formula]
    return BacktestResult(
        candidate_id=candidate["candidate_id"],
        template_id=recipe.METADATA.id,
        config=config,
        window=window,
        equity_curve=equity,
        benchmark_curve=benchmark,
        drawdown_episodes=drawdown_episodes(equity),
        metrics=metrics,
        robustness=robustness,
        composite_score=scorer.score(
            metrics, candidate.get("thesis", {}).get("primary_factors", [])
        ),
        allocation_metrics=allocation,
        tactical_metrics=None,
    )


# --- bt extraction -----------------------------------------------------------


def _prepare(
    prices: pd.DataFrame, coins: list[str], window: tuple[date, date]
) -> pd.DataFrame:
    """Slice to the requested coins + window, coerce Decimal/object to float
    (bt's isnan checks choke on Decimals), forward-fill gaps, and drop the
    ragged head until every coin has data."""
    frame = prices[coins].sort_index().apply(pd.to_numeric, errors="coerce")
    frame.index = pd.to_datetime(frame.index)
    mask = (frame.index.date >= window[0]) & (frame.index.date <= window[1])
    frame = frame[mask].astype(float).ffill().dropna()
    if frame.empty:
        raise ValueError("no price rows in the requested window after cleaning")
    return frame


def _equity_series(backtest: bt.Backtest) -> pd.Series:
    equity = backtest.strategy.values.astype(float)
    equity.index = pd.to_datetime(equity.index).date
    return equity.rename("value")


def _security_weights(result: bt.backtest.Result) -> pd.DataFrame:
    weights = result.get_security_weights()
    weights.index = pd.to_datetime(weights.index).date
    return weights


def _rebalances(result: bt.backtest.Result, equity: pd.Series) -> list[Rebalance]:
    transactions = _transactions(result)
    if transactions.empty:
        return []
    equity_by_date = {day: float(value) for day, value in equity.items()}
    rebalances: list[Rebalance] = []
    for stamp, group in transactions.groupby(level="Date"):
        day = pd.Timestamp(stamp).date()
        notional = float((group["quantity"].abs() * group["price"]).sum())
        value = equity_by_date.get(day, _INITIAL_CAPITAL)
        # Gross notional / value counts both legs; halve for one-way turnover.
        turnover = notional / value / 2.0 if value else 0.0
        cost = float(
            (group["quantity"].abs() * group["price"]).sum()
            * _PROPORTIONAL_BPS
            / 10_000.0
            + _GAS_USD_PER_SWAP * len(group)
        )
        rebalances.append(
            Rebalance(day, _weights_on_date(result, day), turnover, cost)
        )
    return rebalances


def _transactions(result: bt.backtest.Result) -> pd.DataFrame:
    # bt raises IndexError when a strategy never traded (e.g. WeighInvVol under
    # RunOnce has no lookback and never invests); treat that as no rebalances.
    try:
        return result.get_transactions()
    except IndexError:
        return pd.DataFrame(columns=["price", "quantity"])


def _weights_on_date(result: bt.backtest.Result, day: date) -> dict[str, float]:
    weights = result.get_security_weights()
    weights.index = pd.to_datetime(weights.index).date
    if day not in weights.index:
        return {}
    row = weights.loc[day]
    return {
        str(coin): float(weight)
        for coin, weight in row.items()
        if abs(float(weight)) > 1e-9
    }


def _metrics(
    result: bt.backtest.Result, equity: pd.Series, *, total_fees: float
) -> BacktestMetrics:
    stats = result.stats.iloc[:, 0]
    monthly = _monthly_hit_rate(equity)
    return BacktestMetrics(
        total_return=_as_float(stats.get("total_return")),
        cagr=_as_float(stats.get("cagr")),
        period_return=_as_float(stats.get("total_return")),
        volatility=_as_float(stats.get("yearly_vol")),
        max_drawdown=_as_float(stats.get("max_drawdown")),
        max_drawdown_duration_days=0,
        sharpe=_as_float(stats.get("daily_sharpe")),
        sortino=_as_float(stats.get("daily_sortino")),
        calmar=_as_float(stats.get("calmar")),
        total_fees_paid=total_fees,
        total_slippage=0.0,
        return_365d=_as_float(stats.get("cagr")),
        recovery_rate=None,
        pct_horizon_windows_positive=monthly,
    )


def _monthly_hit_rate(equity: pd.Series) -> float:
    series = equity.copy()
    series.index = pd.to_datetime(series.index)
    monthly = series.resample("ME").last().pct_change().dropna()
    return float((monthly > 0).mean()) if len(monthly) else 0.0


def _allocation_metrics(
    security_weights: pd.DataFrame, rebalances: list[Rebalance]
) -> AllocationMetrics:
    # Gross max weight so a future short leg counts at its absolute size.
    max_weight = (
        float(security_weights.abs().max().max())
        if not security_weights.empty
        else 0.0
    )
    holdings_history = [
        {"date": rebalance.date, "weights": rebalance.weights}
        for rebalance in rebalances
    ]
    turnovers = [rebalance.turnover_pct for rebalance in rebalances]
    return AllocationMetrics(
        rebalances,
        sum(turnovers) / len(turnovers) if turnovers else 0.0,
        max_weight,
        holdings_history,
    )


def _price_map(prices_wide: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Rebuild the per-coin long frames the benchmarks expect from the wide
    frame (date + price columns). Reindex to a continuous daily calendar and
    forward-fill interior gaps -- the benchmark module requires a gap-free
    series over the window, and the DB has occasional missing days."""
    index = pd.to_datetime(prices_wide.index)
    daily = pd.date_range(index.min(), index.max(), freq="D")
    result: dict[str, pd.DataFrame] = {}
    for coin in prices_wide.columns:
        series = pd.to_numeric(prices_wide[coin], errors="coerce")
        series.index = index
        series = series.reindex(daily).ffill()
        result[str(coin)] = pd.DataFrame(
            {"date": daily, "price": series.to_numpy()}
        ).dropna()
    return result


def _as_float(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return float("nan")
