"""Run a bounded batch of candidate template backtests."""

from __future__ import annotations

import argparse
import json
import math
import os
import tempfile
from concurrent.futures import ProcessPoolExecutor
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
import polars as pl

from agent_invest_scripts._lib import daily_prices, print_json, storage_root
from agent_invest_scripts._lib.backtest.benchmarks import benchmark_for
from agent_invest_scripts._lib.backtest.composite_scorers import SCORERS
from agent_invest_scripts._lib.backtest.engine import run_backtest
from agent_invest_scripts._lib.backtest.result import (
    AllocationMetrics,
    BacktestMetrics,
    BacktestResult,
    DrawdownEpisode,
    Rebalance,
    RobustnessSignals,
    to_dict,
)
from agent_invest_scripts._lib.backtest.templates import TEMPLATES
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    resolve_timeout_seconds,
    script_timeout,
)
from agent_invest_scripts._lib.data import asset_universe_features
from agent_invest_scripts._lib.storage import normalize_identifier
from agent_invest_scripts._lib.backtest.window import recommend_backtest_window
from agent_invest_scripts.rank_universe import rank_universe_extended

_DEFAULT_MAX_CANDIDATES = 8
_MIN_CANDIDATES = 3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a candidate backtest batch.")
    parser.add_argument("--input", required=True, help="RunCandidateBatchInput JSON")
    add_timeout_argument(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            payload = run(json.loads(args.input))
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)
    print_json(payload)
    return 0


def run(input_payload: dict[str, Any]) -> dict[str, Any]:
    _validate_batch_input(input_payload)
    batch_id = f"candidate_batch_{uuid4().hex}"
    prices = daily_prices()
    features = asset_universe_features()
    jobs = [
        {
            "candidate": candidate,
            "round": input_payload["round"],
            "prices": prices,
            "features": features,
        }
        for candidate in input_payload["candidates"]
    ]

    max_workers = min(len(jobs), os.cpu_count() or 1)
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(_run_candidate_job, jobs))

    output = {
        "batch_id": batch_id,
        "run_id": input_payload["run_id"],
        "round": input_payload["round"],
        "results": results,
    }
    _persist_batch_atomic(output)
    return output


def _validate_batch_input(input_payload: dict[str, Any]) -> None:
    if not isinstance(input_payload, dict):
        raise ValueError("RunCandidateBatchInput must be an object")
    normalize_identifier(str(input_payload.get("run_id", "")), "run_id")
    if input_payload.get("round") not in {1, 2, 3}:
        raise ValueError("round must be 1, 2, or 3")
    candidates = input_payload.get("candidates")
    if not isinstance(candidates, list):
        raise ValueError("candidates must be an array")
    if len(candidates) < _MIN_CANDIDATES:
        raise ValueError("run_candidate_batch requires at least 3 candidates")
    max_candidates = int(os.getenv("RUN_CANDIDATE_BATCH_MAX", _DEFAULT_MAX_CANDIDATES))
    if len(candidates) > max_candidates:
        raise ValueError(
            f"run_candidate_batch accepts at most {max_candidates} candidates"
        )
    ids = [
        candidate.get("candidate_id")
        for candidate in candidates
        if isinstance(candidate, dict)
    ]
    if len(ids) != len(candidates) or any(
        not isinstance(item, str) or not item for item in ids
    ):
        raise ValueError("each candidate must include a non-empty candidate_id")
    if len(set(ids)) != len(ids):
        raise ValueError("candidate_id values must be unique within a batch")


def _run_candidate_job(job: dict[str, Any]) -> dict[str, Any]:
    candidate = job["candidate"]
    if not isinstance(candidate, dict):
        raise ValueError("candidate must be an object")
    template_id = candidate.get("template_id")
    if template_id == "mixed":
        raise ValueError("template_id=mixed is not supported by run_candidate_batch")
    if template_id not in TEMPLATES:
        raise ValueError(f"unknown template_id: {template_id}")
    template = TEMPLATES[template_id]
    config = dict(candidate.get("config") or {})
    if (
        "select_top" in candidate
        and "select_top" not in config
        and "select_top" in template.METADATA.slot_schema
    ):
        config["select_top"] = candidate["select_top"]
    selection_limit = candidate.get("select_top", config.get("select_top"))
    if not isinstance(selection_limit, int) or selection_limit < 1:
        raise ValueError("candidate must provide select_top as a positive integer")
    template.validate_config(config)

    prices = job["prices"].copy()
    features = job["features"].copy()
    window = _resolve_window(candidate, template, prices)
    ranked = rank_universe_extended(
        features,
        prices,
        {
            "universe_selector": _universe_selector(
                candidate.get("universe_override") or template.METADATA.default_universe
            ),
            "filters": candidate.get("filters", []),
            "ranking": candidate.get("ranking") or _default_ranking(template),
            "limit": selection_limit,
        },
        as_of=window[0],
    )
    universe = pd.DataFrame(ranked)
    price_map = _price_map(prices)
    plan = template.build(universe, price_map, config, window)
    holdings = _holdings_from_plan(plan, prices, window)
    targets = _engine_targets(holdings, prices, window)
    engine_result = run_backtest(
        _prices_long(prices, window),
        targets,
        universe=list(_holding_assets(holdings)),
    )
    equity_curve = _equity_curve(engine_result.performance)
    benchmark_curve = _benchmark_curve(candidate, window, price_map, equity_curve.index)
    metrics = _metrics(engine_result.summary)
    allocation_metrics = _allocation_metrics(engine_result)
    robustness = _robustness(
        engine_result, equity_curve, benchmark_curve, allocation_metrics
    )
    scorer = SCORERS[template.METADATA.composite_formula]
    result = BacktestResult(
        candidate_id=candidate["candidate_id"],
        template_id=template_id,
        config=config,
        window=window,
        equity_curve=equity_curve,
        benchmark_curve=benchmark_curve,
        drawdown_episodes=_drawdown_episodes(equity_curve),
        metrics=metrics,
        robustness=robustness,
        composite_score=scorer.score(
            metrics, candidate.get("thesis", {}).get("primary_factors", [])
        ),
        allocation_metrics=allocation_metrics,
        tactical_metrics=None,
    )
    return to_dict(result)


def _resolve_window(
    candidate: dict[str, Any], template: Any, prices: pd.DataFrame
) -> tuple[date, date]:
    override = candidate.get("window_override")
    if isinstance(override, dict):
        return (_parse_date(override["start"]), _parse_date(override["end"]))
    coin_ids = sorted(set(str(value) for value in prices["coin_id"].dropna().unique()))
    payload = recommend_backtest_window(
        pl.from_pandas(prices),
        coin_ids=coin_ids,
        horizon_days=max(template.METADATA.min_history_days, 365),
    )
    return (_parse_date(payload["start"]), _parse_date(payload["end"]))


def _default_ranking(template: Any) -> list[dict[str, Any]]:
    factor = (
        template.METADATA.preferred_factors[0]
        if template.METADATA.preferred_factors
        else "market_cap_rank"
    )
    return [
        {
            "factor": factor,
            "direction": "low" if factor.endswith("rank") else "high",
            "weight": 1.0,
        }
    ]


def _universe_selector(selector: dict[str, Any]) -> dict[str, Any]:
    if "id" in selector:
        return selector
    if "selector" in selector:
        return {
            "id": selector["selector"],
            "params": {
                key: value for key, value in selector.items() if key != "selector"
            },
        }
    return selector


def _price_map(prices: pd.DataFrame) -> dict[str, pd.DataFrame]:
    result: dict[str, pd.DataFrame] = {}
    for coin_id, frame in prices.groupby("coin_id"):
        normalized = frame.copy()
        if "close" not in normalized.columns and "price" in normalized.columns:
            normalized["close"] = normalized["price"]
        result[coin_id] = normalized
    return result


def _prices_long(prices: pd.DataFrame, window: tuple[date, date]) -> pl.DataFrame:
    frame = prices.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    frame = frame[(frame["date"] >= window[0]) & (frame["date"] <= window[1])]
    return pl.from_pandas(frame).select("date", "coin_id", "price")


def _holdings_from_plan(
    plan: dict[str, Any], prices: pd.DataFrame, window: tuple[date, date]
) -> dict[date, dict[str, float]]:
    if "holdings" in plan:
        return plan["holdings"]
    if "signals" not in plan or "sizing" not in plan:
        raise ValueError("template plan must include holdings or signals+sizing")

    dates = sorted(
        pd.to_datetime(
            prices[
                (pd.to_datetime(prices["date"]).dt.date >= window[0])
                & (pd.to_datetime(prices["date"]).dt.date <= window[1])
            ]["date"]
        ).dt.date.unique()
    )
    sizing = {str(key): float(value) for key, value in plan["sizing"].items()}
    signal_states = {
        coin_id: series.cumsum().reindex(dates, fill_value=0).ffill().fillna(0)
        for coin_id, series in plan["signals"].items()
    }
    holdings: dict[date, dict[str, float]] = {}
    last_weights: dict[str, float] | None = None
    for current_date in dates:
        weights = {
            coin_id: sizing.get(coin_id, 0.0)
            for coin_id, state in signal_states.items()
            if float(state.loc[current_date]) > 0.0 and sizing.get(coin_id, 0.0) > 0.0
        }
        total = sum(weights.values())
        if total > 1.0:
            weights = {coin_id: weight / total for coin_id, weight in weights.items()}
        if weights != last_weights:
            holdings[current_date] = weights
            last_weights = dict(weights)
    return holdings or {window[0]: {}}


def _holding_assets(holdings: dict[date, dict[str, float]]) -> set[str]:
    return {coin_id for weights in holdings.values() for coin_id in weights}


def _engine_targets(
    holdings: dict[date, dict[str, float]],
    prices: pd.DataFrame,
    window: tuple[date, date],
) -> dict[date, dict[str, float]]:
    dates = sorted(
        pd.to_datetime(
            prices[
                (pd.to_datetime(prices["date"]).dt.date >= window[0])
                & (pd.to_datetime(prices["date"]).dt.date <= window[1])
            ]["date"]
        ).dt.date.unique()
    )
    targets = dict(holdings)
    if len(dates) > 1 and dates[0] in targets:
        first = targets.pop(dates[0])
        targets.setdefault(dates[1], first)
    return targets


def _equity_curve(performance: pl.DataFrame) -> pd.Series:
    return pd.Series(
        performance.get_column("equity").to_list(),
        index=pd.Index(performance.get_column("date").to_list(), name="date"),
        name="value",
        dtype="float64",
    )


def _benchmark_curve(
    candidate: dict[str, Any],
    window: tuple[date, date],
    prices: dict[str, pd.DataFrame],
    index: pd.Index,
) -> pd.Series:
    objective = candidate.get("thesis", {}).get("objective", "balanced")
    curve = benchmark_for(objective).equity_curve(window, prices)
    curve.index = pd.to_datetime(curve.index).date
    return curve.reindex(index).ffill().bfill().rename("value")


def _metrics(summary: dict[str, Any]) -> BacktestMetrics:
    return BacktestMetrics(
        total_return=float(summary["total_return"]),
        cagr=float(summary["cagr"]),
        period_return=float(summary["period_return"]),
        volatility=float(summary["annualized_volatility"]),
        max_drawdown=float(summary["max_drawdown"]),
        max_drawdown_duration_days=0,
        sharpe=float(summary["sharpe_ratio"]),
        sortino=float(summary["sortino_ratio"]),
        calmar=float(summary["calmar_ratio"]),
        total_fees_paid=float(summary.get("total_trading_cost_usd", 0.0)),
        total_slippage=0.0,
        return_365d=float(summary["cagr"]),
        recovery_rate=None,
        pct_horizon_windows_positive=float(summary.get("monthly_hit_rate", 0.0)),
    )


def _allocation_metrics(result: Any) -> AllocationMetrics:
    rebalances = [
        Rebalance(
            row["date"],
            _weights_on(result.weights, row["date"]),
            float(row["turnover"]),
            float(row["trading_cost_usd"]),
        )
        for row in result.performance.filter(pl.col("turnover") > 0).to_dicts()
    ]
    holdings_history = [
        {"date": day, "weights": _weights_on(result.weights, day)}
        for day in result.weights.select("date")
        .unique()
        .sort("date")
        .get_column("date")
        .to_list()
    ]
    turnovers = [item.turnover_pct for item in rebalances]
    max_weight = max(
        (weight for item in holdings_history for weight in item["weights"].values()),
        default=0.0,
    )
    return AllocationMetrics(
        rebalances,
        sum(turnovers) / len(turnovers) if turnovers else 0.0,
        max_weight,
        holdings_history,
    )


def _weights_on(weights: pl.DataFrame, day: date) -> dict[str, float]:
    return {
        row["coin_id"]: float(row["weight"])
        for row in weights.filter(pl.col("date") == day).to_dicts()
    }


def _robustness(
    result: Any, equity: pd.Series, benchmark: pd.Series, allocation: AllocationMetrics
) -> RobustnessSignals:
    returns = equity.pct_change().dropna()
    benchmark_returns = benchmark.pct_change().dropna().reindex(returns.index).dropna()
    aligned = returns.reindex(benchmark_returns.index).dropna()
    half_score = _half_consistency(returns)
    top_3_months = _top_3_months_pct(result.performance)
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
        n_rebalances=len(allocation.rebalances),
        duration_days=duration,
        sample_size_warning=len(allocation.rebalances) < 3 or duration < 180,
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
        survivorship_warning=bool(result.summary.get("survivorship_warning", False)),
    )


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


def _top_3_months_pct(performance: pl.DataFrame) -> float:
    monthly = (
        performance.with_columns(pl.col("date").dt.strftime("%Y-%m").alias("month"))
        .group_by("month")
        .agg(((pl.col("net_return") + 1.0).product() - 1.0).alias("ret"))
    )
    positives = sorted(
        [float(value) for value in monthly.get_column("ret") if float(value) > 0],
        reverse=True,
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


def _drawdown_episodes(equity: pd.Series) -> list[DrawdownEpisode]:
    drawdown = equity / equity.cummax() - 1.0
    trough = drawdown.idxmin()
    if float(drawdown.min()) >= 0.0:
        return []
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


def _persist_batch_atomic(output: dict[str, Any]) -> None:
    directory = storage_root() / "candidate_batches"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{output['batch_id']}.json"
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{output['batch_id']}.", suffix=".tmp", dir=directory
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(output, handle, indent=2)
            handle.write("\n")
        Path(tmp_name).replace(target)
    except Exception:
        Path(tmp_name).unlink(missing_ok=True)
        raise


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


if __name__ == "__main__":
    raise SystemExit(main())
