from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import polars as pl

from agent_invest_scripts._lib.backtest import BacktestResult


def write_report(
    result: BacktestResult,
    out_dir: Path,
    *,
    spec: dict[str, Any] | None = None,
    benchmark_prices: pl.DataFrame | None = None,
) -> dict[str, Any]:
    """Write chart and report artifacts into out_dir.

    Returns paths to the generated artifacts.
    """

    out_dir.mkdir(parents=True, exist_ok=True)

    equity_curve_png = out_dir / "equity_curve.png"
    drawdown_png = out_dir / "drawdown.png"
    equity_curve_json = out_dir / "equity_curve.json"
    drawdown_json = out_dir / "drawdown.json"
    allocation_json = out_dir / "allocation.json"
    target_allocation_json = out_dir / "target_allocation.json"
    report_json = out_dir / "report.json"
    equity_curve = _equity_curve_points(result.performance)
    benchmark_curve = _bitcoin_benchmark_points(
        result.performance,
        benchmark_prices,
        initial_capital_usd=float(result.summary.get("initial_capital_usd", 1.0)),
    )

    _write_equity_curve_png(result.performance, equity_curve_png)
    _write_drawdown_png(result.performance, drawdown_png)
    _write_json(
        equity_curve_json, _merge_benchmark_equity(equity_curve, benchmark_curve)
    )
    _write_json(drawdown_json, _drawdown_points(equity_curve, benchmark_curve))
    _write_json(allocation_json, _final_allocation_points(result.weights))
    _write_json(target_allocation_json, _target_allocation_points(spec))
    report_payload = {
        "kpis": result.summary,
        "equity_curve": equity_curve,
        "spec": spec,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    report_json.write_text(
        json.dumps(report_payload, indent=2) + "\n", encoding="utf-8"
    )

    return {
        "kpis": result.summary,
        "equity_curve_png": str(equity_curve_png),
        "drawdown_png": str(drawdown_png),
        "equity_curve_json": str(equity_curve_json),
        "drawdown_json": str(drawdown_json),
        "allocation_json": str(allocation_json),
        "target_allocation_json": str(target_allocation_json),
        "report_json": str(report_json),
    }


def _equity_curve_points(performance: pl.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "date": row["date"].isoformat(),
            "equity": float(row["equity"]),
            "equity_usd": float(row["equity_usd"]),
        }
        for row in performance.select("date", "equity", "equity_usd").to_dicts()
    ]


def _drawdown_series(equity_curve: list[float]) -> list[float]:
    peak = 0.0
    drawdowns: list[float] = []

    for equity in equity_curve:
        peak = max(peak, equity)
        if peak <= 0:
            drawdowns.append(0.0)
            continue
        drawdowns.append(equity / peak - 1.0)

    return drawdowns


def _bitcoin_benchmark_points(
    performance: pl.DataFrame,
    benchmark_prices: pl.DataFrame | None,
    *,
    initial_capital_usd: float,
) -> dict[str, dict[str, float]]:
    if benchmark_prices is None or performance.is_empty():
        return {}

    dates = performance.get_column("date").to_list()
    bitcoin_prices = (
        benchmark_prices.filter(
            (pl.col("coin_id") == "bitcoin") & pl.col("date").is_in(dates)
        )
        .select("date", "price")
        .sort("date")
    )
    if bitcoin_prices.is_empty():
        return {}

    first_price = float(bitcoin_prices.get_column("price")[0])
    if first_price <= 0:
        return {}

    return {
        row["date"].isoformat(): {
            "bitcoin_equity": float(row["price"]) / first_price,
            "bitcoin_equity_usd": initial_capital_usd
            * float(row["price"])
            / first_price,
        }
        for row in bitcoin_prices.to_dicts()
    }


def _merge_benchmark_equity(
    equity_curve: list[dict[str, Any]], benchmark_curve: dict[str, dict[str, float]]
) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for point in equity_curve:
        merged = dict(point)
        merged.update(benchmark_curve.get(point["date"], {}))
        points.append(merged)
    return points


def _drawdown_points(
    equity_curve: list[dict[str, Any]], benchmark_curve: dict[str, dict[str, float]]
) -> list[dict[str, Any]]:
    strategy_drawdowns = _drawdown_series([point["equity"] for point in equity_curve])
    benchmark_values = [
        benchmark_curve[point["date"]]["bitcoin_equity"]
        for point in equity_curve
        if point["date"] in benchmark_curve
    ]
    benchmark_drawdowns = _drawdown_series(benchmark_values)
    benchmark_by_date = {
        point["date"]: drawdown
        for point, drawdown in zip(
            (point for point in equity_curve if point["date"] in benchmark_curve),
            benchmark_drawdowns,
            strict=True,
        )
    }

    points: list[dict[str, Any]] = []
    for point, drawdown in zip(equity_curve, strategy_drawdowns, strict=True):
        drawdown_point = {"date": point["date"], "drawdown": drawdown}
        if point["date"] in benchmark_by_date:
            drawdown_point["bitcoin_drawdown"] = benchmark_by_date[point["date"]]
        points.append(drawdown_point)
    return points


def _final_allocation_points(weights: pl.DataFrame) -> list[dict[str, Any]]:
    if weights.is_empty():
        return []

    final_date = weights.get_column("date").max()
    return [
        {
            "date": row["date"].isoformat(),
            "coin_id": row["coin_id"],
            "weight": float(row["weight"]),
        }
        for row in weights.filter(pl.col("date") == final_date)
        .sort("coin_id")
        .to_dicts()
    ]


def _target_allocation_points(spec: dict[str, Any] | None) -> list[dict[str, Any]]:
    allocation = spec.get("allocation") if isinstance(spec, dict) else None
    if not isinstance(allocation, dict):
        return []
    if allocation.get("type") == "static" and isinstance(
        allocation.get("weights"), dict
    ):
        return [
            {"coin_id": str(coin_id), "weight": float(weight)}
            for coin_id, weight in sorted(allocation["weights"].items())
        ]
    if allocation.get("type") == "weights" and isinstance(allocation.get("rows"), list):
        rows = [row for row in allocation["rows"] if isinstance(row, dict)]
        latest_date = max((row.get("date") for row in rows), default=None)
        if latest_date is None:
            return []
        return [
            {
                "date": latest_date,
                "coin_id": row["coin_id"],
                "weight": float(row["weight"]),
            }
            for row in rows
            if row.get("date") == latest_date
            and isinstance(row.get("coin_id"), str)
            and isinstance(row.get("weight"), int | float)
        ]
    return []


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_equity_curve_png(performance: pl.DataFrame, path: Path) -> None:
    dates = performance.get_column("date").to_list()
    equity_curve = [
        float(value) for value in performance.get_column("equity").to_list()
    ]
    plotted_equity = [max(value, 1e-12) for value in equity_curve]
    drawdowns = _drawdown_series(equity_curve)

    figure, (equity_axis, drawdown_axis) = plt.subplots(
        2,
        1,
        figsize=(10, 6),
        sharex=True,
        gridspec_kw={"height_ratios": (3, 1)},
    )

    equity_axis.plot(dates, plotted_equity, color="#1f77b4", linewidth=2)
    equity_axis.set_yscale("log")
    equity_axis.set_ylabel("Equity")
    equity_axis.grid(True, alpha=0.3)

    drawdown_axis.fill_between(dates, drawdowns, 0.0, color="#d62728", alpha=0.25)
    drawdown_axis.plot(dates, drawdowns, color="#d62728", linewidth=1)
    drawdown_axis.set_ylabel("Drawdown")
    drawdown_axis.set_xlabel("Date")
    drawdown_min = min(drawdowns + [0.0])
    drawdown_axis.set_ylim(drawdown_min if drawdown_min < 0 else -0.01, 0.0)
    drawdown_axis.grid(True, alpha=0.3)

    figure.tight_layout()
    figure.savefig(path, dpi=150)
    plt.close(figure)


def _write_drawdown_png(performance: pl.DataFrame, path: Path) -> None:
    dates = performance.get_column("date").to_list()
    equity_curve = [
        float(value) for value in performance.get_column("equity").to_list()
    ]
    drawdowns = _drawdown_series(equity_curve)

    figure, axis = plt.subplots(figsize=(10, 3))
    axis.fill_between(dates, drawdowns, 0.0, color="#d62728", alpha=0.25)
    axis.plot(dates, drawdowns, color="#d62728", linewidth=1.5)
    axis.set_ylabel("Drawdown")
    axis.set_xlabel("Date")
    drawdown_min = min(drawdowns + [0.0])
    axis.set_ylim(drawdown_min if drawdown_min < 0 else -0.01, 0.0)
    axis.grid(True, alpha=0.3)
    figure.tight_layout()
    figure.savefig(path, dpi=150)
    plt.close(figure)
