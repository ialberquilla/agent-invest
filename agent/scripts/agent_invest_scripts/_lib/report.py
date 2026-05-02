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
) -> dict[str, Any]:
    """Write equity_curve.png + report.json into out_dir.

    Returns {kpis, equity_curve_png, drawdown_png, report_json}.
    """

    out_dir.mkdir(parents=True, exist_ok=True)

    equity_curve_png = out_dir / "equity_curve.png"
    drawdown_png = out_dir / "drawdown.png"
    report_json = out_dir / "report.json"
    equity_curve = _equity_curve_points(result.performance)

    _write_equity_curve_png(result.performance, equity_curve_png)
    _write_drawdown_png(result.performance, drawdown_png)
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
