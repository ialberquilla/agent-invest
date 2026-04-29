from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from agent_invest_scripts._lib.backtest import (
    TradingCostModel,
    run_cross_sectional_momentum_backtest,
)
from agent_invest_scripts._lib.report import write_report

ZERO_COST_MODEL = TradingCostModel(
    protocol_bps=0.0,
    widget_bps=0.0,
    slippage_bps=0.0,
    gas_usd_per_swap=0.0,
)


def _build_result():
    start_date = date(2024, 1, 1)
    rows: list[dict[str, object]] = []
    coin_a_price = 100.0
    coin_b_price = 100.0

    for offset in range(40):
        current_date = start_date + timedelta(days=offset)
        coin_a_price *= 1.012 if offset % 6 != 0 else 0.97
        coin_b_price *= 1.002
        rows.append({"date": current_date, "coin_id": "coin-a", "price": coin_a_price})
        rows.append({"date": current_date, "coin_id": "coin-b", "price": coin_b_price})

    return run_cross_sectional_momentum_backtest(
        pl.DataFrame(rows),
        universe=["coin-a", "coin-b"],
        lookback_days=5,
        top_k=1,
        rebalance_frequency="weekly",
        cost_model=ZERO_COST_MODEL,
    )


def test_write_report_writes_png_and_json_and_returns_paths(tmp_path: Path) -> None:
    result = _build_result()
    out_dir = tmp_path / "artifacts"

    report = write_report(
        result, out_dir, spec={"signal_type": "cross_sectional_momentum"}
    )

    equity_curve_png = out_dir / "equity_curve.png"
    report_json = out_dir / "report.json"

    assert equity_curve_png.is_file()
    assert report_json.is_file()
    assert report == {
        "kpis": result.summary,
        "equity_curve_png": str(equity_curve_png),
        "report_json": str(report_json),
    }


def test_write_report_json_contains_required_kpis(tmp_path: Path) -> None:
    result = _build_result()
    out_dir = tmp_path / "artifacts"
    spec = {"signal_type": "cross_sectional_momentum"}

    write_report(result, out_dir, spec=spec)

    payload = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))

    assert payload["kpis"] == result.summary
    assert {
        "cagr",
        "annualized_volatility",
        "sharpe_ratio",
        "sortino_ratio",
        "max_drawdown",
        "calmar_ratio",
        "monthly_hit_rate",
        "average_daily_turnover",
        "average_holding_count",
        "worst_month",
        "best_month",
    }.issubset(payload["kpis"])
    assert payload["equity_curve"] == [
        {
            "date": row["date"].isoformat(),
            "equity": float(row["equity"]),
            "equity_usd": float(row["equity_usd"]),
        }
        for row in result.performance.select("date", "equity", "equity_usd").to_dicts()
    ]
    assert payload["spec"] == spec
    assert isinstance(payload["generated_at"], str)
