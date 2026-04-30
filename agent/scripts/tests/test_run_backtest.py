from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from agent_invest_scripts import run_backtest


def _daily_prices_fixture() -> pd.DataFrame:
    start_date = date(2024, 1, 1)
    rows: list[dict[str, object]] = []
    coin_a_price = 100.0
    coin_b_price = 100.0

    for offset in range(30):
        current_date = start_date + timedelta(days=offset)
        coin_a_price *= 1.01
        coin_b_price *= 1.001
        rows.append(
            {
                "date": current_date.isoformat(),
                "coin_id": "coin-a",
                "price": coin_a_price,
            }
        )
        rows.append(
            {
                "date": current_date.isoformat(),
                "coin_id": "coin-b",
                "price": coin_b_price,
            }
        )

    return pd.DataFrame(rows)


def _universe_history_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "snapshot_date": "2024-01-01",
                "coin_id": "coin-a",
                "market_cap_rank": 1,
                "included": True,
            },
            {
                "snapshot_date": "2024-01-01",
                "coin_id": "coin-b",
                "market_cap_rank": 2,
                "included": True,
            },
        ]
    )


def _write_fixture_datasets(storage_root: Path) -> None:
    datasets_dir = storage_root / "datasets"
    datasets_dir.mkdir(parents=True, exist_ok=True)
    _daily_prices_fixture().to_parquet(
        datasets_dir / "daily_prices.parquet", index=False
    )
    _universe_history_fixture().to_parquet(
        datasets_dir / "universe_history.parquet", index=False
    )


def test_run_backtest_cli_writes_report_contract_and_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    storage_root = tmp_path / "storage"
    _write_fixture_datasets(storage_root)
    monkeypatch.setenv("STORAGE_ROOT", str(storage_root))

    artifacts_dir = (
        storage_root
        / "users"
        / "user-1"
        / "strategies"
        / "strategy-1"
        / "artifacts"
        / "run-1"
    )

    run_backtest.main(
        [
            "--spec",
            json.dumps(
                {
                    "user_id": "user-1",
                    "strategy_id": "strategy-1",
                    "run_id": "run-1",
                    "signal_type": "cross_sectional_momentum",
                    "lookback_days": 5,
                    "top_k": 1,
                    "rebalance_frequency": "weekly",
                    "skip_days": 0,
                    "initial_capital_usd": 1000.0,
                    "costs": {
                        "protocol_bps": 0.0,
                        "widget_bps": 0.0,
                        "slippage_bps": 0.0,
                        "gas_usd_per_swap": 0.0,
                    },
                }
            ),
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert captured.err == ""
    assert payload == {
        "kpis": {
            "cagr": 16.822277586461723,
            "annualized_volatility": 0.07739055791387732,
            "sharpe_ratio": 37.405436325040604,
            "sortino_ratio": 0.0,
            "max_drawdown": 0.0,
            "calmar_ratio": 0.0,
            "monthly_hit_rate": 1.0,
            "average_daily_turnover": 0.017241379310344827,
            "average_holding_count": 0.7931034482758621,
            "worst_month": 0.2571630183484306,
            "best_month": 0.2571630183484306,
            "lookback_days": 5,
            "top_k": 1,
            "rebalance_frequency": "weekly",
            "protocol_bps": 0.0,
            "widget_bps": 0.0,
            "slippage_bps": 0.0,
            "gas_usd_per_swap": 0.0,
            "skip_days": 0,
            "initial_capital_usd": 1000.0,
            "final_equity_usd": 1257.1630183484301,
            "total_trading_cost_usd": 0.0,
            "total_num_swaps": 1,
            "start_date": "2024-01-02",
            "end_date": "2024-01-30",
        },
        "equity_curve_png": str(artifacts_dir / "equity_curve.png"),
        "report_json": str(artifacts_dir / "report.json"),
    }
    assert (artifacts_dir / "equity_curve.png").is_file()
    assert (artifacts_dir / "report.json").is_file()


def test_run_backtest_cli_writes_json_error_for_bad_spec(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as error:
        run_backtest.main(["--spec", json.dumps({"signal_type": "not-supported"})])

    captured = capsys.readouterr()

    assert error.value.code == 1
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "error": {
            "type": "ValueError",
            "message": "unsupported signal_type: not-supported",
        }
    }
