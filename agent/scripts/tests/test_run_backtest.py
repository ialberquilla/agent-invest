from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from agent_invest_scripts import run_backtest


def _daily_prices() -> pd.DataFrame:
    start_date = date(2024, 1, 1)
    rows: list[dict[str, object]] = []
    ethereum_price = 100.0
    bitcoin_price = 100.0

    for offset in range(30):
        current_date = start_date + timedelta(days=offset)
        ethereum_price *= 1.01
        bitcoin_price *= 1.005
        rows.append(
            {
                "date": current_date,
                "coin_id": "ethereum",
                "price": ethereum_price,
            }
        )
        rows.append(
            {
                "date": current_date,
                "coin_id": "bitcoin",
                "price": bitcoin_price,
            }
        )

    return pd.DataFrame(rows, columns=["date", "coin_id", "price"])


def test_run_backtest_static_allocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    monkeypatch.setattr(run_backtest, "daily_prices", _daily_prices)

    run_backtest.main(
        [
            "--allocation",
            json.dumps(
                {
                    "type": "static",
                    "weights": {"ethereum": 1.0},
                    "start": "2024-01-01",
                    "end": "2024-01-30",
                }
            ),
            "--label",
            "test_run",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    kpis = payload["kpis"]

    assert {"cagr", "sharpe_ratio", "max_drawdown", "final_equity_usd"} <= kpis.keys()
    assert payload["label"] == "test_run"
    assert len(payload["target_dates"]) == 1
    assert kpis["total_num_swaps"] == 1
    assert kpis["final_equity_usd"] > 1000.0
    assert Path(payload["equity_curve_png"]).is_file()
    assert Path(payload["drawdown_png"]).is_file()
    assert Path(payload["equity_curve_json"]).is_file()
    assert Path(payload["drawdown_json"]).is_file()
    assert Path(payload["allocation_json"]).is_file()
    assert Path(payload["report_json"]).is_file()
    equity_curve = json.loads(Path(payload["equity_curve_json"]).read_text())
    drawdown = json.loads(Path(payload["drawdown_json"]).read_text())
    assert equity_curve[0].keys() >= {"date", "equity", "bitcoin_equity"}
    assert drawdown[0].keys() >= {"date", "drawdown", "bitcoin_drawdown"}
    allocation = json.loads(Path(payload["allocation_json"]).read_text())
    assert {row["coin_id"] for row in allocation} == {"ethereum"}


def test_run_backtest_explicit_weights(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    monkeypatch.setattr(run_backtest, "daily_prices", _daily_prices)

    run_backtest.main(
        [
            "--allocation",
            json.dumps(
                {
                    "type": "weights",
                    "rows": [
                        {"date": "2024-01-01", "coin_id": "bitcoin", "weight": 1.0},
                        {"date": "2024-01-15", "coin_id": "ethereum", "weight": 1.0},
                    ],
                }
            ),
            "--label",
            "switch_mid_month",
        ]
    )

    payload = json.loads(capsys.readouterr().out)

    assert payload["label"] == "switch_mid_month"
    assert payload["target_dates"][0] == "2024-01-02"
    assert payload["kpis"]["final_equity_usd"] > 0
    allocation = json.loads(Path(payload["allocation_json"]).read_text())
    assert {row["coin_id"] for row in allocation} == {"ethereum"}


def test_run_backtest_rejects_leveraged_weights(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    monkeypatch.setattr(run_backtest, "daily_prices", _daily_prices)

    with pytest.raises(SystemExit) as error:
        run_backtest.main(
            [
                "--allocation",
                json.dumps(
                    {
                        "type": "static",
                        "weights": {"bitcoin": 1.0, "ethereum": 0.5},
                        "start": "2024-01-01",
                        "end": "2024-01-30",
                    }
                ),
            ]
        )

    assert error.value.code == 1
    assert "weights must sum to <= 1.0" in capsys.readouterr().err
