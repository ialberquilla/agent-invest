from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from agent_invest_scripts import run_backtest


def _seed_prices(storage_root: Path) -> None:
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

    datasets_dir = storage_root / "datasets"
    datasets_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(datasets_dir / "daily_prices.parquet", index=False)


def test_run_backtest_static_allocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    _seed_prices(tmp_path)

    run_backtest.main(
        [
            "--allocation",
            json.dumps(
                {
                    "type": "static",
                    "weights": {"coin-a": 1.0},
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
    assert Path(payload["report_json"]).is_file()


def test_run_backtest_explicit_weights(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    _seed_prices(tmp_path)

    run_backtest.main(
        [
            "--allocation",
            json.dumps(
                {
                    "type": "weights",
                    "rows": [
                        {"date": "2024-01-01", "coin_id": "coin-a", "weight": 1.0},
                        {"date": "2024-01-15", "coin_id": "coin-b", "weight": 1.0},
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


def test_run_backtest_rejects_leveraged_weights(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    _seed_prices(tmp_path)

    with pytest.raises(SystemExit) as error:
        run_backtest.main(
            [
                "--allocation",
                json.dumps(
                    {
                        "type": "static",
                        "weights": {"coin-a": 1.0, "coin-b": 0.5},
                        "start": "2024-01-01",
                        "end": "2024-01-30",
                    }
                ),
            ]
        )

    assert error.value.code == 1
    assert "weights must sum to <= 1.0" in capsys.readouterr().err
