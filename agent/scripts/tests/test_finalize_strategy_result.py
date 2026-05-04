from __future__ import annotations

import json
from pathlib import Path

import pytest

from agent_invest_scripts import finalize_strategy_result


def _write_backtest_artifacts(storage_root: Path, label: str) -> None:
    artifact_dir = storage_root / "artifacts" / "run_backtest" / label
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "report.json").write_text(
        json.dumps(
            {
                "kpis": {"cagr": 0.2, "sharpe_ratio": 1.5},
                "spec": {
                    "allocation": {"start": "2024-01-01", "end": "2024-01-31"},
                    "rebalance": "monthly",
                    "initial_capital_usd": 1000,
                },
            }
        ),
        encoding="utf-8",
    )
    (artifact_dir / "equity_curve.json").write_text(
        json.dumps(
            [
                {
                    "date": "2024-01-01",
                    "equity_usd": 1000,
                    "bitcoin_equity_usd": 1000,
                }
            ]
        ),
        encoding="utf-8",
    )
    (artifact_dir / "drawdown.json").write_text(
        json.dumps(
            [
                {
                    "date": "2024-01-01",
                    "drawdown": 0,
                    "bitcoin_drawdown": 0,
                }
            ]
        ),
        encoding="utf-8",
    )
    (artifact_dir / "allocation.json").write_text(
        json.dumps([{"date": "2024-01-01", "coin_id": "bitcoin", "weight": 1}]),
        encoding="utf-8",
    )


def test_finalize_strategy_result_writes_canonical_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    _write_backtest_artifacts(tmp_path, "btc_only")

    finalize_strategy_result.main(
        [
            "--payload",
            json.dumps(
                {
                    "title": "BTC Only",
                    "summary": "A Bitcoin-only strategy.",
                    "reasoning": "The backtest supports concentrated Bitcoin exposure.",
                    "allocation": [
                        {
                            "asset": "Bitcoin",
                            "symbol": "BTC",
                            "coin_id": "bitcoin",
                            "weight": 1,
                            "rationale": "Core exposure.",
                        }
                    ],
                    "assumptions": ["Historical prices are representative."],
                    "risks": ["Drawdowns can be severe."],
                    "next_steps": ["Monitor monthly."],
                    "backtest_label": "btc_only",
                }
            ),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    output_path = Path(payload["strategy_result_json"])
    result = json.loads(output_path.read_text(encoding="utf-8"))

    assert output_path.is_file()
    assert result["title"] == "BTC Only"
    assert result["kpis"] == {"cagr": 0.2, "sharpe_ratio": 1.5}
    assert result["backtest"]["start_date"] == "2024-01-01"
    assert result["charts"]["equity_curve"] == [
        {
            "date": "2024-01-01",
            "strategy_equity": 1000,
            "benchmark_equity": 1000,
        }
    ]


def test_finalize_strategy_result_rejects_missing_backtest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))

    with pytest.raises(SystemExit) as error:
        finalize_strategy_result.main(
            [
                "--payload",
                json.dumps(
                    {
                        "title": "BTC Only",
                        "summary": "A Bitcoin-only strategy.",
                        "reasoning": "Reasoning.",
                        "allocation": [
                            {
                                "asset": "Bitcoin",
                                "weight": 1,
                                "rationale": "Core exposure.",
                            }
                        ],
                        "assumptions": ["Assumption."],
                        "risks": ["Risk."],
                        "next_steps": ["Next step."],
                        "backtest_label": "missing",
                    }
                ),
            ]
        )

    assert error.value.code == 1
    assert "required backtest artifact is missing" in capsys.readouterr().err
