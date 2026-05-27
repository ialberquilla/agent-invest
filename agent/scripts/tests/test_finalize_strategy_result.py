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
                    "allocation": {
                        "type": "static",
                        "weights": {"bitcoin": 1},
                        "start": "2024-01-01",
                        "end": "2024-01-31",
                    },
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
    (artifact_dir / "target_allocation.json").write_text(
        json.dumps([{"coin_id": "bitcoin", "weight": 1}]),
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


def test_finalize_strategy_result_accepts_candidate_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    batch_dir = tmp_path / "candidate_batches"
    batch_dir.mkdir(parents=True)
    (batch_dir / "candidate_batch_test.json").write_text(
        json.dumps(
            {
                "batch_id": "candidate_batch_test",
                "run_id": "run-1",
                "round": 1,
                "results": [
                    {
                        "candidate_id": "winner",
                        "template_id": "buy_and_hold",
                        "config": {"select_top": 3, "weighting": "equal"},
                        "window": {"start": "2024-01-01", "end": "2024-02-01"},
                        "metrics": {"cagr": 0.2, "max_drawdown": -0.1, "sharpe": 1.2},
                        "robustness": {"sample_size_warning": True},
                        "composite_score": 1.5,
                        "equity_curve": [{"date": "2024-01-01", "value": 1.0}],
                        "benchmark_curve": [{"date": "2024-01-01", "value": 1.0}],
                        "drawdown_episodes": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    finalize_strategy_result.main(
        [
            "--payload",
            json.dumps(
                {
                    "title": "V3 Winner",
                    "summary": "Selected from candidate batch.",
                    "reasoning": "Best validated candidate.",
                    "candidate_batch_id": "candidate_batch_test",
                    "winner_candidate_id": "winner",
                    "assumptions": ["Historical prices are representative."],
                    "risks": ["Crypto drawdowns can be severe."],
                    "next_steps": ["Monitor and rerun."],
                }
            ),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    output_path = Path(payload["strategy_result_json"])
    result = json.loads(output_path.read_text(encoding="utf-8"))
    assert result["template_id"] == "buy_and_hold"
    assert result["winner_candidate_id"] == "winner"
    assert result["kpis"]["cagr"] == 0.2
    assert payload["result_id"] == "candidate_batch_test"


def test_finalize_strategy_result_accepts_no_viable_strategy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    batch_dir = tmp_path / "candidate_batches"
    batch_dir.mkdir(parents=True)
    (batch_dir / "candidate_batch_test.json").write_text(
        json.dumps(
            {
                "batch_id": "candidate_batch_test",
                "run_id": "run-1",
                "round": 3,
                "results": [
                    {
                        "candidate_id": "candidate_a",
                        "template_id": "buy_and_hold",
                        "metrics": {"cagr": 0.1, "max_drawdown": -0.5},
                        "composite_score": 0.4,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    round_history = [
        {"round": 1, "candidate_batch_id": "candidate_batch_r1"},
        {"round": 2, "candidate_batch_id": "candidate_batch_r2"},
        {"round": 3, "candidate_batch_id": "candidate_batch_test"},
    ]

    finalize_strategy_result.main(
        [
            "--payload",
            json.dumps(
                {
                    "result_type": "no_viable_strategy",
                    "title": "No Viable Strategy",
                    "summary": "No candidate survived validation after three rounds.",
                    "reasoning": "Each round required refinement or failed the thesis.",
                    "candidate_batch_id": "candidate_batch_test",
                    "round_history": round_history,
                    "assumptions": ["Historical prices are representative."],
                    "risks": ["No strategy met the stated constraints."],
                    "next_steps": ["Relax constraints or rerun with a new thesis."],
                }
            ),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    result = json.loads(
        Path(payload["strategy_result_json"]).read_text(encoding="utf-8")
    )
    assert payload["result_id"] == "candidate_batch_test"
    assert result["result_type"] == "no_viable_strategy"
    assert result["round_history"] == round_history
    assert "winner_candidate_id" not in result
    assert "template_id" not in result


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


def test_finalize_strategy_result_prefers_period_return_for_short_windows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    _write_backtest_artifacts(tmp_path, "btc_only")
    report_path = tmp_path / "artifacts" / "run_backtest" / "btc_only" / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["kpis"] = {"cagr": 1.2, "period_return": 0.2, "sharpe_ratio": 1.5}
    report_path.write_text(json.dumps(report), encoding="utf-8")

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
                            "symbol": "BTC",
                            "coin_id": "bitcoin",
                            "weight": 1,
                            "rationale": "Core exposure.",
                        }
                    ],
                    "assumptions": ["Assumption."],
                    "risks": ["Risk."],
                    "next_steps": ["Next step."],
                    "backtest_label": "btc_only",
                }
            ),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    result = json.loads(
        Path(payload["strategy_result_json"]).read_text(encoding="utf-8")
    )

    assert result["kpis"]["cagr"] == 1.2
    assert result["kpis"]["period_return"] == 0.2
    assert result["kpis"]["reported_return"] == 0.2
    assert result["kpis"]["reported_return_source"] == "period_return"


def test_finalize_strategy_result_rejects_allocation_that_differs_from_backtest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    _write_backtest_artifacts(tmp_path, "btc_only")

    with pytest.raises(SystemExit) as error:
        finalize_strategy_result.main(
            [
                "--payload",
                json.dumps(
                    {
                        "title": "ETH Only",
                        "summary": "A mismatched strategy.",
                        "reasoning": "Reasoning.",
                        "allocation": [
                            {
                                "asset": "Ethereum",
                                "symbol": "ETH",
                                "coin_id": "ethereum",
                                "weight": 1,
                                "rationale": "Different exposure.",
                            }
                        ],
                        "assumptions": ["Assumption."],
                        "risks": ["Risk."],
                        "next_steps": ["Next step."],
                        "backtest_label": "btc_only",
                    }
                ),
            ]
        )

    assert error.value.code == 1
    assert "must match the selected backtest allocation" in capsys.readouterr().err
