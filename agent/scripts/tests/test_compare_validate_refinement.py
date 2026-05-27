from __future__ import annotations

import json
from pathlib import Path

import pytest

from agent_invest_scripts import (
    compare_backtests,
    submit_refinement,
    validate_against_thesis,
)


def _write_batch(storage_root: Path) -> None:
    batch_dir = storage_root / "candidate_batches"
    batch_dir.mkdir(parents=True)
    (batch_dir / "candidate_batch_test.json").write_text(
        json.dumps(
            {
                "batch_id": "candidate_batch_test",
                "run_id": "run-1",
                "round": 1,
                "results": [
                    {
                        "candidate_id": "low_score",
                        "template_id": "buy_and_hold",
                        "config": {"select_top": 2},
                        "metrics": {"cagr": 0.1, "max_drawdown": -0.2, "sharpe": 0.7},
                        "robustness": {},
                        "composite_score": 0.5,
                    },
                    {
                        "candidate_id": "high_score",
                        "template_id": "periodic_rebalance",
                        "config": {"select_top": 4},
                        "metrics": {"cagr": 0.2, "max_drawdown": -0.1, "sharpe": 1.1},
                        "robustness": {"sample_size_warning": True},
                        "composite_score": 1.5,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )


def test_compare_backtests_ranks_candidate_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    _write_batch(tmp_path)

    output = compare_backtests.compare("candidate_batch_test")

    assert output["winner_candidate_id"] == "high_score"
    assert output["ranking"][0]["rank"] == 1
    assert output["ranking"][0]["warnings"] == ["sample_size_warning"]


def test_validate_against_thesis_checks_constraints(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    _write_batch(tmp_path)

    output = validate_against_thesis.validate(
        "candidate_batch_test",
        {
            "constraints": {
                "max_drawdown": -0.15,
                "asset_count_min": 3,
                "asset_count_max": 5,
            }
        },
    )

    assert output["passing_candidate_ids"] == ["high_score"]
    low = output["results"][0]
    assert low["passed"] is False
    assert {violation["constraint"] for violation in low["violations"]} == {
        "max_drawdown",
        "asset_count_min",
    }


def test_submit_refinement_persists_reasons(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))

    output = submit_refinement.submit(
        {
            "run_id": "run-1",
            "round": 2,
            "refinement_reasons": [
                {"candidate_id": "low_score", "reason": "constraint_violation"}
            ],
        }
    )

    assert Path(output["refinement_json"]).is_file()
