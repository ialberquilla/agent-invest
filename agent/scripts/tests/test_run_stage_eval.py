from __future__ import annotations

import json

import pytest

from agent_invest_scripts.run_stage_eval import canonical_fixture_dir, run_stage_eval


def test_run_stage_eval_uses_canonical_fixture_dir() -> None:
    summary = run_stage_eval(
        stage="thesis",
        fixture_dir=None,
        fixture="balanced_30d",
        model="mock-stage-runner",
        save_to_db=False,
    )

    assert summary["fixture_dir"] == str(canonical_fixture_dir("thesis"))
    assert summary["passed"] is True
    assert summary["results"][0]["fixture_id"] == "balanced_30d"
    assert summary["results"][0]["rules"][0]["rule"] == "objective"


def test_run_stage_eval_rejects_invalid_stage() -> None:
    with pytest.raises(ValueError, match="Invalid stage 'unknown'"):
        run_stage_eval(
            stage="unknown",
            fixture_dir=None,
            fixture="placeholder",
            model="test-model",
            save_to_db=False,
        )


def test_run_stage_eval_loads_fixture_directory(tmp_path) -> None:
    fixture_path = tmp_path / "case_one.json"
    fixture_path.write_text(
        json.dumps({"input": {"value": 1}, "expectations": {}}),
        encoding="utf-8",
    )

    summary = run_stage_eval(
        stage="designer",
        fixture_dir=tmp_path,
        fixture=None,
        model="test-model",
        save_to_db=False,
    )

    assert summary["passed_count"] == 1
    assert summary["failed_count"] == 0
    assert summary["results"][0]["fixture_id"] == "case_one"
