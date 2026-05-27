"""Run stage eval fixtures and optionally persist results."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from agent_invest_scripts._lib.cli import fail, print_json
from agent_invest_scripts._lib.eval_scorers import SCORERS, get_scorer

VALID_STAGES = frozenset(SCORERS)
DEFAULT_THRESHOLD = 0.8


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run stage eval fixtures and print a JSON summary."
    )
    parser.add_argument("--stage", required=True)
    parser.add_argument(
        "--fixture-dir",
        type=Path,
        help=(
            "Directory containing fixture JSON files. Defaults to "
            "agent/evals/fixtures/<stage>."
        ),
    )
    parser.add_argument("--fixture", help="Fixture name or JSON filename to run.")
    parser.add_argument("--model", default="mock-stage-runner")
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help="Minimum pass rate required for a zero exit code. Defaults to 0.8.",
    )
    parser.add_argument("--save-to-db", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        summary = run_stage_eval(
            stage=args.stage,
            fixture_dir=args.fixture_dir,
            fixture=args.fixture,
            model=args.model,
            save_to_db=args.save_to_db,
        )
    except (OSError, ValueError, psycopg.Error) as error:
        fail(str(error))

    print_json(summary)
    return 0 if summary["pass_rate"] >= args.threshold else 1


def run_stage_eval(
    *,
    stage: str,
    fixture_dir: Path | None,
    fixture: str | None,
    model: str,
    save_to_db: bool,
) -> dict[str, Any]:
    if stage not in VALID_STAGES:
        get_scorer(stage)

    resolved_fixture_dir = fixture_dir or canonical_fixture_dir(stage)
    fixture_paths = select_fixture_paths(resolved_fixture_dir, fixture)
    if not fixture_paths:
        raise ValueError(f"No fixtures found in {resolved_fixture_dir}.")

    results = [run_fixture(stage, path, model, save_to_db) for path in fixture_paths]
    passed = sum(1 for result in results if result["passed"])
    pass_rate = passed / len(results)

    return {
        "stage": stage,
        "model": model,
        "fixture_dir": str(resolved_fixture_dir),
        "passed": passed == len(results),
        "pass_rate": pass_rate,
        "passed_count": passed,
        "failed_count": len(results) - passed,
        "results": results,
    }


def canonical_fixture_dir(stage: str) -> Path:
    return repo_root() / "agent" / "evals" / "fixtures" / stage


def select_fixture_paths(fixture_dir: Path, fixture: str | None) -> list[Path]:
    if fixture is not None:
        path = fixture_dir / fixture
        if path.suffix != ".json":
            path = path.with_suffix(".json")
        if not path.exists():
            raise ValueError(f"Fixture not found: {path}")
        return [path]

    if not fixture_dir.exists():
        raise ValueError(f"Fixture directory not found: {fixture_dir}")

    return sorted(fixture_dir.glob("*.json"))


def run_fixture(
    stage: str,
    path: Path,
    model: str,
    save_to_db: bool,
) -> dict[str, Any]:
    fixture = load_fixture(path)
    started_at = time.monotonic()
    output = run_stage_at_boundary(stage, fixture["input"], model)
    score_result = get_scorer(stage)(fixture["expectations"], output)
    duration_ms = round((time.monotonic() - started_at) * 1000)

    result = {
        "fixture_id": path.stem,
        "passed": bool(score_result["passed"]),
        "score": score_result.get("score"),
        "rules": score_result.get("rules", []),
        "duration_ms": duration_ms,
    }

    for rule in result["rules"]:
        status = "PASS" if rule.get("passed") else "FAIL"
        print(f"{path.stem}: {status} {rule.get('rule', 'unnamed')}", file=sys.stderr)

    if save_to_db:
        eval_run_id = save_eval_run(
            stage=stage,
            fixture_id=path.stem,
            model=model,
            result=result,
            output=output,
        )
        result["eval_run_id"] = eval_run_id

    return result


def load_fixture(path: Path) -> dict[str, Any]:
    try:
        fixture = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid fixture JSON at {path}: {error}") from error

    if not isinstance(fixture, dict):
        raise ValueError(f"Fixture must be an object: {path}")
    if "input" not in fixture or "expectations" not in fixture:
        raise ValueError(f"Fixture must contain input and expectations: {path}")
    if not isinstance(fixture["expectations"], dict):
        raise ValueError(f"Fixture expectations must be an object: {path}")

    return fixture


def run_stage_at_boundary(stage: str, fixture_input: Any, model: str) -> dict[str, Any]:
    if (
        stage == "thesis"
        and model == "mock-stage-runner"
        and isinstance(fixture_input, dict)
    ):
        return _mock_thesis_output(fixture_input)
    if (
        stage == "designer"
        and model == "mock-stage-runner"
        and isinstance(fixture_input, dict)
    ):
        return _mock_designer_output(fixture_input)
    if (
        stage == "adjudicator"
        and model == "mock-stage-runner"
        and isinstance(fixture_input, dict)
    ):
        return _mock_adjudicator_output(fixture_input)
    if (
        stage == "reporter"
        and model == "mock-stage-runner"
        and isinstance(fixture_input, dict)
    ):
        return _mock_reporter_output(fixture_input)

    return {
        "stage": stage,
        "model": model,
        "input": fixture_input,
        "mocked": True,
    }


def _mock_thesis_output(fixture_input: dict[str, Any]) -> dict[str, Any]:
    return {
        "thesis_id": f"eval-{fixture_input.get('run_id', 'thesis')}",
        "thesis": {
            "run_id": fixture_input.get("run_id", "eval-thesis-run"),
            "objective": fixture_input.get("expected_objective", "balanced"),
            "primary_factors": [
                {
                    "factor": factor_id,
                    "direction": "low"
                    if factor_id in {"drawdown", "volatility"}
                    else "high",
                }
                for factor_id in fixture_input.get("expected_primary_factor_ids", [])
            ],
            "constraints": fixture_input.get("expected_constraints", {}),
            "horizon_days": fixture_input.get("expected_horizon_days", 365),
            "interpretation_notes": fixture_input.get(
                "expected_interpretation_notes", ""
            ),
        },
    }


def _mock_designer_output(fixture_input: dict[str, Any]) -> dict[str, Any]:
    candidates = fixture_input.get("candidates", [])
    window = fixture_input.get("window")
    normalized_candidates = []
    for candidate in candidates if isinstance(candidates, list) else []:
        if not isinstance(candidate, dict):
            continue
        normalized = dict(candidate)
        normalized.setdefault("window", window)
        normalized.setdefault("metrics", {})
        normalized_candidates.append(normalized)
    return {
        "batch_id": fixture_input.get("batch_id", "eval-designer-batch"),
        "run_id": fixture_input.get("run_id", "eval-designer-run"),
        "round": fixture_input.get("round", 1),
        "window": window,
        "candidates": normalized_candidates,
        "results": normalized_candidates,
    }


def _mock_adjudicator_output(fixture_input: dict[str, Any]) -> dict[str, Any]:
    output = fixture_input.get("mock_output")
    if isinstance(output, dict):
        return dict(output)
    return {
        "kind": "refine",
        "reasons": [
            {
                "reason": "insufficient_evidence",
                "detail": "No mock adjudicator output was configured for this fixture.",
            }
        ],
    }


def _mock_reporter_output(fixture_input: dict[str, Any]) -> dict[str, Any]:
    output = fixture_input.get("mock_output")
    if isinstance(output, dict):
        return dict(output)
    return {
        "result_type": "no_viable_strategy",
        "title": "No viable strategy",
        "summary": "No reporter mock output was configured.",
        "reasoning": "The fixture did not include a structured reporter payload.",
        "assumptions": ["Fixture uses the mock stage runner."],
        "risks": ["No live reporter execution occurred."],
        "next_steps": ["Add mock_output to the fixture."],
    }


def save_eval_run(
    *,
    stage: str,
    fixture_id: str,
    model: str,
    result: dict[str, Any],
    output: dict[str, Any],
) -> str:
    eval_run_id = f"eval-run-{uuid4()}"
    query = """
        INSERT INTO stage_eval_runs (
            eval_run_id, stage, fixture_id, model, passed, score,
            diagnostics, output, duration_ms
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with psycopg.connect(_postgres_conninfo()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    eval_run_id,
                    stage,
                    fixture_id,
                    model,
                    result["passed"],
                    result["score"],
                    Jsonb({"rules": result["rules"]}),
                    Jsonb(output),
                    result["duration_ms"],
                ),
            )

    return eval_run_id


def _postgres_conninfo() -> str:
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        return database_url
    raise ValueError("DATABASE_URL must be set when --save-to-db is passed.")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


if __name__ == "__main__":
    raise SystemExit(main())
