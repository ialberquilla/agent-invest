"""Validate candidate-batch results against a structured thesis."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from typing import Any

from agent_invest_scripts._lib import print_json
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    resolve_timeout_seconds,
    script_timeout,
)
from agent_invest_scripts._lib.storage import normalize_identifier, storage_root

THESIS_EXAMPLE: dict[str, Any] = {
    "objective": "balanced_growth",
    "horizon_days": 365,
    "constraints": {
        "max_drawdown": 0.35,
        "asset_count_min": 5,
        "asset_count_max": 10,
        "max_weight_per_asset": 0.20,
        "max_cash_weight": 0.10,
    },
    "primary_factors": ["sharpe_365d", "max_drawdown"],
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate candidates against thesis constraints."
    )
    # Either pass --input with the full candidate-batch JSON inline
    # (preferred: no filesystem coupling), or --batch-id to read a
    # previously-persisted batch by id (legacy path; will be removed
    # once the old strategist pipeline is retired).
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--input", dest="input_json", help="Inline candidate batch JSON"
    )
    source.add_argument("--batch-id", help="candidate_batch_* id (legacy)")
    parser.add_argument("--thesis", required=True, help="Structured thesis JSON")
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            thesis = _json_object(args.thesis, "--thesis")
            if args.input_json is not None:
                raw = (
                    sys.stdin.read() if args.input_json == "-" else args.input_json
                )
                batch = _json_object(raw, "--input")
                payload = validate_batch(batch, thesis)
            else:
                payload = validate(args.batch_id, thesis)
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)
    print_json(payload)
    return 0


def validate(batch_id: str, thesis: Mapping[str, Any]) -> dict[str, Any]:
    return validate_batch(_read_batch(batch_id), thesis)


def validate_batch(
    batch: Mapping[str, Any], thesis: Mapping[str, Any]
) -> dict[str, Any]:
    constraints = (
        thesis.get("constraints") if isinstance(thesis.get("constraints"), dict) else {}
    )
    results = [
        _validate_result(result, constraints, thesis)
        for result in batch.get("results", [])
    ]
    return {
        "batch_id": batch.get("batch_id"),
        "run_id": batch.get("run_id"),
        "round": batch.get("round"),
        "results": results,
        "passing_candidate_ids": [
            row["candidate_id"] for row in results if row["passed"]
        ],
    }


def _validate_result(
    result: Mapping[str, Any], constraints: Mapping[str, Any], thesis: Mapping[str, Any]
) -> dict[str, Any]:
    metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
    config = result.get("config") if isinstance(result.get("config"), dict) else {}
    allocation = (
        result.get("allocation_metrics")
        if isinstance(result.get("allocation_metrics"), dict)
        else {}
    )
    violations: list[dict[str, Any]] = []

    if "max_drawdown" in constraints:
        _check_minimum_floor(
            violations,
            "max_drawdown",
            metrics.get("max_drawdown"),
            constraints["max_drawdown"],
        )
    if "asset_count_min" in constraints:
        _check_minimum_floor(
            violations,
            "asset_count_min",
            config.get("select_top"),
            constraints["asset_count_min"],
        )
    if "asset_count_max" in constraints:
        _check_maximum_ceiling(
            violations,
            "asset_count_max",
            config.get("select_top"),
            constraints["asset_count_max"],
        )
    if "max_weight_per_asset" in constraints:
        _check_maximum_ceiling(
            violations,
            "max_weight_per_asset",
            allocation.get("max_single_weight"),
            constraints["max_weight_per_asset"],
        )
    # horizon_days is the forward-looking holding period, NOT a backtest
    # length floor. The window recommender already targets enough history
    # automatically (max(2 * horizon_days, 1460) days), and select_window
    # surfaces below_horizon to decide when the realised window falls
    # short of the requested horizon. Validating it again here as a
    # per-candidate floor would conflate "hold for 1 year" with "must
    # have >= 1 year of history".

    return {
        "candidate_id": result.get("candidate_id"),
        "template_id": result.get("template_id"),
        "passed": len(violations) == 0,
        "violations": violations,
    }


def _check_minimum_floor(
    violations: list[dict[str, Any]], name: str, actual: Any, expected: Any
) -> None:
    if not isinstance(actual, int | float) or not isinstance(expected, int | float):
        return
    if float(actual) < float(expected):
        violations.append({"constraint": name, "actual": actual, "expected": expected})


def _check_maximum_ceiling(
    violations: list[dict[str, Any]], name: str, actual: Any, expected: Any
) -> None:
    if not isinstance(actual, int | float) or not isinstance(expected, int | float):
        return
    if float(actual) > float(expected):
        violations.append({"constraint": name, "actual": actual, "expected": expected})


def _read_batch(batch_id: str) -> dict[str, Any]:
    normalized = normalize_identifier(batch_id, "batch_id")
    path = storage_root() / "candidate_batches" / f"{normalized}.json"
    if not path.is_file():
        raise ValueError(f"candidate batch not found: {normalized}")
    return _json_object(path.read_text(encoding="utf-8"), str(path))


def _json_object(raw: str, name: str) -> dict[str, Any]:
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
