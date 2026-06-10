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
    # Exposure constraints for short-bearing books (pair/hedge/long-short).
    # max_weight_per_asset is a long-book concept; these measure the gross and
    # net leverage and the per-leg cap from the actual signed target weights.
    if any(
        key in constraints
        for key in ("max_gross_exposure", "max_net_exposure", "max_leg_weight")
    ):
        max_gross, max_net = _exposure_from_allocation(allocation)
        if "max_gross_exposure" in constraints:
            _check_maximum_ceiling(
                violations, "max_gross_exposure", max_gross, constraints["max_gross_exposure"]
            )
        if "max_net_exposure" in constraints:
            _check_maximum_ceiling(
                violations, "max_net_exposure", max_net, constraints["max_net_exposure"]
            )
        if "max_leg_weight" in constraints:
            _check_maximum_ceiling(
                violations,
                "max_leg_weight",
                allocation.get("max_single_weight"),
                constraints["max_leg_weight"],
            )
    # Net market beta: regress the realised strategy returns on the benchmark
    # returns. target_net_beta is the intended exposure (0 for market-neutral,
    # ~1 for fully-long); a violation is a realised beta more than
    # BETA_TOLERANCE away from it. Skipped when there is too little data or a
    # degenerate (zero-variance) benchmark.
    if "target_net_beta" in constraints:
        beta = _net_beta_from_result(result)
        if beta is not None:
            target = constraints["target_net_beta"]
            if isinstance(target, int | float) and abs(beta - float(target)) > BETA_TOLERANCE:
                violations.append(
                    {
                        "constraint": "target_net_beta",
                        "actual": round(beta, 4),
                        "expected": target,
                    }
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


# How far a realised net beta may sit from the thesis target before it counts
# as a violation. A band, not an exact match -- backtested beta is a noisy
# estimate, so a market-neutral (target 0) book passes at |beta| <= 0.25 and a
# fully-long (target 1) book fails only if it has materially shed market beta.
BETA_TOLERANCE = 0.25


def _curve_returns(curve: Any) -> dict[str, float]:
    """date -> simple return from a [{date, value}, ...] equity/benchmark curve.
    Beta is scale-invariant, so a normalised benchmark curve is fine."""
    if not isinstance(curve, list):
        return {}
    points: list[tuple[str, float]] = []
    for entry in curve:
        if not isinstance(entry, dict):
            continue
        date = entry.get("date")
        value = entry.get("value")
        if isinstance(date, str) and isinstance(value, int | float):
            points.append((date, float(value)))
    returns: dict[str, float] = {}
    for (_, prev), (date, value) in zip(points, points[1:]):
        if prev != 0:
            returns[date] = value / prev - 1.0
    return returns


def _net_beta_from_result(result: Mapping[str, Any]) -> float | None:
    """cov(strategy, benchmark) / var(benchmark) over date-aligned returns.
    None when there is too little overlap or the benchmark has no variance."""
    strat = _curve_returns(result.get("equity_curve"))
    bench = _curve_returns(result.get("benchmark_curve"))
    common = [date for date in strat if date in bench]
    if len(common) < 20:
        return None
    xs = [bench[date] for date in common]
    ys = [strat[date] for date in common]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    var_x = sum((x - mean_x) ** 2 for x in xs)
    if var_x <= 0:
        return None
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    return cov / var_x


def _exposure_from_allocation(
    allocation: Mapping[str, Any],
) -> tuple[float, float]:
    """Peak gross and peak |net| exposure across the rebalance target weights.

    gross = sum(|w|) (1.0 for a fully-invested long book; ~2.0 for a balanced
    long/short). net = sum(signed w) (1.0 long-only; ~0.0 market-neutral). We
    take the worst (max) over all rebalance dates so a constraint is a true
    ceiling, never an average that hides a spike."""
    rebalances = allocation.get("rebalances")
    if not isinstance(rebalances, list):
        return 0.0, 0.0
    max_gross = 0.0
    max_net = 0.0
    for rebalance in rebalances:
        if not isinstance(rebalance, dict):
            continue
        weights = rebalance.get("weights")
        if not isinstance(weights, dict):
            continue
        values = [float(w) for w in weights.values() if isinstance(w, int | float)]
        if not values:
            continue
        max_gross = max(max_gross, sum(abs(w) for w in values))
        max_net = max(max_net, abs(sum(values)))
    return max_gross, max_net


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
