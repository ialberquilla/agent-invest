"""Build the canonical structured strategy result from a backtest run."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from agent_invest_scripts._lib import print_json
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    resolve_timeout_seconds,
    script_timeout,
)
from agent_invest_scripts._lib.storage import normalize_identifier, storage_root

_MAX_WEIGHT_SUM = 1.0
_WEIGHT_SUM_TOLERANCE = 1e-9


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Finalize a portfolio strategy result from a backtest label."
    )
    parser.add_argument("--payload", required=True, help="Final strategy JSON payload")
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            payload = _parse_json_object(args.payload, "--payload")
            result = _build_result(payload)
            label = normalize_identifier(
                str(payload["backtest_label"]), "backtest_label"
            )
            output_dir = storage_root() / "artifacts" / "strategy_result" / label
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / "strategy_result.json"
            output_path.write_text(
                json.dumps(result, indent=2) + "\n", encoding="utf-8"
            )
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)

    print_json({"strategy_result_json": str(output_path), "structured_result": result})
    return 0


def _parse_json_object(raw: str, argument_name: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError(f"{argument_name} must be valid JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{argument_name} must decode to a JSON object")
    return value


def _build_result(payload: Mapping[str, Any]) -> dict[str, Any]:
    label = _read_text(payload, "backtest_label")
    backtest_dir = (
        storage_root()
        / "artifacts"
        / "run_backtest"
        / normalize_identifier(label, "backtest_label")
    )
    report = _read_json_object(backtest_dir / "report.json")
    spec = report.get("spec") if isinstance(report.get("spec"), dict) else {}

    allocation = _read_allocation(payload.get("allocation"))
    _validate_allocation_matches_backtest(allocation, spec)
    return {
        "title": _read_text(payload, "title"),
        "summary": _read_text(payload, "summary"),
        "reasoning": _read_text(payload, "reasoning"),
        "allocation": allocation,
        "kpis": _read_object(report, "kpis"),
        "assumptions": _read_text_list(payload.get("assumptions"), "assumptions"),
        "risks": _read_text_list(payload.get("risks"), "risks"),
        "next_steps": _read_text_list(payload.get("next_steps"), "next_steps"),
        "constraint_violations": _read_text_list(
            payload.get("constraint_violations"),
            "constraint_violations",
            required=False,
        ),
        "backtest": _backtest_context(spec),
        "charts": {
            "equity_curve": _equity_curve(backtest_dir / "equity_curve.json"),
            "drawdown": _drawdown(backtest_dir / "drawdown.json"),
            "allocation": _chart_allocation(backtest_dir / "target_allocation.json"),
            "target_allocation": _chart_allocation(
                backtest_dir / "target_allocation.json"
            ),
            "final_allocation": _chart_allocation(backtest_dir / "allocation.json"),
        },
    }


def _read_json(path: Path) -> Any:
    if not path.is_file():
        raise ValueError(f"required backtest artifact is missing: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _read_json_object(path: Path) -> dict[str, Any]:
    value = _read_json(path)
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return value


def _read_object(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be an object")
    return dict(value)


def _read_text(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return value.strip()


def _read_text_list(value: Any, key: str, *, required: bool = True) -> list[str]:
    if value is None and not required:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{key} must be an array of strings")
    items = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{key}[{index}] must be a non-empty string")
        items.append(item.strip())
    return items


def _read_allocation(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError("allocation must be a non-empty array")

    items: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"allocation[{index}] must be an object")
        weight = _read_finite_number(item.get("weight"), f"allocation[{index}].weight")
        if weight < 0 or weight > 1:
            raise ValueError(f"allocation[{index}].weight must be between 0 and 1")
        items.append(
            {
                "asset": _read_text(item, "asset"),
                "symbol": _optional_text(item.get("symbol")),
                "coin_id": _optional_text(item.get("coin_id")),
                "weight": weight,
                "rationale": _read_text(item, "rationale"),
            }
        )

    weight_sum = sum(item["weight"] for item in items)
    if weight_sum > _MAX_WEIGHT_SUM + _WEIGHT_SUM_TOLERANCE:
        raise ValueError("allocation weights must sum to <= 1.0")
    return items


def _validate_allocation_matches_backtest(
    allocation: list[dict[str, Any]], spec: Mapping[str, Any]
) -> None:
    expected = _target_weights_from_spec(spec)
    if not expected:
        return

    actual: dict[str, float] = {}
    missing_coin_ids = []
    for item in allocation:
        coin_id = item.get("coin_id")
        if not isinstance(coin_id, str) or not coin_id.strip():
            missing_coin_ids.append(item["asset"])
            continue
        actual[coin_id.strip()] = float(item["weight"])

    if missing_coin_ids:
        formatted = ", ".join(missing_coin_ids)
        raise ValueError(
            "allocation entries must include coin_id values that match the backtest: "
            f"{formatted}"
        )

    expected_keys = set(expected)
    actual_keys = set(actual)
    if actual_keys != expected_keys:
        raise ValueError(
            "final allocation coin_ids must match the selected backtest allocation; "
            f"expected {sorted(expected_keys)}, got {sorted(actual_keys)}"
        )

    mismatches = [
        coin_id
        for coin_id in sorted(expected)
        if abs(expected[coin_id] - actual[coin_id]) > _WEIGHT_SUM_TOLERANCE
    ]
    if mismatches:
        raise ValueError(
            "final allocation weights must match the selected backtest allocation for: "
            + ", ".join(mismatches)
        )


def _target_weights_from_spec(spec: Mapping[str, Any]) -> dict[str, float]:
    allocation = (
        spec.get("allocation") if isinstance(spec.get("allocation"), dict) else {}
    )
    if allocation.get("type") == "static":
        raw_weights = allocation.get("weights")
        if isinstance(raw_weights, dict):
            return {
                str(coin_id): _read_finite_number(
                    weight, f"allocation.weights.{coin_id}"
                )
                for coin_id, weight in raw_weights.items()
            }
    if allocation.get("type") == "weights":
        rows = allocation.get("rows")
        if isinstance(rows, list):
            latest_date = max(
                (row.get("date") for row in rows if isinstance(row, dict)), default=None
            )
            if latest_date is None:
                return {}
            weights: dict[str, float] = {}
            for row in rows:
                if not isinstance(row, dict) or row.get("date") != latest_date:
                    continue
                coin_id = row.get("coin_id")
                if isinstance(coin_id, str):
                    weights[coin_id] = _read_finite_number(
                        row.get("weight"), f"allocation.rows.{coin_id}.weight"
                    )
            return weights
    return {}


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("optional text fields must be strings or null")
    stripped = value.strip()
    return stripped or None


def _read_finite_number(value: Any, key: str) -> float:
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise ValueError(f"{key} must be a number")
    number = float(value)
    if number != number or number in {float("inf"), float("-inf")}:
        raise ValueError(f"{key} must be finite")
    return number


def _backtest_context(spec: Mapping[str, Any]) -> dict[str, Any]:
    allocation = (
        spec.get("allocation") if isinstance(spec.get("allocation"), dict) else {}
    )
    return {
        "label": spec.get("label"),
        "start_date": allocation.get("start"),
        "end_date": allocation.get("end"),
        "rebalance": spec.get("rebalance"),
        "initial_capital_usd": spec.get("initial_capital_usd"),
        "capital_mode": spec.get("capital_mode"),
        "benchmark": "bitcoin",
    }


def _equity_curve(path: Path) -> list[dict[str, Any]]:
    return [
        {
            "date": _read_text(point, "date"),
            "strategy_equity": _read_finite_number(
                point.get("equity_usd"), "equity_usd"
            ),
            "benchmark_equity": _optional_number(point.get("bitcoin_equity_usd")),
        }
        for point in _read_json_array(path)
    ]


def _drawdown(path: Path) -> list[dict[str, Any]]:
    return [
        {
            "date": _read_text(point, "date"),
            "strategy_drawdown": _read_finite_number(point.get("drawdown"), "drawdown"),
            "benchmark_drawdown": _optional_number(point.get("bitcoin_drawdown")),
        }
        for point in _read_json_array(path)
    ]


def _chart_allocation(path: Path) -> list[dict[str, Any]]:
    return [
        {
            "asset": _read_text(point, "coin_id"),
            "weight": _read_finite_number(point.get("weight"), "weight"),
        }
        for point in _read_json_array(path)
    ]


def _read_json_array(path: Path) -> list[dict[str, Any]]:
    value = _read_json(path)
    if not isinstance(value, list):
        raise ValueError(f"{path.name} must contain a JSON array")
    if not all(isinstance(item, dict) for item in value):
        raise ValueError(f"{path.name} must contain an array of objects")
    return value


def _optional_number(value: Any) -> float | None:
    if value is None:
        return None
    return _read_finite_number(value, "optional number")


if __name__ == "__main__":
    raise SystemExit(main())
