"""Compare candidate-batch backtests and rank them deterministically."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from typing import Any

from agent_invest_scripts._lib import print_json
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    resolve_timeout_seconds,
    script_timeout,
)
from agent_invest_scripts._lib.storage import normalize_identifier, storage_root


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare a candidate backtest batch.")
    parser.add_argument("--batch-id", required=True, help="candidate_batch_* id")
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            payload = compare(args.batch_id)
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)
    print_json(payload)
    return 0


def compare(batch_id: str) -> dict[str, Any]:
    batch = _read_batch(batch_id)
    rows = [_comparison_row(result) for result in batch.get("results", [])]
    rows.sort(key=lambda row: row["composite_score"], reverse=True)
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
    return {
        "batch_id": batch["batch_id"],
        "run_id": batch["run_id"],
        "round": batch["round"],
        "ranking": rows,
        "winner_candidate_id": rows[0]["candidate_id"] if rows else None,
    }


def _read_batch(batch_id: str) -> dict[str, Any]:
    normalized = normalize_identifier(batch_id, "batch_id")
    path = storage_root() / "candidate_batches" / f"{normalized}.json"
    if not path.is_file():
        raise ValueError(f"candidate batch not found: {normalized}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("candidate batch artifact must be a JSON object")
    return value


def _comparison_row(result: dict[str, Any]) -> dict[str, Any]:
    metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
    robustness = (
        result.get("robustness") if isinstance(result.get("robustness"), dict) else {}
    )
    warning_keys = [
        key for key, value in robustness.items() if key.endswith("_warning") and value
    ]
    return {
        "candidate_id": result.get("candidate_id"),
        "template_id": result.get("template_id"),
        "composite_score": float(result.get("composite_score") or 0.0),
        "total_return": metrics.get("total_return"),
        "cagr": metrics.get("cagr"),
        "period_return": metrics.get("period_return"),
        "volatility": metrics.get("volatility"),
        "max_drawdown": metrics.get("max_drawdown"),
        "sharpe": metrics.get("sharpe"),
        "sortino": metrics.get("sortino"),
        "calmar": metrics.get("calmar"),
        "warnings": warning_keys,
    }


if __name__ == "__main__":
    raise SystemExit(main())
