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
    parser = argparse.ArgumentParser(description="Compare candidate backtest batches.")
    batch_group = parser.add_mutually_exclusive_group(required=True)
    batch_group.add_argument(
        "--batch-id",
        help="candidate_batch_* id; accepts a comma-separated list for compatibility",
    )
    batch_group.add_argument(
        "--batch-ids",
        help="comma-separated candidate_batch_* ids to rank together",
    )
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            payload = compare(args.batch_ids or args.batch_id)
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)
    print_json(payload)
    return 0


def compare(batch_ids: str) -> dict[str, Any]:
    requested_batch_ids = _parse_batch_ids(batch_ids)
    batches = [_read_batch(batch_id) for batch_id in requested_batch_ids]
    rows = [
        _comparison_row(result, batch)
        for batch in batches
        for result in batch.get("results", [])
    ]
    for batch_id in [batch["batch_id"] for batch in batches]:
        batch_rows = [row for row in rows if row["batch_id"] == batch_id]
        batch_rows.sort(key=lambda row: row["composite_score"], reverse=True)
        for rank, row in enumerate(batch_rows, start=1):
            row["batch_rank"] = rank
    rows.sort(key=lambda row: row["composite_score"], reverse=True)
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
        row["global_rank"] = rank
    per_batch_winners = _per_batch_winners(rows)
    single_batch = len(batches) == 1
    return {
        **(
            {
                "batch_id": batches[0]["batch_id"],
                "run_id": batches[0]["run_id"],
                "round": batches[0]["round"],
            }
            if single_batch
            else {}
        ),
        "batch_ids": [batch["batch_id"] for batch in batches],
        "batches": [_batch_summary(batch) for batch in batches],
        "ranking": rows,
        "per_batch_winners": per_batch_winners,
        "winner_batch_id": rows[0]["batch_id"] if rows else None,
        "winner_candidate_id": rows[0]["candidate_id"] if rows else None,
    }


def _parse_batch_ids(batch_ids: str) -> list[str]:
    parsed = [batch_id.strip() for batch_id in batch_ids.split(",") if batch_id.strip()]
    if not parsed:
        raise ValueError("at least one batch id is required")
    return parsed


def _read_batch(batch_id: str) -> dict[str, Any]:
    normalized = normalize_identifier(batch_id, "batch_id")
    path = storage_root() / "candidate_batches" / f"{normalized}.json"
    if not path.is_file():
        raise ValueError(f"candidate batch not found: {normalized}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("candidate batch artifact must be a JSON object")
    return value


def _batch_summary(batch: dict[str, Any]) -> dict[str, Any]:
    summary = {
        "batch_id": batch["batch_id"],
        "run_id": batch.get("run_id"),
        "round": batch.get("round"),
    }
    if "iteration_hypothesis" in batch:
        summary["iteration_hypothesis"] = batch["iteration_hypothesis"]
    return summary


def _comparison_row(result: dict[str, Any], batch: dict[str, Any]) -> dict[str, Any]:
    metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
    robustness = (
        result.get("robustness") if isinstance(result.get("robustness"), dict) else {}
    )
    warning_keys = [
        key for key, value in robustness.items() if key.endswith("_warning") and value
    ]
    row = {
        "batch_id": batch["batch_id"],
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
    if "iteration_hypothesis" in batch:
        row["iteration_hypothesis"] = batch["iteration_hypothesis"]
    return row


def _per_batch_winners(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    winners: dict[str, dict[str, Any]] = {}
    for row in rows:
        batch_id = row["batch_id"]
        if batch_id not in winners:
            winners[batch_id] = {
                "batch_id": batch_id,
                "winner_candidate_id": row["candidate_id"],
                "composite_score": row["composite_score"],
                "global_rank": row["global_rank"],
            }
    return list(winners.values())


if __name__ == "__main__":
    raise SystemExit(main())
