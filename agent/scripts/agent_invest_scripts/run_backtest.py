"""CLI entrypoint for scoring agent-supplied portfolio allocations."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

import polars as pl

from agent_invest_scripts._lib import daily_prices, print_json
from agent_invest_scripts._lib.backtest import TradingCostModel, run_backtest
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    resolve_timeout_seconds,
    script_timeout,
)
from agent_invest_scripts._lib.report import write_report
from agent_invest_scripts._lib.storage import normalize_identifier, storage_root

_REBALANCE_FREQUENCIES = {"none", "daily", "weekly", "monthly"}
_MAX_WEIGHT_SUM = 1.0
_WEIGHT_SUM_TOLERANCE = 1e-9


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a backtest from allocation JSON.")
    parser.add_argument("--allocation", required=True, help="JSON allocation payload")
    parser.add_argument(
        "--rebalance",
        choices=sorted(_REBALANCE_FREQUENCIES),
        default="none",
        help="Target reset cadence for expanding sparse allocations",
    )
    parser.add_argument("--costs", help="JSON TradingCostModel keyword arguments")
    parser.add_argument(
        "--initial-capital-usd", type=float, default=1000.0, help="Starting equity"
    )
    parser.add_argument("--label", default="default", help="Artifact label")
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            allocation = _parse_json_object(args.allocation, "--allocation")
            costs = _parse_json_object(args.costs, "--costs") if args.costs else {}
            prices = _load_prices_frame()
            targets = _build_targets(allocation, prices, args.rebalance)
            target_dates = [target_date.isoformat() for target_date in sorted(targets)]
            result = run_backtest(
                prices,
                targets,
                cost_model=TradingCostModel(**costs),
                initial_capital_usd=args.initial_capital_usd,
            )
            label = normalize_identifier(args.label, "--label")
            payload = write_report(
                result,
                storage_root() / "artifacts" / "run_backtest" / label,
                spec={
                    "allocation": allocation,
                    "rebalance": args.rebalance,
                    "costs": costs,
                    "initial_capital_usd": args.initial_capital_usd,
                    "label": args.label,
                    "target_dates": target_dates,
                },
            )
            payload = {"label": args.label, "target_dates": target_dates, **payload}
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)

    print_json(payload)
    return 0


def _parse_json_object(raw: str, argument_name: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError(f"{argument_name} must be valid JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{argument_name} must decode to a JSON object")
    return value


def _load_prices_frame() -> pl.DataFrame:
    frame = pl.from_pandas(daily_prices())
    _require_columns(frame, {"date", "coin_id", "price"}, dataset_name="daily_prices")
    return (
        frame.with_columns(
            pl.col("date").cast(pl.Date),
            pl.col("coin_id").cast(pl.String),
            pl.col("price").cast(pl.Float64),
        )
        .select("date", "coin_id", "price")
        .sort(["date", "coin_id"])
    )


def _build_targets(
    allocation: Mapping[str, Any], prices: pl.DataFrame, rebalance_frequency: str
) -> dict[date, dict[str, float]]:
    allocation_type = allocation.get("type")
    dates = prices.select("date").unique().sort("date").get_column("date").to_list()
    if allocation_type == "static":
        start = _read_date(allocation, "start", location="allocation.start")
        end = _read_date(allocation, "end", location="allocation.end")
        if start > end:
            raise ValueError("allocation.start must be on or before allocation.end")
        weights = _read_weights(
            allocation.get("weights"), location="allocation.weights"
        )
        price_dates = [day for day in dates if start <= day <= end]
        if not price_dates:
            raise ValueError("allocation date range does not overlap daily_prices")
        target_dates = (
            [price_dates[0]]
            if rebalance_frequency == "none"
            else _cadence_dates(price_dates, rebalance_frequency)
        )
        return _shift_first_price_date_target(
            {target_date: weights for target_date in target_dates}, dates
        )

    if allocation_type == "weights":
        rows = allocation.get("rows")
        if not isinstance(rows, list) or not rows:
            raise ValueError("allocation.rows must be a non-empty array")
        sparse_targets: dict[date, dict[str, float]] = {}
        for index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                raise ValueError(f"allocation.rows[{index}] must be an object")
            target_date = _read_date(
                row, "date", location=f"allocation.rows[{index}].date"
            )
            coin_id = row.get("coin_id")
            if not isinstance(coin_id, str) or not coin_id.strip():
                raise ValueError(f"allocation.rows[{index}].coin_id must be a string")
            weight = _read_number(
                row, "weight", location=f"allocation.rows[{index}].weight"
            )
            sparse_targets.setdefault(target_date, {})[coin_id.strip()] = weight
        return _shift_first_price_date_target(
            _expand_sparse_targets(sparse_targets, dates, rebalance_frequency), dates
        )

    raise ValueError('allocation.type must be "weights" or "static"')


def _expand_sparse_targets(
    sparse_targets: dict[date, dict[str, float]],
    dates: list[date],
    rebalance_frequency: str,
) -> dict[date, dict[str, float]]:
    _validate_targets(sparse_targets)
    if rebalance_frequency == "none":
        expanded = {
            day: weights for day, weights in sparse_targets.items() if day in dates
        }
        if not expanded:
            raise ValueError("allocation has no target dates that overlap daily_prices")
        return expanded

    first = min(sparse_targets)
    last = max(sparse_targets)
    cadence_dates = _cadence_dates(
        [day for day in dates if first <= day <= last], rebalance_frequency
    )
    explicit_dates = sorted(day for day in sparse_targets if day in dates)
    target_dates = sorted(set(cadence_dates) | set(explicit_dates))
    expanded: dict[date, dict[str, float]] = {}
    current: dict[str, float] | None = None
    for day in sorted(day for day in dates if first <= day <= last):
        if day in sparse_targets:
            current = sparse_targets[day]
        if current is not None and day in target_dates:
            expanded[day] = current
    if not expanded:
        raise ValueError("allocation has no target dates that overlap daily_prices")
    return expanded


def _shift_first_price_date_target(
    targets: dict[date, dict[str, float]], dates: list[date]
) -> dict[date, dict[str, float]]:
    if len(dates) < 2 or dates[0] not in targets:
        return targets
    shifted_targets = dict(targets)
    first_weights = shifted_targets.pop(dates[0])
    shifted_targets.setdefault(dates[1], first_weights)
    return shifted_targets


def _cadence_dates(dates: list[date], rebalance_frequency: str) -> list[date]:
    if not dates:
        raise ValueError("allocation date range does not overlap daily_prices")
    if rebalance_frequency == "none":
        return [dates[0]]
    if rebalance_frequency == "daily":
        return dates
    cadence: list[date] = []
    previous_key: tuple[int, int] | None = None
    for day in dates:
        key = _period_key(day, rebalance_frequency)
        if key != previous_key:
            cadence.append(day)
            previous_key = key
    return cadence


def _period_key(day: date, rebalance_frequency: str) -> tuple[int, int]:
    if rebalance_frequency == "weekly":
        iso = day.isocalendar()
        return (iso.year, iso.week)
    if rebalance_frequency == "monthly":
        return (day.year, day.month)
    raise ValueError("--rebalance must be daily, weekly, or monthly")


def _read_weights(raw_weights: Any, *, location: str) -> dict[str, float]:
    if not isinstance(raw_weights, Mapping) or not raw_weights:
        raise ValueError(f"{location} must be a non-empty object")
    weights: dict[str, float] = {}
    for coin_id, weight in raw_weights.items():
        if not isinstance(coin_id, str) or not coin_id.strip():
            raise ValueError(f"{location} keys must be non-empty strings")
        weights[coin_id.strip()] = _validate_weight(
            weight, location=f"{location}.{coin_id}"
        )
    _validate_weight_sum(weights, location=location)
    return weights


def _validate_targets(targets: Mapping[date, dict[str, float]]) -> None:
    for target_date, weights in targets.items():
        _validate_weight_sum(
            weights, location=f"allocation target {target_date.isoformat()}"
        )


def _validate_weight_sum(weights: Mapping[str, float], *, location: str) -> None:
    weight_sum = sum(weights.values())
    if weight_sum > _MAX_WEIGHT_SUM + _WEIGHT_SUM_TOLERANCE:
        raise ValueError(f"{location} weights must sum to <= 1.0")


def _read_date(payload: Mapping[str, Any], key: str, *, location: str) -> date:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{location} must be an ISO date string")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{location} must be an ISO date string") from error


def _read_number(payload: Mapping[str, Any], key: str, *, location: str) -> float:
    return _validate_weight(payload.get(key), location=location)


def _validate_weight(value: Any, *, location: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{location} must be a number")
    weight = float(value)
    if weight < 0:
        raise ValueError(f"{location} must be >= 0")
    return weight


def _require_columns(
    frame: pl.DataFrame, required_columns: set[str], *, dataset_name: str
) -> None:
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        missing_list = ", ".join(missing_columns)
        raise ValueError(
            f"{dataset_name} is missing required column(s): {missing_list}"
        )


if __name__ == "__main__":
    raise SystemExit(main())
