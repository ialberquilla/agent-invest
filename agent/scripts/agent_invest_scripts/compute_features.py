"""CLI for inspecting the as-of feature compute."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import date
from typing import Any

import pandas as pd

from agent_invest_scripts._lib import compute_universe_features_as_of, print_json
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    resolve_timeout_seconds,
    script_timeout,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compute the universe screening features as of a given date."
    )
    parser.add_argument(
        "--as-of",
        required=True,
        type=_date,
        help="Anchor date for feature compute.",
    )
    parser.add_argument(
        "--coin-id",
        action="append",
        dest="coin_ids",
        help="Optional coin filter; repeat for multiple coins.",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        help="Optional row cap on the JSON output.",
    )
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            features = compute_universe_features_as_of(as_of=args.as_of)
            if args.coin_ids:
                features = features[features["coin_id"].isin(set(args.coin_ids))]
            if args.limit is not None:
                features = features.head(args.limit)
            payload = [_normalize_row(row) for row in features.to_dict(orient="records")]
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)

    print_json(payload)
    return 0


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _normalize_scalar(value) for key, value in row.items()}


def _normalize_scalar(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except ValueError:
            return value
    return value


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a YYYY-MM-DD date") from error


if __name__ == "__main__":
    raise SystemExit(main())
