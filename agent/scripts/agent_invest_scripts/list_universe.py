"""CLI for listing the top-N universe entries by market cap."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Sequence

import pandas as pd

from agent_invest_scripts._lib import (
    asset_universe,
    print_json,
)
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    resolve_timeout_seconds,
    script_timeout,
)

_OUTPUT_COLUMNS = ("coin_id", "symbol", "name", "market_cap")


class SnapshotNotFoundError(LookupError):
    """Raised when the requested universe snapshot date is not present."""

    def __init__(self, as_of: str) -> None:
        super().__init__(f"No universe snapshot found for {as_of}")
        self.as_of = as_of


class JsonArgumentParser(argparse.ArgumentParser):
    """Argument parser that emits JSON errors to stderr."""

    def error(self, message: str) -> None:
        _print_error({"error": message})
        raise SystemExit(2)


def _print_error(payload: dict[str, Any]) -> None:
    json.dump(payload, sys.stderr)
    sys.stderr.write("\n")


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("--top-n must be an integer") from error

    if parsed < 1:
        raise argparse.ArgumentTypeError("--top-n must be greater than zero")

    return parsed


def _build_parser() -> JsonArgumentParser:
    parser = JsonArgumentParser(prog="python -m agent_invest_scripts.list_universe")
    parser.add_argument("--top-n", required=True, type=_positive_int)
    parser.add_argument("--as-of")
    add_timeout_argument(parser)
    return parser


def _normalize_scalar(value: Any) -> Any:
    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        try:
            return value.item()
        except ValueError:
            return value

    return value


def _rank_universe(frame: pd.DataFrame, *, top_n: int) -> list[dict[str, Any]]:
    if frame.empty:
        raise ValueError("asset_universe dataset is empty")

    if "coin_id" not in frame.columns:
        raise ValueError('asset_universe dataset must contain a "coin_id" column')

    if "market_cap" not in frame.columns:
        raise ValueError('asset_universe dataset must contain a "market_cap" column')

    ranked_frame = frame.copy()
    missing_columns = [
        column for column in _OUTPUT_COLUMNS if column not in ranked_frame.columns
    ]

    if missing_columns:
        formatted = ", ".join(sorted(missing_columns))
        raise ValueError(
            f"asset_universe output is missing required column(s): {formatted}"
        )

    if "market_cap_rank" not in ranked_frame.columns:
        ranked_frame["market_cap_rank"] = pd.NA

    ranked_frame["market_cap_rank"] = pd.to_numeric(
        ranked_frame["market_cap_rank"], errors="coerce"
    )
    ranked_frame["market_cap"] = pd.to_numeric(
        ranked_frame["market_cap"], errors="coerce"
    )
    ranked_frame = ranked_frame.dropna(subset=["coin_id"])
    ranked_frame = ranked_frame.assign(
        _has_rank=ranked_frame["market_cap_rank"].notna(),
        _fallback_market_cap=ranked_frame["market_cap"].where(
            ranked_frame["market_cap_rank"].isna()
        ),
    )
    ranked = ranked_frame.sort_values(
        by=["_has_rank", "market_cap_rank", "_fallback_market_cap", "coin_id"],
        ascending=[False, True, False, True],
        kind="mergesort",
    ).head(top_n)

    rows: list[dict[str, Any]] = []

    for rank, row in enumerate(ranked.itertuples(index=False), start=1):
        values = row._asdict()
        rows.append(
            {
                "coin_id": _normalize_scalar(values["coin_id"]),
                "symbol": _normalize_scalar(values["symbol"]),
                "name": _normalize_scalar(values["name"]),
                "market_cap": _normalize_scalar(values["market_cap"]),
                "rank": rank,
            }
        )

    return rows


def run(*, top_n: int, as_of: str | None) -> list[dict[str, Any]]:
    if as_of is not None:
        raise SnapshotNotFoundError(as_of)

    frame = asset_universe()
    return _rank_universe(frame, top_n=top_n)


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            payload = run(top_n=args.top_n, as_of=args.as_of)
    except SnapshotNotFoundError as error:
        _print_error({"error": str(error), "as_of": error.as_of})
        raise SystemExit(1) from error
    except Exception as error:
        _print_error({"error": str(error)})
        raise SystemExit(1) from error

    print_json(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
