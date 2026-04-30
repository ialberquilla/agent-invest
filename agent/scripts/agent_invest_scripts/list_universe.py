"""CLI for listing the top-N universe entries by market cap."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from typing import Any, Sequence

import pandas as pd

from agent_invest_scripts._lib import (
    DatasetNotFoundError,
    coin_metadata,
    print_json,
    universe_history,
)
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    resolve_timeout_seconds,
    script_timeout,
)

_OUTPUT_COLUMNS = ("coin_id", "symbol", "name", "market_cap")


class SnapshotNotFoundError(LookupError):
    """Raised when the requested universe snapshot date is not present."""

    def __init__(self, as_of: date) -> None:
        super().__init__(f"No universe snapshot found for {as_of.isoformat()}")
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


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("--as-of must use YYYY-MM-DD") from error


def _build_parser() -> JsonArgumentParser:
    parser = JsonArgumentParser(prog="python -m agent_invest_scripts.list_universe")
    parser.add_argument("--top-n", required=True, type=_positive_int)
    parser.add_argument("--as-of", type=_iso_date)
    add_timeout_argument(parser)
    return parser


def _date_column(frame: pd.DataFrame) -> str:
    for column in ("date", "as_of"):
        if column in frame.columns:
            return column

    raise ValueError(
        'universe_history dataset must contain either a "date" or "as_of" column'
    )


def _normalize_scalar(value: Any) -> Any:
    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        try:
            return value.item()
        except ValueError:
            return value

    return value


def _coerce_snapshot_dates(frame: pd.DataFrame) -> pd.Series:
    column = _date_column(frame)
    dates = pd.to_datetime(frame[column], errors="coerce")

    if dates.isna().all():
        raise ValueError(f'universe_history dataset has no valid values in "{column}"')

    return dates.dt.date


def _with_metadata(snapshot: pd.DataFrame) -> pd.DataFrame:
    missing_columns = [
        column for column in ("symbol", "name") if column not in snapshot.columns
    ]

    if not missing_columns:
        return snapshot

    metadata = coin_metadata()

    if "coin_id" not in metadata.columns:
        raise ValueError('coin_metadata dataset must contain a "coin_id" column')

    unavailable_columns = [
        column for column in missing_columns if column not in metadata.columns
    ]

    if unavailable_columns:
        formatted = ", ".join(sorted(unavailable_columns))
        raise ValueError(
            f"coin_metadata dataset is missing required column(s): {formatted}"
        )

    metadata_columns = ["coin_id", *missing_columns]
    return snapshot.merge(
        metadata.loc[:, metadata_columns].drop_duplicates(subset=["coin_id"]),
        on="coin_id",
        how="left",
    )


def _select_snapshot(frame: pd.DataFrame, *, as_of: date | None) -> pd.DataFrame:
    if frame.empty:
        raise ValueError("universe_history dataset is empty")

    dated = frame.assign(_snapshot_date=_coerce_snapshot_dates(frame)).dropna(
        subset=["_snapshot_date"]
    )

    if dated.empty:
        raise ValueError(
            "universe_history dataset has no rows with a valid snapshot date"
        )

    snapshot_date = as_of or dated["_snapshot_date"].max()
    snapshot = dated.loc[dated["_snapshot_date"] == snapshot_date].copy()

    if snapshot.empty:
        raise SnapshotNotFoundError(snapshot_date)

    return snapshot


def _rank_snapshot(snapshot: pd.DataFrame, *, top_n: int) -> list[dict[str, Any]]:
    if "coin_id" not in snapshot.columns:
        raise ValueError('universe_history dataset must contain a "coin_id" column')

    if "market_cap" not in snapshot.columns:
        raise ValueError('universe_history dataset must contain a "market_cap" column')

    enriched = _with_metadata(snapshot).copy()
    missing_columns = [
        column for column in _OUTPUT_COLUMNS if column not in enriched.columns
    ]

    if missing_columns:
        formatted = ", ".join(sorted(missing_columns))
        raise ValueError(
            f"universe_history output is missing required column(s): {formatted}"
        )

    enriched["market_cap"] = pd.to_numeric(enriched["market_cap"], errors="coerce")
    enriched = enriched.dropna(subset=["market_cap", "coin_id"])
    ranked = enriched.sort_values(
        by=["market_cap", "coin_id"],
        ascending=[False, True],
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


def run(*, top_n: int, as_of: date | None) -> list[dict[str, Any]]:
    frame = universe_history()
    snapshot = _select_snapshot(frame, as_of=as_of)
    return _rank_snapshot(snapshot, top_n=top_n)


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            payload = run(top_n=args.top_n, as_of=args.as_of)
    except SnapshotNotFoundError as error:
        _print_error({"error": str(error), "as_of": error.as_of.isoformat()})
        raise SystemExit(1) from error
    except DatasetNotFoundError as error:
        _print_error(
            {
                "error": str(error),
                "dataset": error.dataset,
                "key": error.key,
                "path": error.path,
            }
        )
        raise SystemExit(1) from error
    except Exception as error:
        _print_error({"error": str(error)})
        raise SystemExit(1) from error

    print_json(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
