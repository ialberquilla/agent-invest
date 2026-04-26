"""Refresh the daily CoinGecko universe snapshot.

Same-day reruns replace the prior snapshot date so the latest successful fetch
wins without rewriting older dates.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from typing import Any, Sequence

import pandas as pd

from agent_invest_scripts._lib import print_json

from ._storage import DatasetStorage
from .coingecko import CoinGeckoClient

DEFAULT_PAGE_SIZE = 250
DEFAULT_VS_CURRENCY = "usd"
_UNIVERSE_COLUMNS = [
    "coin_id",
    "symbol",
    "name",
    "image",
    "market_cap_rank",
    "current_price",
    "market_cap",
    "fully_diluted_valuation",
    "total_volume",
    "circulating_supply",
    "total_supply",
    "max_supply",
    "ath",
    "ath_change_percentage",
    "ath_date",
    "atl",
    "atl_change_percentage",
    "atl_date",
    "price_change_24h",
    "price_change_percentage_24h",
    "market_cap_change_24h",
    "market_cap_change_percentage_24h",
    "last_updated",
]


class JsonArgumentParser(argparse.ArgumentParser):
    """Argument parser that emits JSON errors to stderr."""

    def error(self, message: str) -> None:
        _print_error({"error": message})
        raise SystemExit(2)


def _print_error(payload: dict[str, Any]) -> None:
    json.dump(payload, sys.stderr)
    sys.stderr.write("\n")


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("--date must use YYYY-MM-DD") from error


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def _build_parser() -> JsonArgumentParser:
    parser = JsonArgumentParser(prog="python -m scripts.ingestion.refresh_universe")
    parser.add_argument("--date", type=_iso_date, default=_today_utc())
    parser.add_argument("--vs-currency", default=DEFAULT_VS_CURRENCY)
    return parser


def fetch_universe_snapshot(
    client: CoinGeckoClient,
    *,
    snapshot_date: date,
    vs_currency: str,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> pd.DataFrame:
    pages: list[dict[str, Any]] = []
    page = 1

    while True:
        rows = client.get_markets(
            vs_currency=vs_currency,
            order="market_cap_desc",
            per_page=page_size,
            page=page,
            sparkline=False,
        )
        if not rows:
            break

        pages.extend(rows)

        if len(rows) < page_size:
            break

        page += 1

    if not pages:
        raise RuntimeError("CoinGecko returned an empty universe snapshot")

    frame = pd.DataFrame.from_records(pages).rename(columns={"id": "coin_id"})
    if "coin_id" not in frame.columns:
        raise ValueError('CoinGecko markets payload is missing the "id" field')

    available_columns = [
        column for column in _UNIVERSE_COLUMNS if column in frame.columns
    ]
    snapshot = frame.loc[:, ["coin_id", *available_columns[1:]]].copy()
    snapshot.insert(0, "date", pd.Timestamp(snapshot_date))

    for column in ("ath_date", "atl_date", "last_updated"):
        if column in snapshot.columns:
            snapshot[column] = pd.to_datetime(
                snapshot[column], errors="coerce", utc=True
            )

    if "market_cap_rank" in snapshot.columns:
        snapshot["market_cap_rank"] = pd.to_numeric(
            snapshot["market_cap_rank"], errors="coerce"
        ).astype("Int64")

    snapshot = snapshot.dropna(subset=["coin_id"])
    snapshot = snapshot.drop_duplicates(subset=["coin_id"], keep="first")
    sort_columns = [
        column
        for column in ("market_cap_rank", "market_cap", "coin_id")
        if column in snapshot.columns
    ]
    ascending = [
        True if column == "market_cap_rank" else False for column in sort_columns
    ]
    if "coin_id" in sort_columns:
        ascending[sort_columns.index("coin_id")] = True
    snapshot = snapshot.sort_values(
        sort_columns, ascending=ascending, na_position="last"
    )
    return snapshot.reset_index(drop=True)


def merge_universe_history(
    existing: pd.DataFrame | None,
    snapshot: pd.DataFrame,
) -> pd.DataFrame:
    snapshot = snapshot.copy()
    snapshot_dates = pd.to_datetime(snapshot["date"], errors="coerce").dt.date
    if snapshot_dates.isna().all():
        raise ValueError("snapshot frame must contain at least one valid date")

    snapshot_date = snapshot_dates.iloc[0]

    if existing is None or existing.empty:
        combined = snapshot
    else:
        retained = existing.copy()
        retained_dates = pd.to_datetime(retained.get("date"), errors="coerce").dt.date
        retained = retained.loc[retained_dates != snapshot_date]
        combined = pd.concat([retained, snapshot], ignore_index=True, sort=False)

    combined_dates = pd.to_datetime(combined["date"], errors="coerce").dt.date
    combined = combined.assign(_snapshot_date=combined_dates)
    combined = combined.dropna(subset=["_snapshot_date", "coin_id"])
    combined = combined.drop_duplicates(
        subset=["_snapshot_date", "coin_id"], keep="last"
    )

    sort_columns = [
        column
        for column in ("_snapshot_date", "market_cap_rank", "market_cap", "coin_id")
        if column in combined.columns
    ]
    ascending = [True, True, False, True][: len(sort_columns)]
    if sort_columns:
        combined = combined.sort_values(
            sort_columns, ascending=ascending, na_position="last"
        )

    return combined.drop(columns=["_snapshot_date"]).reset_index(drop=True)


def run(
    *,
    storage: DatasetStorage,
    client: CoinGeckoClient,
    snapshot_date: date,
    vs_currency: str,
) -> dict[str, Any]:
    snapshot = fetch_universe_snapshot(
        client,
        snapshot_date=snapshot_date,
        vs_currency=vs_currency,
    )
    existing = storage.read_dataset("universe_history")
    merged = merge_universe_history(existing, snapshot)
    key = storage.write_dataset("universe_history", merged)

    return {
        "dataset": "universe_history",
        "mode": "append_by_date_replace_same_day",
        "date": snapshot_date.isoformat(),
        "coins": int(len(snapshot)),
        "rows": int(len(merged)),
        "storage_root": str(storage.root),
        "key": key,
        "uri": storage.dataset_uri("universe_history"),
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        storage = DatasetStorage()
        with CoinGeckoClient() as client:
            payload = run(
                storage=storage,
                client=client,
                snapshot_date=args.date,
                vs_currency=args.vs_currency,
            )
    except Exception as error:
        _print_error({"error": str(error)})
        raise SystemExit(1) from error

    print_json(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
