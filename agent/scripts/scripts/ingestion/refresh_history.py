"""Refresh CoinGecko price, market-cap, volume, and metadata datasets.

Daily metric datasets are upserted by ``(coin_id, date)``. Each rerun starts
one day before the oldest existing metric cutoff for a coin so a previous
partial multi-file refresh is healed on the next successful run.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Sequence

import pandas as pd

from agent_invest_scripts._lib import print_json

from ._storage import DatasetStorage
from .coingecko import CoinGeckoClient
from .refresh_universe import (
    DEFAULT_VS_CURRENCY,
    JsonArgumentParser,
    _iso_date,
    _print_error,
    _today_utc,
    fetch_universe_snapshot,
)

DEFAULT_BOOTSTRAP_DAYS = 365


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be an integer") from error

    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be greater than zero")

    return parsed


def _build_parser() -> JsonArgumentParser:
    parser = JsonArgumentParser(prog="python -m scripts.ingestion.refresh_history")
    parser.add_argument("--date", type=_iso_date, default=_today_utc())
    parser.add_argument("--vs-currency", default=DEFAULT_VS_CURRENCY)
    parser.add_argument(
        "--bootstrap-days", type=_positive_int, default=DEFAULT_BOOTSTRAP_DAYS
    )
    return parser


def _latest_dates(frame: pd.DataFrame | None) -> dict[str, date]:
    if frame is None or frame.empty:
        return {}

    if "coin_id" not in frame.columns or "date" not in frame.columns:
        return {}

    dates = pd.to_datetime(frame["date"], errors="coerce").dt.date
    parsed = frame.assign(_date=dates).dropna(subset=["coin_id", "_date"])
    if parsed.empty:
        return {}

    grouped = parsed.groupby("coin_id", sort=False)["_date"].max()
    return {str(coin_id): snapshot_date for coin_id, snapshot_date in grouped.items()}


def _series_frame(
    coin_id: str,
    samples: list[list[float]] | list[tuple[float, float]],
    *,
    value_column: str,
) -> pd.DataFrame:
    if not samples:
        return pd.DataFrame(columns=["date", "coin_id", value_column])

    frame = pd.DataFrame(samples, columns=["timestamp_ms", value_column])
    frame["date"] = pd.to_datetime(frame["timestamp_ms"], unit="ms", utc=True).dt.date
    frame[value_column] = pd.to_numeric(frame[value_column], errors="coerce")
    frame["coin_id"] = coin_id
    frame = frame.dropna(subset=["date", "coin_id", value_column])
    frame = frame.sort_values(["date", "timestamp_ms"])
    frame = frame.drop_duplicates(subset=["date", "coin_id"], keep="last")
    return frame.loc[:, ["date", "coin_id", value_column]].reset_index(drop=True)


def _merge_metric_dataset(
    existing: pd.DataFrame | None,
    updates: pd.DataFrame,
    *,
    value_column: str,
) -> pd.DataFrame:
    if existing is None or existing.empty:
        combined = updates.copy()
    else:
        combined = pd.concat([existing, updates], ignore_index=True, sort=False)

    if combined.empty:
        return pd.DataFrame(columns=["date", "coin_id", value_column])

    combined["date"] = pd.to_datetime(combined["date"], errors="coerce").dt.date
    combined[value_column] = pd.to_numeric(combined[value_column], errors="coerce")
    combined = combined.dropna(subset=["date", "coin_id", value_column])
    combined = combined.drop_duplicates(subset=["date", "coin_id"], keep="last")
    combined = combined.sort_values(["date", "coin_id"]).reset_index(drop=True)
    return combined.loc[:, ["date", "coin_id", value_column]]


def _merge_metadata(
    existing: pd.DataFrame | None,
    universe_snapshot: pd.DataFrame,
    *,
    snapshot_date: date,
) -> pd.DataFrame:
    required_columns = ["coin_id", "symbol", "name"]
    missing_columns = [
        column for column in required_columns if column not in universe_snapshot.columns
    ]
    if missing_columns:
        formatted = ", ".join(sorted(missing_columns))
        raise ValueError(
            f"universe snapshot is missing required metadata column(s): {formatted}"
        )

    metadata_columns = [
        column
        for column in ("coin_id", "symbol", "name", "image", "market_cap_rank")
        if column in universe_snapshot.columns
    ]
    metadata = universe_snapshot.loc[:, metadata_columns].copy()
    metadata["symbol"] = metadata["symbol"].astype("string").str.lower()
    metadata["refreshed_at"] = pd.Timestamp(
        datetime.combine(snapshot_date, time.min, tzinfo=timezone.utc)
    )
    metadata = metadata.dropna(subset=["coin_id"])
    metadata = metadata.drop_duplicates(subset=["coin_id"], keep="first")

    if existing is None or existing.empty or "coin_id" not in existing.columns:
        return metadata.sort_values(
            ["market_cap_rank", "coin_id"], na_position="last"
        ).reset_index(drop=True)

    retained = existing.loc[~existing["coin_id"].isin(metadata["coin_id"])].copy()
    combined = pd.concat([retained, metadata], ignore_index=True, sort=False)
    sort_columns = [
        column
        for column in ("market_cap_rank", "coin_id")
        if column in combined.columns
    ]
    if sort_columns:
        combined = combined.sort_values(
            sort_columns, ascending=[True, True], na_position="last"
        )
    return combined.reset_index(drop=True)


def _range_start_dates(
    coin_ids: list[str],
    *,
    snapshot_date: date,
    bootstrap_days: int,
    daily_prices: pd.DataFrame | None,
    daily_market_caps: pd.DataFrame | None,
    daily_volumes: pd.DataFrame | None,
) -> dict[str, date]:
    baseline = snapshot_date - timedelta(days=bootstrap_days)
    price_dates = _latest_dates(daily_prices)
    market_cap_dates = _latest_dates(daily_market_caps)
    volume_dates = _latest_dates(daily_volumes)
    starts: dict[str, date] = {}

    for coin_id in coin_ids:
        candidates = [
            known_date
            for known_date in (
                price_dates.get(coin_id),
                market_cap_dates.get(coin_id),
                volume_dates.get(coin_id),
            )
            if known_date is not None
        ]
        if candidates:
            starts[coin_id] = min(candidates) - timedelta(days=1)
        else:
            starts[coin_id] = baseline

    return starts


def _range_end_unix(snapshot_date: date) -> int:
    today = _today_utc()
    if snapshot_date >= today:
        return int(datetime.now(timezone.utc).timestamp())

    end_of_day = datetime.combine(
        snapshot_date + timedelta(days=1), time.min, tzinfo=timezone.utc
    ) - timedelta(seconds=1)
    return int(end_of_day.timestamp())


def _range_start_unix(snapshot_date: date) -> int:
    return int(
        datetime.combine(snapshot_date, time.min, tzinfo=timezone.utc).timestamp()
    )


def run(
    *,
    storage: DatasetStorage,
    client: CoinGeckoClient,
    snapshot_date: date,
    vs_currency: str,
    bootstrap_days: int,
) -> dict[str, Any]:
    universe_snapshot = fetch_universe_snapshot(
        client,
        snapshot_date=snapshot_date,
        vs_currency=vs_currency,
    )
    coin_ids = [str(value) for value in universe_snapshot["coin_id"].dropna().tolist()]
    if not coin_ids:
        raise RuntimeError("CoinGecko returned a universe snapshot without coin ids")

    existing_prices = storage.read_dataset("daily_prices")
    existing_market_caps = storage.read_dataset("daily_market_caps")
    existing_volumes = storage.read_dataset("daily_volumes")
    existing_metadata = storage.read_dataset("coin_metadata")
    range_starts = _range_start_dates(
        coin_ids,
        snapshot_date=snapshot_date,
        bootstrap_days=bootstrap_days,
        daily_prices=existing_prices,
        daily_market_caps=existing_market_caps,
        daily_volumes=existing_volumes,
    )

    price_updates: list[pd.DataFrame] = []
    market_cap_updates: list[pd.DataFrame] = []
    volume_updates: list[pd.DataFrame] = []
    range_end_unix = _range_end_unix(snapshot_date)

    for coin_id in coin_ids:
        history = client.get_market_chart_range(
            coin_id,
            vs_currency=vs_currency,
            from_unix=_range_start_unix(range_starts[coin_id]),
            to_unix=range_end_unix,
            interval="daily",
        )
        price_updates.append(
            _series_frame(coin_id, history.get("prices", []), value_column="price")
        )
        market_cap_updates.append(
            _series_frame(
                coin_id,
                history.get("market_caps", []),
                value_column="market_cap",
            )
        )
        volume_updates.append(
            _series_frame(
                coin_id,
                history.get("total_volumes", []),
                value_column="volume",
            )
        )

    merged_prices = _merge_metric_dataset(
        existing_prices,
        pd.concat(price_updates, ignore_index=True)
        if price_updates
        else pd.DataFrame(),
        value_column="price",
    )
    merged_market_caps = _merge_metric_dataset(
        existing_market_caps,
        pd.concat(market_cap_updates, ignore_index=True)
        if market_cap_updates
        else pd.DataFrame(),
        value_column="market_cap",
    )
    merged_volumes = _merge_metric_dataset(
        existing_volumes,
        pd.concat(volume_updates, ignore_index=True)
        if volume_updates
        else pd.DataFrame(),
        value_column="volume",
    )
    merged_metadata = _merge_metadata(
        existing_metadata,
        universe_snapshot,
        snapshot_date=snapshot_date,
    )

    price_key = storage.write_dataset("daily_prices", merged_prices)
    market_cap_key = storage.write_dataset("daily_market_caps", merged_market_caps)
    volume_key = storage.write_dataset("daily_volumes", merged_volumes)
    metadata_key = storage.write_dataset("coin_metadata", merged_metadata)

    return {
        "coins": len(coin_ids),
        "mode": "upsert_by_coin_id_and_date",
        "date": snapshot_date.isoformat(),
        "bootstrap_days": bootstrap_days,
        "daily_prices": {
            "rows": int(len(merged_prices)),
            "uri": storage.dataset_uri("daily_prices"),
            "key": price_key,
        },
        "daily_market_caps": {
            "rows": int(len(merged_market_caps)),
            "uri": storage.dataset_uri("daily_market_caps"),
            "key": market_cap_key,
        },
        "daily_volumes": {
            "rows": int(len(merged_volumes)),
            "uri": storage.dataset_uri("daily_volumes"),
            "key": volume_key,
        },
        "coin_metadata": {
            "rows": int(len(merged_metadata)),
            "uri": storage.dataset_uri("coin_metadata"),
            "key": metadata_key,
        },
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
                bootstrap_days=args.bootstrap_days,
            )
    except Exception as error:
        _print_error({"error": str(error)})
        raise SystemExit(1) from error

    print_json(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
