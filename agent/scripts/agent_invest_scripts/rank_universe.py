"""CLI for screening the materialized asset feature universe."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from typing import Any, Literal

import pandas as pd

from agent_invest_scripts._lib import asset_universe_features, print_json
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    resolve_timeout_seconds,
    script_timeout,
)

SortMode = Literal["market_cap_rank", "momentum_180d", "sharpe_180d", "low_volatility"]

_SORT_MODES: tuple[SortMode, ...] = (
    "market_cap_rank",
    "momentum_180d",
    "sharpe_180d",
    "low_volatility",
)
_REQUIRED_COLUMNS = (
    "asset_id",
    "coin_id",
    "symbol",
    "name",
    "market_cap_rank",
    "market_cap",
    "latest_price",
    "first_price_date",
    "last_price_date",
    "data_days_365d",
    "return_30d",
    "return_90d",
    "return_180d",
    "return_365d",
    "volatility_30d",
    "volatility_90d",
    "volatility_180d",
    "max_drawdown_180d",
    "sharpe_180d",
    "price_above_sma_200d",
)
_NUMERIC_COLUMNS = (
    "market_cap_rank",
    "market_cap",
    "latest_price",
    "data_days_365d",
    "return_30d",
    "return_90d",
    "return_180d",
    "return_365d",
    "volatility_30d",
    "volatility_90d",
    "volatility_180d",
    "max_drawdown_180d",
    "sharpe_180d",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rank the asset universe by materialized screening features."
    )
    parser.add_argument("--top-n", required=True, type=_positive_int)
    parser.add_argument("--max-market-cap-rank", type=_positive_int)
    parser.add_argument("--min-data-days-365d", type=_non_negative_int, default=180)
    parser.add_argument("--positive-trend-only", action="store_true")
    parser.add_argument("--max-volatility-180d", type=float)
    parser.add_argument("--sort", choices=_SORT_MODES, default="market_cap_rank")
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            payload = run(
                top_n=args.top_n,
                max_market_cap_rank=args.max_market_cap_rank,
                min_data_days_365d=args.min_data_days_365d,
                positive_trend_only=args.positive_trend_only,
                max_volatility_180d=args.max_volatility_180d,
                sort=args.sort,
            )
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)

    print_json(payload)
    return 0


def run(
    *,
    top_n: int,
    max_market_cap_rank: int | None = None,
    min_data_days_365d: int = 180,
    positive_trend_only: bool = False,
    max_volatility_180d: float | None = None,
    sort: SortMode = "market_cap_rank",
) -> list[dict[str, Any]]:
    frame = asset_universe_features()
    return rank_universe(
        frame,
        top_n=top_n,
        max_market_cap_rank=max_market_cap_rank,
        min_data_days_365d=min_data_days_365d,
        positive_trend_only=positive_trend_only,
        max_volatility_180d=max_volatility_180d,
        sort=sort,
    )


def rank_universe(
    frame: pd.DataFrame,
    *,
    top_n: int,
    max_market_cap_rank: int | None = None,
    min_data_days_365d: int = 180,
    positive_trend_only: bool = False,
    max_volatility_180d: float | None = None,
    sort: SortMode = "market_cap_rank",
) -> list[dict[str, Any]]:
    _require_columns(frame)
    ranked_frame = frame.copy()

    for column in _NUMERIC_COLUMNS:
        ranked_frame[column] = pd.to_numeric(ranked_frame[column], errors="coerce")

    ranked_frame = ranked_frame.dropna(subset=["coin_id"])
    ranked_frame = ranked_frame[
        ranked_frame["data_days_365d"].notna()
        & (ranked_frame["data_days_365d"] >= min_data_days_365d)
    ]

    if max_market_cap_rank is not None:
        ranked_frame = ranked_frame[
            ranked_frame["market_cap_rank"].notna()
            & (ranked_frame["market_cap_rank"] <= max_market_cap_rank)
        ]

    if positive_trend_only:
        ranked_frame = ranked_frame[
            (ranked_frame["return_180d"].isna() | (ranked_frame["return_180d"] > 0))
            & (
                ranked_frame["price_above_sma_200d"].isna()
                | (ranked_frame["price_above_sma_200d"].astype("boolean"))
            )
        ]

    if max_volatility_180d is not None:
        ranked_frame = ranked_frame[
            ranked_frame["volatility_180d"].notna()
            & (ranked_frame["volatility_180d"] <= max_volatility_180d)
        ]

    ranked_frame = _sort_frame(ranked_frame, sort=sort).head(top_n)

    return [
        {"rank": rank, **_output_row(row._asdict())}
        for rank, row in enumerate(ranked_frame.itertuples(index=False), start=1)
    ]


def _sort_frame(frame: pd.DataFrame, *, sort: SortMode) -> pd.DataFrame:
    if sort == "market_cap_rank":
        return _drop_missing_sort_value(frame, "market_cap_rank").sort_values(
            by=["market_cap_rank", "coin_id"], ascending=[True, True], kind="mergesort"
        )
    if sort == "momentum_180d":
        return _drop_missing_sort_value(frame, "return_180d").sort_values(
            by=["return_180d", "coin_id"], ascending=[False, True], kind="mergesort"
        )
    if sort == "sharpe_180d":
        return _drop_missing_sort_value(frame, "sharpe_180d").sort_values(
            by=["sharpe_180d", "coin_id"], ascending=[False, True], kind="mergesort"
        )
    if sort == "low_volatility":
        return _drop_missing_sort_value(frame, "volatility_180d").sort_values(
            by=["volatility_180d", "coin_id"], ascending=[True, True], kind="mergesort"
        )
    raise ValueError(f"unsupported sort mode: {sort}")


def _drop_missing_sort_value(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    return frame[frame[column].notna()]


def _output_row(values: dict[str, Any]) -> dict[str, Any]:
    return {column: _normalize_scalar(values[column]) for column in _REQUIRED_COLUMNS}


def _require_columns(frame: pd.DataFrame) -> None:
    missing_columns = [
        column for column in _REQUIRED_COLUMNS if column not in frame.columns
    ]
    if missing_columns:
        formatted = ", ".join(sorted(missing_columns))
        raise ValueError(f"asset_universe_features is missing column(s): {formatted}")


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


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


if __name__ == "__main__":
    raise SystemExit(main())
