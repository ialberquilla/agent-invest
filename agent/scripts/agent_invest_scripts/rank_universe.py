"""CLI for screening the materialized asset feature universe."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import date
from typing import Any, Literal

import pandas as pd

from agent_invest_scripts._lib import (
    asset_universe_features,
    compute_universe_features_as_of,
    print_json,
)
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    resolve_timeout_seconds,
    script_timeout,
)

SortMode = Literal["market_cap_rank", "momentum_180d", "sharpe_180d", "low_volatility"]
RiskProfile = Literal["preserve", "balanced", "aggressive", "max_upside"]
Objective = Literal["return", "low_volatility", "balanced", "max_upside"]

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
    parser.add_argument(
        "--as-of",
        type=_date,
        help="Anchor screening features to this date instead of the latest snapshot. Required to avoid forward-peeking when the chosen coins will then be backtested over an overlapping window.",
    )
    parser.add_argument("--max-market-cap-rank", type=_positive_int)
    parser.add_argument("--min-market-cap", type=float)
    parser.add_argument("--min-data-days-365d", type=_non_negative_int, default=180)
    parser.add_argument("--min-history-days", type=_non_negative_int)
    parser.add_argument("--first-price-before", type=_date)
    parser.add_argument("--positive-trend-only", action="store_true")
    parser.add_argument("--min-return-180d", type=float)
    parser.add_argument("--min-return-365d", type=float)
    parser.add_argument("--max-volatility-180d", type=float)
    parser.add_argument("--max-drawdown-180d", type=float)
    parser.add_argument("--exclude-stablecoins", action="store_true")
    parser.add_argument("--exclude-wrapped", action="store_true")
    parser.add_argument(
        "--risk-profile",
        choices=["preserve", "balanced", "aggressive", "max_upside"],
        help="Preference-aware ranking: preserve favors lower volatility, balanced favors Sharpe, aggressive favors momentum, max_upside tilts toward smaller high-momentum assets.",
    )
    parser.add_argument("--sort", choices=_SORT_MODES, default="market_cap_rank")
    parser.add_argument(
        "--objective",
        choices=["return", "low_volatility", "balanced", "max_upside"],
        help="Objective-aware default ranking when --risk-profile is not enough.",
    )
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            payload = run(
                top_n=args.top_n,
                as_of=args.as_of,
                max_market_cap_rank=args.max_market_cap_rank,
                min_market_cap=args.min_market_cap,
                min_data_days_365d=args.min_data_days_365d,
                min_history_days=args.min_history_days,
                first_price_before=args.first_price_before,
                positive_trend_only=args.positive_trend_only,
                min_return_180d=args.min_return_180d,
                min_return_365d=args.min_return_365d,
                max_volatility_180d=args.max_volatility_180d,
                max_drawdown_180d=args.max_drawdown_180d,
                exclude_stablecoins=args.exclude_stablecoins,
                exclude_wrapped=args.exclude_wrapped,
                risk_profile=args.risk_profile,
                sort=args.sort,
                objective=args.objective,
            )
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)

    print_json(payload)
    return 0


def run(
    *,
    top_n: int,
    as_of: date | None = None,
    max_market_cap_rank: int | None = None,
    min_market_cap: float | None = None,
    min_data_days_365d: int = 180,
    min_history_days: int | None = None,
    first_price_before: date | None = None,
    positive_trend_only: bool = False,
    min_return_180d: float | None = None,
    min_return_365d: float | None = None,
    max_volatility_180d: float | None = None,
    max_drawdown_180d: float | None = None,
    exclude_stablecoins: bool = False,
    exclude_wrapped: bool = False,
    risk_profile: RiskProfile | None = None,
    sort: SortMode = "market_cap_rank",
    objective: Objective | None = None,
) -> list[dict[str, Any]]:
    frame = (
        compute_universe_features_as_of(as_of=as_of)
        if as_of is not None
        else asset_universe_features()
    )
    return rank_universe(
        frame,
        top_n=top_n,
        max_market_cap_rank=max_market_cap_rank,
        min_market_cap=min_market_cap,
        min_data_days_365d=min_data_days_365d,
        min_history_days=min_history_days,
        first_price_before=first_price_before,
        positive_trend_only=positive_trend_only,
        min_return_180d=min_return_180d,
        min_return_365d=min_return_365d,
        max_volatility_180d=max_volatility_180d,
        max_drawdown_180d=max_drawdown_180d,
        exclude_stablecoins=exclude_stablecoins,
        exclude_wrapped=exclude_wrapped,
        risk_profile=risk_profile,
        sort=sort,
        objective=objective,
    )


def rank_universe(
    frame: pd.DataFrame,
    *,
    top_n: int,
    max_market_cap_rank: int | None = None,
    min_market_cap: float | None = None,
    min_data_days_365d: int = 180,
    min_history_days: int | None = None,
    first_price_before: date | None = None,
    positive_trend_only: bool = False,
    min_return_180d: float | None = None,
    min_return_365d: float | None = None,
    max_volatility_180d: float | None = None,
    max_drawdown_180d: float | None = None,
    exclude_stablecoins: bool = False,
    exclude_wrapped: bool = False,
    risk_profile: RiskProfile | None = None,
    sort: SortMode = "market_cap_rank",
    objective: Objective | None = None,
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

    ranked_frame["first_price_date"] = pd.to_datetime(
        ranked_frame["first_price_date"], errors="coerce"
    )
    ranked_frame["last_price_date"] = pd.to_datetime(
        ranked_frame["last_price_date"], errors="coerce"
    )

    if min_history_days is not None:
        ranked_frame = ranked_frame[
            ranked_frame["first_price_date"].notna()
            & ranked_frame["last_price_date"].notna()
        ].copy()
        ranked_frame["history_days"] = (
            ranked_frame["last_price_date"] - ranked_frame["first_price_date"]
        ).dt.days + 1
        ranked_frame = ranked_frame[ranked_frame["history_days"] >= min_history_days]

    if first_price_before is not None:
        cutoff = pd.Timestamp(first_price_before)
        ranked_frame = ranked_frame[
            ranked_frame["first_price_date"].notna()
            & (ranked_frame["first_price_date"] <= cutoff)
        ]

    if max_market_cap_rank is not None:
        ranked_frame = ranked_frame[
            ranked_frame["market_cap_rank"].notna()
            & (ranked_frame["market_cap_rank"] <= max_market_cap_rank)
        ]

    if min_market_cap is not None:
        ranked_frame = ranked_frame[
            ranked_frame["market_cap"].notna()
            & (ranked_frame["market_cap"] >= min_market_cap)
        ]

    if exclude_stablecoins:
        ranked_frame = ranked_frame[~ranked_frame.apply(_is_stablecoin, axis=1)]

    if exclude_wrapped:
        ranked_frame = ranked_frame[~ranked_frame.apply(_is_wrapped_asset, axis=1)]

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

    if min_return_180d is not None:
        ranked_frame = ranked_frame[
            ranked_frame["return_180d"].notna()
            & (ranked_frame["return_180d"] >= min_return_180d)
        ]

    if min_return_365d is not None:
        ranked_frame = ranked_frame[
            ranked_frame["return_365d"].notna()
            & (ranked_frame["return_365d"] >= min_return_365d)
        ]

    if max_drawdown_180d is not None:
        ranked_frame = ranked_frame[
            ranked_frame["max_drawdown_180d"].notna()
            & (ranked_frame["max_drawdown_180d"] >= max_drawdown_180d)
        ]

    ranked_frame = _sort_for_objective(
        ranked_frame, sort=sort, risk_profile=risk_profile, objective=objective
    ).head(top_n)

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


def _sort_for_preferences(
    frame: pd.DataFrame, *, sort: SortMode, risk_profile: RiskProfile | None
) -> pd.DataFrame:
    if risk_profile is None:
        return _sort_frame(frame, sort=sort)
    if risk_profile == "preserve":
        return frame.dropna(subset=["volatility_180d", "market_cap_rank"]).sort_values(
            by=["volatility_180d", "market_cap_rank", "coin_id"],
            ascending=[True, True, True],
            kind="mergesort",
        )
    if risk_profile == "balanced":
        return frame.dropna(subset=["sharpe_180d", "market_cap_rank"]).sort_values(
            by=["sharpe_180d", "market_cap_rank", "coin_id"],
            ascending=[False, True, True],
            kind="mergesort",
        )
    if risk_profile == "aggressive":
        return frame.dropna(subset=["return_180d", "market_cap_rank"]).sort_values(
            by=["return_180d", "market_cap_rank", "coin_id"],
            ascending=[False, True, True],
            kind="mergesort",
        )
    if risk_profile == "max_upside":
        return frame.dropna(subset=["return_180d", "market_cap_rank"]).sort_values(
            by=["return_180d", "market_cap_rank", "coin_id"],
            ascending=[False, False, True],
            kind="mergesort",
        )
    raise ValueError(f"unsupported risk profile: {risk_profile}")


def _sort_for_objective(
    frame: pd.DataFrame,
    *,
    sort: SortMode,
    risk_profile: RiskProfile | None,
    objective: Objective | None,
) -> pd.DataFrame:
    if objective is None:
        return _sort_for_preferences(frame, sort=sort, risk_profile=risk_profile)
    if objective == "return":
        return frame.dropna(subset=["return_365d", "return_180d"]).sort_values(
            by=["return_365d", "return_180d", "coin_id"],
            ascending=[False, False, True],
            kind="mergesort",
        )
    if objective == "max_upside":
        return frame.dropna(
            subset=["return_365d", "return_180d", "market_cap_rank"]
        ).sort_values(
            by=["return_365d", "return_180d", "market_cap_rank", "coin_id"],
            ascending=[False, False, False, True],
            kind="mergesort",
        )
    if objective == "low_volatility":
        return frame.dropna(
            subset=["volatility_180d", "max_drawdown_180d"]
        ).sort_values(
            by=["volatility_180d", "max_drawdown_180d", "coin_id"],
            ascending=[True, False, True],
            kind="mergesort",
        )
    if objective == "balanced":
        return frame.dropna(subset=["sharpe_180d", "max_drawdown_180d"]).sort_values(
            by=["sharpe_180d", "max_drawdown_180d", "coin_id"],
            ascending=[False, False, True],
            kind="mergesort",
        )
    raise ValueError(f"unsupported objective: {objective}")


def _is_stablecoin(row: pd.Series) -> bool:
    text = f"{row.get('coin_id', '')} {row.get('symbol', '')} {row.get('name', '')}".lower()
    stable_terms = ("stable", "usd", "usdt", "usdc", "dai", "tusd", "busd")
    return any(term in text for term in stable_terms)


def _is_wrapped_asset(row: pd.Series) -> bool:
    coin_id = str(row.get("coin_id", "")).lower()
    symbol = str(row.get("symbol", "")).lower()
    name = str(row.get("name", "")).lower()
    return (
        "wrapped" in name
        or coin_id.startswith("wrapped-")
        or symbol in {"wbtc", "weth", "wsteth"}
    )


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


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a YYYY-MM-DD date") from error


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
