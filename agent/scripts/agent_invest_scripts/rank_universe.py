"""CLI for screening the materialized asset feature universe."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from datetime import date
from typing import Any, Literal

import pandas as pd

from agent_invest_scripts._lib import (
    asset_universe_features,
    compute_universe_features_as_of,
    print_json,
)
from agent_invest_scripts._lib.backtest.factors import FACTORS, SECTOR_TAGS_PATH
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    resolve_timeout_seconds,
    script_timeout,
)
from agent_invest_scripts._lib.data import daily_prices
from agent_invest_scripts._lib.registries import list_registry

SortMode = Literal["market_cap_rank", "momentum_180d", "sharpe_180d", "low_volatility"]
RiskProfile = Literal[
    "preserve",
    "balanced",
    "aggressive",
    "max_upside",
    "high_growth",
    "preserve_capital",
    "income",
]
Objective = Literal[
    "return",
    "low_volatility",
    "balanced",
    "max_upside",
    "high_growth",
    "preserve_capital",
    "income",
]

# Defensive eligibility defaults for free-text thesis paths that bypass wizard
# filters. Request filters with the same meaning intentionally take precedence.
_RISK_PROFILE_ELIGIBILITY_DEFAULTS: dict[str, dict[str, Any]] = {
    "high_growth": {"max_market_cap_rank": 100, "market_cap_floor": 250_000_000},
    "balanced": {"max_market_cap_rank": 50, "market_cap_floor": 1_000_000_000},
    "preserve_capital": {"max_market_cap_rank": 20, "market_cap_floor": 5_000_000_000},
    # TODO: require yield availability when staking/income data is available.
    "income": {"max_market_cap_rank": 30, "market_cap_floor": 1_000_000_000},
}

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
    parser.add_argument("--top-n", type=_positive_int)
    parser.add_argument(
        "--input",
        dest="input_json",
        help=(
            "Extended RankUniverseInput JSON. When supplied, legacy CLI filters "
            "are ignored except --as-of and --timeout-seconds."
        ),
    )
    parser.add_argument(
        "--as-of",
        type=_date,
        help=(
            "Anchor screening features to this date instead of the latest snapshot. "
            "Required to avoid forward-peeking when the chosen coins will then be "
            "backtested over an overlapping window."
        ),
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
        choices=[
            "preserve",
            "balanced",
            "aggressive",
            "max_upside",
            "high_growth",
            "preserve_capital",
            "income",
        ],
        help=(
            "Preference-aware ranking: preserve favors lower volatility, balanced "
            "favors Sharpe, aggressive favors momentum, max_upside tilts toward "
            "smaller high-momentum assets."
        ),
    )
    parser.add_argument("--sort", choices=_SORT_MODES, default="market_cap_rank")
    parser.add_argument(
        "--objective",
        choices=[
            "return",
            "low_volatility",
            "balanced",
            "max_upside",
            "high_growth",
            "preserve_capital",
            "income",
        ],
        help="Objective-aware default ranking when --risk-profile is not enough.",
    )
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            if args.input_json:
                payload = run_extended(json.loads(args.input_json), as_of=args.as_of)
            else:
                if args.top_n is None:
                    parser.error("--top-n is required unless --input is supplied")
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


def run_extended(
    input_payload: dict[str, Any],
    *,
    as_of: date | None = None,
    frame: pd.DataFrame | None = None,
    prices: pd.DataFrame | None = None,
) -> Any:
    if not isinstance(input_payload, dict):
        raise ValueError("RankUniverseInput must be an object")
    frame = frame.copy() if frame is not None else _load_feature_frame(as_of)
    prices = prices.copy() if prices is not None else daily_prices()
    return rank_universe_extended(frame, prices, input_payload, as_of=as_of)


def rank_universe_extended(
    frame: pd.DataFrame,
    prices: pd.DataFrame,
    input_payload: dict[str, Any],
    *,
    as_of: date | None = None,
) -> Any:
    _require_columns(frame)
    ranked_frame = _apply_universe_selector(
        frame.copy(), prices, input_payload.get("universe_selector"), as_of=as_of
    )
    filters = input_payload.get("filters", [])
    if filters is None:
        filters = []
    if not isinstance(filters, list):
        raise ValueError("filters must be an array")
    for item in filters:
        if not isinstance(item, dict):
            raise ValueError("filters entries must be objects")
    risk_profile = _risk_profile_from_input(input_payload)
    applied_defaults = _eligibility_defaults_for_filters(risk_profile, filters)
    ranked_frame = _apply_eligibility_defaults(ranked_frame, applied_defaults)
    ranked_frame = _apply_registered_filters(
        ranked_frame,
        prices,
        [item for item in filters if item.get("id") != "correlation_prune"],
        as_of=as_of,
    )
    ranking = _validate_ranking(input_payload.get("ranking"))
    limit = input_payload.get("limit", len(ranked_frame))
    if limit is None:
        limit = len(ranked_frame)
    if not isinstance(limit, int) or limit < 1:
        raise ValueError("limit must be a positive integer")

    factor_values = _compute_factor_values(ranked_frame, prices, ranking, as_of=as_of)
    scored = _score_factor_values(factor_values, ranking)
    scored = scored.sort_values(
        by=["score", "coin_id"], ascending=[False, True], kind="mergesort"
    )
    scored = _apply_correlation_prune_filter(
        scored, prices, filters, limit, as_of=as_of
    )

    results = [
        {
            "coin_id": row.coin_id,
            "rank": rank,
            "factor_values": {
                factor: _normalize_scalar(getattr(row, factor))
                for factor in factor_values.columns
                if factor != "coin_id"
            },
        }
        for rank, row in enumerate(scored.itertuples(index=False), start=1)
    ]
    if applied_defaults:
        return {"results": results, "applied_defaults": applied_defaults}
    return results


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

    defaults = _eligibility_defaults_for_legacy(
        _risk_profile_for_eligibility(risk_profile, objective),
        max_market_cap_rank=max_market_cap_rank,
        min_market_cap=min_market_cap,
    )
    ranked_frame = _apply_eligibility_defaults(ranked_frame, defaults)

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


def _risk_profile_from_input(input_payload: dict[str, Any]) -> str | None:
    value = input_payload.get("risk_profile") or input_payload.get("objective")
    return str(value) if isinstance(value, str) else None


def _risk_profile_for_eligibility(
    risk_profile: RiskProfile | None, objective: Objective | None
) -> str | None:
    return risk_profile or objective


def _eligibility_defaults_for_filters(
    risk_profile: str | None, filters: list[dict[str, Any]]
) -> dict[str, Any]:
    defaults = dict(_RISK_PROFILE_ELIGIBILITY_DEFAULTS.get(risk_profile or "", {}))
    if not defaults:
        return {}
    filter_ids = {item.get("id") for item in filters}
    if "market_cap_floor" in filter_ids:
        defaults.pop("market_cap_floor", None)
    return defaults


def _eligibility_defaults_for_legacy(
    risk_profile: str | None,
    *,
    max_market_cap_rank: int | None,
    min_market_cap: float | None,
) -> dict[str, Any]:
    defaults = dict(_RISK_PROFILE_ELIGIBILITY_DEFAULTS.get(risk_profile or "", {}))
    if max_market_cap_rank is not None:
        defaults.pop("max_market_cap_rank", None)
    if min_market_cap is not None:
        defaults.pop("market_cap_floor", None)
    return defaults


def _apply_eligibility_defaults(
    frame: pd.DataFrame, defaults: dict[str, Any]
) -> pd.DataFrame:
    filtered = frame.copy()
    max_rank = defaults.get("max_market_cap_rank")
    if max_rank is not None:
        market_cap_rank = pd.to_numeric(filtered["market_cap_rank"], errors="coerce")
        filtered = filtered[market_cap_rank.notna() & (market_cap_rank <= max_rank)]
    market_cap_floor = defaults.get("market_cap_floor")
    if market_cap_floor is not None:
        market_cap = pd.to_numeric(filtered["market_cap"], errors="coerce")
        filtered = filtered[market_cap.notna() & (market_cap >= market_cap_floor)]
    return filtered


def _load_feature_frame(as_of: date | None) -> pd.DataFrame:
    return (
        compute_universe_features_as_of(as_of=as_of)
        if as_of
        else asset_universe_features()
    )


def _registry_ids(name: str) -> set[str]:
    return {entry["id"] for entry in list_registry(name)}


def _apply_universe_selector(
    frame: pd.DataFrame, prices: pd.DataFrame, selector: Any, *, as_of: date | None
) -> pd.DataFrame:
    if selector is None:
        return frame
    if not isinstance(selector, dict):
        raise ValueError("universe_selector must be an object")
    selector_id = selector.get("id")
    if selector_id not in _registry_ids("universe_selectors"):
        raise ValueError(
            f"Unknown universe selector '{selector_id}' in universe_selectors registry"
        )
    params = selector.get("params") or {}
    if not isinstance(params, dict):
        raise ValueError("universe_selector.params must be an object")
    frame = frame.copy()
    for column in ("market_cap_rank", "market_cap"):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if selector_id == "top_n_by_mcap":
        n = params.get("n") or params.get("top_n") or params.get("limit")
        if not isinstance(n, int) or n < 1:
            raise ValueError("top_n_by_mcap requires params.n as a positive integer")
        return (
            frame.dropna(subset=["market_cap_rank"])
            .sort_values(
                by=["market_cap_rank", "coin_id"],
                ascending=[True, True],
                kind="mergesort",
            )
            .head(n)
        )
    if selector_id == "top_n_by_volume":
        n = params.get("n") or params.get("top_n") or params.get("limit")
        if not isinstance(n, int) or n < 1:
            raise ValueError("top_n_by_volume requires params.n as a positive integer")
        volume = _single_factor_series(
            frame, prices, "avg_daily_volume_usd_30d", as_of=as_of
        )
        selected = frame.assign(_selector_volume=volume)
        return (
            selected.dropna(subset=["_selector_volume"])
            .sort_values(
                by=["_selector_volume", "coin_id"],
                ascending=[False, True],
                kind="mergesort",
            )
            .head(n)
            .drop(columns=["_selector_volume"])
        )
    if selector_id == "hand_picked":
        coin_ids = params.get("coin_ids")
        if not isinstance(coin_ids, list) or not all(
            isinstance(c, str) for c in coin_ids
        ):
            raise ValueError("hand_picked requires params.coin_ids as a string array")
        return frame[frame["coin_id"].isin(coin_ids)]
    if selector_id == "sector_filtered":
        sectors = params.get("sectors")
        if not isinstance(sectors, list) or not all(
            isinstance(s, str) for s in sectors
        ):
            raise ValueError(
                "sector_filtered requires params.sectors as a string array"
            )
        sector_set = set(sectors)
        tags = _sector_tags()
        has_sector = frame["coin_id"].map(
            lambda coin_id: bool(sector_set & set(tags.get(str(coin_id), [])))
        )
        return frame[has_sector]
    if selector_id == "liquidity_floor":
        floor = params.get("market_cap_usd") or params.get("usd")
        if not isinstance(floor, int | float) or floor < 0:
            raise ValueError(
                "liquidity_floor requires params.usd as a non-negative number"
            )
        return frame[frame["market_cap"].notna() & (frame["market_cap"] >= floor)]
    return frame


def _apply_registered_filters(
    frame: pd.DataFrame, prices: pd.DataFrame, filters: Any, *, as_of: date | None
) -> pd.DataFrame:
    if filters is None:
        return frame
    if not isinstance(filters, list):
        raise ValueError("filters must be an array")
    valid_filter_ids = _registry_ids("filters")
    filtered = frame.copy()
    for item in filters:
        if not isinstance(item, dict):
            raise ValueError("filters entries must be objects")
        filter_id = item.get("id")
        if filter_id not in valid_filter_ids:
            raise ValueError(f"Unknown filter '{filter_id}' in filters registry")
        value = item.get("value")
        filtered = _apply_registered_filter(
            filtered, prices, str(filter_id), value, as_of=as_of
        )
    return filtered


def _apply_registered_filter(
    frame: pd.DataFrame,
    prices: pd.DataFrame,
    filter_id: str,
    value: Any,
    *,
    as_of: date | None,
) -> pd.DataFrame:
    filtered = frame.copy()
    if filter_id == "min_history_days":
        days = value.get("days") if isinstance(value, dict) else value
        if not isinstance(days, int) or days < 1:
            raise ValueError(
                "min_history_days filter requires an integer day threshold"
            )
        first = pd.to_datetime(filtered["first_price_date"], errors="coerce")
        last = pd.to_datetime(filtered["last_price_date"], errors="coerce")
        history_days = (last - first).dt.days + 1
        return filtered[history_days >= days]
    if filter_id == "market_cap_floor":
        usd = value.get("usd") if isinstance(value, dict) else value
        if not isinstance(usd, int | float) or usd < 0:
            raise ValueError(
                "market_cap_floor filter requires a non-negative USD value"
            )
        market_cap = pd.to_numeric(filtered["market_cap"], errors="coerce")
        return filtered[market_cap.notna() & (market_cap >= usd)]
    if filter_id == "max_drawdown_full_le":
        threshold = value.get("threshold") if isinstance(value, dict) else value
        if not isinstance(threshold, int | float):
            raise ValueError("max_drawdown_full_le filter requires a numeric threshold")
        drawdown = _single_factor_series(
            filtered, prices, "max_drawdown_full", as_of=as_of
        )
        return filtered[drawdown.notna() & (drawdown >= threshold)]
    if filter_id == "volume_floor":
        usd = value.get("usd") if isinstance(value, dict) else value
        if not isinstance(usd, int | float) or usd < 0:
            raise ValueError("volume_floor filter requires a non-negative USD value")
        volume = _single_factor_series(
            filtered, prices, "avg_daily_volume_usd_30d", as_of=as_of
        )
        return filtered[volume.notna() & (volume >= usd)]
    if filter_id in {"sector_in", "sector_not_in"}:
        sectors = value.get("sectors") if isinstance(value, dict) else value
        if not isinstance(sectors, list) or not all(
            isinstance(s, str) for s in sectors
        ):
            raise ValueError(f"{filter_id} filter requires a sectors string array")
        sector_set = set(sectors)
        tags = _sector_tags()
        has_sector = filtered["coin_id"].map(
            lambda coin_id: bool(sector_set & set(tags.get(str(coin_id), [])))
        )
        return (
            filtered[has_sector] if filter_id == "sector_in" else filtered[~has_sector]
        )
    if filter_id == "correlation_prune":
        return filtered
    return filtered


def _apply_correlation_prune_filter(
    scored: pd.DataFrame,
    prices: pd.DataFrame,
    filters: Any,
    limit: int,
    *,
    as_of: date | None,
) -> pd.DataFrame:
    filter_items = [item for item in filters if item.get("id") == "correlation_prune"]
    if not filter_items:
        return scored.head(limit)
    if len(filter_items) > 1:
        raise ValueError("correlation_prune filter may only be specified once")
    value = filter_items[0].get("value") or {}
    if not isinstance(value, dict):
        raise ValueError("correlation_prune filter requires an object value")
    threshold = value.get("threshold", 0.85)
    window_days = value.get("window_days", 365)
    if not isinstance(threshold, int | float) or threshold < -1 or threshold > 1:
        raise ValueError("correlation_prune.threshold must be between -1 and 1")
    if not isinstance(window_days, int) or window_days < 2:
        raise ValueError("correlation_prune.window_days must be an integer >= 2")

    returns = _daily_returns_by_coin(prices, window_days=window_days, as_of=as_of)
    selected_indexes: list[Any] = []
    selected_coin_ids: list[str] = []
    for row in scored.itertuples():
        candidate = str(row.coin_id)
        if all(
            _pairwise_return_correlation(returns, candidate, selected) <= threshold
            for selected in selected_coin_ids
        ):
            selected_indexes.append(row.Index)
            selected_coin_ids.append(candidate)
            if len(selected_indexes) >= limit:
                break
    return scored.loc[selected_indexes]


def _daily_returns_by_coin(
    prices: pd.DataFrame, *, window_days: int, as_of: date | None
) -> dict[str, pd.Series]:
    required = {"date", "coin_id", "price"}
    if required - set(prices.columns):
        missing = ", ".join(sorted(required - set(prices.columns)))
        raise ValueError(f"prices is missing required column(s): {missing}")
    priced = prices.copy()
    priced["date"] = pd.to_datetime(priced["date"], errors="coerce")
    priced["price"] = pd.to_numeric(priced["price"], errors="coerce")
    priced = priced.dropna(subset=["date", "coin_id", "price"])
    end = pd.Timestamp(as_of) if as_of else priced["date"].max()
    start = end - pd.Timedelta(days=window_days)
    priced = priced[(priced["date"] > start) & (priced["date"] <= end)]

    returns: dict[str, pd.Series] = {}
    for coin_id, coin_prices in priced.sort_values("date").groupby("coin_id"):
        series = coin_prices.drop_duplicates("date").set_index("date")["price"]
        returns[str(coin_id)] = series.pct_change().dropna()
    return returns


def _pairwise_return_correlation(
    returns: dict[str, pd.Series], left: str, right: str
) -> float:
    left_returns = returns.get(left)
    right_returns = returns.get(right)
    if left_returns is None or right_returns is None:
        return 0.0
    joined = pd.concat([left_returns, right_returns], axis=1, join="inner").dropna()
    if len(joined) < 2:
        return 0.0
    correlation = joined.iloc[:, 0].corr(joined.iloc[:, 1])
    return 0.0 if pd.isna(correlation) else float(correlation)


def _validate_ranking(ranking: Any) -> list[dict[str, Any]]:
    if not isinstance(ranking, list) or not ranking:
        raise ValueError("ranking must be a non-empty array")
    valid_factor_ids = set(FACTORS)
    validated: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in ranking:
        if not isinstance(item, dict):
            raise ValueError("ranking entries must be objects")
        factor = item.get("factor")
        if factor not in valid_factor_ids:
            raise ValueError(f"Unknown factor '{factor}' in ranking_factors registry")
        if str(factor) in seen:
            raise ValueError(f"Duplicate ranking factor '{factor}'")
        seen.add(str(factor))
        direction = item.get("direction") or FACTORS[str(factor)].direction_better
        if direction not in {"high", "low"}:
            raise ValueError("ranking.direction must be 'high' or 'low'")
        weight = item.get("weight", 1.0)
        if not isinstance(weight, int | float) or weight <= 0:
            raise ValueError("ranking.weight must be positive")
        validated.append(
            {"factor": str(factor), "direction": direction, "weight": float(weight)}
        )
    return validated


def _sector_tags() -> dict[str, list[str]]:
    with SECTOR_TAGS_PATH.open("r", encoding="utf-8") as file:
        data = json.load(file)
    return {str(coin_id): list(tags) for coin_id, tags in data.items()}


def _single_factor_series(
    frame: pd.DataFrame, prices: pd.DataFrame, factor_id: str, *, as_of: date | None
) -> pd.Series:
    values = _compute_factor_values(
        frame,
        prices,
        [{"factor": factor_id, "direction": "high", "weight": 1.0}],
        as_of=as_of,
    ).set_index("coin_id")[factor_id]
    return frame["coin_id"].astype(str).map(values)


def _compute_factor_values(
    frame: pd.DataFrame,
    prices: pd.DataFrame,
    ranking: list[dict[str, Any]],
    *,
    as_of: date | None,
) -> pd.DataFrame:
    required = {"date", "coin_id", "price"}
    if required - set(prices.columns):
        missing = ", ".join(sorted(required - set(prices.columns)))
        raise ValueError(f"prices is missing required column(s): {missing}")
    prices = prices.copy()
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    prices = _attach_factor_metadata(prices, frame)
    rows: list[dict[str, Any]] = []
    factor_ids = [entry["factor"] for entry in ranking]
    for coin_id in sorted(frame["coin_id"].dropna().astype(str).unique()):
        coin_prices = prices[prices["coin_id"] == coin_id].copy()
        row: dict[str, Any] = {"coin_id": coin_id}
        for factor_id in factor_ids:
            value, low_sample = FACTORS[factor_id].compute_with_flags(
                coin_prices, as_of
            )
            row[factor_id] = None if low_sample else value
        rows.append(row)
    return pd.DataFrame(rows, columns=["coin_id", *factor_ids])


def _attach_factor_metadata(prices: pd.DataFrame, frame: pd.DataFrame) -> pd.DataFrame:
    metadata = frame[["coin_id", "market_cap", "market_cap_rank"]].rename(
        columns={"market_cap": "market_cap_usd"}
    )
    metadata["coin_id"] = metadata["coin_id"].astype(str)
    prices["coin_id"] = prices["coin_id"].astype(str)
    return prices.merge(metadata, on="coin_id", how="left")


def _score_factor_values(
    factor_values: pd.DataFrame, ranking: list[dict[str, Any]]
) -> pd.DataFrame:
    scored = factor_values.copy()
    scored["score"] = 0.0
    # Rank-normalization keeps heterogeneous factor units commensurable. Missing
    # values simply contribute no score for that factor; other factors still apply.
    for entry in ranking:
        factor = entry["factor"]
        values = pd.to_numeric(scored[factor], errors="coerce")
        valid = values.notna()
        if not valid.any():
            continue
        ascending = entry["direction"] == "low"
        ranks = values[valid].rank(method="average", ascending=ascending)
        count = float(len(ranks))
        normalized = (
            pd.Series(1.0, index=ranks.index)
            if count == 1
            else (count - ranks) / (count - 1.0)
        )
        scored.loc[valid, "score"] += normalized * entry["weight"]
    return scored


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
    if risk_profile in {"preserve", "preserve_capital"}:
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
    if risk_profile in {"aggressive", "high_growth"}:
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
    if risk_profile == "income":
        return _sort_frame(frame, sort="market_cap_rank")
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
    if objective in {"return", "high_growth"}:
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
    if objective in {"low_volatility", "preserve_capital"}:
        return frame.dropna(
            subset=["volatility_180d", "max_drawdown_180d"]
        ).sort_values(
            by=["volatility_180d", "max_drawdown_180d", "coin_id"],
            ascending=[True, False, True],
            kind="mergesort",
        )
    if objective in {"balanced", "income"}:
        return frame.dropna(subset=["sharpe_180d", "max_drawdown_180d"]).sort_values(
            by=["sharpe_180d", "max_drawdown_180d", "coin_id"],
            ascending=[False, False, True],
            kind="mergesort",
        )
    raise ValueError(f"unsupported objective: {objective}")


def _is_stablecoin(row: pd.Series) -> bool:
    text = (
        f"{row.get('coin_id', '')} {row.get('symbol', '')} {row.get('name', '')}"
    ).lower()
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
