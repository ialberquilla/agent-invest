"""CLI entrypoint for deterministic recovery analysis."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

import polars as pl

from agent_invest_scripts._lib import daily_prices, print_json
from agent_invest_scripts._lib.backtest import TradingCostModel, run_backtest
from agent_invest_scripts._lib.backtest.recovery import analyze_price_series
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    resolve_timeout_seconds,
    script_timeout,
)
from agent_invest_scripts.run_backtest import (
    _build_targets,
    _slice_prices_to_allocation_window,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze strict drawdown recoveries.")
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--coin-ids", help="Comma-separated coin IDs")
    inputs.add_argument("--allocation", help="JSON allocation payload")
    parser.add_argument("--horizon-days", required=True, type=int)
    parser.add_argument("--min-drawdown-pct", type=float, default=0.20)
    parser.add_argument("--as-of", type=_parse_date)
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            prices = _load_prices_frame()
            if args.coin_ids:
                payload = {
                    "as_of": _effective_as_of(prices, args.as_of).isoformat(),
                    "horizon_days": args.horizon_days,
                    "min_drawdown_pct": args.min_drawdown_pct,
                    "coins": _analyze_coins(prices, args),
                }
            else:
                allocation = _parse_json_object(args.allocation, "--allocation")
                payload = {
                    "as_of": _effective_as_of(prices, args.as_of).isoformat(),
                    "horizon_days": args.horizon_days,
                    "min_drawdown_pct": args.min_drawdown_pct,
                    "portfolio": _analyze_portfolio(prices, allocation, args),
                }
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)

    print_json(payload)
    return 0


def _load_prices_frame() -> pl.DataFrame:
    frame = pl.from_pandas(daily_prices())
    if not {"date", "coin_id", "price"} <= set(frame.columns):
        raise ValueError("daily_prices must include date, coin_id, and price columns")
    return (
        frame.with_columns(
            pl.col("date").cast(pl.Date),
            pl.col("coin_id").cast(pl.String),
            pl.col("price").cast(pl.Float64),
        )
        .select("date", "coin_id", "price")
        .sort(["date", "coin_id"])
    )


def _analyze_coins(
    prices: pl.DataFrame, args: argparse.Namespace
) -> list[dict[str, Any]]:
    coin_ids = [
        coin_id.strip() for coin_id in args.coin_ids.split(",") if coin_id.strip()
    ]
    if not coin_ids:
        raise ValueError("--coin-ids must include at least one coin ID")
    available = set(prices.get_column("coin_id").unique().to_list())
    missing = sorted(set(coin_ids) - available)
    if missing:
        raise ValueError(f"coin_id(s) not found in daily_prices: {', '.join(missing)}")
    return [
        analyze_price_series(
            prices.filter(pl.col("coin_id") == coin_id).select("date", "price"),
            horizon_days=args.horizon_days,
            min_drawdown_pct=args.min_drawdown_pct,
            as_of=args.as_of,
            coin_id=coin_id,
        )
        for coin_id in coin_ids
    ]


def _analyze_portfolio(
    prices: pl.DataFrame, allocation: Mapping[str, Any], args: argparse.Namespace
) -> dict[str, Any]:
    sliced = _slice_prices_to_allocation_window(prices, allocation)
    if args.as_of is not None:
        sliced = sliced.filter(pl.col("date") <= args.as_of)
    targets = _build_targets(allocation, sliced, "none")
    result = run_backtest(sliced, targets, cost_model=TradingCostModel())
    return analyze_price_series(
        result.performance.select("date", pl.col("equity").alias("price")),
        horizon_days=args.horizon_days,
        min_drawdown_pct=args.min_drawdown_pct,
        as_of=args.as_of,
    )


def _effective_as_of(prices: pl.DataFrame, as_of: date | None) -> date:
    if as_of is not None:
        return as_of
    return prices.get_column("date").max()


def _parse_json_object(raw: str, argument_name: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError(f"{argument_name} must be valid JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{argument_name} must decode to a JSON object")
    return value


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("dates must use YYYY-MM-DD") from error


if __name__ == "__main__":
    raise SystemExit(main())
