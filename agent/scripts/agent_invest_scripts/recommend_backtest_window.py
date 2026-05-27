"""CLI entrypoint for deterministic backtest window selection."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

import polars as pl

from agent_invest_scripts._lib import daily_prices, print_json
from agent_invest_scripts._lib.backtest.window import recommend_backtest_window
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    resolve_timeout_seconds,
    script_timeout,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Recommend a deterministic backtest window."
    )
    parser.add_argument("--coin-ids", required=True, help="Comma-separated coin IDs")
    parser.add_argument("--horizon-days", required=True, type=int)
    parser.add_argument("--require-drawdown-pct", type=float, default=0.30)
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            payload = recommend_backtest_window(
                pl.from_pandas(daily_prices()),
                coin_ids=args.coin_ids.split(","),
                horizon_days=args.horizon_days,
                require_drawdown_pct=args.require_drawdown_pct,
            )
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)

    print_json(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
