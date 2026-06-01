"""Compute a strategy's *live* target weights as of a date.

Phase 3 of plans/integrate_contracts.md. Reuses run_candidate_batch's
per-candidate path (``_run_candidate_job``) so the live decision runs the exact
same recipe, universe ranking, and window logic as the backtest -- the live
path cannot drift from the backtested one. Returns the latest completed
rebalance's weights (see test_live_allocation.py for the parity guarantee).

Input (a StrategyMandate projection)::

    {
      "template_id": "relative_momentum_rotation",
      "select_top": 5,
      "config": {"weighting": "equal", "rebalance_trigger": "periodic_30d"},
      "coin_ids": ["bitcoin", "ethereum", "solana"],
      "objective": "balanced",      # script benchmark enum
      "horizon_days": 365,          # optional lookback length (default 365)
      "as_of": "2026-06-01",        # optional; null => latest available data
      "window": {"start": "...", "end": "..."}  # optional explicit override
    }
"""

from __future__ import annotations

import argparse
import json
from typing import Any

import pandas as pd

from agent_invest_scripts._lib import daily_prices, print_json
from agent_invest_scripts._lib.backtest.bt_templates import TEMPLATES
from agent_invest_scripts._lib.backtest.live_allocation import to_live_allocation
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    resolve_timeout_seconds,
    script_timeout,
)
from agent_invest_scripts._lib.data import asset_universe_features
from agent_invest_scripts.run_candidate_batch import _run_candidate_job

INPUT_EXAMPLE: dict[str, Any] = {
    "template_id": "relative_momentum_rotation",
    "select_top": 5,
    "config": {"weighting": "equal", "rebalance_trigger": "periodic_30d"},
    "coin_ids": ["bitcoin", "ethereum", "solana"],
    "objective": "balanced",
    "horizon_days": 365,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compute a strategy's live target weights as of a date."
    )
    parser.add_argument("--input", required=True, help="Mandate projection JSON")
    add_timeout_argument(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            payload = run(json.loads(args.input))
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)
    print_json(payload)
    return 0


def _validate(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("input must be an object")
    if payload.get("template_id") not in TEMPLATES:
        raise ValueError(f"unknown template_id: {payload.get('template_id')!r}")
    select_top = payload.get("select_top")
    if not isinstance(select_top, int) or isinstance(select_top, bool) or select_top < 1:
        raise ValueError("select_top must be a positive integer")
    coin_ids = payload.get("coin_ids")
    if not isinstance(coin_ids, list) or not coin_ids:
        raise ValueError("coin_ids must be a non-empty array")
    if any(not isinstance(c, str) or not c for c in coin_ids):
        raise ValueError("coin_ids must be non-empty strings")


def run(payload: dict[str, Any]) -> dict[str, Any]:
    _validate(payload)

    as_of = payload.get("as_of")
    horizon_days = int(payload.get("horizon_days") or 365)
    config = dict(payload.get("config") or {})

    candidate: dict[str, Any] = {
        "candidate_id": "live",
        "template_id": payload["template_id"],
        "select_top": payload["select_top"],
        "config": config,
        "thesis": {"objective": payload.get("objective", "balanced")},
        "universe_override": {
            "id": "hand_picked",
            "params": {"coin_ids": payload["coin_ids"]},
        },
    }

    window = payload.get("window")
    if isinstance(window, dict) and window.get("start") and window.get("end"):
        candidate["window_override"] = {
            "start": window["start"],
            "end": window["end"],
        }
    else:
        candidate["window_override"] = {"horizon_days": horizon_days}

    prices = daily_prices()
    # Bound "latest" at as_of so the recommended window ends there and the live
    # decision uses only data the keeper would actually have at that point.
    if as_of:
        cutoff = pd.Timestamp(as_of)
        prices = prices[pd.to_datetime(prices["date"]) <= cutoff]
        if prices.empty:
            raise ValueError(f"no price data on or before as_of {as_of}")

    job = {
        "candidate": candidate,
        "round": 1,
        "prices": prices,
        "features": asset_universe_features(),
        "batch_universe_override": None,
        "batch_filters": None,
        "batch_window_override": None,
    }

    result = _run_candidate_job(job)
    holdings = result["allocation_metrics"]["holdings_history"]
    allocation = to_live_allocation(holdings, as_of)
    allocation["template_id"] = payload["template_id"]
    allocation["coin_ids"] = payload["coin_ids"]
    return allocation


if __name__ == "__main__":
    raise SystemExit(main())
