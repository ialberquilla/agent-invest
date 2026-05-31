"""Run a bounded batch of candidate template backtests."""

from __future__ import annotations

import argparse
import json
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import date
from typing import Any
from uuid import uuid4

import pandas as pd
import polars as pl

from agent_invest_scripts._lib import daily_prices, print_json
from agent_invest_scripts._lib.backtest.bt_templates import TEMPLATES, run_recipe
from agent_invest_scripts._lib.backtest.result import to_dict
from agent_invest_scripts._lib.backtest.window import recommend_backtest_window
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    resolve_timeout_seconds,
    script_timeout,
)
from agent_invest_scripts._lib.data import asset_universe_features
from agent_invest_scripts._lib.storage import normalize_identifier
from agent_invest_scripts.rank_universe import rank_universe_extended

_DEFAULT_MAX_CANDIDATES = 8
_MIN_CANDIDATES = 3

INPUT_EXAMPLE: dict[str, Any] = {
    "run_id": "<run_id-from-the-task>",
    "round": 1,
    "iteration_hypothesis": "Diversified large-cap basket with 10% cash sleeve.",
    "universe_override": {
        "id": "top_n_by_mcap",
        "params": {"n": 25},
    },
    "filters": [
        {"id": "exclude_stablecoins"},
        {"id": "exclude_wrapped"},
        {"id": "market_cap_floor", "value": {"usd": 1_000_000_000}},
    ],
    "window_override": {"horizon_days": 365},
    "candidates": [
        {
            "candidate_id": "c1",
            "template_id": "periodic_rebalance",
            "select_top": 5,
            "config": {"weighting": "equal", "rebalance_trigger": "periodic_30d"},
        },
        {
            "candidate_id": "c2",
            "template_id": "periodic_rebalance",
            "select_top": 7,
            "config": {"weighting": "equal", "rebalance_trigger": "periodic_30d"},
        },
        {
            "candidate_id": "c3",
            "template_id": "buy_and_hold",
            "select_top": 8,
            "config": {"weighting": "market_cap"},
        },
    ],
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a candidate backtest batch.")
    parser.add_argument("--input", required=True, help="RunCandidateBatchInput JSON")
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


def run(input_payload: dict[str, Any]) -> dict[str, Any]:
    _validate_batch_input(input_payload)
    batch_id = f"candidate_batch_{uuid4().hex}"
    prices = daily_prices()
    features = asset_universe_features()
    batch_universe_override = input_payload.get("universe_override")
    if batch_universe_override is None and "basket" in input_payload:
        batch_universe_override = _basket_universe_override(input_payload["basket"])
    jobs = [
        {
            "candidate": candidate,
            "round": input_payload["round"],
            "prices": prices,
            "features": features,
            "batch_universe_override": batch_universe_override,
            "batch_filters": input_payload.get("filters"),
            "batch_window_override": input_payload.get("window_override"),
        }
        for candidate in input_payload["candidates"]
    ]

    max_workers = min(len(jobs), os.cpu_count() or 1)
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(_run_candidate_job, jobs))

    output = {
        "batch_id": batch_id,
        "run_id": input_payload["run_id"],
        "round": input_payload["round"],
        "results": results,
    }
    if "iteration_hypothesis" in input_payload:
        output["iteration_hypothesis"] = input_payload["iteration_hypothesis"]
    # Batches are no longer written to disk. Callers that need to
    # validate a batch should pipe this stdout payload to
    # `validate_against_thesis --input <json>` rather than relying on a
    # batch-id filesystem handle.
    return output


def _validate_batch_input(input_payload: dict[str, Any]) -> None:
    if not isinstance(input_payload, dict):
        raise ValueError("RunCandidateBatchInput must be an object")
    normalize_identifier(str(input_payload.get("run_id", "")), "run_id")
    # `round` is a 1-based attempt index, echoed back as metadata. The
    # workflow can run more than three batches across a run (each
    # reinterpret_brief / broaden_universe edge starts another
    # propose->validate cycle without resetting the attempt counter), so
    # any positive integer is valid here.
    round_value = input_payload.get("round")
    if not isinstance(round_value, int) or isinstance(round_value, bool) or round_value < 1:
        raise ValueError("round must be a positive integer")
    if "iteration_hypothesis" in input_payload and not isinstance(
        input_payload["iteration_hypothesis"], str
    ):
        raise ValueError("iteration_hypothesis must be a string")
    if "basket" in input_payload:
        _basket_universe_override(input_payload["basket"])
    candidates = input_payload.get("candidates")
    if not isinstance(candidates, list):
        raise ValueError("candidates must be an array")
    if len(candidates) < _MIN_CANDIDATES:
        raise ValueError("run_candidate_batch requires at least 3 candidates")
    max_candidates = int(os.getenv("RUN_CANDIDATE_BATCH_MAX", _DEFAULT_MAX_CANDIDATES))
    if len(candidates) > max_candidates:
        raise ValueError(
            f"run_candidate_batch accepts at most {max_candidates} candidates"
        )
    ids = [
        candidate.get("candidate_id")
        for candidate in candidates
        if isinstance(candidate, dict)
    ]
    if len(ids) != len(candidates) or any(
        not isinstance(item, str) or not item for item in ids
    ):
        raise ValueError("each candidate must include a non-empty candidate_id")
    if len(set(ids)) != len(ids):
        raise ValueError("candidate_id values must be unique within a batch")


def _run_candidate_job(job: dict[str, Any]) -> dict[str, Any]:
    candidate = job["candidate"]
    if not isinstance(candidate, dict):
        raise ValueError("candidate must be an object")
    template_id = candidate.get("template_id")
    if template_id == "mixed":
        raise ValueError("template_id=mixed is not supported by run_candidate_batch")
    if template_id not in TEMPLATES:
        raise ValueError(f"unknown template_id: {template_id}")
    recipe = TEMPLATES[template_id]
    config = dict(candidate.get("config") or {})
    if (
        "select_top" in candidate
        and "select_top" not in config
        and "select_top" in recipe.METADATA.slot_schema
    ):
        config["select_top"] = candidate["select_top"]
    selection_limit = candidate.get("select_top", config.get("select_top"))
    if not isinstance(selection_limit, int) or selection_limit < 1:
        raise ValueError("candidate must provide select_top as a positive integer")
    recipe.validate_config(config)

    prices = job["prices"].copy()
    features = job["features"].copy()
    window = _resolve_window(
        candidate.get("window_override", job.get("batch_window_override")),
        recipe,
        prices,
    )
    universe_override = candidate.get(
        "universe_override", job.get("batch_universe_override")
    )
    if universe_override is None and "basket" in candidate:
        universe_override = _basket_universe_override(candidate["basket"])
    filters = candidate.get("filters", job.get("batch_filters", []))
    ranked = rank_universe_extended(
        features,
        prices,
        {
            "universe_selector": _universe_selector(
                universe_override or recipe.METADATA.default_universe
            ),
            "filters": filters,
            "ranking": candidate.get("ranking") or _default_ranking(recipe),
            "limit": selection_limit,
        },
        as_of=window[0],
    )
    universe = pd.DataFrame(ranked)
    # Full wide frame (all coins): the recipe slices to its universe, and the
    # benchmark needs BTC/ETH/USDC regardless of the candidate's universe.
    prices_wide = prices.pivot(index="date", columns="coin_id", values="price")
    prices_wide.index = pd.to_datetime(prices_wide.index)
    result = run_recipe(
        recipe, universe, prices_wide, config, window, candidate=candidate
    )
    return to_dict(result)


def _resolve_window(
    override: Any, template: Any, prices: pd.DataFrame
) -> tuple[date, date]:
    if isinstance(override, dict):
        start = (
            override.get("start") or override.get("start_date") or override.get("from")
        )
        end = override.get("end") or override.get("end_date") or override.get("to")
        if isinstance(start, str) and isinstance(end, str):
            return (_parse_date(start), _parse_date(end))
        horizon_days = override.get("horizon_days") or override.get("days")
        if isinstance(horizon_days, int) and horizon_days > 0:
            coin_ids = sorted(
                set(str(value) for value in prices["coin_id"].dropna().unique())
            )
            payload = recommend_backtest_window(
                pl.from_pandas(prices),
                coin_ids=coin_ids,
                horizon_days=max(template.METADATA.min_history_days, horizon_days),
            )
            return (_parse_date(payload["start"]), _parse_date(payload["end"]))
        raise ValueError(
            "window_override must include start/end, start_date/end_date, from/to, "
            "or horizon_days"
        )
    coin_ids = sorted(set(str(value) for value in prices["coin_id"].dropna().unique()))
    payload = recommend_backtest_window(
        pl.from_pandas(prices),
        coin_ids=coin_ids,
        horizon_days=max(template.METADATA.min_history_days, 365),
    )
    return (_parse_date(payload["start"]), _parse_date(payload["end"]))


def _default_ranking(template: Any) -> list[dict[str, Any]]:
    factor = (
        template.METADATA.preferred_factors[0]
        if template.METADATA.preferred_factors
        else "market_cap_rank"
    )
    return [
        {
            "factor": factor,
            "direction": "low" if factor.endswith("rank") else "high",
            "weight": 1.0,
        }
    ]


def _universe_selector(selector: dict[str, Any]) -> dict[str, Any]:
    if selector.get("id") == "fixed":
        params = (
            selector.get("params") if isinstance(selector.get("params"), dict) else {}
        )
        coin_ids = params.get("coin_ids") or selector.get("coin_ids")
        if isinstance(coin_ids, list):
            return {"id": "hand_picked", "params": {"coin_ids": coin_ids}}
    if "id" in selector:
        return selector
    if "selector" in selector:
        return {
            "id": selector["selector"],
            "params": {
                key: value for key, value in selector.items() if key != "selector"
            },
        }
    return selector


def _basket_universe_override(basket: Any) -> dict[str, Any]:
    if not isinstance(basket, list):
        raise ValueError("basket must be an array of objects with coin_id")
    coin_ids: list[str] = []
    for item in basket:
        if not isinstance(item, dict) or not isinstance(item.get("coin_id"), str):
            raise ValueError("basket must include a coin_id column")
        coin_id = item["coin_id"].strip()
        if coin_id and coin_id not in coin_ids:
            coin_ids.append(coin_id)
    if not coin_ids:
        raise ValueError("basket must include at least one coin_id")
    return {"id": "hand_picked", "params": {"coin_ids": coin_ids}}


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


if __name__ == "__main__":
    raise SystemExit(main())
