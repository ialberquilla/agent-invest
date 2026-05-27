"""Mechanical scorer for Designer stage eval fixtures."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

import polars as pl

from agent_invest_scripts._lib.backtest.window import recommend_backtest_window
from agent_invest_scripts.validate_against_thesis import validate


def score(expectations: dict[str, Any], output: Any) -> dict[str, Any]:
    if not expectations:
        return {
            "passed": True,
            "score": 1.0,
            "rules": [
                _rule("placeholder_expectations", True, "no expectations configured")
            ],
        }

    batch = _extract_batch(output)
    candidates = batch.get("candidates", [])
    if not isinstance(candidates, list):
        candidates = []

    rules = [
        _min_candidates_rule(expectations, candidates),
        _template_ids_rule(expectations, candidates),
        _validated_candidates_rule(expectations, batch),
        _window_rule(expectations, batch, candidates),
        _unique_config_rule(candidates),
    ]
    passed = all(rule["passed"] for rule in rules)
    score_value = sum(1.0 if rule["passed"] else 0.0 for rule in rules) / len(rules)
    return {"passed": passed, "score": round(score_value, 4), "rules": rules}


def _extract_batch(output: Any) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {}
    batch = output.get("batch", output)
    return batch if isinstance(batch, dict) else {}


def _min_candidates_rule(
    expectations: dict[str, Any], candidates: list[Any]
) -> dict[str, Any]:
    expected = int(expectations.get("min_candidates", 0))
    actual = len(candidates)
    return _rule(
        "min_candidates",
        actual >= expected,
        f"expected at least {expected} candidates, got {actual}",
    )


def _template_ids_rule(
    expectations: dict[str, Any], candidates: list[Any]
) -> dict[str, Any]:
    acceptable = set(expectations.get("acceptable_template_ids", []))
    actual = [
        candidate.get("template_id")
        for candidate in candidates
        if isinstance(candidate, dict)
    ]
    invalid = sorted(
        {template_id for template_id in actual if template_id not in acceptable}
    )
    return _rule(
        "acceptable_template_ids",
        not invalid and len(actual) == len(candidates),
        f"acceptable={sorted(acceptable)}; invalid={invalid}; actual={actual}",
    )


def _validated_candidates_rule(
    expectations: dict[str, Any], batch: dict[str, Any]
) -> dict[str, Any]:
    expected = int(expectations.get("must_validate_count", 0))
    thesis = expectations.get("thesis")
    if not isinstance(thesis, dict):
        return _rule(
            "validate_against_thesis",
            False,
            "expectations.thesis must be an object",
        )

    validation = _validate_batch(batch, thesis)
    passing_ids = validation.get("passing_candidate_ids", [])
    actual = len(passing_ids) if isinstance(passing_ids, list) else 0
    return _rule(
        "validate_against_thesis",
        actual >= expected,
        f"expected at least {expected} passing candidates, got {actual}: {passing_ids}",
    )


def _validate_batch(batch: dict[str, Any], thesis: dict[str, Any]) -> dict[str, Any]:
    batch_id = str(batch.get("batch_id") or "eval_designer_batch")
    payload = {
        "batch_id": batch_id,
        "run_id": batch.get("run_id", "eval-designer-run"),
        "round": batch.get("round", 1),
        "results": batch.get("results", batch.get("candidates", [])),
    }
    previous_storage_root = os.environ.get("STORAGE_ROOT")
    with tempfile.TemporaryDirectory() as temp_dir:
        storage_root = Path(temp_dir)
        batch_dir = storage_root / "candidate_batches"
        batch_dir.mkdir(parents=True)
        (batch_dir / f"{batch_id}.json").write_text(
            json.dumps(payload), encoding="utf-8"
        )
        os.environ["STORAGE_ROOT"] = str(storage_root)
        try:
            return validate(batch_id, thesis)
        finally:
            if previous_storage_root is None:
                os.environ.pop("STORAGE_ROOT", None)
            else:
                os.environ["STORAGE_ROOT"] = previous_storage_root


def _window_rule(
    expectations: dict[str, Any], batch: dict[str, Any], candidates: list[Any]
) -> dict[str, Any]:
    price_history = expectations.get("price_history")
    coin_ids = expectations.get("coin_ids")
    horizon_days = expectations.get("horizon_days")
    if not isinstance(price_history, list) or not isinstance(coin_ids, list):
        return _rule(
            "recommended_backtest_window",
            False,
            "expectations.price_history and expectations.coin_ids are required",
        )
    if not isinstance(horizon_days, int):
        return _rule(
            "recommended_backtest_window", False, "horizon_days must be an int"
        )

    recommended = recommend_backtest_window(
        pl.DataFrame(price_history),
        coin_ids=[str(coin_id) for coin_id in coin_ids],
        horizon_days=horizon_days,
    )
    expected = {"start": recommended["start"], "end": recommended["end"]}
    actual = batch.get("window") or _common_candidate_window(candidates)
    return _rule(
        "recommended_backtest_window",
        actual == expected,
        f"expected {expected}, got {actual}",
    )


def _common_candidate_window(candidates: list[Any]) -> dict[str, Any] | None:
    windows = [
        candidate.get("window") or candidate.get("window_override")
        for candidate in candidates
        if isinstance(candidate, dict)
    ]
    if not windows:
        return None
    first = windows[0]
    return first if all(window == first for window in windows) else None


def _unique_config_rule(candidates: list[Any]) -> dict[str, Any]:
    hashes = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        hashes.append(
            json.dumps(
                {
                    "template_id": candidate.get("template_id"),
                    "config": candidate.get("config", {}),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    duplicate_count = len(hashes) - len(set(hashes))
    return _rule(
        "unique_candidate_configs",
        duplicate_count == 0 and len(hashes) == len(candidates),
        f"duplicate_config_count={duplicate_count}; candidate_count={len(candidates)}",
    )


def _rule(rule: str, passed: bool, message: str) -> dict[str, Any]:
    return {"rule": rule, "passed": passed, "message": message}
