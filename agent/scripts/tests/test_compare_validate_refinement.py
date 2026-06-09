from __future__ import annotations

import json
from pathlib import Path

import pytest

from agent_invest_scripts import (
    compare_backtests,
    validate_against_thesis,
)


def _write_batch(storage_root: Path) -> None:
    batch_dir = storage_root / "candidate_batches"
    batch_dir.mkdir(parents=True)
    (batch_dir / "candidate_batch_test.json").write_text(
        json.dumps(
            {
                "batch_id": "candidate_batch_test",
                "run_id": "run-1",
                "round": 1,
                "iteration_hypothesis": "Test a conservative quality screen.",
                "results": [
                    {
                        "candidate_id": "low_score",
                        "template_id": "buy_and_hold",
                        "config": {"select_top": 2},
                        "window": {"start": "2024-01-01", "end": "2024-02-01"},
                        "metrics": {"cagr": 0.1, "max_drawdown": -0.2, "sharpe": 0.7},
                        "allocation_metrics": {"max_single_weight": 0.60},
                        "robustness": {},
                        "composite_score": 0.5,
                    },
                    {
                        "candidate_id": "high_score",
                        "template_id": "periodic_rebalance",
                        "config": {"select_top": 4},
                        "window": {"start": "2023-01-01", "end": "2024-01-01"},
                        "metrics": {"cagr": 0.2, "max_drawdown": -0.1, "sharpe": 1.1},
                        "allocation_metrics": {"max_single_weight": 0.20},
                        "robustness": {"sample_size_warning": True},
                        "composite_score": 1.5,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )


def _write_second_batch(storage_root: Path) -> None:
    batch_dir = storage_root / "candidate_batches"
    batch_dir.mkdir(parents=True, exist_ok=True)
    (batch_dir / "candidate_batch_second.json").write_text(
        json.dumps(
            {
                "batch_id": "candidate_batch_second",
                "run_id": "run-1",
                "round": 2,
                "iteration_hypothesis": (
                    "Increase momentum exposure after weak first pass."
                ),
                "results": [
                    {
                        "candidate_id": "second_best",
                        "template_id": "periodic_rebalance",
                        "metrics": {"cagr": 0.3, "max_drawdown": -0.1, "sharpe": 1.3},
                        "robustness": {},
                        "composite_score": 2.0,
                    },
                    {
                        "candidate_id": "second_low",
                        "template_id": "buy_and_hold",
                        "metrics": {"cagr": 0.05, "max_drawdown": -0.2, "sharpe": 0.4},
                        "robustness": {},
                        "composite_score": 0.3,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )


def test_compare_backtests_ranks_candidate_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    _write_batch(tmp_path)

    output = compare_backtests.compare("candidate_batch_test")

    assert output["winner_candidate_id"] == "high_score"
    assert output["ranking"][0]["rank"] == 1
    assert output["ranking"][0]["warnings"] == ["sample_size_warning"]


def test_compare_backtests_ranks_multiple_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    _write_batch(tmp_path)
    _write_second_batch(tmp_path)

    output = compare_backtests.compare("candidate_batch_test,candidate_batch_second")

    assert output["batch_ids"] == ["candidate_batch_test", "candidate_batch_second"]
    assert output["batches"] == [
        {
            "batch_id": "candidate_batch_test",
            "run_id": "run-1",
            "round": 1,
            "iteration_hypothesis": "Test a conservative quality screen.",
        },
        {
            "batch_id": "candidate_batch_second",
            "run_id": "run-1",
            "round": 2,
            "iteration_hypothesis": "Increase momentum exposure after weak first pass.",
        },
    ]
    assert output["winner_batch_id"] == "candidate_batch_second"
    assert output["winner_candidate_id"] == "second_best"
    assert [row["candidate_id"] for row in output["ranking"]] == [
        "second_best",
        "high_score",
        "low_score",
        "second_low",
    ]
    assert output["ranking"][0]["global_rank"] == 1
    assert (
        output["ranking"][0]["iteration_hypothesis"]
        == "Increase momentum exposure after weak first pass."
    )
    assert output["ranking"][1]["batch_rank"] == 1
    assert output["per_batch_winners"] == [
        {
            "batch_id": "candidate_batch_second",
            "winner_candidate_id": "second_best",
            "composite_score": 2.0,
            "global_rank": 1,
        },
        {
            "batch_id": "candidate_batch_test",
            "winner_candidate_id": "high_score",
            "composite_score": 1.5,
            "global_rank": 2,
        },
    ]


def test_validate_against_thesis_checks_constraints(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
    _write_batch(tmp_path)

    output = validate_against_thesis.validate(
        "candidate_batch_test",
            {
            "horizon_days": 365,
            "constraints": {
                "max_drawdown": -0.15,
                "asset_count_min": 3,
                "asset_count_max": 5,
                "max_weight_per_asset": 0.25,
            },
        },
    )

    assert output["passing_candidate_ids"] == ["high_score"]
    low = output["results"][0]
    assert low["passed"] is False
    # horizon_days is the forward-looking holding period -- no longer a
    # per-candidate validation floor (the window recommender owns that).
    assert {violation["constraint"] for violation in low["violations"]} == {
        "max_drawdown",
        "asset_count_min",
        "max_weight_per_asset",
    }



def test_validate_exposure_constraints_for_long_short_book() -> None:
    """Gross/net/per-leg exposure constraints are checked from the signed
    rebalance weights -- the long-book max_weight_per_asset rule does not
    apply to a short-bearing pair/long-short book."""
    batch = {
        "batch_id": "b",
        "run_id": "r",
        "round": 1,
        "results": [
            {
                "candidate_id": "balanced_pair",
                "template_id": "explicit_pair_trade",
                "config": {},
                "metrics": {"max_drawdown": -0.1},
                "allocation_metrics": {
                    "max_single_weight": 1.0,
                    "rebalances": [{"weights": {"solana": 1.0, "bitcoin": -0.8}}],
                },
            },
            {
                "candidate_id": "over_levered",
                "template_id": "explicit_pair_trade",
                "config": {},
                "metrics": {"max_drawdown": -0.1},
                "allocation_metrics": {
                    "max_single_weight": 1.5,
                    "rebalances": [{"weights": {"solana": 1.5, "bitcoin": -1.5}}],
                },
            },
        ],
    }
    thesis = {
        "objective": "growth",
        "constraints": {
            "max_gross_exposure": 2.0,
            "max_net_exposure": 0.5,
            "max_leg_weight": 1.2,
        },
    }
    out = validate_against_thesis.validate_batch(batch, thesis)

    # balanced_pair: gross 1.8 <= 2, net 0.2 <= 0.5, leg 1.0 <= 1.2 -> pass.
    # over_levered: gross 3.0 > 2 and leg 1.5 > 1.2 -> fail (net 0.0 is fine).
    assert out["passing_candidate_ids"] == ["balanced_pair"]
    over = next(r for r in out["results"] if r["candidate_id"] == "over_levered")
    failed = {v["constraint"] for v in over["violations"]}
    assert failed == {"max_gross_exposure", "max_leg_weight"}
