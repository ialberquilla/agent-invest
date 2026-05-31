from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from agent_invest_scripts import run_candidate_batch
from agent_invest_scripts._lib.backtest import robustness


def test_rejects_fewer_than_three_candidates() -> None:
    with pytest.raises(ValueError, match="at least 3 candidates"):
        run_candidate_batch.run(
            {"run_id": "run-1", "round": 1, "candidates": [_candidate("one")]}
        )


def test_rejects_non_positive_round() -> None:
    with pytest.raises(ValueError, match="round must be a positive integer"):
        run_candidate_batch.run(
            {
                "run_id": "run-1",
                "round": 0,
                "candidates": [_candidate(f"c{i}") for i in range(3)],
            }
        )


def test_accepts_round_above_three(monkeypatch: pytest.MonkeyPatch) -> None:
    # The workflow can run more than three batches per run (reinterpret_brief
    # / broaden_universe edges keep incrementing the attempt counter), so the
    # echoed round is not capped at 3.
    monkeypatch.setattr(run_candidate_batch, "daily_prices", _daily_prices)
    monkeypatch.setattr(run_candidate_batch, "asset_universe_features", _features)

    output = run_candidate_batch.run(
        {
            "run_id": "run-1",
            "round": 4,
            "candidates": [_candidate(f"c{i}") for i in range(3)],
        }
    )

    assert output["round"] == 4


def test_rejects_default_cap() -> None:
    with pytest.raises(ValueError, match="at most 8 candidates"):
        run_candidate_batch.run(
            {
                "run_id": "run-1",
                "round": 1,
                "candidates": [_candidate(f"c{i}") for i in range(9)],
            }
        )


def test_runs_batch_and_returns_results(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_candidate_batch, "daily_prices", _daily_prices)
    monkeypatch.setattr(run_candidate_batch, "asset_universe_features", _features)

    output = run_candidate_batch.run(
        {
            "run_id": "run-1",
            "round": 1,
            "candidates": [_candidate(f"c{i}") for i in range(3)],
        }
    )

    assert output["batch_id"].startswith("candidate_batch_")
    assert len(output["results"]) == 3
    # Batches are no longer written to disk -- the caller pipes this payload to
    # validate_against_thesis.
    assert "iteration_hypothesis" not in output
    for result in output["results"]:
        # Every family is allocation-shaped now (no tactical signal plans).
        assert result["allocation_metrics"] is not None
        assert result["tactical_metrics"] is None
        assert result["robustness"]["n_rebalances"] == 1
        assert result["robustness"]["sample_size_warning"] is True


def test_returns_iteration_hypothesis_when_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(run_candidate_batch, "daily_prices", _daily_prices)
    monkeypatch.setattr(run_candidate_batch, "asset_universe_features", _features)

    output = run_candidate_batch.run(
        {
            "run_id": "run-1",
            "round": 1,
            "iteration_hypothesis": "Test lower turnover equal weighting.",
            "candidates": [_candidate(f"c{i}") for i in range(3)],
        }
    )

    assert output["iteration_hypothesis"] == "Test lower turnover equal weighting."


def test_batch_level_overrides_apply_to_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(run_candidate_batch, "daily_prices", _daily_prices)
    monkeypatch.setattr(run_candidate_batch, "asset_universe_features", _features)

    output = run_candidate_batch.run(
        {
            "run_id": "run-1",
            "round": 1,
            "universe_override": {"id": "top_n_by_mcap", "params": {"n": 3}},
            "filters": [
                {"id": "exclude_stablecoins"},
                {"id": "market_cap_floor", "params": {"usd": 1_000_000_000}},
            ],
            "window_override": {"start_date": "2024-01-01", "end_date": "2024-01-20"},
            "candidates": [
                {
                    key: value
                    for key, value in _candidate(f"c{i}").items()
                    if key != "window_override"
                }
                for i in range(3)
            ],
        }
    )

    for result in output["results"]:
        assert result["window"] == {"start": "2024-01-01", "end": "2024-01-20"}
        history = result["allocation_metrics"]["holdings_history"]
        assert set(history[0]["weights"]) == {"bitcoin"}


def test_top_level_basket_becomes_hand_picked_universe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(run_candidate_batch, "daily_prices", _daily_prices)
    monkeypatch.setattr(run_candidate_batch, "asset_universe_features", _features)

    output = run_candidate_batch.run(
        {
            "run_id": "run-1",
            "round": 1,
            "basket": [{"coin_id": "bitcoin", "weight": 1.0}],
            "candidates": [_candidate(f"c{i}") for i in range(3)],
        }
    )

    for result in output["results"]:
        history = result["allocation_metrics"]["holdings_history"]
        assert set(history[0]["weights"]) == {"bitcoin"}


def test_warning_thresholds_flip_on_bad_case() -> None:
    equity = pd.Series(
        [1.0, 2.0, 2.02, 0.5],
        index=pd.Index(
            [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1), date(2024, 4, 1)],
            name="date",
        ),
    )
    benchmark = pd.Series([1.0, 2.0, 2.02, 0.5], index=equity.index)

    signals = robustness.compute_robustness(
        equity, benchmark, n_rebalances=0, survivorship_warning=True
    )

    assert signals.concentration_warning is True
    assert signals.worst_window_warning is True
    assert signals.benchmark_coupling_warning is True
    assert signals.significance_warning is True
    assert signals.survivorship_warning is True


def _candidate(candidate_id: str) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "template_id": "synthetic_long_allocation",
        "ranking": [{"factor": "market_cap_rank", "direction": "low", "weight": 1.0}],
        "select_top": 1,
        "config": {"weighting": "equal"},
        "window_override": {"start": "2024-01-01", "end": "2024-01-20"},
        "thesis": {"objective": "balanced", "primary_factors": []},
    }


def _daily_prices() -> pd.DataFrame:
    rows = []
    for offset in range(20):
        day = date(2024, 1, 1) + timedelta(days=offset)
        rows.append({"date": day, "coin_id": "bitcoin", "price": 100.0 + offset})
        rows.append({"date": day, "coin_id": "ethereum", "price": 100.0 + offset * 2})
        rows.append({"date": day, "coin_id": "usd-coin", "price": 1.0})
    return pd.DataFrame(rows)


def _features() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "asset_id": "bitcoin",
                "coin_id": "bitcoin",
                "symbol": "BTC",
                "name": "Bitcoin",
                "market_cap_rank": 1,
                "market_cap": 1_000_000_000,
                "latest_price": 100,
                "first_price_date": date(2024, 1, 1),
                "last_price_date": date(2024, 1, 20),
                "data_days_365d": 20,
                "return_30d": 0.1,
                "return_90d": 0.1,
                "return_180d": 0.1,
                "return_365d": 0.1,
                "volatility_30d": 0.1,
                "volatility_90d": 0.1,
                "volatility_180d": 0.1,
                "max_drawdown_180d": -0.1,
                "sharpe_180d": 1.0,
                "price_above_sma_200d": True,
            }
        ]
    )
