from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from agent_invest_scripts import run_candidate_batch


def test_rejects_fewer_than_three_candidates() -> None:
    with pytest.raises(ValueError, match="at least 3 candidates"):
        run_candidate_batch.run(
            {"run_id": "run-1", "round": 1, "candidates": [_candidate("one")]}
        )


def test_rejects_default_cap() -> None:
    with pytest.raises(ValueError, match="at most 8 candidates"):
        run_candidate_batch.run(
            {
                "run_id": "run-1",
                "round": 1,
                "candidates": [_candidate(f"c{i}") for i in range(9)],
            }
        )


def test_runs_batch_and_persists_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))
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
    assert (tmp_path / "candidate_batches" / f"{output['batch_id']}.json").is_file()
    for result in output["results"]:
        assert (result["allocation_metrics"] is None) != (
            result["tactical_metrics"] is None
        )
        assert result["robustness"]["n_rebalances"] == 1
        assert result["robustness"]["sample_size_warning"] is True


def test_warning_thresholds_flip_on_bad_case() -> None:
    start = date(2024, 1, 1)
    equity = pd.Series(
        [1.0, 2.0, 2.02, 0.5],
        index=pd.Index(
            [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1), date(2024, 4, 1)],
            name="date",
        ),
    )
    benchmark = pd.Series([1.0, 2.0, 2.02, 0.5], index=equity.index)
    performance = pd.DataFrame(
        {
            "date": equity.index,
            "net_return": equity.pct_change().fillna(0.0).to_numpy(),
        }
    )
    engine_result = type(
        "EngineResult",
        (),
        {
            "performance": run_candidate_batch.pl.from_pandas(performance),
            "summary": {"survivorship_warning": True},
        },
    )()
    allocation = run_candidate_batch.AllocationMetrics([], 0.0, 1.0, [])

    signals = run_candidate_batch._robustness(  # noqa: SLF001
        engine_result, equity, benchmark, allocation
    )

    assert signals.concentration_warning is True
    assert signals.worst_window_warning is True
    assert signals.benchmark_coupling_warning is True
    assert signals.significance_warning is True
    assert signals.survivorship_warning is True


def _candidate(candidate_id: str) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "template_id": "buy_and_hold",
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
