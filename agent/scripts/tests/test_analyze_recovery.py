from __future__ import annotations

import json
from datetime import date, timedelta

import pandas as pd
import polars as pl
import pytest

from agent_invest_scripts import analyze_recovery
from agent_invest_scripts._lib.backtest.recovery import analyze_price_series


def _series(values: list[float], *, start: date = date(2024, 1, 1)) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": [start + timedelta(days=index) for index in range(len(values))],
            "price": values,
        }
    )


def test_minimum_drawdown_filter() -> None:
    report = analyze_price_series(
        _series([100, 91, 102, 80, 100]),
        horizon_days=1,
        min_drawdown_pct=0.20,
        as_of=date(2024, 1, 5),
    )

    assert report["recovery_stats"]["n_episodes"] == 1
    assert report["drawdown_episodes"][0]["drawdown_pct"] == -0.215686


def test_missing_as_of_defaults_to_last_price_date(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    rows = []
    for index in range(3):
        rows.append(
            {
                "date": date(2024, 1, 1) + timedelta(days=index),
                "coin_id": "bitcoin",
                "price": 100.0 + index,
            }
        )
    monkeypatch.setattr(analyze_recovery, "daily_prices", lambda: pd.DataFrame(rows))

    analyze_recovery.main(["--coin-ids", "bitcoin", "--horizon-days", "1"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["as_of"] == "2024-01-03"
    assert payload["coins"][0]["coverage"]["last_price_date"] == "2024-01-03"


def test_recovery_stats_null_when_episode_sample_is_small() -> None:
    report = analyze_price_series(
        _series([100, 70, 100, 80, 100]),
        horizon_days=1,
        min_drawdown_pct=0.20,
        as_of=date(2024, 1, 5),
    )

    assert report["recovery_stats"]["n_episodes"] == 2
    assert report["recovery_stats"]["recovery_rate"] is None
    assert report["recovery_stats"]["median_recovery_days"] is None
    assert report["recovery_stats"]["p90_recovery_days"] is None


def test_lower_peak_classifies_previous_episode_as_unrecovered() -> None:
    report = analyze_price_series(
        _series([100, 70, 90, 60, 75]),
        horizon_days=1,
        min_drawdown_pct=0.20,
        as_of=date(2024, 1, 5),
    )

    assert report["recovery_stats"]["n_episodes"] == 2
    assert report["recovery_stats"]["n_unrecovered"] == 1
    assert report["recovery_stats"]["n_in_progress"] == 1
    assert report["drawdown_episodes"][0]["peak_date"] == "2024-01-01"
    assert report["drawdown_episodes"][0]["recovery_date"] is None
