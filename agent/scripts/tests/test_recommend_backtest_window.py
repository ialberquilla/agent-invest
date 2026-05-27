from __future__ import annotations

import json
from datetime import date, timedelta

import pandas as pd
import polars as pl
import pytest

from agent_invest_scripts import recommend_backtest_window as cli
from agent_invest_scripts._lib.backtest.window import recommend_backtest_window


def test_recommends_window_covering_required_btc_drawdown() -> None:
    prices = _fixture_prices()

    payload = recommend_backtest_window(
        prices,
        coin_ids=["bitcoin", "ethereum", "solana"],
        horizon_days=1095,
    )

    start = date.fromisoformat(payload["start"])
    end = date.fromisoformat(payload["end"])
    assert (end - start).days >= 1460
    assert payload["covered_drawdowns"]
    assert (
        min(drawdown["drawdown_pct"] for drawdown in payload["covered_drawdowns"])
        <= -0.30
    )
    assert payload["history_constraints"]["limiting_coin"] == "solana"


def test_relaxes_drawdown_requirement_when_common_history_cannot_cover_it() -> None:
    prices = _fixture_prices(solana_start=date(2023, 1, 1))

    payload = recommend_backtest_window(
        prices,
        coin_ids=["bitcoin", "ethereum", "solana"],
        horizon_days=1095,
    )

    assert payload["start"] == "2023-01-01"
    assert payload["covered_drawdowns"] == []
    assert "relaxed the BTC drawdown requirement" in payload["rationale"]


def test_cli_returns_recommendation(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli, "daily_prices", lambda: _fixture_prices().to_pandas())

    cli.main(["--coin-ids", "bitcoin,ethereum,solana", "--horizon-days", "1095"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["history_constraints"]["limiting_coin"] == "solana"
    assert payload["covered_drawdowns"]


def _fixture_prices(*, solana_start: date = date(2019, 12, 1)) -> pl.DataFrame:
    start = date(2018, 1, 1)
    end = date(2024, 1, 1)
    rows = []
    values = {
        "bitcoin": [
            (date(2018, 1, 1), 100.0),
            (date(2020, 3, 1), 200.0),
            (date(2020, 3, 15), 100.0),
            (date(2021, 11, 1), 300.0),
            (date(2022, 11, 1), 180.0),
            (date(2024, 1, 1), 280.0),
        ],
        "ethereum": [
            (date(2019, 1, 1), 50.0),
            (date(2024, 1, 1), 150.0),
        ],
        "solana": [
            (solana_start, 10.0),
            (date(2024, 1, 1), 80.0),
        ],
    }
    for coin_id, anchors in values.items():
        for day in _days(start, end):
            if day < anchors[0][0]:
                continue
            rows.append(
                {"date": day, "coin_id": coin_id, "price": _interpolate(day, anchors)}
            )
    return pl.from_pandas(pd.DataFrame(rows))


def _days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def _interpolate(day: date, anchors: list[tuple[date, float]]) -> float:
    for index, (anchor_day, anchor_price) in enumerate(anchors):
        if day == anchor_day or index == len(anchors) - 1:
            return anchor_price
        next_day, next_price = anchors[index + 1]
        if anchor_day < day < next_day:
            span = (next_day - anchor_day).days
            position = (day - anchor_day).days / span
            return anchor_price + (next_price - anchor_price) * position
    raise AssertionError("unreachable")
