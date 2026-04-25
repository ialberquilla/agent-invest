from __future__ import annotations

import json

import pandas as pd
import pytest

from agent_invest_scripts import list_universe


def test_main_lists_top_n_from_latest_snapshot(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        list_universe,
        "universe_history",
        lambda: pd.DataFrame(
            [
                {
                    "date": "2024-01-01",
                    "coin_id": "bitcoin",
                    "symbol": "btc",
                    "name": "Bitcoin",
                    "market_cap": 900.0,
                },
                {
                    "date": "2024-01-02",
                    "coin_id": "ethereum",
                    "symbol": "eth",
                    "name": "Ethereum",
                    "market_cap": 600.0,
                },
                {
                    "date": "2024-01-02",
                    "coin_id": "bitcoin",
                    "symbol": "btc",
                    "name": "Bitcoin",
                    "market_cap": 1200.0,
                },
                {
                    "date": "2024-01-02",
                    "coin_id": "solana",
                    "symbol": "sol",
                    "name": "Solana",
                    "market_cap": 400.0,
                },
            ]
        ),
    )

    exit_code = list_universe.main(["--top-n", "2"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    assert json.loads(captured.out) == [
        {
            "coin_id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "market_cap": 1200.0,
            "rank": 1,
        },
        {
            "coin_id": "ethereum",
            "symbol": "eth",
            "name": "Ethereum",
            "market_cap": 600.0,
            "rank": 2,
        },
    ]


def test_main_uses_as_of_snapshot_and_falls_back_to_coin_metadata(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        list_universe,
        "universe_history",
        lambda: pd.DataFrame(
            [
                {"date": "2024-01-01", "coin_id": "bitcoin", "market_cap": 900.0},
                {"date": "2024-01-01", "coin_id": "ethereum", "market_cap": 600.0},
                {"date": "2024-01-02", "coin_id": "bitcoin", "market_cap": 1200.0},
            ]
        ),
    )
    monkeypatch.setattr(
        list_universe,
        "coin_metadata",
        lambda: pd.DataFrame(
            [
                {"coin_id": "bitcoin", "symbol": "btc", "name": "Bitcoin"},
                {"coin_id": "ethereum", "symbol": "eth", "name": "Ethereum"},
            ]
        ),
    )

    exit_code = list_universe.main(["--top-n", "2", "--as-of", "2024-01-01"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    assert json.loads(captured.out) == [
        {
            "coin_id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "market_cap": 900.0,
            "rank": 1,
        },
        {
            "coin_id": "ethereum",
            "symbol": "eth",
            "name": "Ethereum",
            "market_cap": 600.0,
            "rank": 2,
        },
    ]


def test_main_writes_json_error_when_snapshot_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        list_universe,
        "universe_history",
        lambda: pd.DataFrame(
            [
                {"date": "2024-01-01", "coin_id": "bitcoin", "market_cap": 900.0},
                {"date": "2024-01-02", "coin_id": "ethereum", "market_cap": 600.0},
            ]
        ),
    )

    with pytest.raises(SystemExit) as error:
        list_universe.main(["--top-n", "2", "--as-of", "2024-01-03"])

    captured = capsys.readouterr()

    assert error.value.code == 1
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "error": "No universe snapshot found for 2024-01-03",
        "as_of": "2024-01-03",
    }
