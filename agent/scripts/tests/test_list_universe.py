from __future__ import annotations

import json

import pandas as pd
import pytest

from agent_invest_scripts import list_universe


def test_main_lists_top_n_by_market_cap_rank(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        list_universe,
        "asset_universe",
        lambda: pd.DataFrame(
            [
                {
                    "coin_id": "ethereum",
                    "symbol": "eth",
                    "name": "Ethereum",
                    "market_cap": 600.0,
                    "market_cap_rank": 2,
                },
                {
                    "coin_id": "bitcoin",
                    "symbol": "btc",
                    "name": "Bitcoin",
                    "market_cap": 1200.0,
                    "market_cap_rank": 1,
                },
                {
                    "coin_id": "solana",
                    "symbol": "sol",
                    "name": "Solana",
                    "market_cap": 400.0,
                    "market_cap_rank": 3,
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


def test_main_falls_back_to_market_cap_only_when_rank_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        list_universe,
        "asset_universe",
        lambda: pd.DataFrame(
            [
                {
                    "coin_id": "unranked-null-cap",
                    "symbol": "nil",
                    "name": "Unranked Null Cap",
                    "market_cap": None,
                    "market_cap_rank": None,
                },
                {
                    "coin_id": "unranked-large",
                    "symbol": "big",
                    "name": "Unranked Large",
                    "market_cap": 2000.0,
                    "market_cap_rank": None,
                },
                {
                    "coin_id": "ranked-small",
                    "symbol": "sml",
                    "name": "Ranked Small",
                    "market_cap": 100.0,
                    "market_cap_rank": 2,
                },
                {
                    "coin_id": "unranked-medium",
                    "symbol": "mid",
                    "name": "Unranked Medium",
                    "market_cap": 1000.0,
                    "market_cap_rank": None,
                },
            ]
        ),
    )

    exit_code = list_universe.main(["--top-n", "4"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    assert json.loads(captured.out) == [
        {
            "coin_id": "ranked-small",
            "symbol": "sml",
            "name": "Ranked Small",
            "market_cap": 100.0,
            "rank": 1,
        },
        {
            "coin_id": "unranked-large",
            "symbol": "big",
            "name": "Unranked Large",
            "market_cap": 2000.0,
            "rank": 2,
        },
        {
            "coin_id": "unranked-medium",
            "symbol": "mid",
            "name": "Unranked Medium",
            "market_cap": 1000.0,
            "rank": 3,
        },
        {
            "coin_id": "unranked-null-cap",
            "symbol": "nil",
            "name": "Unranked Null Cap",
            "market_cap": None,
            "rank": 4,
        },
    ]


def test_main_allows_nullable_market_cap(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        list_universe,
        "asset_universe",
        lambda: pd.DataFrame(
            [
                {
                    "coin_id": "bitcoin",
                    "symbol": "btc",
                    "name": "Bitcoin",
                    "market_cap": None,
                    "market_cap_rank": 1,
                },
            ]
        ),
    )

    exit_code = list_universe.main(["--top-n", "1"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    assert json.loads(captured.out) == [
        {
            "coin_id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "market_cap": None,
            "rank": 1,
        }
    ]


def test_main_writes_json_error_when_as_of_is_requested(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as error:
        list_universe.main(["--top-n", "2", "--as-of", "2024-01-03"])

    captured = capsys.readouterr()

    assert error.value.code == 1
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "error": "No universe snapshot found for 2024-01-03",
        "as_of": "2024-01-03",
    }
