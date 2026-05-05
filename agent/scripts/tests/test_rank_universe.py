from __future__ import annotations

import json

import pandas as pd
import pytest

from agent_invest_scripts import rank_universe


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "asset_id": 1,
        "coin_id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "market_cap_rank": 1,
        "market_cap": 1000.0,
        "latest_price": 50000.0,
        "first_price_date": "2023-01-01",
        "last_price_date": "2024-01-01",
        "data_days_365d": 365,
        "return_30d": 0.1,
        "return_90d": 0.2,
        "return_180d": 0.3,
        "return_365d": 0.4,
        "volatility_30d": 0.2,
        "volatility_90d": 0.3,
        "volatility_180d": 0.4,
        "max_drawdown_180d": -0.2,
        "sharpe_180d": 1.2,
        "price_above_sma_200d": True,
    }
    row.update(overrides)
    return row


def test_main_requires_top_n(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as error:
        rank_universe.main([])

    captured = capsys.readouterr()

    assert error.value.code == 2
    assert captured.out == ""
    assert "--top-n" in captured.err


def test_main_defaults_to_market_cap_rank_and_limits_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        rank_universe,
        "asset_universe_features",
        lambda: pd.DataFrame(
            [
                _row(asset_id=2, coin_id="ethereum", market_cap_rank=2),
                _row(asset_id=1, coin_id="bitcoin", market_cap_rank=1),
                _row(asset_id=3, coin_id="solana", market_cap_rank=3),
            ]
        ),
    )

    exit_code = rank_universe.main(["--top-n", "2"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert [row["coin_id"] for row in payload] == ["bitcoin", "ethereum"]
    assert [row["rank"] for row in payload] == [1, 2]
    assert {
        "rank",
        "asset_id",
        "coin_id",
        "symbol",
        "name",
        "market_cap_rank",
        "market_cap",
        "latest_price",
        "first_price_date",
        "last_price_date",
        "data_days_365d",
        "return_30d",
        "return_90d",
        "return_180d",
        "return_365d",
        "volatility_30d",
        "volatility_90d",
        "volatility_180d",
        "max_drawdown_180d",
        "sharpe_180d",
        "price_above_sma_200d",
    } == set(payload[0])


@pytest.mark.parametrize(
    ("sort", "expected_coin_ids"),
    [
        ("market_cap_rank", ["best-rank", "middle", "worst-rank"]),
        ("momentum_180d", ["best-momentum", "middle", "worst-momentum"]),
        ("sharpe_180d", ["best-sharpe", "middle", "worst-sharpe"]),
        ("low_volatility", ["lowest-vol", "middle", "highest-vol"]),
    ],
)
def test_main_sorts_supported_modes(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sort: str,
    expected_coin_ids: list[str],
) -> None:
    monkeypatch.setattr(
        rank_universe,
        "asset_universe_features",
        lambda: pd.DataFrame(
            [
                _row(
                    coin_id="middle",
                    market_cap_rank=2,
                    return_180d=0.5,
                    sharpe_180d=1.5,
                    volatility_180d=0.3,
                ),
                _row(
                    coin_id="best-rank",
                    market_cap_rank=1,
                    return_180d=0.2,
                    sharpe_180d=0.5,
                    volatility_180d=0.6,
                ),
                _row(
                    coin_id="worst-rank",
                    market_cap_rank=3,
                    return_180d=0.1,
                    sharpe_180d=0.4,
                    volatility_180d=0.7,
                ),
                _row(
                    coin_id="best-momentum",
                    market_cap_rank=4,
                    return_180d=0.9,
                    sharpe_180d=0.3,
                    volatility_180d=0.8,
                ),
                _row(
                    coin_id="worst-momentum",
                    market_cap_rank=5,
                    return_180d=0.4,
                    sharpe_180d=0.2,
                    volatility_180d=0.9,
                ),
                _row(
                    coin_id="best-sharpe",
                    market_cap_rank=6,
                    return_180d=0.3,
                    sharpe_180d=2.0,
                    volatility_180d=0.45,
                ),
                _row(
                    coin_id="worst-sharpe",
                    market_cap_rank=7,
                    return_180d=0.25,
                    sharpe_180d=1.0,
                    volatility_180d=0.5,
                ),
                _row(
                    coin_id="lowest-vol",
                    market_cap_rank=8,
                    return_180d=0.15,
                    sharpe_180d=0.7,
                    volatility_180d=0.1,
                ),
                _row(
                    coin_id="highest-vol",
                    market_cap_rank=9,
                    return_180d=0.12,
                    sharpe_180d=0.6,
                    volatility_180d=0.35,
                ),
            ]
        ),
    )

    exit_code = rank_universe.main(["--top-n", "3", "--sort", sort])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert [row["coin_id"] for row in payload] == expected_coin_ids
    assert [row["rank"] for row in payload] == [1, 2, 3]


def test_main_applies_filters_and_default_min_data_days(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        rank_universe,
        "asset_universe_features",
        lambda: pd.DataFrame(
            [
                _row(coin_id="pass", market_cap_rank=3, return_180d=0.2),
                _row(coin_id="too-new", market_cap_rank=1, data_days_365d=179),
                _row(coin_id="too-large", market_cap_rank=6, return_180d=0.2),
                _row(coin_id="negative", market_cap_rank=2, return_180d=-0.1),
                _row(
                    coin_id="below-sma",
                    market_cap_rank=4,
                    return_180d=0.2,
                    price_above_sma_200d=False,
                ),
                _row(
                    coin_id="too-volatile",
                    market_cap_rank=5,
                    return_180d=0.2,
                    volatility_180d=0.6,
                ),
            ]
        ),
    )

    exit_code = rank_universe.main(
        [
            "--top-n",
            "5",
            "--max-market-cap-rank",
            "5",
            "--positive-trend-only",
            "--max-volatility-180d",
            "0.5",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    assert [row["coin_id"] for row in json.loads(captured.out)] == ["pass"]


@pytest.mark.parametrize(
    ("sort", "missing_column", "kept_coin_id"),
    [
        ("market_cap_rank", "market_cap_rank", "has-rank"),
        ("momentum_180d", "return_180d", "has-momentum"),
        ("sharpe_180d", "sharpe_180d", "has-sharpe"),
        ("low_volatility", "volatility_180d", "has-volatility"),
    ],
)
def test_main_drops_rows_missing_selected_sort_value(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sort: str,
    missing_column: str,
    kept_coin_id: str,
) -> None:
    monkeypatch.setattr(
        rank_universe,
        "asset_universe_features",
        lambda: pd.DataFrame(
            [
                _row(coin_id="missing-sort-value", **{missing_column: None}),
                _row(coin_id=kept_coin_id),
            ]
        ),
    )

    exit_code = rank_universe.main(["--top-n", "5", "--sort", sort])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.err == ""
    assert [row["coin_id"] for row in json.loads(captured.out)] == [kept_coin_id]


def test_main_writes_structured_json_error_to_stderr(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        rank_universe,
        "asset_universe_features",
        lambda: pd.DataFrame([_row()]).drop(columns=["coin_id"]),
    )

    with pytest.raises(SystemExit) as error:
        rank_universe.main(["--top-n", "1"])
    captured = capsys.readouterr()

    assert error.value.code == 1
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload == {
        "error": {
            "message": "asset_universe_features is missing column(s): coin_id",
            "type": "ValueError",
        }
    }


def test_filters_by_conservative_default_data_days() -> None:
    payload = rank_universe.rank_universe(
        pd.DataFrame(
            [
                _row(coin_id="too-new", data_days_365d=179),
                _row(coin_id="old-enough", data_days_365d=180),
            ]
        ),
        top_n=5,
    )

    assert [row["coin_id"] for row in payload] == ["old-enough"]


def test_filters_by_market_cap_rank_positive_trend_and_volatility() -> None:
    payload = rank_universe.rank_universe(
        pd.DataFrame(
            [
                _row(coin_id="pass", market_cap_rank=5, return_180d=0.2),
                _row(coin_id="too-large", market_cap_rank=6, return_180d=0.2),
                _row(coin_id="negative", market_cap_rank=4, return_180d=-0.1),
                _row(
                    coin_id="below-sma",
                    market_cap_rank=3,
                    return_180d=0.2,
                    price_above_sma_200d=False,
                ),
                _row(
                    coin_id="too-volatile",
                    market_cap_rank=2,
                    return_180d=0.2,
                    volatility_180d=0.9,
                ),
                _row(
                    coin_id="missing-trend-ok",
                    market_cap_rank=1,
                    return_180d=None,
                    price_above_sma_200d=None,
                ),
            ]
        ),
        top_n=5,
        max_market_cap_rank=5,
        positive_trend_only=True,
        max_volatility_180d=0.5,
    )

    assert [row["coin_id"] for row in payload] == ["missing-trend-ok", "pass"]


def test_sort_modes_only_drop_missing_selected_sort_values() -> None:
    frame = pd.DataFrame(
        [
            _row(coin_id="missing-rank", market_cap_rank=None, return_180d=0.5),
            _row(coin_id="high-momentum", market_cap_rank=2, return_180d=0.8),
            _row(coin_id="missing-momentum", market_cap_rank=1, return_180d=None),
        ]
    )

    market_cap_payload = rank_universe.rank_universe(
        frame, top_n=5, sort="market_cap_rank"
    )
    momentum_payload = rank_universe.rank_universe(frame, top_n=5, sort="momentum_180d")

    assert [row["coin_id"] for row in market_cap_payload] == [
        "missing-momentum",
        "high-momentum",
    ]
    assert [row["coin_id"] for row in momentum_payload] == [
        "high-momentum",
        "missing-rank",
    ]


def test_sort_modes_rank_sharpe_and_low_volatility() -> None:
    frame = pd.DataFrame(
        [
            _row(coin_id="middle", sharpe_180d=1.0, volatility_180d=0.4),
            _row(coin_id="best-sharpe", sharpe_180d=2.0, volatility_180d=0.5),
            _row(coin_id="lowest-vol", sharpe_180d=0.5, volatility_180d=0.1),
        ]
    )

    sharpe_payload = rank_universe.rank_universe(frame, top_n=3, sort="sharpe_180d")
    volatility_payload = rank_universe.rank_universe(
        frame, top_n=3, sort="low_volatility"
    )

    assert [row["coin_id"] for row in sharpe_payload] == [
        "best-sharpe",
        "middle",
        "lowest-vol",
    ]
    assert [row["coin_id"] for row in volatility_payload] == [
        "lowest-vol",
        "middle",
        "best-sharpe",
    ]
