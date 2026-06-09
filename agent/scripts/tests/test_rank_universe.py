from __future__ import annotations

import json

import pandas as pd
import pytest

from agent_invest_scripts import rank_universe
from agent_invest_scripts._lib.registries import list_registry


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


def _prices(returns: dict[str, float], *, days: int = 366) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dates = pd.date_range("2023-01-01", periods=days, freq="D")
    for coin_id, total_return in returns.items():
        for index, day in enumerate(dates):
            rows.append(
                {
                    "date": day.date(),
                    "coin_id": coin_id,
                    "price": 100.0 * (1.0 + total_return * index / (days - 1)),
                }
            )
    return pd.DataFrame(rows)


def _prices_from_daily_returns(
    daily_returns: dict[str, list[float]], *, start: str = "2023-01-01"
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dates = pd.date_range(start, periods=len(next(iter(daily_returns.values()))) + 1)
    for coin_id, returns in daily_returns.items():
        price = 100.0
        rows.append({"date": dates[0].date(), "coin_id": coin_id, "price": price})
        for index, daily_return in enumerate(returns, start=1):
            price *= 1.0 + daily_return
            rows.append(
                {"date": dates[index].date(), "coin_id": coin_id, "price": price}
            )
    return pd.DataFrame(rows)


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


def test_filters_by_market_cap_and_exclusions() -> None:
    payload = rank_universe.rank_universe(
        pd.DataFrame(
            [
                _row(coin_id="pass", symbol="pass", name="Pass", market_cap=500.0),
                _row(coin_id="too-small", market_cap=99.0),
                _row(
                    coin_id="usd-coin", symbol="usdc", name="USD Coin", market_cap=500.0
                ),
                _row(
                    coin_id="wrapped-bitcoin",
                    symbol="wbtc",
                    name="Wrapped Bitcoin",
                    market_cap=500.0,
                ),
            ]
        ),
        top_n=5,
        min_market_cap=100.0,
        exclude_stablecoins=True,
        exclude_wrapped=True,
    )

    assert [row["coin_id"] for row in payload] == ["pass"]


def test_risk_profile_max_upside_tilts_to_smaller_high_momentum_assets() -> None:
    payload = rank_universe.rank_universe(
        pd.DataFrame(
            [
                _row(coin_id="large", market_cap_rank=1, return_180d=0.5),
                _row(coin_id="smaller", market_cap_rank=40, return_180d=0.5),
                _row(coin_id="low-momentum", market_cap_rank=60, return_180d=0.2),
            ]
        ),
        top_n=3,
        risk_profile="max_upside",
    )

    assert [row["coin_id"] for row in payload] == [
        "smaller",
        "large",
        "low-momentum",
    ]


def test_risk_profile_preserve_capital_applies_eligibility_defaults() -> None:
    payload = rank_universe.rank_universe(
        pd.DataFrame(
            [
                _row(coin_id="large", market_cap_rank=1, market_cap=6_000_000_000),
                _row(coin_id="too-small", market_cap_rank=2, market_cap=4_999_999_999),
                _row(
                    coin_id="too-low-rank",
                    market_cap_rank=21,
                    market_cap=10_000_000_000,
                ),
            ]
        ),
        top_n=5,
        risk_profile="preserve_capital",
    )

    assert [row["coin_id"] for row in payload] == ["large"]


def test_risk_profile_explicit_market_cap_floor_overrides_default() -> None:
    payload = rank_universe.rank_universe(
        pd.DataFrame(
            [
                _row(coin_id="large", market_cap_rank=1, market_cap=6_000_000_000),
                _row(
                    coin_id="explicit-pass", market_cap_rank=2, market_cap=100_000_000
                ),
                _row(coin_id="too-small", market_cap_rank=3, market_cap=99_999_999),
            ]
        ),
        top_n=5,
        risk_profile="preserve_capital",
        min_market_cap=100_000_000,
    )

    assert [row["coin_id"] for row in payload] == ["large", "explicit-pass"]


def test_filters_by_long_history_days() -> None:
    payload = rank_universe.rank_universe(
        pd.DataFrame(
            [
                _row(
                    coin_id="old",
                    first_price_date="2022-01-01",
                    last_price_date="2026-01-01",
                ),
                _row(
                    coin_id="new",
                    first_price_date="2025-01-01",
                    last_price_date="2026-01-01",
                ),
            ]
        ),
        top_n=5,
        min_history_days=1000,
    )

    assert [row["coin_id"] for row in payload] == ["old"]


def test_extended_risk_profile_surfaces_applied_defaults() -> None:
    frame = pd.DataFrame(
        [
            _row(coin_id="large", market_cap_rank=1, market_cap=6_000_000_000),
            _row(coin_id="too-small", market_cap_rank=2, market_cap=4_999_999_999),
        ]
    )
    payload = rank_universe.rank_universe_extended(
        frame,
        _prices({"large": 0.5, "too-small": 1.0}),
        {
            "risk_profile": "preserve_capital",
            "ranking": [{"factor": "return_365d", "direction": "high", "weight": 1.0}],
        },
    )

    assert payload["applied_defaults"] == {
        "max_market_cap_rank": 20,
        "market_cap_floor": 5_000_000_000,
    }
    assert [row["coin_id"] for row in payload["results"]] == ["large"]


def test_extended_explicit_market_cap_floor_overrides_default() -> None:
    frame = pd.DataFrame(
        [
            _row(coin_id="large", market_cap_rank=1, market_cap=6_000_000_000),
            _row(coin_id="explicit-pass", market_cap_rank=2, market_cap=100_000_000),
            _row(coin_id="too-small", market_cap_rank=3, market_cap=99_999_999),
        ]
    )
    payload = rank_universe.rank_universe_extended(
        frame,
        _prices({"large": 0.5, "explicit-pass": 1.0, "too-small": 2.0}),
        {
            "risk_profile": "preserve_capital",
            "filters": [{"id": "market_cap_floor", "value": 100_000_000}],
            "ranking": [{"factor": "return_365d", "direction": "high", "weight": 1.0}],
            "limit": 3,
        },
    )

    assert payload["applied_defaults"] == {"max_market_cap_rank": 20}
    assert [row["coin_id"] for row in payload["results"]] == [
        "explicit-pass",
        "large",
    ]


def test_objective_low_volatility_sorts_by_volatility_and_drawdown() -> None:
    payload = rank_universe.rank_universe(
        pd.DataFrame(
            [
                _row(coin_id="lowest-vol", volatility_180d=0.2, max_drawdown_180d=-0.4),
                _row(
                    coin_id="better-drawdown",
                    volatility_180d=0.2,
                    max_drawdown_180d=-0.2,
                ),
                _row(coin_id="higher-vol", volatility_180d=0.5, max_drawdown_180d=-0.1),
            ]
        ),
        top_n=3,
        objective="low_volatility",
    )

    assert [row["coin_id"] for row in payload] == [
        "better-drawdown",
        "lowest-vol",
        "higher-vol",
    ]


def test_return_and_drawdown_filters() -> None:
    payload = rank_universe.rank_universe(
        pd.DataFrame(
            [
                _row(coin_id="pass", return_180d=0.4, max_drawdown_180d=-0.2),
                _row(coin_id="low-return", return_180d=0.1, max_drawdown_180d=-0.2),
                _row(coin_id="deep-drawdown", return_180d=0.4, max_drawdown_180d=-0.7),
            ]
        ),
        top_n=3,
        min_return_180d=0.2,
        max_drawdown_180d=-0.35,
    )

    assert [row["coin_id"] for row in payload] == ["pass"]


def test_extended_ranks_by_registered_return_365d_factor() -> None:
    frame = pd.DataFrame(
        [
            _row(coin_id="middle", market_cap_rank=2),
            _row(coin_id="winner", market_cap_rank=3),
            _row(coin_id="lowest", market_cap_rank=1),
        ]
    )
    payload = rank_universe.rank_universe_extended(
        frame,
        _prices({"middle": 0.5, "winner": 1.0, "lowest": 0.1}),
        {
            "universe_selector": {"id": "top_n_by_mcap", "params": {"n": 3}},
            "ranking": [{"factor": "return_365d", "direction": "high", "weight": 1.0}],
            "limit": 3,
        },
    )

    assert [row["coin_id"] for row in payload] == ["winner", "middle", "lowest"]
    assert (
        payload[0]["factor_values"]["return_365d"]
        > payload[1]["factor_values"]["return_365d"]
    )


def test_extended_min_history_filter_excludes_short_history() -> None:
    frame = pd.DataFrame(
        [
            _row(
                coin_id="old",
                first_price_date="2020-01-01",
                last_price_date="2024-01-01",
            ),
            _row(
                coin_id="new",
                first_price_date="2023-01-01",
                last_price_date="2024-01-01",
            ),
        ]
    )
    payload = rank_universe.rank_universe_extended(
        frame,
        _prices({"old": 0.5, "new": 1.0}),
        {
            "universe_selector": {
                "id": "hand_picked",
                "params": {"coin_ids": ["old", "new"]},
            },
            "filters": [{"id": "min_history_days", "value": 1095}],
            "ranking": [{"factor": "return_365d", "direction": "high", "weight": 1.0}],
        },
    )

    assert [row["coin_id"] for row in payload] == ["old"]


def test_extended_correlation_prune_keeps_first_correlated_asset_and_diversifiers() -> (
    None
):
    coin_ids = [
        *(f"l1-{index}" for index in range(1, 6)),
        *(f"div-{index}" for index in range(1, 6)),
    ]
    frame = pd.DataFrame(
        [
            _row(
                coin_id=coin_id,
                market_cap_rank=index,
                return_365d=1.0 - index * 0.01,
            )
            for index, coin_id in enumerate(coin_ids, start=1)
        ]
    )
    l1_pattern = [0.02, 0.005, 0.016, 0.007] * 92
    daily_returns = {**{coin_id: l1_pattern for coin_id in coin_ids[:5]}}
    for offset, coin_id in enumerate(coin_ids[5:]):
        daily_returns[coin_id] = [
            0.012 if index % 5 == offset else 0.002 for index in range(len(l1_pattern))
        ]

    payload = rank_universe.rank_universe_extended(
        frame,
        _prices_from_daily_returns(daily_returns),
        {
            "universe_selector": {
                "id": "hand_picked",
                "params": {"coin_ids": coin_ids},
            },
            "filters": [
                {
                    "id": "correlation_prune",
                    "value": {"threshold": 0.85, "window_days": 365},
                }
            ],
            "ranking": [{"factor": "return_365d", "direction": "high", "weight": 1.0}],
            "limit": 6,
        },
    )

    selected = [row["coin_id"] for row in payload]
    assert selected[0] == "l1-1"
    assert set(selected[1:]) == set(coin_ids[5:])


def test_extended_correlation_prune_threshold_one_drops_nothing() -> None:
    coin_ids = ["l1-1", "l1-2", "div-1"]
    frame = pd.DataFrame(
        [
            _row(coin_id=coin_id, return_365d=1.0 - index * 0.1)
            for index, coin_id in enumerate(coin_ids)
        ]
    )
    daily_returns = {
        "l1-1": [0.02, 0.005, 0.016, 0.007] * 92,
        "l1-2": [0.02, 0.005, 0.016, 0.007] * 92,
        "div-1": [0.005, 0.004, 0.001, 0.006] * 92,
    }

    payload = rank_universe.rank_universe_extended(
        frame,
        _prices_from_daily_returns(daily_returns),
        {
            "universe_selector": {
                "id": "hand_picked",
                "params": {"coin_ids": coin_ids},
            },
            "filters": [
                {
                    "id": "correlation_prune",
                    "value": {"threshold": 1.0, "window_days": 365},
                }
            ],
            "ranking": [{"factor": "return_365d", "direction": "high", "weight": 1.0}],
            "limit": 3,
        },
    )

    assert [row["coin_id"] for row in payload] == coin_ids


def test_correlation_prune_is_visible_in_filters_registry() -> None:
    assert "correlation_prune" in {entry["id"] for entry in list_registry("filters")}


def test_extended_multi_factor_mixed_direction_is_deterministic() -> None:
    frame = pd.DataFrame([_row(coin_id="a"), _row(coin_id="b"), _row(coin_id="c")])
    payload = rank_universe.rank_universe_extended(
        frame,
        _prices({"a": 0.3, "b": 0.3, "c": 0.3}),
        {
            "universe_selector": {
                "id": "hand_picked",
                "params": {"coin_ids": ["a", "b", "c"]},
            },
            "ranking": [
                {"factor": "return_365d", "direction": "high", "weight": 1.0},
                {"factor": "volatility_90d", "direction": "low", "weight": 1.0},
            ],
            "limit": 3,
        },
    )

    assert [row["coin_id"] for row in payload] == sorted(
        row["coin_id"] for row in payload
    )


def test_extended_unknown_factor_or_filter_references_registry() -> None:
    frame = pd.DataFrame([_row(coin_id="bitcoin")])
    prices = _prices({"bitcoin": 0.5})

    with pytest.raises(ValueError, match="ranking_factors registry"):
        rank_universe.rank_universe_extended(
            frame,
            prices,
            {
                "universe_selector": {
                    "id": "hand_picked",
                    "params": {"coin_ids": ["bitcoin"]},
                },
                "ranking": [{"factor": "unknown", "direction": "high", "weight": 1.0}],
            },
        )

    with pytest.raises(ValueError, match="filters registry"):
        rank_universe.rank_universe_extended(
            frame,
            prices,
            {
                "universe_selector": {
                    "id": "hand_picked",
                    "params": {"coin_ids": ["bitcoin"]},
                },
                "filters": [{"id": "unknown", "value": 1}],
                "ranking": [
                    {"factor": "return_365d", "direction": "high", "weight": 1.0}
                ],
            },
        )
