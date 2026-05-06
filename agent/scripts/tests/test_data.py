from __future__ import annotations

import pandas as pd

from agent_invest_scripts._lib import data


def test_daily_prices_queries_agent_daily_close_prices(monkeypatch) -> None:
    calls: list[str] = []
    expected = pd.DataFrame(
        [
            {"date": "2024-01-01", "coin_id": "bitcoin", "price": 42000.0},
            {"date": "2024-01-01", "coin_id": "ethereum", "price": 2300.0},
        ],
        columns=["date", "coin_id", "price"],
    )

    def fake_read_sql_frame(sql: str) -> pd.DataFrame:
        calls.append(sql)
        return expected

    monkeypatch.setattr(data, "read_sql_frame", fake_read_sql_frame)

    actual = data.daily_prices()

    pd.testing.assert_frame_equal(actual, expected)
    assert list(actual.columns) == ["date", "coin_id", "price"]
    assert "open" not in actual.columns
    assert "high" not in actual.columns
    assert "low" not in actual.columns
    assert "close" not in actual.columns
    assert "volume" not in actual.columns
    sql = calls[0]
    assert 'FROM "agent_daily_close_prices"' in sql
    assert '"date"' in sql
    assert '"coin_id"' in sql
    assert '"price"' in sql
    assert 'ORDER BY "date", "coin_id"' in sql


def test_asset_universe_queries_agent_asset_universe(monkeypatch) -> None:
    calls: list[str] = []
    columns = ["coin_id", "symbol", "name", "market_cap", "market_cap_rank", "price"]
    expected = pd.DataFrame(
        [
            {
                "coin_id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "market_cap": 1.0,
                "market_cap_rank": 1,
                "price": 42000.0,
            }
        ],
        columns=columns,
    )

    def fake_read_sql_frame(sql: str) -> pd.DataFrame:
        calls.append(sql)
        return expected

    monkeypatch.setattr(data, "read_sql_frame", fake_read_sql_frame)

    actual = data.asset_universe()

    pd.testing.assert_frame_equal(actual, expected)
    assert list(actual.columns) == columns
    sql = calls[0]
    assert 'FROM "agent_asset_universe"' in sql
    for column in columns:
        assert f'"{column}"' in sql
    assert 'ORDER BY "market_cap_rank" NULLS LAST, "coin_id"' in sql


def test_asset_universe_features_queries_planned_feature_columns(monkeypatch) -> None:
    calls: list[str] = []
    columns = [
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
    ]
    expected = pd.DataFrame(
        [
            {
                "asset_id": 1,
                "coin_id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "market_cap_rank": 1,
                "market_cap": 1_000_000.0,
                "latest_price": 42000.0,
                "first_price_date": "2023-01-01",
                "last_price_date": "2024-01-01",
                "data_days_365d": 365,
                "return_30d": 0.1,
                "return_90d": 0.2,
                "return_180d": 0.3,
                "return_365d": 0.4,
                "volatility_30d": 0.5,
                "volatility_90d": 0.6,
                "volatility_180d": 0.7,
                "max_drawdown_180d": -0.2,
                "sharpe_180d": 1.3,
                "price_above_sma_200d": True,
            },
            {
                "asset_id": 2,
                "coin_id": "new-coin",
                "symbol": "new",
                "name": "New Coin",
                "market_cap_rank": 2,
                "market_cap": 100_000.0,
                "latest_price": 1.0,
                "first_price_date": "2024-01-01",
                "last_price_date": "2024-01-01",
                "data_days_365d": 1,
                "return_30d": None,
                "return_90d": None,
                "return_180d": None,
                "return_365d": None,
                "volatility_30d": None,
                "volatility_90d": None,
                "volatility_180d": None,
                "max_drawdown_180d": None,
                "sharpe_180d": None,
                "price_above_sma_200d": None,
            },
        ],
        columns=columns,
    )

    def fake_read_sql_frame(sql: str) -> pd.DataFrame:
        calls.append(sql)
        return expected

    monkeypatch.setattr(data, "read_sql_frame", fake_read_sql_frame)

    actual = data.asset_universe_features()

    pd.testing.assert_frame_equal(actual, expected)
    assert list(actual.columns) == columns
    assert actual["coin_id"].is_unique
    assert len(actual) == actual["coin_id"].nunique()
    new_coin = actual.loc[actual["coin_id"] == "new-coin"].squeeze()
    assert pd.isna(new_coin["return_30d"])
    assert pd.isna(new_coin["volatility_180d"])
    assert pd.isna(new_coin["price_above_sma_200d"])
    sql = calls[0]
    assert 'FROM "agent_asset_universe_features"' in sql
    for column in columns:
        assert f'"{column}"' in sql
    assert 'ORDER BY "market_cap_rank" NULLS LAST, "coin_id"' in sql
