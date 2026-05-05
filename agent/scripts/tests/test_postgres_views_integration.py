from __future__ import annotations

import json
import os
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg
import pytest

from agent_invest_scripts import list_universe, rank_universe, run_backtest
from agent_invest_scripts._lib import db

INTEGRATION_FLAG = "AGENT_INVEST_POSTGRES_INTEGRATION"


def _integration_enabled() -> bool:
    return os.environ.get(INTEGRATION_FLAG) == "1"


@pytest.fixture()
def postgres_fixture() -> Iterator[psycopg.Connection[dict[str, Any]]]:
    if not _integration_enabled():
        pytest.skip(f"set {INTEGRATION_FLAG}=1 to run Postgres integration tests")

    try:
        connect_kwargs = db._connect_kwargs()
    except db.DatabaseError as error:
        pytest.skip(str(error))

    with psycopg.connect(**connect_kwargs) as connection:
        if not _fixture_tables_are_empty(connection):
            pytest.skip("Postgres integration tests require an empty fixture database")

        _seed_fixture(connection)
        yield connection
        connection.execute(
            'DELETE FROM "asset_prices" WHERE "asset_id" IN (%s, %s)', ("BTC", "ETH")
        )
        connection.execute(
            'DELETE FROM "asset_source_mappings" WHERE "asset_id" IN (%s, %s)',
            ("BTC", "ETH"),
        )
        connection.execute(
            'DELETE FROM "assets" WHERE "asset_id" IN (%s, %s)', ("BTC", "ETH")
        )
        connection.execute('REFRESH MATERIALIZED VIEW "agent_asset_universe_features"')


def test_agent_asset_universe_uses_identity_rank_and_latest_close(
    postgres_fixture: psycopg.Connection[dict[str, Any]],
) -> None:
    rows = postgres_fixture.execute(
        """
        SELECT "asset_id", "coin_id", "symbol", "market_cap_rank", "market_cap", "price"
        FROM "agent_asset_universe"
        ORDER BY "market_cap_rank"
        """
    ).fetchall()

    assert rows == [
        {
            "asset_id": "BTC",
            "coin_id": "bitcoin",
            "symbol": "BTC",
            "market_cap_rank": 1,
            "market_cap": 1_250_000_000,
            "price": 140,
        },
        {
            "asset_id": "ETH",
            "coin_id": "ethereum",
            "symbol": "ETH",
            "market_cap_rank": 2,
            "market_cap": 625_000_000,
            "price": 360,
        },
    ]


def test_agent_asset_universe_features_refreshes_after_new_close(
    postgres_fixture: psycopg.Connection[dict[str, Any]],
) -> None:
    postgres_fixture.execute(
        """
        INSERT INTO "asset_prices" (
          "asset_id", "timestamp", "source", "open", "high", "low", "close",
          "volume", "market_cap"
        ) VALUES (%s, %s, 'coingecko', %s, %s, %s, %s, %s, %s)
        """,
        (
            "BTC",
            datetime(2024, 7, 20, tzinfo=timezone.utc),
            145,
            145,
            145,
            145,
            1_000,
            1_300_000_000,
        ),
    )
    postgres_fixture.execute(
        'REFRESH MATERIALIZED VIEW "agent_asset_universe_features"'
    )

    row = postgres_fixture.execute(
        """
        SELECT "latest_price", "last_price_date"
        FROM "agent_asset_universe_features"
        WHERE "coin_id" = 'bitcoin'
        """
    ).fetchone()

    assert row == {"latest_price": 145, "last_price_date": date(2024, 7, 20)}


def test_list_universe_reads_postgres_view(
    postgres_fixture: psycopg.Connection[dict[str, Any]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = list_universe.main(["--top-n", "2"])

    assert exit_code == 0
    assert [row["coin_id"] for row in json.loads(capsys.readouterr().out)] == [
        "bitcoin",
        "ethereum",
    ]


def test_rank_universe_reads_materialized_postgres_features(
    postgres_fixture: psycopg.Connection[dict[str, Any]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = rank_universe.main(
        ["--top-n", "2", "--positive-trend-only", "--sort", "sharpe_180d"]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [row["coin_id"] for row in payload] == ["ethereum", "bitcoin"]
    assert [row["rank"] for row in payload] == [1, 2]


def test_run_backtest_uses_db_backed_daily_prices(
    postgres_fixture: psycopg.Connection[dict[str, Any]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path))

    exit_code = run_backtest.main(
        [
            "--allocation",
            json.dumps(
                {
                    "type": "static",
                    "weights": {"bitcoin": 0.5, "ethereum": 0.5},
                    "start": "2024-01-01",
                    "end": "2024-07-19",
                }
            ),
            "--label",
            "postgres_fixture",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["label"] == "postgres_fixture"
    assert payload["kpis"]["final_equity_usd"] > 1000
    assert Path(payload["report_json"]).is_file()


def _fixture_tables_are_empty(connection: psycopg.Connection[dict[str, Any]]) -> bool:
    row = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM "assets") AS "assets",
          (SELECT count(*) FROM "asset_prices") AS "asset_prices",
          (SELECT count(*) FROM "asset_source_mappings") AS "asset_source_mappings"
        """
    ).fetchone()
    assert row is not None
    return all(value == 0 for value in row.values())


def _seed_fixture(connection: psycopg.Connection[dict[str, Any]]) -> None:
    connection.execute(
        """
        INSERT INTO "assets" (
          "asset_id", "source", "source_asset_id", "symbol", "name", "market_cap_rank"
        ) VALUES
          ('BTC', 'gmx', 'BTC', 'BTC', 'Bitcoin', 1),
          ('ETH', 'gmx', 'ETH', 'ETH', 'Ethereum', 2)
        """
    )
    connection.execute(
        """
        INSERT INTO "asset_source_mappings" (
          "asset_id", "source", "source_asset_id", "confidence"
        )
        VALUES
          ('BTC', 'coingecko', 'bitcoin', 'fixture'),
          ('ETH', 'coingecko', 'ethereum', 'fixture')
        """
    )
    connection.executemany(
        """
        INSERT INTO "asset_prices" (
          "asset_id", "timestamp", "source", "open", "high", "low", "close",
          "volume", "market_cap"
        ) VALUES (%s, %s, 'coingecko', %s, %s, %s, %s, %s, %s)
        """,
        _price_rows(),
    )
    connection.execute('REFRESH MATERIALIZED VIEW "agent_asset_universe_features"')


def _price_rows() -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    start = date(2024, 1, 1)
    for offset in range(201):
        day = datetime.combine(
            start + timedelta(days=offset), datetime.min.time(), timezone.utc
        )
        btc_close = 100 + (offset * 0.2)
        eth_close = 200 + (offset * 0.8)
        rows.extend(
            [
                (
                    "BTC",
                    day,
                    btc_close,
                    btc_close,
                    btc_close,
                    btc_close,
                    1_000 + offset,
                    1_250_000_000,
                ),
                (
                    "ETH",
                    day,
                    eth_close,
                    eth_close,
                    eth_close,
                    eth_close,
                    2_000 + offset,
                    625_000_000,
                ),
            ]
        )
    return rows
