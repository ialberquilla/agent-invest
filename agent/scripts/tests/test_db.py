from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agent_invest_scripts._lib import db


def _clear_db_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "DATABASE_URL",
        "PGHOST",
        "PGPORT",
        "PGDATABASE",
        "PGUSER",
        "PGPASSWORD",
    ):
        monkeypatch.delenv(name, raising=False)


def test_connect_kwargs_prefers_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_db_env(monkeypatch)
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:password@db:5432/app")
    monkeypatch.setenv("PGHOST", "ignored")

    connect_kwargs = db._connect_kwargs()

    assert connect_kwargs["conninfo"] == "postgresql://user:password@db:5432/app"
    assert "host" not in connect_kwargs


def test_connect_kwargs_supports_individual_postgres_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_db_env(monkeypatch)
    monkeypatch.setenv("PGHOST", "localhost")
    monkeypatch.setenv("PGPORT", "5544")
    monkeypatch.setenv("PGDATABASE", "agent_invest")
    monkeypatch.setenv("PGUSER", "agent")
    monkeypatch.setenv("PGPASSWORD", "secret")

    connect_kwargs = db._connect_kwargs()

    assert connect_kwargs["host"] == "localhost"
    assert connect_kwargs["port"] == 5544
    assert connect_kwargs["dbname"] == "agent_invest"
    assert connect_kwargs["user"] == "agent"
    assert connect_kwargs["password"] == "secret"


def test_connect_kwargs_raises_actionable_error_when_config_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_db_env(monkeypatch)
    monkeypatch.setenv("PGHOST", "localhost")

    with pytest.raises(db.DatabaseError, match="Set DATABASE_URL") as exc_info:
        db._connect_kwargs()

    message = str(exc_info.value)
    assert "PGDATABASE" in message
    assert "PGUSER" in message
    assert "PGPASSWORD" in message


def test_load_env_files_uses_repo_root_before_agent_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _clear_db_env(monkeypatch)
    agent_dir = tmp_path / "agent"
    agent_dir.mkdir()
    (tmp_path / ".env").write_text(
        "DATABASE_URL=postgresql://root-db\n", encoding="utf-8"
    )
    (agent_dir / ".env").write_text(
        "DATABASE_URL=postgresql://agent-db\n", encoding="utf-8"
    )
    monkeypatch.setattr(db, "_repo_root", lambda: tmp_path)

    db._load_env_files()

    assert db._read_optional_env("DATABASE_URL") == "postgresql://root-db"


def test_read_sql_frame_preserves_database_column_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [{"coin_id": "bitcoin", "market_cap_usd": 123.45}]

    class Column:
        def __init__(self, name: str) -> None:
            self.name = name

    class Cursor:
        description = [Column("coin_id"), Column("market_cap_usd")]

        def __enter__(self) -> Cursor:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def execute(self, sql: str, params: dict[str, Any] | None) -> None:
            assert sql == "SELECT coin_id, market_cap_usd FROM daily_market_caps"
            assert params == {"coin_id": "bitcoin"}

        def fetchall(self) -> list[dict[str, object]]:
            return rows

    class Connection:
        def __enter__(self) -> Connection:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def cursor(self) -> Cursor:
            return Cursor()

    monkeypatch.setattr(db, "_connect_postgres", lambda: Connection())

    frame = db.read_sql_frame(
        "SELECT coin_id, market_cap_usd FROM daily_market_caps",
        {"coin_id": "bitcoin"},
    )

    pd.testing.assert_frame_equal(frame, pd.DataFrame(rows))
