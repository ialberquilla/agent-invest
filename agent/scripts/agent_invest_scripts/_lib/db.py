"""Postgres helpers for agent-facing Python scripts."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
from psycopg.rows import dict_row


class DatabaseError(RuntimeError):
    """Raised when database configuration or query execution fails."""


def read_sql_frame(sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    """Run a SQL query against Postgres and return rows as a pandas DataFrame."""
    try:
        with _connect_postgres() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                columns = [column.name for column in cursor.description or ()]
    except DatabaseError:
        raise
    except psycopg.Error as error:
        raise DatabaseError(f"Postgres query failed: {error}") from error

    return pd.DataFrame(rows, columns=columns)


def _connect_postgres() -> psycopg.Connection[dict[str, Any]]:
    _load_env_files()
    return psycopg.connect(**_connect_kwargs())


def _connect_kwargs() -> dict[str, Any]:
    database_url = _read_optional_env("DATABASE_URL")
    if database_url:
        return {"conninfo": database_url, "row_factory": dict_row}

    missing = [
        env_name
        for env_name in ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD")
        if _read_optional_env(env_name) is None
    ]
    if missing:
        raise DatabaseError(
            "Missing Postgres configuration. Set DATABASE_URL or set "
            "PGHOST, PGDATABASE, PGUSER, and PGPASSWORD. Missing: "
            f"{', '.join(missing)}."
        )

    port = _read_optional_env("PGPORT")
    connect_kwargs: dict[str, Any] = {
        "host": _read_optional_env("PGHOST"),
        "dbname": _read_optional_env("PGDATABASE"),
        "user": _read_optional_env("PGUSER"),
        "password": _read_optional_env("PGPASSWORD"),
        "row_factory": dict_row,
    }

    if port is not None:
        try:
            connect_kwargs["port"] = int(port)
        except ValueError as error:
            raise DatabaseError(f"Invalid PGPORT value: {port}") from error

    return connect_kwargs


def _load_env_files() -> None:
    for env_path in (_repo_root() / ".env", _repo_root() / "agent" / ".env"):
        if env_path.exists():
            _load_env_file(env_path)
            break


def _load_env_file(path: Path) -> None:
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        name, value = line.split("=", 1)
        name = name.strip()
        if not name or name in os.environ:
            continue

        os.environ[name] = _parse_env_value(value.strip())


def _parse_env_value(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def _read_optional_env(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return None
    return value


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


__all__ = ["DatabaseError", "read_sql_frame"]
