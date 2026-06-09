"""Run short, guarded Python research snippets against read-only market data."""

from __future__ import annotations

import argparse
import ast
import contextlib
import io
import os
import tempfile
from collections.abc import Sequence
from decimal import Decimal
from typing import Any

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from agent_invest_scripts import research_api
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    print_json,
    resolve_timeout_seconds,
    script_timeout,
)

MAX_ROWS = int(os.environ.get("AGENT_RESEARCH_QUERY_MAX_ROWS", "1000000"))
MAX_STDOUT_BYTES = int(os.environ.get("AGENT_RESEARCH_STDOUT_MAX_BYTES", "20000"))
MAX_RESULT_BYTES = int(os.environ.get("AGENT_RESEARCH_RESULT_MAX_BYTES", "200000"))
ALLOWED_IMPORT_ROOTS = {"math", "statistics", "datetime", "pandas", "numpy", "polars", "scipy", "matplotlib", "research_api"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run guarded research Python code.")
    parser.add_argument("--code", required=True)
    parser.add_argument("--purpose", required=True)
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    timeout_seconds = resolve_timeout_seconds(args.timeout_seconds)
    try:
        output = run_research_code(args.code, args.purpose, timeout_seconds=timeout_seconds)
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)
    print_json(output)
    return 0


def run_research_code(code: str, purpose: str, *, timeout_seconds: int) -> dict[str, Any]:
    _validate_code(code)
    with tempfile.TemporaryDirectory(prefix="agent-research-") as scratch:
        research_api.configure_artifacts(scratch)
        stdout = io.StringIO()
        namespace: dict[str, Any] = {
            "query": query,
            "research_api": research_api,
            "result": {},
        }
        try:
            with script_timeout(timeout_seconds), contextlib.redirect_stdout(stdout):
                exec(compile(code, "<agent_research_code>", "exec"), namespace)
            error = None
        except Exception as exc:
            error = {"type": type(exc).__name__, "message": str(exc)}
        raw_stdout = stdout.getvalue().encode("utf-8")[:MAX_STDOUT_BYTES].decode("utf-8", errors="replace")
        result = namespace.get("result")
        if isinstance(result, pd.DataFrame):
            result = result.to_dict(orient="records")
        limited_result = _limit_jsonish(_json_safe(result), MAX_RESULT_BYTES)
        return {
            "code": code,
            "purpose": purpose,
            "stdout": raw_stdout,
            "result": limited_result,
            "artifacts": research_api.artifacts(),
            "assumptions": [
                "Research code can only query read-only market-data surfaces through query(sql).",
                "Research output is evidence for review, not financial advice.",
                "Small samples and historical relationships are unreliable without uncertainty checks.",
            ],
            "limits": {"timeout_seconds": timeout_seconds, "max_rows": MAX_ROWS},
            **({"error": error} if error else {}),
        }


def query(sql: str) -> pd.DataFrame:
    statement = _select_only(sql)
    dsn = os.environ.get("AGENT_READONLY_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("AGENT_READONLY_DATABASE_URL is not configured")
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM ({statement}) AS research_query LIMIT %s", (MAX_ROWS + 1,))
            rows = cursor.fetchall()
            columns = [column.name for column in cursor.description or ()]
    if len(rows) > MAX_ROWS:
        raise RuntimeError(f"query exceeded row cap of {MAX_ROWS}")
    return pd.DataFrame(rows, columns=columns)


def _select_only(sql: str) -> str:
    stripped = sql.strip().rstrip(";")
    lowered = stripped.lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise ValueError("query() only accepts SELECT or WITH statements")
    forbidden = (";", " insert ", " update ", " delete ", " drop ", " alter ", " create ", " copy ", " grant ", " revoke ", " truncate ")
    padded = f" {lowered} "
    if any(token in padded for token in forbidden):
        raise ValueError("query() rejected a non-read-only SQL token")
    return stripped


def _validate_code(code: str) -> None:
    tree = ast.parse(code)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [alias.name for alias in node.names] if isinstance(node, ast.Import) else [node.module or ""]
            for name in names:
                root = name.split(".", 1)[0]
                if root not in ALLOWED_IMPORT_ROOTS:
                    raise ValueError(f"import not allowed: {name}")


def _limit_jsonish(value: Any, max_bytes: int) -> Any:
    import json

    raw = json.dumps(value, allow_nan=False)
    if len(raw.encode("utf-8")) <= max_bytes:
        return value
    return {"truncated": True, "preview": raw[:max_bytes]}


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if pd.notna(value) else None
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if value is pd.NaT:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return _json_safe(value.item())
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())
