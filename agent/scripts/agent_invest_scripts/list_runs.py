"""List prior runs for a strategy from Postgres with optional S3 summaries."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import boto3
import psycopg
from botocore.exceptions import BotoCoreError, ClientError
from psycopg.rows import dict_row

from agent_invest_scripts._lib.cli import fail, print_json

_MISSING_S3_ERROR_CODES = {"404", "NoSuchKey", "NotFound"}


@dataclass(slots=True)
class RunRow:
    run_id: str
    status: str
    started_at: datetime
    ended_at: datetime | None
    exit_code: int | None
    user_id: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="List prior runs for a strategy as JSON."
    )
    parser.add_argument("--strategy-id", required=True)
    parser.add_argument("--limit", type=_positive_int)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        strategy_id = str(UUID(args.strategy_id))
        print_json(list_runs(strategy_id, limit=args.limit))
    except (ValueError, RuntimeError, psycopg.Error) as error:
        fail(str(error))

    return 0


def list_runs(strategy_id: str, *, limit: int | None = None) -> list[dict[str, Any]]:
    rows = _fetch_run_rows(strategy_id, limit=limit)
    output_payloads = _fetch_output_payloads(strategy_id, rows)

    return [
        {
            "run_id": row.run_id,
            "status": row.status,
            "started_at": row.started_at.isoformat(),
            "ended_at": row.ended_at.isoformat() if row.ended_at is not None else None,
            "summary": build_run_summary(
                row, output_payload=output_payloads.get(row.run_id)
            ),
        }
        for row in rows
    ]


def build_run_summary(row: RunRow, *, output_payload: object | None) -> str:
    fallback = _fallback_summary(row)
    output_summary = _extract_output_summary(output_payload)

    if output_summary is None:
        return fallback

    return f"{fallback}; {output_summary}"


def build_run_output_key(user_id: str, strategy_id: str, run_id: str) -> str:
    prefix = _normalize_prefix(_read_optional_env("S3_PREFIX", "AWS_S3_PREFIX"))
    return _join_key(
        prefix,
        "users",
        user_id,
        "strategies",
        strategy_id,
        "artifacts",
        run_id,
        "output.json",
    )


def _fetch_run_rows(strategy_id: str, *, limit: int | None) -> list[RunRow]:
    query = """
        SELECT r.run_id, r.status, r.started_at, r.ended_at, r.exit_code, s.user_id
        FROM runs AS r
        JOIN strategies AS s ON s.strategy_id = r.strategy_id
        WHERE r.strategy_id = %s
        ORDER BY r.started_at DESC
    """
    params: tuple[Any, ...] = (strategy_id,)

    if limit is not None:
        query += " LIMIT %s"
        params = (strategy_id, limit)

    with _connect_postgres() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

    return [
        RunRow(
            run_id=str(row["run_id"]),
            status=str(row["status"]),
            started_at=row["started_at"],
            ended_at=row["ended_at"],
            exit_code=row["exit_code"],
            user_id=str(row["user_id"]),
        )
        for row in rows
    ]


def _fetch_output_payloads(
    strategy_id: str, rows: list[RunRow]
) -> dict[str, object | None]:
    if not rows:
        return {}

    bucket = _read_optional_env("S3_BUCKET", "AWS_S3_BUCKET")
    if not bucket:
        return {}

    client = _build_s3_client()
    payloads: dict[str, object | None] = {}

    for row in rows:
        key = build_run_output_key(row.user_id, strategy_id, row.run_id)
        payloads[row.run_id] = _read_output_payload(client, bucket=bucket, key=key)

    return payloads


def _read_output_payload(client: Any, *, bucket: str, key: str) -> object | None:
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if _is_missing_s3_object(error):
            return None

        return None
    except BotoCoreError:
        return None

    try:
        raw_body = response["Body"].read()
        payload = raw_body.decode("utf-8") if isinstance(raw_body, bytes) else raw_body
        return json.loads(payload)
    except (AttributeError, TypeError, UnicodeDecodeError, json.JSONDecodeError):
        return None


def _connect_postgres() -> psycopg.Connection[dict[str, Any]]:
    database_url = _read_optional_env("DATABASE_URL")
    if database_url:
        return psycopg.connect(database_url, row_factory=dict_row)

    port = _read_optional_env("PGPORT")
    connect_kwargs: dict[str, Any] = {"row_factory": dict_row}

    if port is not None:
        try:
            connect_kwargs["port"] = int(port)
        except ValueError as error:
            raise RuntimeError(f"Invalid PGPORT value: {port}") from error

    for env_name, kwarg_name in (
        ("PGHOST", "host"),
        ("PGDATABASE", "dbname"),
        ("PGUSER", "user"),
        ("PGPASSWORD", "password"),
    ):
        value = _read_optional_env(env_name)
        if value is not None:
            connect_kwargs[kwarg_name] = value

    return psycopg.connect(**connect_kwargs)


def _build_s3_client() -> Any:
    region = _read_optional_env("AWS_REGION", "AWS_DEFAULT_REGION")
    if region:
        return boto3.client("s3", region_name=region)

    return boto3.client("s3")


def _extract_output_summary(payload: object | None) -> str | None:
    if not isinstance(payload, dict):
        return None

    for key in ("summary_line", "summary", "message", "error"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return _truncate_summary(value)

    metrics = _find_metric_mapping(payload)
    if metrics is None:
        return None

    fragments: list[str] = []

    sharpe_ratio = _coerce_float(metrics.get("sharpe_ratio"))
    if sharpe_ratio is not None:
        fragments.append(f"Sharpe {sharpe_ratio:.2f}")

    cagr = _coerce_float(metrics.get("cagr"))
    if cagr is not None:
        fragments.append(f"CAGR {_format_percent(cagr)}")

    max_drawdown = _coerce_float(metrics.get("max_drawdown"))
    if max_drawdown is not None:
        fragments.append(f"MaxDD {_format_percent(max_drawdown)}")

    final_equity = _coerce_float(metrics.get("final_equity_usd"))
    if final_equity is not None:
        fragments.append(f"Final ${final_equity:,.2f}")

    if not fragments:
        return None

    return ", ".join(fragments)


def _find_metric_mapping(payload: dict[str, object]) -> dict[str, object] | None:
    candidates: list[dict[str, object]] = [payload]

    for key in ("summary", "metrics", "result"):
        value = payload.get(key)
        if isinstance(value, dict):
            candidates.append(value)

            nested_summary = value.get("summary")
            if isinstance(nested_summary, dict):
                candidates.append(nested_summary)

            nested_metrics = value.get("metrics")
            if isinstance(nested_metrics, dict):
                candidates.append(nested_metrics)

    metric_keys = {"cagr", "final_equity_usd", "max_drawdown", "sharpe_ratio"}
    return next(
        (candidate for candidate in candidates if metric_keys & candidate.keys()),
        None,
    )


def _fallback_summary(row: RunRow) -> str:
    status = row.status.replace("_", " ")
    fragments = [status]
    duration = _format_duration(row.started_at, row.ended_at)

    if duration is not None:
        qualifier = "in" if row.ended_at is not None else "for"
        fragments.append(f"{qualifier} {duration}")

    if row.exit_code not in (None, 0):
        fragments.append(f"exit {row.exit_code}")

    return ", ".join(fragments)


def _format_duration(started_at: datetime, ended_at: datetime | None) -> str | None:
    end_time = ended_at
    if end_time is None:
        now_tz = started_at.tzinfo if started_at.tzinfo is not None else UTC
        end_time = datetime.now(now_tz)

    total_seconds = max(int((end_time - started_at).total_seconds()), 0)
    days, remainder = divmod(total_seconds, 86_400)
    hours, remainder = divmod(remainder, 3_600)
    minutes, seconds = divmod(remainder, 60)

    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def _coerce_float(value: object) -> float | None:
    if isinstance(value, bool) or value is None:
        return None

    if isinstance(value, int | float):
        return float(value)

    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None

    return None


def _truncate_summary(value: str, *, max_length: int = 160) -> str:
    trimmed = " ".join(value.split())
    if len(trimmed) <= max_length:
        return trimmed
    return f"{trimmed[: max_length - 3].rstrip()}..."


def _is_missing_s3_object(error: ClientError) -> bool:
    code = error.response.get("Error", {}).get("Code")
    return code in _MISSING_S3_ERROR_CODES


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error

    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")

    return parsed


def _read_optional_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return None


def _normalize_prefix(prefix: str | None) -> str:
    if not prefix:
        return ""
    return prefix.strip("/")


def _join_key(*segments: str) -> str:
    return "/".join(segment for segment in segments if segment)


if __name__ == "__main__":
    raise SystemExit(main())
