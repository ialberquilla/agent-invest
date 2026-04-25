"""Read user or strategy memory files from S3."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from typing import Any

import boto3
from botocore.exceptions import ClientError

from agent_invest_scripts._lib.cli import fail_json, print_json

_MISSING_S3_ERROR_CODES = {"404", "NoSuchKey", "NotFound"}


class JsonArgumentParser(argparse.ArgumentParser):
    """Argument parser that emits JSON-formatted stderr errors."""

    def error(self, message: str) -> None:
        print_json({"error": message}, stream=sys.stderr)
        raise SystemExit(2)


def build_parser() -> JsonArgumentParser:
    parser = JsonArgumentParser(description="Read user or strategy memory from S3.")
    parser.add_argument("--scope", required=True, choices=("user", "strategy"))
    parser.add_argument("--user", required=True)
    parser.add_argument("--strategy")
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.scope == "strategy" and not args.strategy:
        parser.error("--strategy is required when --scope=strategy")

    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        payload = read_memory(
            scope=args.scope,
            user_id=args.user,
            strategy_id=args.strategy,
        )
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)

    print_json(payload)
    return 0


def read_memory(
    *,
    scope: str,
    user_id: str,
    strategy_id: str | None = None,
) -> dict[str, str]:
    path = build_memory_path(scope=scope, user_id=user_id, strategy_id=strategy_id)
    return {
        "scope": scope,
        "path": path,
        "content": _read_memory_text(path),
    }


def build_memory_path(
    *,
    scope: str,
    user_id: str,
    strategy_id: str | None = None,
) -> str:
    prefix = _normalize_prefix(_read_optional_env("S3_PREFIX", "AWS_S3_PREFIX"))
    normalized_user_id = _normalize_identifier(user_id, "user")

    if scope == "user":
        return _join_key(prefix, "users", normalized_user_id, "profile.md")

    if scope == "strategy":
        if strategy_id is None:
            raise ValueError("--strategy is required when --scope=strategy")

        normalized_strategy_id = _normalize_identifier(strategy_id, "strategy")
        return _join_key(
            prefix,
            "users",
            normalized_user_id,
            "strategies",
            normalized_strategy_id,
            "memory.md",
        )

    raise ValueError(f"unsupported scope: {scope}")


def _read_memory_text(key: str) -> str:
    response = None

    try:
        response = _build_s3_client().get_object(Bucket=_bucket_name(), Key=key)
    except ClientError as error:
        if _is_missing_s3_object(error):
            return ""

        raise

    return _read_body_text(response.get("Body"))


def _read_body_text(body: Any) -> str:
    if body is None:
        return ""

    payload = body.read() if hasattr(body, "read") else body

    if isinstance(payload, bytes):
        return payload.decode("utf-8")

    if isinstance(payload, str):
        return payload

    raise TypeError("S3 response body must be bytes or text")


def _bucket_name() -> str:
    return _read_required_env("S3_BUCKET", "AWS_S3_BUCKET")


def _build_s3_client() -> Any:
    region = _read_optional_env("AWS_REGION", "AWS_DEFAULT_REGION")

    if region:
        return boto3.client("s3", region_name=region)

    return boto3.client("s3")


def _is_missing_s3_object(error: ClientError) -> bool:
    code = error.response.get("Error", {}).get("Code")
    return code in _MISSING_S3_ERROR_CODES


def _normalize_identifier(value: str, name: str) -> str:
    normalized = value.strip()

    if not normalized:
        raise ValueError(f"{name} must not be empty")

    if "/" in normalized:
        raise ValueError(f"{name} must not contain '/'")

    return normalized


def _read_required_env(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()

        if value:
            return value

    formatted_names = ", ".join(names)
    raise RuntimeError(f"Missing required environment variable(s): {formatted_names}")


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
