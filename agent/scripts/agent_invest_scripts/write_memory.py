"""CLI for updating named markdown memory sections in S3."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Any, Sequence

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from agent_invest_scripts._lib.cli import fail_json, print_json

_ALLOWED_SCOPES = ("user", "strategy")
_ALLOWED_SECTIONS = (
    "preferences",
    "patterns",
    "tried",
    "decisions",
    "open_threads",
    "next",
    "spec",
)
_ALLOWED_MODES = ("append", "replace")
_MARKDOWN_CONTENT_TYPE = "text/markdown; charset=utf-8"
_MISSING_S3_ERROR_CODES = {"404", "NoSuchKey", "NotFound"}
_CONFLICT_S3_ERROR_CODES = {"PreconditionFailed", "ConditionalRequestConflict"}
_MAX_WRITE_ATTEMPTS = 2


class JsonArgumentParser(argparse.ArgumentParser):
    """Argument parser that emits structured JSON errors to stderr."""

    def error(self, message: str) -> None:
        fail_json(message, error_type="ArgumentError", exit_code=2)


@dataclass(slots=True)
class MemoryTarget:
    scope: str
    bucket: str
    key: str

    @property
    def path(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


@dataclass(slots=True)
class MemoryObject:
    body: str
    etag: str | None


class S3ConditionalWriteConflictError(RuntimeError):
    """Raised when an S3 conditional put fails."""


def _build_parser() -> JsonArgumentParser:
    parser = JsonArgumentParser(prog="python -m agent_invest_scripts.write_memory")
    parser.add_argument("--scope", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--strategy")
    parser.add_argument("--section", required=True)
    parser.add_argument("--mode", required=True)
    parser.add_argument("--content", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        result = write_memory(
            scope=args.scope,
            user_id=args.user,
            strategy_id=args.strategy,
            section=args.section,
            mode=args.mode,
            content=args.content,
        )
    except (ValueError, RuntimeError) as error:
        fail_json(str(error), error_type=type(error).__name__)
    except (BotoCoreError, ClientError) as error:
        fail_json(_format_s3_error(error), error_type=type(error).__name__)

    print_json(result)
    return 0


def write_memory(
    *,
    scope: str,
    user_id: str,
    strategy_id: str | None,
    section: str,
    mode: str,
    content: str,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    normalized_scope = _validate_scope(scope)
    normalized_section = _validate_section(section)
    normalized_mode = _validate_mode(mode)
    target = _resolve_memory_target(
        scope=normalized_scope,
        user_id=user_id,
        strategy_id=strategy_id,
    )
    client = s3_client or _build_s3_client()
    updated_section = ""

    for attempt in range(_MAX_WRITE_ATTEMPTS):
        current = _read_memory_object(client, bucket=target.bucket, key=target.key)
        updated_document, updated_section = _update_memory_document(
            current.body if current is not None else "",
            section=normalized_section,
            mode=normalized_mode,
            content=content,
        )

        try:
            etag = _put_memory_object(
                client,
                bucket=target.bucket,
                key=target.key,
                body=updated_document,
                etag=current.etag if current is not None else None,
            )
            return {
                "scope": normalized_scope,
                "path": target.path,
                "section": normalized_section,
                "mode": normalized_mode,
                "etag": etag,
                "content": updated_section,
            }
        except S3ConditionalWriteConflictError as error:
            if attempt == _MAX_WRITE_ATTEMPTS - 1:
                message = (
                    "Concurrent write conflict for "
                    f"{target.path} after {_MAX_WRITE_ATTEMPTS} attempts"
                )
                raise RuntimeError(message) from error

    raise AssertionError("unreachable")


def _validate_scope(scope: str) -> str:
    normalized = scope.strip()
    if normalized not in _ALLOWED_SCOPES:
        formatted = ", ".join(_ALLOWED_SCOPES)
        raise ValueError(f"Unknown scope: {scope}. Expected one of: {formatted}")
    return normalized


def _validate_section(section: str) -> str:
    normalized = section.strip()
    if normalized not in _ALLOWED_SECTIONS:
        formatted = ", ".join(_ALLOWED_SECTIONS)
        raise ValueError(f"Unknown section: {section}. Expected one of: {formatted}")
    return normalized


def _validate_mode(mode: str) -> str:
    normalized = mode.strip()
    if normalized not in _ALLOWED_MODES:
        formatted = ", ".join(_ALLOWED_MODES)
        raise ValueError(f"Unknown mode: {mode}. Expected one of: {formatted}")
    return normalized


def _resolve_memory_target(
    *, scope: str, user_id: str, strategy_id: str | None
) -> MemoryTarget:
    bucket = _read_required_env("S3_BUCKET", "AWS_S3_BUCKET")
    prefix = _normalize_prefix(_read_optional_env("S3_PREFIX", "AWS_S3_PREFIX"))
    normalized_user_id = _normalize_key_segment(user_id, "user_id")

    if scope == "user":
        key = _join_key(prefix, "users", normalized_user_id, "profile.md")
    else:
        if strategy_id is None:
            raise ValueError("--strategy is required when --scope=strategy")
        normalized_strategy_id = _normalize_key_segment(strategy_id, "strategy_id")
        key = _join_key(
            prefix,
            "users",
            normalized_user_id,
            "strategies",
            normalized_strategy_id,
            "memory.md",
        )

    return MemoryTarget(scope=scope, bucket=bucket, key=key)


def _build_s3_client() -> Any:
    region = _read_optional_env("AWS_REGION", "AWS_DEFAULT_REGION")
    if region:
        return boto3.client("s3", region_name=region)
    return boto3.client("s3")


def _read_memory_object(client: Any, *, bucket: str, key: str) -> MemoryObject | None:
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if _is_missing_s3_object(error):
            return None
        raise

    raw_body = response["Body"].read()
    if isinstance(raw_body, bytes):
        body = raw_body.decode("utf-8")
    else:
        body = str(raw_body)

    return MemoryObject(body=body, etag=_normalize_etag(response.get("ETag")))


def _put_memory_object(
    client: Any,
    *,
    bucket: str,
    key: str,
    body: str,
    etag: str | None,
) -> str | None:
    put_kwargs: dict[str, Any] = {
        "Bucket": bucket,
        "Key": key,
        "Body": body,
        "ContentType": _MARKDOWN_CONTENT_TYPE,
    }

    if etag is None:
        put_kwargs["IfNoneMatch"] = "*"
    else:
        put_kwargs["IfMatch"] = _quote_etag(etag)

    try:
        response = client.put_object(**put_kwargs)
    except ClientError as error:
        if _is_conflict_s3_error(error):
            raise S3ConditionalWriteConflictError(key) from error
        raise

    return _normalize_etag(response.get("ETag"))


def _update_memory_document(
    document: str,
    *,
    section: str,
    mode: str,
    content: str,
) -> tuple[str, str]:
    sections = _parse_memory_document(document)
    normalized_content = _normalize_section_body(content)

    if mode == "append":
        sections[section] = _append_section_body(sections[section], normalized_content)
    elif mode == "replace":
        sections[section] = normalized_content
    else:
        raise ValueError(f"Unsupported mode: {mode}")

    return _render_memory_document(sections), sections[section]


def _parse_memory_document(document: str) -> dict[str, str]:
    sections = {name: "" for name in _ALLOWED_SECTIONS}
    normalized_document = document.replace("\r\n", "\n")

    if not normalized_document.strip():
        return sections

    normalized_document = normalized_document.strip("\n")

    current_section: str | None = None
    current_lines: list[str] = []
    seen_sections: set[str] = set()

    for line in normalized_document.split("\n"):
        if line.startswith("## "):
            if current_section is not None:
                sections[current_section] = _normalize_section_body(
                    "\n".join(current_lines)
                )

            current_section = line[3:].strip()
            if current_section not in sections:
                raise ValueError(f"Unknown memory section header: {current_section}")
            if current_section in seen_sections:
                raise ValueError(f"Duplicate memory section header: {current_section}")

            seen_sections.add(current_section)
            current_lines = []
            continue

        if current_section is None:
            if line.strip():
                raise ValueError(
                    "Memory file must contain only named '## <section>' headings"
                )
            continue

        current_lines.append(line)

    if current_section is None:
        raise ValueError("Memory file must contain only named '## <section>' headings")

    sections[current_section] = _normalize_section_body("\n".join(current_lines))
    return sections


def _render_memory_document(sections: dict[str, str]) -> str:
    parts: list[str] = []

    for section in _ALLOWED_SECTIONS:
        body = _normalize_section_body(sections.get(section, ""))
        rendered = f"## {section}"
        if body:
            rendered = f"{rendered}\n\n{body}"
        parts.append(rendered)

    return "\n\n".join(parts) + "\n"


def _append_section_body(existing: str, content: str) -> str:
    if not content:
        return _normalize_section_body(existing)

    normalized_existing = _normalize_section_body(existing)
    if not normalized_existing:
        return content
    return f"{normalized_existing}\n{content}"


def _normalize_section_body(content: str) -> str:
    return content.replace("\r\n", "\n").strip("\n")


def _format_s3_error(error: ClientError | BotoCoreError) -> str:
    if isinstance(error, ClientError):
        details = error.response.get("Error", {})
        code = details.get("Code")
        message = details.get("Message")
        if code and message:
            return f"{code}: {message}"
        if code:
            return str(code)

    return str(error)


def _is_missing_s3_object(error: ClientError) -> bool:
    code = error.response.get("Error", {}).get("Code")
    return code in _MISSING_S3_ERROR_CODES


def _is_conflict_s3_error(error: ClientError) -> bool:
    code = error.response.get("Error", {}).get("Code")
    status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in _CONFLICT_S3_ERROR_CODES or status_code == 412


def _normalize_etag(etag: object) -> str | None:
    if not isinstance(etag, str) or not etag:
        return None
    return etag.replace('"', "")


def _quote_etag(etag: str) -> str:
    return etag if etag.startswith('"') and etag.endswith('"') else f'"{etag}"'


def _read_required_env(*names: str) -> str:
    value = _read_optional_env(*names)
    if value is None:
        formatted = ", ".join(names)
        raise RuntimeError(f"Missing required environment variable: {formatted}")
    return value


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


def _normalize_key_segment(value: str, name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    if "/" in normalized:
        raise ValueError(f"{name} must not contain '/'")
    return normalized


def _join_key(*segments: str) -> str:
    return "/".join(segment for segment in segments if segment)


if __name__ == "__main__":
    raise SystemExit(main())
