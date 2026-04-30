"""CLI for updating named markdown memory sections in local storage."""

from __future__ import annotations

import argparse
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Sequence

from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    print_json,
    resolve_timeout_seconds,
    script_timeout,
)
from agent_invest_scripts._lib.storage import key_path, memory_key

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
_MAX_WRITE_ATTEMPTS = 2


class JsonArgumentParser(argparse.ArgumentParser):
    """Argument parser that emits structured JSON errors to stderr."""

    def error(self, message: str) -> None:
        fail_json(message, error_type="ArgumentError", exit_code=2)


@dataclass(slots=True)
class MemoryTarget:
    scope: str
    key: str
    file_path: Path

    @property
    def path(self) -> str:
        return self.key


@dataclass(slots=True)
class MemoryObject:
    body: str
    etag: str | None


class ConditionalWriteConflictError(RuntimeError):
    """Raised when a conditional filesystem write fails."""


def _build_parser() -> JsonArgumentParser:
    parser = JsonArgumentParser(prog="python -m agent_invest_scripts.write_memory")
    parser.add_argument("--scope", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--strategy")
    parser.add_argument("--section", required=True)
    parser.add_argument("--mode", required=True)
    parser.add_argument("--content", required=True)
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            result = write_memory(
                scope=args.scope,
                user_id=args.user,
                strategy_id=args.strategy,
                section=args.section,
                mode=args.mode,
                content=args.content,
            )
    except (TimeoutError, ValueError, RuntimeError) as error:
        fail_json(str(error), error_type=type(error).__name__)

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
) -> dict[str, Any]:
    normalized_scope = _validate_scope(scope)
    normalized_section = _validate_section(section)
    normalized_mode = _validate_mode(mode)
    target = _resolve_memory_target(
        scope=normalized_scope,
        user_id=user_id,
        strategy_id=strategy_id,
    )
    updated_section = ""

    for attempt in range(_MAX_WRITE_ATTEMPTS):
        current = _read_memory_object(target.file_path)
        updated_document, updated_section = _update_memory_document(
            current.body if current is not None else "",
            section=normalized_section,
            mode=normalized_mode,
            content=content,
        )

        try:
            etag = _write_memory_object(
                target.file_path,
                body=updated_document,
                expected_etag=current.etag if current is not None else None,
            )
            return {
                "scope": normalized_scope,
                "path": target.path,
                "section": normalized_section,
                "mode": normalized_mode,
                "etag": etag,
                "content": updated_section,
            }
        except ConditionalWriteConflictError as error:
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
    key = memory_key(scope=scope, user_id=user_id, strategy_id=strategy_id)
    return MemoryTarget(scope=scope, key=key, file_path=key_path(key))


def _read_memory_object(path: Path) -> MemoryObject | None:
    if not path.exists():
        return None

    body = path.read_text(encoding="utf-8")
    return MemoryObject(body=body, etag=_etag(body))


def _write_memory_object(
    path: Path,
    *,
    body: str,
    expected_etag: str | None,
) -> str:
    current = _read_memory_object(path)

    if expected_etag is None:
        if current is not None:
            raise ConditionalWriteConflictError(str(path))
    elif current is None or current.etag != expected_etag:
        raise ConditionalWriteConflictError(str(path))

    _write_text_atomically(path, body)
    return _etag(body)


def _etag(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _write_text_atomically(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None

    try:
        with NamedTemporaryFile(
            dir=path.parent,
            delete=False,
            encoding="utf-8",
            mode="w",
        ) as handle:
            handle.write(body)
            temp_path = Path(handle.name)

        os.replace(temp_path, path)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()


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


if __name__ == "__main__":
    raise SystemExit(main())
