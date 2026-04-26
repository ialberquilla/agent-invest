"""Shared filesystem storage helpers."""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath

_REPO_ROOT = Path(__file__).resolve().parents[4]
_DEFAULT_STORAGE_ROOT = (_REPO_ROOT / ".data" / "storage").resolve()


def join_key(*segments: str) -> str:
    return "/".join(segment for segment in segments if segment)


def normalize_identifier(value: str, name: str) -> str:
    normalized = value.strip()

    if not normalized:
        raise ValueError(f"{name} must not be empty")

    if "/" in normalized:
        raise ValueError(f"{name} must not contain '/'")

    return normalized


def storage_root() -> Path:
    configured = os.getenv("STORAGE_ROOT", "").strip()

    if not configured:
        return _DEFAULT_STORAGE_ROOT

    path = Path(configured).expanduser()
    if path.is_absolute():
        return path

    return (_REPO_ROOT / path).resolve()


def key_path(key: str) -> Path:
    root = storage_root()
    relative_key = PurePosixPath(key)

    if relative_key.is_absolute() or any(part == ".." for part in relative_key.parts):
        raise ValueError(f"storage key must resolve inside STORAGE_ROOT: {key}")

    if not relative_key.parts:
        raise ValueError("storage key must not be empty")

    return root.joinpath(*relative_key.parts)


def memory_key(*, scope: str, user_id: str, strategy_id: str | None = None) -> str:
    normalized_user_id = normalize_identifier(user_id, "user")

    if scope == "user":
        return join_key("users", normalized_user_id, "profile.md")

    if scope == "strategy":
        if strategy_id is None:
            raise ValueError("--strategy is required when --scope=strategy")

        normalized_strategy_id = normalize_identifier(strategy_id, "strategy")
        return join_key(
            "users",
            normalized_user_id,
            "strategies",
            normalized_strategy_id,
            "memory.md",
        )

    raise ValueError(f"unsupported scope: {scope}")


def dataset_key(filename: str) -> str:
    return join_key("datasets", filename)


def dataset_path(filename: str) -> Path:
    return key_path(dataset_key(filename))
