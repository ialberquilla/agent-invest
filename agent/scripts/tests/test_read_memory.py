from __future__ import annotations

import io
import json
from pathlib import Path
from unittest import mock

from agent_invest_scripts import read_memory


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_reads_user_profile_from_storage_root(tmp_path: Path, monkeypatch) -> None:
    storage_root = tmp_path / "storage"
    _write_text(
        storage_root / "users" / "user-1" / "profile.md",
        "# Preferences\n- weekly rebalance\n",
    )
    monkeypatch.setenv("STORAGE_ROOT", str(storage_root))

    payload = read_memory.read_memory(scope="user", user_id="user-1")

    assert payload == {
        "scope": "user",
        "path": "users/user-1/profile.md",
        "content": "# Preferences\n- weekly rebalance\n",
    }


def test_reads_strategy_memory_from_storage_root(tmp_path: Path, monkeypatch) -> None:
    storage_root = tmp_path / "storage"
    _write_text(
        storage_root / "users" / "user-1" / "strategies" / "strategy-1" / "memory.md",
        "# Tried\n- lookback=90\n",
    )
    monkeypatch.setenv("STORAGE_ROOT", str(storage_root))

    payload = read_memory.read_memory(
        scope="strategy",
        user_id="user-1",
        strategy_id="strategy-1",
    )

    assert payload == {
        "scope": "strategy",
        "path": "users/user-1/strategies/strategy-1/memory.md",
        "content": "# Tried\n- lookback=90\n",
    }


def test_missing_memory_returns_empty_content(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("STORAGE_ROOT", str(tmp_path / "storage"))

    payload = read_memory.read_memory(scope="user", user_id="user-1")

    assert payload["content"] == ""
    assert payload["path"] == "users/user-1/profile.md"


def test_parse_args_requires_strategy_for_strategy_scope() -> None:
    stderr = io.StringIO()

    with mock.patch("sys.stderr", stderr):
        with mock.patch("sys.stdout", io.StringIO()):
            try:
                read_memory.parse_args(["--scope", "strategy", "--user", "user-1"])
            except SystemExit as error:
                assert error.code == 2
            else:
                raise AssertionError("parse_args should exit for missing --strategy")

    assert json.loads(stderr.getvalue()) == {
        "error": "--strategy is required when --scope=strategy"
    }
