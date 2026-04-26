from __future__ import annotations

import json
from pathlib import Path

import pytest

from agent_invest_scripts import write_memory


@pytest.fixture
def storage_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "storage"
    monkeypatch.setenv("STORAGE_ROOT", str(root))
    return root


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_write_memory_replace_then_append_round_trips_known_section(
    storage_root: Path,
) -> None:
    replaced = write_memory.write_memory(
        scope="strategy",
        user_id="user-1",
        strategy_id="strategy-1",
        section="tried",
        mode="replace",
        content="- first attempt",
    )
    appended = write_memory.write_memory(
        scope="strategy",
        user_id="user-1",
        strategy_id="strategy-1",
        section="tried",
        mode="append",
        content="- second attempt",
    )

    key = "users/user-1/strategies/strategy-1/memory.md"
    assert replaced["path"] == key
    assert appended["content"] == "- first attempt\n- second attempt"
    assert _read_text(storage_root / key) == (
        "## preferences\n\n"
        "## patterns\n\n"
        "## tried\n\n"
        "- first attempt\n"
        "- second attempt\n\n"
        "## decisions\n\n"
        "## open_threads\n\n"
        "## next\n\n"
        "## spec\n"
    )


def test_write_memory_main_rejects_unknown_section_with_json_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as error:
        write_memory.main(
            [
                "--scope",
                "user",
                "--user",
                "user-1",
                "--section",
                "unknown",
                "--mode",
                "append",
                "--content",
                "hello",
            ]
        )

    captured = capsys.readouterr()
    assert error.value.code == 1
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "error": {
            "type": "ValueError",
            "message": (
                "Unknown section: unknown. Expected one of: preferences, patterns, "
                "tried, decisions, open_threads, next, spec"
            ),
        }
    }


def test_write_memory_retries_once_after_conditional_conflict(
    storage_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = storage_root / "users" / "user-1" / "strategies" / "strategy-1" / "memory.md"
    write_memory._write_text_atomically(
        key,
        (
            "## preferences\n\n"
            "## patterns\n\n"
            "## tried\n\n"
            "- baseline\n\n"
            "## decisions\n\n"
            "## open_threads\n\n"
            "## next\n\n"
            "## spec\n"
        ),
    )

    original = write_memory._write_memory_object
    injected = {"done": False}

    def inject_concurrent_write(
        path: Path, *, body: str, expected_etag: str | None
    ) -> str:
        if not injected["done"]:
            injected["done"] = True
            write_memory._write_text_atomically(
                path,
                (
                    "## preferences\n\n"
                    "## patterns\n\n"
                    "## tried\n\n"
                    "- baseline\n"
                    "- concurrent writer\n\n"
                    "## decisions\n\n"
                    "## open_threads\n\n"
                    "## next\n\n"
                    "## spec\n"
                ),
            )
            raise write_memory.ConditionalWriteConflictError(str(path))

        return original(path, body=body, expected_etag=expected_etag)

    monkeypatch.setattr(write_memory, "_write_memory_object", inject_concurrent_write)

    result = write_memory.write_memory(
        scope="strategy",
        user_id="user-1",
        strategy_id="strategy-1",
        section="tried",
        mode="append",
        content="- retry winner",
    )

    assert result["content"] == "- baseline\n- concurrent writer\n- retry winner"
    assert _read_text(key) == (
        "## preferences\n\n"
        "## patterns\n\n"
        "## tried\n\n"
        "- baseline\n"
        "- concurrent writer\n"
        "- retry winner\n\n"
        "## decisions\n\n"
        "## open_threads\n\n"
        "## next\n\n"
        "## spec\n"
    )


def test_write_memory_surfaces_second_conditional_conflict(
    storage_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = storage_root / "users" / "user-1" / "profile.md"
    write_memory._write_text_atomically(
        key,
        (
            "## preferences\n\n"
            "- existing\n\n"
            "## patterns\n\n"
            "## tried\n\n"
            "## decisions\n\n"
            "## open_threads\n\n"
            "## next\n\n"
            "## spec\n"
        ),
    )

    attempts = {"count": 0}

    def always_conflict(path: Path, *, body: str, expected_etag: str | None) -> str:
        del path, body, expected_etag
        attempts["count"] += 1
        raise write_memory.ConditionalWriteConflictError("users/user-1/profile.md")

    monkeypatch.setattr(write_memory, "_write_memory_object", always_conflict)

    with pytest.raises(RuntimeError, match="Concurrent write conflict"):
        write_memory.write_memory(
            scope="user",
            user_id="user-1",
            strategy_id=None,
            section="preferences",
            mode="append",
            content="- new",
        )

    assert attempts["count"] == 2
