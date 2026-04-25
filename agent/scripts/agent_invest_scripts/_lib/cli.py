"""Shared output helpers for agent-facing CLIs."""

from __future__ import annotations

import json
import sys
from typing import Any, NoReturn, TextIO


def print_json(payload: Any, *, stream: TextIO | None = None) -> None:
    """Write a JSON payload to the target stream with a trailing newline."""
    if stream is None:
        stream = sys.stdout
    json.dump(payload, stream)
    stream.write("\n")


def fail_json(
    message: str,
    *,
    error_type: str = "Error",
    exit_code: int = 1,
) -> NoReturn:
    """Write a structured JSON error to stderr and exit with a non-zero code."""
    print_json(
        {
            "error": {
                "type": error_type,
                "message": message,
            }
        },
        stream=sys.stderr,
    )
    raise SystemExit(exit_code)


def fail(message: str, *, exit_code: int = 1) -> NoReturn:
    """Write an error message to stderr and exit with a non-zero code."""
    print(message, file=sys.stderr)
    raise SystemExit(exit_code)
