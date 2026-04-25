"""Shared output helpers for agent-facing CLIs."""

from __future__ import annotations

import json
import sys
from typing import Any, NoReturn


def print_json(payload: Any) -> None:
    """Write a JSON payload to stdout with a trailing newline."""
    json.dump(payload, sys.stdout)
    sys.stdout.write("\n")


def fail(message: str, *, exit_code: int = 1) -> NoReturn:
    """Write an error message to stderr and exit with a non-zero code."""
    print(message, file=sys.stderr)
    raise SystemExit(exit_code)
