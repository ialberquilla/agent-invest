"""Shared output helpers for agent-facing CLIs."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from contextlib import contextmanager
from types import FrameType
from typing import Any, Iterator, Mapping, NoReturn, TextIO

DEFAULT_SCRIPT_TIMEOUT_SECONDS = 30
SCRIPT_TIMEOUT_ENV_VAR = "AGENT_SCRIPT_TIMEOUT_SECONDS"


def print_json(payload: Any, *, stream: TextIO | None = None) -> None:
    """Write a JSON payload to the target stream with a trailing newline."""
    if stream is None:
        stream = sys.stdout
    json.dump(payload, stream)
    stream.write("\n")


def add_timeout_argument(parser: argparse.ArgumentParser) -> None:
    """Add a shared per-script timeout argument to a CLI parser."""
    parser.add_argument(
        "--timeout-seconds",
        type=_parse_timeout_seconds,
        default=None,
        help=(
            "Per-script timeout in seconds. Defaults to "
            f"${SCRIPT_TIMEOUT_ENV_VAR} or {DEFAULT_SCRIPT_TIMEOUT_SECONDS}."
        ),
    )


def resolve_timeout_seconds(
    cli_timeout_seconds: int | None,
    *,
    env: Mapping[str, str] | None = None,
) -> int:
    """Resolve the effective timeout from CLI args or the environment."""
    if cli_timeout_seconds is not None:
        return cli_timeout_seconds

    if env is None:
        env = os.environ

    raw_timeout = env.get(SCRIPT_TIMEOUT_ENV_VAR)

    if raw_timeout is None:
        return DEFAULT_SCRIPT_TIMEOUT_SECONDS

    try:
        return _parse_timeout_seconds(raw_timeout)
    except argparse.ArgumentTypeError as error:
        raise ValueError(
            f"Invalid {SCRIPT_TIMEOUT_ENV_VAR} value: {raw_timeout}"
        ) from error


def format_timeout_message(timeout_seconds: int) -> str:
    """Return the shared human-readable timeout message."""
    return f"Script timed out after {timeout_seconds}s"


@contextmanager
def script_timeout(timeout_seconds: int) -> Iterator[None]:
    """Raise TimeoutError when a CLI exceeds its per-script runtime budget."""

    def handle_timeout(signum: int, frame: FrameType | None) -> NoReturn:
        del signum, frame
        raise TimeoutError(format_timeout_message(timeout_seconds))

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_alarm = signal.alarm(0)
    signal.signal(signal.SIGALRM, handle_timeout)
    signal.alarm(timeout_seconds)

    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_alarm > 0:
            signal.alarm(previous_alarm)


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


def _parse_timeout_seconds(value: str) -> int:
    try:
        timeout_seconds = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "timeout must be a positive integer number of seconds"
        ) from error

    if timeout_seconds < 1:
        raise argparse.ArgumentTypeError(
            "timeout must be a positive integer number of seconds"
        )

    return timeout_seconds
