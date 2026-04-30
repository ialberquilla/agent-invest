"""Sleep helper used to verify CLI-enforced timeouts."""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Sequence

from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    resolve_timeout_seconds,
    script_timeout,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, required=True)
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            time.sleep(args.seconds)
    except (TimeoutError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
