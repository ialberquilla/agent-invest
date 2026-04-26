"""Read user or strategy memory files from local storage."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from agent_invest_scripts._lib.cli import fail_json, print_json
from agent_invest_scripts._lib.storage import key_path, memory_key


class JsonArgumentParser(argparse.ArgumentParser):
    """Argument parser that emits JSON-formatted stderr errors."""

    def error(self, message: str) -> None:
        print_json({"error": message}, stream=sys.stderr)
        raise SystemExit(2)


def build_parser() -> JsonArgumentParser:
    parser = JsonArgumentParser(
        description="Read user or strategy memory from local storage."
    )
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
    key = build_memory_path(scope=scope, user_id=user_id, strategy_id=strategy_id)
    return {
        "scope": scope,
        "path": key,
        "content": _read_memory_text(key),
    }


def build_memory_path(
    *,
    scope: str,
    user_id: str,
    strategy_id: str | None = None,
) -> str:
    return memory_key(scope=scope, user_id=user_id, strategy_id=strategy_id)


def _read_memory_text(key: str) -> str:
    path = key_path(key)

    if not path.exists():
        return ""

    return path.read_text(encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
