"""CLI for listing closed-set registries available to the agent."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from agent_invest_scripts._lib.cli import fail_json, print_json
from agent_invest_scripts._lib.registries import list_registry


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="List an agent closed-set registry.")
    parser.add_argument("--registry", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        payload = list_registry(args.registry)
    except ValueError as error:
        fail_json(str(error), error_type=type(error).__name__)

    print_json(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
