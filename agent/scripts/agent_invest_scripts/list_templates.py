"""CLI for listing backtest templates available to the agent."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict

from agent_invest_scripts._lib.backtest.bt_templates import TEMPLATES
from agent_invest_scripts._lib.cli import print_json


def list_templates() -> list[dict]:
    return [asdict(template.METADATA) for template in TEMPLATES.values()]


def main(argv: Sequence[str] | None = None) -> int:
    _ = argv
    print_json(list_templates())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
