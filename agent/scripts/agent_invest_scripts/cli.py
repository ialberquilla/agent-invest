"""agent-invest CLI - single entrypoint dispatching to individual scripts.

Run `agent-invest list` to see available commands and
`agent-invest help <command>` for command-specific arguments. All commands
accept a `--payload '<json>'` argument and emit JSON on stdout.
"""

from __future__ import annotations

import importlib
import json
import sys
from collections.abc import Sequence

# Static command table: name -> (module, one-line description).
# Kept static so a broken module does not crash discovery.
COMMANDS: dict[str, str] = {
    "analyze_recovery": "Deterministic recovery analysis for an asset universe.",
    "compare_backtests": "Compare candidate-batch backtests and rank them deterministically.",
    "list_registry": "List closed-set registries (sectors, factors, etc.) available to the agent.",
    "list_runs": "List prior runs for a strategy with optional local artifact summaries.",
    "list_templates": "List backtest templates available to the agent.",
    "list_universe": "List the top-N asset universe entries by market cap.",
    "rank_universe": "Screen the materialized asset feature universe by sort/filter rules.",
    "recommend_backtest_window": "Pick a deterministic backtest window for the given universe.",
    "run_backtest": "Score an agent-supplied portfolio allocation against a backtest window.",
    "run_candidate_batch": "Run a bounded batch of candidate template backtests.",
    "validate_against_thesis": "Validate candidate-batch results against a structured thesis.",
}

# Commands that the strategist should NOT call directly.
INTERNAL_COMMANDS: frozenset[str] = frozenset({
    "compute_features",
})


def print_list() -> None:
    sys.stdout.write("agent-invest CLI commands:\n\n")
    for cmd, description in COMMANDS.items():
        sys.stdout.write(f"  {cmd:<28}  {description}\n")
    sys.stdout.write(
        "\nUse `agent-invest help <command>` to see command-specific arguments.\n"
        "All commands accept --payload '<json>' (most also accept individual flags) "
        "and emit JSON on stdout.\n"
    )


def print_help_summary() -> None:
    sys.stdout.write(__doc__ or "")
    sys.stdout.write("\n")
    print_list()


def run_command(command: str, args: Sequence[str]) -> int:
    if command in INTERNAL_COMMANDS:
        sys.stderr.write(
            f"error: '{command}' is an internal command and is not exposed via the CLI.\n"
        )
        return 2
    if command not in COMMANDS:
        sys.stderr.write(
            f"error: unknown command '{command}'. Run `agent-invest list`.\n"
        )
        return 2
    module = importlib.import_module(f"agent_invest_scripts.{command}")
    return int(module.main(list(args)) or 0)


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help"):
        print_help_summary()
        return 0
    head, *rest = argv
    if head == "list":
        print_list()
        return 0
    if head == "help":
        if not rest:
            print_help_summary()
            return 0
        target = rest[0]
        if target in INTERNAL_COMMANDS:
            sys.stderr.write(
                f"error: '{target}' is internal and not exposed.\n"
            )
            return 2
        if target not in COMMANDS:
            sys.stderr.write(
                f"error: unknown command '{target}'. Run `agent-invest list`.\n"
            )
            return 2
        try:
            run_command(target, ["--help"])
        except SystemExit:
            pass
        _print_examples(target)
        return 0
    return run_command(head, rest)


def _print_examples(command: str) -> None:
    module = importlib.import_module(f"agent_invest_scripts.{command}")
    examples: list[tuple[str, dict]] = []
    for attr in ("INPUT_EXAMPLE", "THESIS_EXAMPLE"):
        value = getattr(module, attr, None)
        if isinstance(value, dict):
            examples.append((attr, value))
    if not examples:
        return
    sys.stdout.write("\nExample payload(s):\n")
    for name, value in examples:
        flag = "--input" if name == "INPUT_EXAMPLE" else f"--{name.split('_')[0].lower()}"
        sys.stdout.write(f"\n  {flag} '{json.dumps(value)}'\n")


if __name__ == "__main__":
    raise SystemExit(main())
