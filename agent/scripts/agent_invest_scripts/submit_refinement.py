"""Persist structured refinement reasons between candidate rounds."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from agent_invest_scripts._lib import print_json
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    resolve_timeout_seconds,
    script_timeout,
)
from agent_invest_scripts._lib.storage import normalize_identifier, storage_root


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Record candidate refinement reasons.")
    parser.add_argument("--input", required=True, help="Refinement payload JSON")
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            payload = submit(json.loads(args.input))
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)
    print_json(payload)
    return 0


def submit(input_payload: dict[str, Any]) -> dict[str, Any]:
    run_id = normalize_identifier(str(input_payload.get("run_id", "")), "run_id")
    round_number = input_payload.get("round")
    if round_number not in {2, 3}:
        raise ValueError("refinement round must be 2 or 3")
    reasons = input_payload.get("refinement_reasons")
    if not isinstance(reasons, list) or not reasons:
        raise ValueError("refinement_reasons must be a non-empty array")
    output = {"run_id": run_id, "round": round_number, "refinement_reasons": reasons}
    directory = storage_root() / "refinements"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{run_id}_round_{round_number}.json"
    fd, tmp_name = tempfile.mkstemp(prefix=f".{run_id}.", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(output, handle, indent=2)
            handle.write("\n")
        Path(tmp_name).replace(target)
    except Exception:
        Path(tmp_name).unlink(missing_ok=True)
        raise
    return {"refinement_json": str(target), **output}


if __name__ == "__main__":
    raise SystemExit(main())
