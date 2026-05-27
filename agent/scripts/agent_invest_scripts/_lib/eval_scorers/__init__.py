"""Stage eval scorer registry."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from agent_invest_scripts._lib.eval_scorers import (
    adjudicator,
    designer,
    placeholder,
    reporter,
    thesis,
)

ScoreResult = dict[str, Any]
Scorer = Callable[[dict[str, Any], Any], ScoreResult]

SCORERS: dict[str, Scorer] = {
    "adjudicator": adjudicator.score,
    "designer": designer.score,
    "reporter": reporter.score,
    "thesis": thesis.score,
}


def get_scorer(stage: str) -> Scorer:
    try:
        return SCORERS[stage]
    except KeyError as error:
        allowed = ", ".join(sorted(SCORERS))
        message = f"Invalid stage '{stage}'. Expected one of: {allowed}."
        raise ValueError(message) from error


__all__ = ["SCORERS", "Scorer", "ScoreResult", "get_scorer"]
