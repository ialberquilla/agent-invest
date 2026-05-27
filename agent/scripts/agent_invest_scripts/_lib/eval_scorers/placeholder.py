"""Placeholder scorer used until stage-specific scorers are implemented."""

from __future__ import annotations

from typing import Any


def score(expectations: dict[str, Any], output: Any) -> dict[str, Any]:
    del expectations, output
    return {
        "passed": True,
        "score": 1.0,
        "rules": [
            {
                "rule": "placeholder_scorer",
                "passed": True,
                "message": (
                    "Placeholder scorer accepts output until "
                    "stage-specific rules land."
                ),
            }
        ],
    }
