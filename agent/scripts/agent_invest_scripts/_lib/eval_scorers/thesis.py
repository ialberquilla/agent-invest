"""Scorer for Thesis stage eval fixtures."""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

JUDGE_MODEL = "claude-haiku-4-5-20251001"
JUDGE_THRESHOLD = 0.7


def score(expectations: dict[str, Any], output: Any) -> dict[str, Any]:
    thesis = _extract_thesis(output)
    rules: list[dict[str, Any]] = []

    rules.append(
        _rule(
            "objective",
            thesis.get("objective") == expectations.get("objective"),
            f"expected {expectations.get('objective')!r}, "
            f"got {thesis.get('objective')!r}",
        )
    )
    rules.append(_primary_factors_rule(expectations, thesis))
    rules.extend(_numeric_constraint_rules(expectations, thesis))

    judge = _judge_notes(
        str(expectations.get("interpretation_notes", "")),
        str(thesis.get("interpretation_notes", "")),
    )
    rules.append(
        _rule(
            "interpretation_notes_judge",
            judge["score"] >= JUDGE_THRESHOLD,
            f"judge={judge['score']:.2f}: {judge['reason']}",
            judge["score"],
        )
    )

    mechanical_passed = all(rule["passed"] for rule in rules[:-1])
    passed = mechanical_passed and rules[-1]["passed"]
    average_score = sum(
        float(rule.get("score", 1.0 if rule["passed"] else 0.0)) for rule in rules
    ) / len(rules)
    return {
        "passed": passed,
        "score": round(average_score, 4),
        "rules": rules,
    }


def _extract_thesis(output: Any) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {}
    thesis = output.get("thesis", output)
    return thesis if isinstance(thesis, dict) else {}


def _primary_factors_rule(
    expectations: dict[str, Any], thesis: dict[str, Any]
) -> dict[str, Any]:
    expected = set(expectations.get("primary_factor_ids", []))
    actual = {
        factor.get("factor")
        for factor in thesis.get("primary_factors", [])
        if isinstance(factor, dict) and isinstance(factor.get("factor"), str)
    }
    missing = sorted(expected - actual)
    return _rule(
        "primary_factor_ids_superset",
        not missing,
        f"missing={missing}; actual={sorted(actual)}",
    )


def _numeric_constraint_rules(
    expectations: dict[str, Any], thesis: dict[str, Any]
) -> list[dict[str, Any]]:
    expected = expectations.get("numeric_constraints", {})
    if not isinstance(expected, dict):
        return [
            _rule(
                "numeric_constraints",
                False,
                "expectations.numeric_constraints must be an object",
            )
        ]

    constraints = thesis.get("constraints", {})
    if not isinstance(constraints, dict):
        constraints = {}

    rules: list[dict[str, Any]] = []
    for key, expected_value in sorted(expected.items()):
        actual_value = (
            thesis.get(key) if key == "horizon_days" else constraints.get(key)
        )
        rules.append(
            _rule(
                f"numeric_constraint:{key}",
                actual_value == expected_value,
                f"expected {expected_value!r}, got {actual_value!r}",
            )
        )
    return rules


def _judge_notes(expected_notes: str, actual_notes: str) -> dict[str, Any]:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        judged = _judge_with_anthropic(api_key, expected_notes, actual_notes)
        if judged is not None:
            return judged

    return _judge_with_overlap(expected_notes, actual_notes)


def _judge_with_anthropic(
    api_key: str, expected_notes: str, actual_notes: str
) -> dict[str, Any] | None:
    prompt = (
        _judge_prompt()
        .replace("{{expected_notes}}", expected_notes)
        .replace("{{actual_notes}}", actual_notes)
    )
    request = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(
            {
                "model": JUDGE_MODEL,
                "max_tokens": 160,
                "messages": [{"role": "user", "content": prompt}],
            }
        ).encode("utf-8"),
        headers={
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
            "x-api-key": api_key,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, TimeoutError, urllib.error.HTTPError, json.JSONDecodeError):
        return None

    content = payload.get("content", [])
    text = "".join(part.get("text", "") for part in content if isinstance(part, dict))
    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        return None
    score_value = result.get("score")
    if not isinstance(score_value, int | float):
        return None
    return {
        "score": max(0.0, min(1.0, float(score_value))),
        "reason": str(result.get("reason", "Haiku judge")),
    }


def _judge_with_overlap(expected_notes: str, actual_notes: str) -> dict[str, Any]:
    expected_tokens = _tokens(expected_notes)
    actual_tokens = _tokens(actual_notes)
    if not expected_tokens or not actual_tokens:
        return {"score": 0.0, "reason": "offline fallback found empty notes"}
    overlap = len(expected_tokens & actual_tokens) / len(expected_tokens)
    return {
        "score": overlap,
        "reason": "offline lexical fallback; set ANTHROPIC_API_KEY for Haiku judge",
    }


def _tokens(value: str) -> set[str]:
    stop = {"and", "for", "the", "with", "that", "into", "user", "brief"}
    return {
        token
        for token in re.findall(r"[a-z0-9_]+", value.lower())
        if len(token) > 2 and token not in stop
    }


def _judge_prompt() -> str:
    path = Path(__file__).resolve().parents[4] / "evals" / "judges" / "thesis.md"
    return path.read_text(encoding="utf-8")


def _rule(
    rule: str, passed: bool, message: str, score_value: float | None = None
) -> dict[str, Any]:
    result: dict[str, Any] = {"rule": rule, "passed": passed, "message": message}
    if score_value is not None:
        result["score"] = score_value
    return result
