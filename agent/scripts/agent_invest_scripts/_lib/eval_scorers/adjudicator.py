"""Scorer for Adjudicator stage eval fixtures."""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

JUDGE_MODEL = "claude-haiku-4-5-20251001"
JUDGE_THRESHOLD = 0.6


def score(expectations: dict[str, Any], output: Any) -> dict[str, Any]:
    adjudication = _extract_adjudication(output)
    expected_kind = expectations.get("kind")
    actual_kind = adjudication.get("kind")

    rules = [
        _rule(
            "kind",
            actual_kind == expected_kind,
            f"expected {expected_kind!r}, got {actual_kind!r}",
        )
    ]

    if actual_kind == "winner":
        rules.append(_winner_rule(expectations, adjudication))
        judge = _judge_justification(expectations, adjudication)
        rules.append(
            _rule(
                "justification_judge",
                judge["score"] >= JUDGE_THRESHOLD,
                f"judge={judge['score']:.2f}: {judge['reason']}",
                judge["score"],
            )
        )
    elif actual_kind == "refine":
        rules.append(_refinement_codes_rule(expectations, adjudication))
    else:
        rules.append(_rule("known_kind", False, "kind must be winner or refine"))

    passed = all(rule["passed"] for rule in rules)
    score_value = sum(
        float(rule.get("score", 1.0 if rule["passed"] else 0.0)) for rule in rules
    ) / len(rules)
    return {"passed": passed, "score": round(score_value, 4), "rules": rules}


def _extract_adjudication(output: Any) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {}
    adjudication = output.get("adjudication", output)
    return adjudication if isinstance(adjudication, dict) else {}


def _winner_rule(
    expectations: dict[str, Any], adjudication: dict[str, Any]
) -> dict[str, Any]:
    acceptable = set(expectations.get("acceptable_winners", []))
    candidate_id = adjudication.get("candidate_id")
    return _rule(
        "acceptable_winner",
        isinstance(candidate_id, str) and candidate_id in acceptable,
        f"acceptable={sorted(acceptable)}; got={candidate_id!r}",
    )


def _refinement_codes_rule(
    expectations: dict[str, Any], adjudication: dict[str, Any]
) -> dict[str, Any]:
    expected_codes = set(expectations.get("expected_refinement_codes", []))
    reasons = adjudication.get("reasons", [])
    if not isinstance(reasons, list) or not reasons:
        return _rule("expected_refinement_codes", False, "reasons must be non-empty")

    actual_codes = [
        reason.get("reason") for reason in reasons if isinstance(reason, dict)
    ]
    invalid = sorted({code for code in actual_codes if code not in expected_codes})
    return _rule(
        "expected_refinement_codes",
        not invalid and len(actual_codes) == len(reasons),
        f"expected={sorted(expected_codes)}; invalid={invalid}; actual={actual_codes}",
    )


def _judge_justification(
    expectations: dict[str, Any], adjudication: dict[str, Any]
) -> dict[str, Any]:
    criteria = str(expectations.get("justification_criteria", ""))
    justification = str(adjudication.get("justification", ""))
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        judged = _judge_with_anthropic(api_key, criteria, justification)
        if judged is not None:
            return judged
    return _judge_with_overlap(criteria, justification)


def _judge_with_anthropic(
    api_key: str, criteria: str, justification: str
) -> dict[str, Any] | None:
    prompt = (
        _judge_prompt()
        .replace("{{criteria}}", criteria)
        .replace("{{justification}}", justification)
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


def _judge_with_overlap(criteria: str, justification: str) -> dict[str, Any]:
    expected_tokens = _tokens(criteria)
    actual_tokens = _tokens(justification)
    if not expected_tokens or not actual_tokens:
        return {"score": 0.0, "reason": "offline fallback found empty text"}
    overlap = len(expected_tokens & actual_tokens) / len(expected_tokens)
    return {
        "score": overlap,
        "reason": "offline lexical fallback; set ANTHROPIC_API_KEY for Haiku judge",
    }


def _tokens(value: str) -> set[str]:
    stop = {"and", "for", "the", "with", "that", "this", "candidate", "winner"}
    return {
        token
        for token in re.findall(r"[a-z0-9_]+", value.lower())
        if len(token) > 2 and token not in stop
    }


def _judge_prompt() -> str:
    path = Path(__file__).resolve().parents[4] / "evals" / "judges" / "adjudicator.md"
    return path.read_text(encoding="utf-8")


def _rule(
    rule: str, passed: bool, message: str, score_value: float | None = None
) -> dict[str, Any]:
    result: dict[str, Any] = {"rule": rule, "passed": passed, "message": message}
    if score_value is not None:
        result["score"] = score_value
    return result
