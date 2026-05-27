"""Scorer for Reporter stage eval fixtures."""

from __future__ import annotations

import json
import math
import os
import re
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any

JUDGE_MODEL = "claude-haiku-4-5-20251001"
JUDGE_THRESHOLD = 0.7


def score(expectations: dict[str, Any], output: Any) -> dict[str, Any]:
    report = _extract_report(output)
    batch = expectations.get("batch") if isinstance(expectations.get("batch"), dict) else {}

    rules = [
        _required_sections_rule(report),
        _survivorship_warning_rule(batch, report),
        _numeric_grounding_rule(batch, report),
    ]
    judge = _judge_report(expectations, report)
    rules.append(
        _rule(
            "summary_reasoning_judge",
            judge["score"] >= JUDGE_THRESHOLD,
            f"judge={judge['score']:.2f}: {judge['reason']}",
            judge["score"],
        )
    )

    mechanical_passed = all(rule["passed"] for rule in rules[:-1])
    passed = mechanical_passed and rules[-1]["passed"]
    score_value = sum(
        float(rule.get("score", 1.0 if rule["passed"] else 0.0)) for rule in rules
    ) / len(rules)
    return {"passed": passed, "score": round(score_value, 4), "rules": rules}


def _extract_report(output: Any) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {}
    report = output.get("structured_result", output.get("report", output))
    return report if isinstance(report, dict) else {}


def _required_sections_rule(report: dict[str, Any]) -> dict[str, Any]:
    required_text = ["title", "summary", "reasoning"]
    required_lists = ["assumptions", "risks", "next_steps"]
    missing = [
        key
        for key in required_text
        if not isinstance(report.get(key), str) or not report[key].strip()
    ]
    missing.extend(
        key
        for key in required_lists
        if not isinstance(report.get(key), list) or not report[key]
    )
    if report.get("result_type") != "no_viable_strategy" and not isinstance(
        report.get("kpis"), dict
    ):
        missing.append("kpis")
    return _rule(
        "required_sections",
        not missing,
        f"missing_or_empty={missing}",
    )


def _survivorship_warning_rule(
    batch: dict[str, Any], report: dict[str, Any]
) -> dict[str, Any]:
    requires_warning = _window_is_more_than_one_year_old(batch)
    if not requires_warning:
        return _rule(
            "survivorship_warning",
            True,
            "batch window does not require a survivorship warning",
        )
    present = report.get("survivorship_warning") is True or any(
        "survivorship" in str(item).lower() for item in report.get("risks", [])
    )
    return _rule(
        "survivorship_warning",
        present,
        "old batch windows must include survivorship_warning=true or a survivorship risk",
    )


def _window_is_more_than_one_year_old(batch: dict[str, Any]) -> bool:
    window = batch.get("window") if isinstance(batch.get("window"), dict) else {}
    end = window.get("end")
    if not isinstance(end, str):
        return False
    try:
        end_date = date.fromisoformat(end)
    except ValueError:
        return False
    return (date.today() - end_date).days > 365


def _numeric_grounding_rule(
    batch: dict[str, Any], report: dict[str, Any]
) -> dict[str, Any]:
    allowed = _actual_kpi_values(batch, report)
    numbers = _narrative_numbers(report)
    invented = [value for value in numbers if not _matches_any_kpi(value, allowed)]
    return _rule(
        "numeric_grounding",
        not invented,
        f"narrative_numbers={numbers}; invented={invented}; kpi_values={allowed}",
    )


def _actual_kpi_values(batch: dict[str, Any], report: dict[str, Any]) -> list[float]:
    values: list[float] = []
    result_type = report.get("result_type")
    if result_type == "no_viable_strategy":
        for candidate in _batch_results(batch):
            values.extend(_numeric_values(candidate.get("metrics", {})))
        return values

    winner_id = report.get("winner_candidate_id")
    candidates = _batch_results(batch)
    winner = next(
        (
            candidate
            for candidate in candidates
            if isinstance(winner_id, str) and candidate.get("candidate_id") == winner_id
        ),
        None,
    )
    if winner is not None:
        values.extend(_numeric_values(winner.get("metrics", {})))
    values.extend(_numeric_values(report.get("kpis", {})))
    return values


def _batch_results(batch: dict[str, Any]) -> list[dict[str, Any]]:
    results = batch.get("results") if isinstance(batch.get("results"), list) else []
    return [result for result in results if isinstance(result, dict)]


def _numeric_values(value: Any) -> list[float]:
    if isinstance(value, bool):
        return []
    if isinstance(value, int | float) and math.isfinite(float(value)):
        return [float(value)]
    if isinstance(value, dict):
        values: list[float] = []
        for nested in value.values():
            values.extend(_numeric_values(nested))
        return values
    if isinstance(value, list):
        values = []
        for nested in value:
            values.extend(_numeric_values(nested))
        return values
    return []


def _narrative_numbers(report: dict[str, Any]) -> list[float]:
    text = " ".join(
        str(report.get(key, "")) for key in ("summary", "reasoning")
    )
    values = []
    for match in re.finditer(r"(?<![A-Za-z0-9_])-?\d+(?:\.\d+)?%?", text):
        token = match.group(0)
        value = float(token.rstrip("%"))
        values.append(value / 100.0 if token.endswith("%") else value)
    return values


def _matches_any_kpi(value: float, allowed: list[float]) -> bool:
    for candidate in allowed:
        tolerance = max(abs(candidate) * 0.005, 0.001)
        if abs(value - candidate) <= tolerance:
            return True
    return False


def _judge_report(expectations: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    criteria = str(expectations.get("judge_criteria", ""))
    summary = str(report.get("summary", ""))
    reasoning = str(report.get("reasoning", ""))
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        judged = _judge_with_anthropic(api_key, criteria, summary, reasoning)
        if judged is not None:
            return judged
    return _judge_with_overlap(criteria, f"{summary} {reasoning}")


def _judge_with_anthropic(
    api_key: str, criteria: str, summary: str, reasoning: str
) -> dict[str, Any] | None:
    prompt = (
        _judge_prompt()
        .replace("{{criteria}}", criteria)
        .replace("{{summary}}", summary)
        .replace("{{reasoning}}", reasoning)
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


def _judge_with_overlap(criteria: str, text: str) -> dict[str, Any]:
    expected_tokens = _tokens(criteria)
    actual_tokens = _tokens(text)
    if not expected_tokens or not actual_tokens:
        return {"score": 0.0, "reason": "offline fallback found empty text"}
    overlap = len(expected_tokens & actual_tokens) / len(expected_tokens)
    return {
        "score": overlap,
        "reason": "offline lexical fallback; set ANTHROPIC_API_KEY for Haiku judge",
    }


def _tokens(value: str) -> set[str]:
    stop = {"and", "for", "the", "with", "that", "this", "candidate", "strategy"}
    return {
        token
        for token in re.findall(r"[a-z0-9_]+", value.lower())
        if len(token) > 2 and token not in stop and not token.isdigit()
    }


def _judge_prompt() -> str:
    path = Path(__file__).resolve().parents[4] / "evals" / "judges" / "reporter.md"
    return path.read_text(encoding="utf-8")


def _rule(
    rule: str, passed: bool, message: str, score_value: float | None = None
) -> dict[str, Any]:
    result: dict[str, Any] = {"rule": rule, "passed": passed, "message": message}
    if score_value is not None:
        result["score"] = score_value
    return result
