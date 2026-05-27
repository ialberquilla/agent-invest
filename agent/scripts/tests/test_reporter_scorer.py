from __future__ import annotations

from agent_invest_scripts._lib.eval_scorers import reporter


def test_reporter_numeric_grounding_catches_fake_number() -> None:
    expectations = {
        "batch": {
            "window": {"start": "2024-01-01", "end": "2024-06-30"},
            "results": [
                {
                    "candidate_id": "winner",
                    "metrics": {"total_return": 0.123, "sharpe": 1.4},
                }
            ],
        },
        "judge_criteria": "Winner explanation cites total_return and sharpe coherently.",
    }
    output = {
        "title": "Winner",
        "summary": "The winner returned 99.9% with Sharpe 1.4.",
        "reasoning": "The selected strategy has total_return and sharpe support.",
        "winner_candidate_id": "winner",
        "kpis": {"total_return": 0.123, "sharpe": 1.4},
        "assumptions": ["Costs are unchanged."],
        "risks": ["Drawdowns can recur."],
        "next_steps": ["Monitor live slippage."],
    }

    result = reporter.score(expectations, output)

    numeric_rule = next(rule for rule in result["rules"] if rule["rule"] == "numeric_grounding")
    assert numeric_rule["passed"] is False
    assert result["passed"] is False
