from __future__ import annotations

import unittest
from datetime import UTC, datetime
from unittest.mock import patch

from agent_invest_scripts.list_runs import (
    RunRow,
    build_run_output_key,
    build_run_summary,
)


class BuildRunOutputKeyTest(unittest.TestCase):
    def test_uses_strategy_artifacts_layout(self) -> None:
        with patch.dict("os.environ", {"S3_PREFIX": "agent-invest/dev"}, clear=True):
            key = build_run_output_key("user-1", "strategy-1", "run-1")

        self.assertEqual(
            key,
            "agent-invest/dev/users/user-1/strategies/strategy-1/artifacts/run-1/output.json",
        )


class BuildRunSummaryTest(unittest.TestCase):
    def test_includes_metrics_from_output_payload(self) -> None:
        row = RunRow(
            run_id="run-1",
            status="completed",
            started_at=datetime(2026, 4, 25, 10, 0, tzinfo=UTC),
            ended_at=datetime(2026, 4, 25, 10, 5, tzinfo=UTC),
            exit_code=0,
            user_id="user-1",
        )

        summary = build_run_summary(
            row,
            output_payload={
                "summary": {
                    "sharpe_ratio": 1.236,
                    "cagr": 0.154,
                    "max_drawdown": -0.082,
                    "final_equity_usd": 1275.55,
                }
            },
        )

        self.assertEqual(
            summary,
            (
                "completed, in 5m 0s; Sharpe 1.24, CAGR 15.4%, "
                "MaxDD -8.2%, Final $1,275.55"
            ),
        )

    def test_falls_back_to_status_duration_and_exit_code(self) -> None:
        row = RunRow(
            run_id="run-2",
            status="failed",
            started_at=datetime(2026, 4, 25, 10, 0, tzinfo=UTC),
            ended_at=datetime(2026, 4, 25, 10, 1, 30, tzinfo=UTC),
            exit_code=2,
            user_id="user-1",
        )

        summary = build_run_summary(row, output_payload=None)

        self.assertEqual(summary, "failed, in 1m 30s, exit 2")


if __name__ == "__main__":
    unittest.main()
