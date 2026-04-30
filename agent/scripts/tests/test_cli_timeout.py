from __future__ import annotations

import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_sleep_fixture_times_out_via_signal_alarm() -> None:
    result = subprocess.run(
        [
            "uv",
            "run",
            "--project",
            str(PROJECT_ROOT),
            "python",
            "-m",
            "agent_invest_scripts.test_fixtures.sleep",
            "--seconds",
            "5",
            "--timeout-seconds",
            "1",
        ],
        capture_output=True,
        cwd=PROJECT_ROOT,
        text=True,
        timeout=20,
        check=False,
    )

    assert result.returncode != 0
    assert result.stdout == ""
    assert "Script timed out after 1s" in result.stderr
