from __future__ import annotations

from decimal import Decimal

import pandas as pd
import pytest

from agent_invest_scripts import run_research_code


def test_select_only_rejects_writes() -> None:
    with pytest.raises(ValueError, match="SELECT"):
        run_research_code._select_only("delete from asset_prices")


def test_code_validator_rejects_unapproved_imports() -> None:
    with pytest.raises(ValueError, match="import not allowed"):
        run_research_code._validate_code("import os\nresult = {}")


def test_research_code_returns_result_and_notes_without_query() -> None:
    output = run_research_code.run_research_code(
        "result = {'sample_size': 3, 'mean': 2}",
        "unit test",
        timeout_seconds=5,
    )

    assert output["code"].startswith("result =")
    assert output["result"] == {"sample_size": 3, "mean": 2}
    assert any("not financial advice" in note for note in output["assumptions"])


def test_research_output_converts_decimal_and_pandas_values() -> None:
    output = run_research_code.run_research_code(
        "import pandas as pd\n"
        "result = {'rows': [{'date': pd.Timestamp('2024-01-01')}], 'missing': pd.NA}",
        "json safety test",
        timeout_seconds=5,
    )

    assert output["result"] == {
        "rows": [{"date": "2024-01-01T00:00:00"}],
        "missing": None,
    }


def test_json_safe_converts_dataframe_records() -> None:
    frame = pd.DataFrame([{"price": Decimal("1.25")}])

    assert run_research_code._json_safe(frame.to_dict(orient="records")) == [
        {"price": 1.25}
    ]
