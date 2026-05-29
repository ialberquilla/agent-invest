import json

from agent_invest_scripts.list_templates import list_templates


def test_list_templates_returns_registered_template_metadata() -> None:
    payload = list_templates()

    assert [template["id"] for template in payload] == [
        "buy_and_hold",
        "periodic_rebalance",
        "momentum",
        "dual_momentum",
        "trend_following",
        "mean_reversion",
        "breakout",
    ]
    for template in payload:
        assert set(template) == {
            "id",
            "category",
            "preferred_factors",
            "default_universe",
            "min_history_days",
            "composite_formula",
            "slot_schema",
        }


def test_list_templates_slot_schema_is_round_trippable_json() -> None:
    for template in list_templates():
        assert (
            json.loads(json.dumps(template["slot_schema"])) == template["slot_schema"]
        )
