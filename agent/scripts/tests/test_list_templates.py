import json

from agent_invest_scripts.list_templates import list_templates


def test_list_templates_returns_registered_template_metadata() -> None:
    payload = list_templates()

    assert [template["id"] for template in payload] == [
        "synthetic_long_allocation",
        "periodic_rebalanced_allocation",
        "threshold_rebalanced_allocation",
        "core_satellite_allocation",
        "barbell_allocation",
        "volatility_targeted_exposure",
        "relative_momentum_rotation",
        "trend_following_long_neutral",
        "partial_hedge_overlay",
        "beta_hedged_alt_exposure",
        "relative_value_pair_trade",
        "trend_following_long_short",
        "drawdown_based_hedge",
        "explicit_pair_trade",
        "single_asset_trend_setup",
        "long_flat_momentum_rotation",
        "long_short_momentum_rotation",
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
