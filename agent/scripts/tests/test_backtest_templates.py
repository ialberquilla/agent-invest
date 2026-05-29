from datetime import date, timedelta

import pandas as pd
import pytest

from agent_invest_scripts._lib.backtest.templates import (
    TEMPLATES,
    AllocationPlan,
    BaseTemplate,
    SignalPlan,
    TemplateMetadata,
    validate_against_slot_schema,
)

SLOT_SCHEMA = {
    "lookback_days": {"type": "int", "required": True, "min": 1, "max": 365},
    "threshold": {"type": "float", "default": 0.1, "min": 0.0, "max": 1.0},
    "selector": {"type": "registry", "required": True},
    "weights": {"type": "object", "default": {}},
}


def test_template_types_are_importable() -> None:
    assert TemplateMetadata is not None
    assert AllocationPlan is not None
    assert SignalPlan is not None
    assert BaseTemplate is not None
    assert "buy_and_hold" in TEMPLATES
    assert "periodic_rebalance" in TEMPLATES
    assert "momentum" in TEMPLATES
    assert "trend_following" in TEMPLATES
    assert "mean_reversion" in TEMPLATES
    assert "breakout" in TEMPLATES
    assert "dual_momentum" in TEMPLATES


def test_buy_and_hold_valid_config_passes() -> None:
    TEMPLATES["buy_and_hold"].validate_config({"select_top": 5, "weighting": "equal"})


def test_buy_and_hold_out_of_range_select_top_raises() -> None:
    with pytest.raises(ValueError, match="select_top"):
        TEMPLATES["buy_and_hold"].validate_config(
            {"select_top": 51, "weighting": "equal"}
        )


def test_buy_and_hold_build_returns_single_equal_weight_rebalance() -> None:
    universe = pd.DataFrame({"coin_id": [f"coin-{i}" for i in range(10)]})
    start = date(2024, 1, 1)

    plan = TEMPLATES["buy_and_hold"].build(
        universe,
        prices={},
        config={"select_top": 5, "weighting": "equal"},
        window=(start, date(2024, 12, 31)),
    )

    assert plan["rebalance_dates"] == [start]
    assert list(plan["holdings"]) == [start]
    assert len(plan["holdings"][start]) == 5
    assert sum(plan["holdings"][start].values()) == pytest.approx(1.0)


def test_buy_and_hold_slot_schema_accepts_valid_config() -> None:
    validate_against_slot_schema(
        {"select_top": 5, "weighting": "equal"},
        TEMPLATES["buy_and_hold"].METADATA.slot_schema,
    )


def test_periodic_rebalance_30d_trigger_rebalances_roughly_monthly() -> None:
    start = date(2024, 1, 1)
    plan = TEMPLATES["periodic_rebalance"].build(
        pd.DataFrame({"coin_id": ["btc", "eth"]}),
        prices={},
        config={
            "select_top": 2,
            "weighting": "equal",
            "rebalance_trigger": "periodic_30d",
        },
        window=(start, date(2024, 4, 15)),
    )

    assert plan["rebalance_dates"] == [
        date(2024, 1, 1),
        date(2024, 1, 31),
        date(2024, 3, 1),
        date(2024, 3, 31),
    ]
    assert list(plan["holdings"]) == plan["rebalance_dates"]


def test_periodic_rebalance_90d_trigger_rebalances_quarterly() -> None:
    start = date(2024, 1, 1)
    plan = TEMPLATES["periodic_rebalance"].build(
        pd.DataFrame({"coin_id": ["btc", "eth"]}),
        prices={},
        config={
            "select_top": 2,
            "weighting": "equal",
            "rebalance_trigger": "periodic_90d",
        },
        window=(start, date(2024, 8, 1)),
    )

    assert plan["rebalance_dates"] == [
        date(2024, 1, 1),
        date(2024, 3, 31),
        date(2024, 6, 29),
    ]


def test_periodic_rebalance_none_trigger_matches_buy_and_hold_semantics() -> None:
    start = date(2024, 1, 1)
    plan = TEMPLATES["periodic_rebalance"].build(
        pd.DataFrame({"coin_id": ["btc", "eth"]}),
        prices={},
        config={"select_top": 2, "weighting": "equal", "rebalance_trigger": "none"},
        window=(start, date(2024, 4, 15)),
    )

    assert plan["rebalance_dates"] == [start]
    assert list(plan["holdings"]) == [start]


def test_periodic_rebalance_threshold_drift_ignores_flat_prices() -> None:
    start = date(2024, 1, 1)
    prices = _prices(["btc", "eth"], [100.0, 100.0, 100.0], start)

    plan = TEMPLATES["periodic_rebalance"].build(
        pd.DataFrame({"coin_id": ["btc", "eth"]}),
        prices=prices,
        config={
            "select_top": 2,
            "weighting": "equal",
            "rebalance_trigger": "threshold_drift_10pct",
        },
        window=(start, start + timedelta(days=2)),
    )

    assert plan["rebalance_dates"] == [start]


def test_periodic_rebalance_threshold_drift_rebalances_after_weight_drift() -> None:
    start = date(2024, 1, 1)
    prices = {
        "btc": pd.DataFrame(
            {
                "date": [start, start + timedelta(days=1), start + timedelta(days=2)],
                "close": [100.0, 200.0, 200.0],
            }
        ),
        "eth": pd.DataFrame(
            {
                "date": [start, start + timedelta(days=1), start + timedelta(days=2)],
                "close": [100.0, 100.0, 100.0],
            }
        ),
    }

    plan = TEMPLATES["periodic_rebalance"].build(
        pd.DataFrame({"coin_id": ["btc", "eth"]}),
        prices=prices,
        config={
            "select_top": 2,
            "weighting": "equal",
            "rebalance_trigger": "threshold_drift_10pct",
        },
        window=(start, start + timedelta(days=2)),
    )

    assert plan["rebalance_dates"] == [start, start + timedelta(days=1)]


def test_momentum_validates_position_size_pct_range() -> None:
    valid_config = {
        "signal_indicator": "roc",
        "signal_indicator_params": {"period": 90},
        "signal_threshold": 0.1,
        "exit_rule": "none",
        "position_size_pct": 0.2,
    }

    TEMPLATES["momentum"].validate_config(valid_config)
    with pytest.raises(ValueError, match="position_size_pct"):
        TEMPLATES["momentum"].validate_config(
            {**valid_config, "position_size_pct": 1.5}
        )


def test_momentum_build_emits_signals_when_roc_crosses_threshold() -> None:
    start = date(2024, 1, 1)
    dates = [start + timedelta(days=offset) for offset in range(95)]
    prices = {
        "btc": pd.DataFrame(
            {
                "date": dates,
                "close": [100.0] * 90 + [112.0, 115.0, 108.0, 105.0, 120.0],
            }
        )
    }

    plan = TEMPLATES["momentum"].build(
        pd.DataFrame({"coin_id": ["btc"]}),
        prices=prices,
        config={
            "signal_indicator": "roc",
            "signal_indicator_params": {"period": 90},
            "signal_threshold": 0.1,
            "exit_rule": "none",
            "position_size_pct": 0.2,
        },
        window=(start, dates[-1]),
    )

    signals = plan["signals"]["btc"]
    assert signals.abs().sum() > 0
    assert signals.iloc[90] == 1
    assert signals.iloc[92] == -1
    assert plan["sizing"] == {"btc": 0.2}
    assert plan["exit_rule"] == "none"


def test_trend_following_validates_position_size_pct_range() -> None:
    valid_config = {
        "signal_indicator": "sma_cross",
        "signal_indicator_params": {"fast_period": 2, "slow_period": 3},
        "exit_rule": "none",
        "position_size_pct": 0.2,
    }

    TEMPLATES["trend_following"].validate_config(valid_config)
    with pytest.raises(ValueError, match="position_size_pct"):
        TEMPLATES["trend_following"].validate_config(
            {**valid_config, "position_size_pct": 1.5}
        )


def test_trend_following_build_enters_uptrend_and_exits_downtrend() -> None:
    start = date(2024, 1, 1)
    dates = [start + timedelta(days=offset) for offset in range(11)]
    prices = {
        "btc": pd.DataFrame(
            {
                "date": dates,
                "close": [
                    10.0,
                    10.0,
                    10.0,
                    12.0,
                    14.0,
                    16.0,
                    18.0,
                    12.0,
                    8.0,
                    6.0,
                    5.0,
                ],
            }
        )
    }

    plan = TEMPLATES["trend_following"].build(
        pd.DataFrame({"coin_id": ["btc"]}),
        prices=prices,
        config={
            "signal_indicator": "sma_cross",
            "signal_indicator_params": {"fast_period": 2, "slow_period": 3},
            "exit_rule": "none",
            "position_size_pct": 0.2,
        },
        window=(start, dates[-1]),
    )

    signals = plan["signals"]["btc"]
    assert signals[signals == 1].count() == 1
    assert signals[signals == -1].count() == 1
    assert signals.iloc[3] == 1
    assert signals.iloc[7] == -1
    assert plan["sizing"] == {"btc": 0.2}
    assert plan["exit_rule"] == "none"


def test_mean_reversion_rejects_entry_threshold_above_exit_threshold() -> None:
    valid_config = {
        "signal_indicator": "z_score",
        "signal_indicator_params": {"period": 3},
        "entry_threshold": -1.0,
        "exit_threshold": 0.0,
        "exit_rule": "time_stop",
        "position_size_pct": 0.2,
    }

    TEMPLATES["mean_reversion"].validate_config(valid_config)
    with pytest.raises(ValueError, match="entry_threshold"):
        TEMPLATES["mean_reversion"].validate_config(
            {**valid_config, "entry_threshold": 0.0, "exit_threshold": 0.0}
        )


def test_mean_reversion_build_enters_oversold_and_exits_on_revert() -> None:
    start = date(2024, 1, 1)
    dates = [start + timedelta(days=offset) for offset in range(13)]
    prices = {
        "btc": pd.DataFrame(
            {
                "date": dates,
                "close": [
                    100.0,
                    100.0,
                    100.0,
                    90.0,
                    90.0,
                    100.0,
                    100.0,
                    100.0,
                    90.0,
                    90.0,
                    100.0,
                    100.0,
                    100.0,
                ],
            }
        )
    }

    plan = TEMPLATES["mean_reversion"].build(
        pd.DataFrame({"coin_id": ["btc"]}),
        prices=prices,
        config={
            "signal_indicator": "z_score",
            "signal_indicator_params": {"period": 3},
            "entry_threshold": -1.0,
            "exit_threshold": 0.5,
            "exit_rule": "time_stop",
            "position_size_pct": 0.2,
        },
        window=(start, dates[-1]),
    )

    signals = plan["signals"]["btc"]
    assert signals[signals == 1].count() == 2
    assert signals[signals == -1].count() == 2
    assert signals.iloc[3] == 1
    assert signals.iloc[5] == -1
    assert signals.iloc[8] == 1
    assert signals.iloc[10] == -1
    assert plan["sizing"] == {"btc": 0.2}
    assert plan["exit_rule"] == "time_stop"


def test_breakout_rejects_non_positive_window() -> None:
    with pytest.raises(ValueError, match="breakout_window_days"):
        TEMPLATES["breakout"].validate_config(
            {
                "breakout_window_days": 0,
                "exit_rule": "trailing_stop",
                "position_size_pct": 0.2,
            }
        )


def test_breakout_build_enters_on_clean_60_day_high_breakout() -> None:
    start = date(2024, 1, 1)
    dates = [start + timedelta(days=offset) for offset in range(65)]
    closes = [100.0] * 60 + [101.0, 101.0, 99.0, 102.0, 102.0]
    highs = [100.0] * 60 + [101.0, 101.0, 101.0, 102.0, 102.0]

    plan = TEMPLATES["breakout"].build(
        pd.DataFrame({"coin_id": ["btc"]}),
        prices={"btc": pd.DataFrame({"date": dates, "close": closes, "high": highs})},
        config={
            "breakout_window_days": 60,
            "exit_rule": "trailing_stop",
            "position_size_pct": 0.2,
        },
        window=(start, dates[-1]),
    )

    signals = plan["signals"]["btc"]
    assert signals.iloc[60] == 1
    assert signals[signals == 1].count() == 2
    assert signals.iloc[61] == -1
    assert signals.iloc[63] == 1
    assert plan["sizing"] == {"btc": 0.2}
    assert plan["exit_rule"] == "trailing_stop"


def test_dual_momentum_stays_with_leader_then_switches_on_overtake() -> None:
    start = date(2024, 1, 1)
    dates = [start + timedelta(days=offset) for offset in range(211)]
    prices = {
        "btc": pd.DataFrame(
            {
                "date": dates,
                "close": [100.0 + min(offset, 170) for offset in range(len(dates))],
            }
        ),
        "eth": pd.DataFrame(
            {
                "date": dates,
                "close": [100.0] * 180 + [300.0] * 31,
            }
        ),
        "usd-coin": pd.DataFrame({"date": dates, "close": [1.0] * len(dates)}),
    }

    plan = TEMPLATES["dual_momentum"].build(
        pd.DataFrame({"coin_id": ["btc", "eth"]}),
        prices=prices,
        config={"lookback_days": 30, "rebalance_frequency_days": 30},
        window=(start, dates[-1]),
    )

    assert plan["signals"]["btc"].iloc[30] == 1
    assert plan["signals"]["btc"].iloc[60] == 0
    assert plan["signals"]["btc"].iloc[180] == -1
    assert plan["signals"]["eth"].iloc[180] == 1
    assert plan["signals"]["usd-coin"].iloc[30] == -1
    assert plan["sizing"] == {"btc": 1.0, "eth": 1.0, "usd-coin": 1.0}
    assert plan["exit_rule"] == "rebalance"


def test_dual_momentum_uses_reserve_when_leader_below_absolute_floor() -> None:
    start = date(2024, 1, 1)
    dates = [start + timedelta(days=offset) for offset in range(61)]
    prices = {
        "btc": pd.DataFrame({"date": dates, "close": [100.0] * len(dates)}),
        "eth": pd.DataFrame({"date": dates, "close": [100.0] * len(dates)}),
        "usdc": pd.DataFrame({"date": dates, "close": [1.0] * len(dates)}),
    }

    plan = TEMPLATES["dual_momentum"].build(
        pd.DataFrame({"coin_id": ["btc", "eth"]}),
        prices=prices,
        config={
            "lookback_days": 30,
            "absolute_floor": 0.01,
            "reserve_asset": "usdc",
            "rebalance_frequency_days": 30,
            "position_size_pct": 0.5,
        },
        window=(start, dates[-1]),
    )

    assert plan["signals"]["usdc"].iloc[0] == 1
    assert plan["signals"]["usdc"].sum() == 1
    assert plan["signals"]["btc"].sum() == 0
    assert plan["signals"]["eth"].sum() == 0
    assert plan["sizing"] == {"btc": 0.5, "eth": 0.5, "usdc": 0.5}


def test_dual_momentum_default_universe_is_fixed_and_config_is_overrideable() -> None:
    metadata = TEMPLATES["dual_momentum"].METADATA

    assert metadata.default_universe == {
        "selector": "fixed",
        "coin_ids": ["bitcoin", "ethereum", "solana", "binancecoin", "usd-coin"],
    }
    TEMPLATES["dual_momentum"].validate_config({"reserve_asset": "custom-usdc"})


def _prices(
    coin_ids: list[str], values: list[float], start: date
) -> dict[str, pd.DataFrame]:
    return {
        coin_id: pd.DataFrame(
            {
                "date": [
                    start + timedelta(days=offset) for offset in range(len(values))
                ],
                "close": values,
            }
        )
        for coin_id in coin_ids
    }


def test_valid_config_passes_slot_schema_validation() -> None:
    validate_against_slot_schema(
        {
            "lookback_days": 30,
            "threshold": 0.25,
            "selector": "top_market_cap",
            "weights": {"btc": 0.5, "eth": 0.5},
        },
        SLOT_SCHEMA,
    )


def test_missing_required_key_raises() -> None:
    with pytest.raises(ValueError, match="selector"):
        validate_against_slot_schema({"lookback_days": 30}, SLOT_SCHEMA)


def test_out_of_range_numeric_raises() -> None:
    with pytest.raises(ValueError, match="lookback_days"):
        validate_against_slot_schema(
            {"lookback_days": 500, "selector": "top_market_cap"},
            SLOT_SCHEMA,
        )


def test_unknown_key_raises() -> None:
    with pytest.raises(ValueError, match="unknown"):
        validate_against_slot_schema(
            {"lookback_days": 30, "selector": "top_market_cap", "unknown": True},
            SLOT_SCHEMA,
        )
