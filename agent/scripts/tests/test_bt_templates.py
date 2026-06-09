"""Tests for the bt-backed strategy templates.

Each recipe must build a runnable bt.Strategy, run on synthetic + live data,
reject malformed configs, and -- critically -- return the SAME BacktestResult
contract shape regardless of strategy logic (the uniform-output invariant the
agent depends on). Replaces the old per-template plan assertions; the legacy
hand-rolled templates are gone.
"""

from __future__ import annotations

import json
import os
import warnings
from datetime import date

import bt
import numpy as np
import pandas as pd
import pytest

from agent_invest_scripts._lib.backtest.bt_templates import TEMPLATES, run_recipe
from agent_invest_scripts._lib.backtest.result import to_dict

# Eight long-only families ship in Phase 1.
LONG_ONLY_FAMILIES = [
    "synthetic_long_allocation",
    "periodic_rebalanced_allocation",
    "threshold_rebalanced_allocation",
    "core_satellite_allocation",
    "barbell_allocation",
    "volatility_targeted_exposure",
    "relative_momentum_rotation",
    "trend_following_long_neutral",
]

# Five short/hedge families add negative weights in Phase 2.
SHORT_FAMILIES = [
    "partial_hedge_overlay",
    "beta_hedged_alt_exposure",
    "relative_value_pair_trade",
    "trend_following_long_short",
    "drawdown_based_hedge",
]

# Single-asset family (single_asset strategy_mode): one market, long/flat.
# Kept separate from LONG_ONLY_FAMILIES because it carries no select_top /
# weighting slots.
SINGLE_ASSET_FAMILIES = [
    "single_asset_trend_setup",
]

ALL_FAMILIES = LONG_ONLY_FAMILIES + SHORT_FAMILIES + SINGLE_ASSET_FAMILIES

_CONTRACT_KEYS = {
    "candidate_id",
    "template_id",
    "config",
    "window",
    "equity_curve",
    "benchmark_curve",
    "drawdown_episodes",
    "metrics",
    "robustness",
    "composite_score",
    "allocation_metrics",
    "tactical_metrics",
}


def _wide_prices(
    *, days: int = 800, seed: int = 0, drift: float = 0.0006
) -> pd.DataFrame:
    """Synthetic daily-close wide frame including the benchmark coins (the
    balanced benchmark reads bitcoin/ethereum/usd-coin)."""
    rng = np.random.default_rng(seed)
    index = pd.date_range("2022-01-01", periods=days, freq="D")
    coins = ["bitcoin", "ethereum", "solana", "avalanche-2", "chainlink", "uniswap"]
    frame: dict[str, np.ndarray] = {}
    for offset, coin in enumerate(coins):
        rets = rng.normal(drift + offset * 0.0001, 0.03, days)
        frame[coin] = 100 * np.exp(np.cumsum(rets))
    out = pd.DataFrame(frame, index=index)
    out["usd-coin"] = 1.0
    return out


def _universe(coins: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        [{"coin_id": coin, "rank": i + 1} for i, coin in enumerate(coins)]
    )


def _config(template_id: str, *, weighting: str = "equal") -> dict:
    # single_asset has no select_top / weighting slots: the book is one
    # position. weighting is accepted and ignored so the shared parametrized
    # tests can call _config uniformly.
    if template_id == "single_asset_trend_setup":
        return {"sma_lookback": 50}
    config: dict = {"select_top": 6, "weighting": weighting}
    if template_id in ("core_satellite_allocation", "barbell_allocation"):
        config["core_weight"] = 0.7
    if template_id == "barbell_allocation":
        config["sleeve_cap"] = 0.05
    return config


def _run(template_id: str, prices: pd.DataFrame, **kwargs) -> dict:
    coins = ["bitcoin", "ethereum", "solana", "avalanche-2", "chainlink", "uniswap"]
    window = (prices.index[0].date(), prices.index[-1].date())
    candidate = {
        "candidate_id": "c1",
        "thesis": {"objective": "balanced", "primary_factors": []},
    }
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = run_recipe(
            TEMPLATES[template_id],
            _universe(coins),
            prices,
            _config(template_id, **kwargs),
            window,
            candidate=candidate,
        )
    return to_dict(result)


def test_registry_holds_all_families() -> None:
    assert set(TEMPLATES) == set(ALL_FAMILIES)


@pytest.mark.parametrize("template_id", ALL_FAMILIES)
def test_recipe_builds_a_runnable_strategy(template_id: str) -> None:
    prices = _wide_prices()
    coins = ["bitcoin", "ethereum", "solana", "avalanche-2", "chainlink", "uniswap"]
    window = (prices.index[0].date(), prices.index[-1].date())
    strategy = TEMPLATES[template_id].build(
        _universe(coins), prices, _config(template_id), window
    )
    assert isinstance(strategy, bt.Strategy)


@pytest.mark.parametrize("template_id", ALL_FAMILIES)
@pytest.mark.parametrize(
    "weighting", ["equal", "vol_inverse", "ranking_proportional", "cap"]
)
def test_recipe_runs_and_returns_contract(template_id: str, weighting: str) -> None:
    payload = _run(template_id, _wide_prices(), weighting=weighting)
    assert set(payload) == _CONTRACT_KEYS
    # JSON serializable end-to-end (the CLI prints this to stdout).
    json.dumps(payload)
    metrics = payload["metrics"]
    assert np.isfinite(metrics["max_drawdown"])
    assert metrics["max_drawdown"] <= 0.0
    assert payload["allocation_metrics"]["max_single_weight"] >= 0.0


def test_output_schema_identical_across_families() -> None:
    """The whole point: structurally different families emit identical contract
    keys, so the agent sees one shape regardless of strategy."""
    prices = _wide_prices()
    shapes = {tuple(sorted(_run(fam, prices))) for fam in ALL_FAMILIES}
    assert len(shapes) == 1


@pytest.mark.parametrize("template_id", SHORT_FAMILIES)
def test_short_family_opens_a_negative_leg(template_id: str) -> None:
    """Phase 2: every short/hedge family must actually short -- a negative leg
    in the holdings -- and the gross max_single_weight counts its absolute
    size (so a hedge tells against max_weight_per_asset)."""
    payload = _run(template_id, _wide_prices())
    weights = [
        weight
        for rebalance in payload["allocation_metrics"]["rebalances"]
        for weight in rebalance["weights"].values()
    ]
    assert min(weights) < 0.0, "expected at least one short leg"
    # max_single_weight is gross (abs) over the daily drifted weights, so it is
    # at least the gross of any rebalance-date target weight.
    gross = max(abs(weight) for weight in weights)
    assert payload["allocation_metrics"]["max_single_weight"] >= gross - 1e-9


def test_uptrending_market_yields_positive_return() -> None:
    payload = _run("synthetic_long_allocation", _wide_prices(drift=0.004))
    assert payload["metrics"]["total_return"] > 0.0
    assert payload["metrics"]["cagr"] > 0.0


def test_malformed_config_rejected() -> None:
    recipe = TEMPLATES["synthetic_long_allocation"]
    with pytest.raises(ValueError, match="Unknown config key"):
        recipe.validate_config({"select_top": 5, "weighting": "equal", "bogus": 1})
    with pytest.raises(ValueError, match="Missing required config key"):
        recipe.validate_config({"select_top": 5})


def test_rebalance_trigger_only_where_supported() -> None:
    # trend_following has no rebalance_trigger slot.
    with pytest.raises(ValueError, match="Unknown config key"):
        TEMPLATES["trend_following_long_neutral"].validate_config(
            {"select_top": 5, "weighting": "equal", "rebalance_trigger": "periodic_30d"}
        )
    # periodic_rebalanced does.
    TEMPLATES["periodic_rebalanced_allocation"].validate_config(
        {"select_top": 5, "weighting": "equal", "rebalance_trigger": "periodic_30d"}
    )


def test_single_asset_honors_target_coin_id() -> None:
    """single_asset_trend_setup trades only the target coin -- never another
    name from the resolved universe."""
    prices = _wide_prices()
    coins = ["bitcoin", "ethereum", "solana", "avalanche-2", "chainlink", "uniswap"]
    window = (prices.index[0].date(), prices.index[-1].date())
    candidate = {
        "candidate_id": "c1",
        "thesis": {"objective": "balanced", "primary_factors": []},
    }
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        payload = to_dict(
            run_recipe(
                TEMPLATES["single_asset_trend_setup"],
                _universe(coins),
                prices,
                {"target_coin_id": "solana", "sma_lookback": 50},
                window,
                candidate=candidate,
            )
        )
    held = {
        coin
        for rebalance in payload["allocation_metrics"]["rebalances"]
        for coin, weight in rebalance["weights"].items()
        if abs(weight) > 1e-9
    }
    assert held <= {"solana"}, f"held a non-target coin: {held}"
    assert "solana" in held  # uptrending synthetic data -> held at least once


def test_single_asset_rejects_basket_config_and_unknown_target() -> None:
    recipe = TEMPLATES["single_asset_trend_setup"]
    # No select_top / weighting slots on this family.
    with pytest.raises(ValueError, match="Unknown config key"):
        recipe.validate_config({"select_top": 5, "weighting": "equal"})
    # A target not present in the resolved universe is an error, not a
    # silent fallback to the top coin.
    with pytest.raises(ValueError, match="not in the resolved universe"):
        recipe.build(
            _universe(["bitcoin", "ethereum"]),
            _wide_prices(),
            {"target_coin_id": "dogecoin"},
            (date(2022, 1, 1), date(2023, 1, 1)),
        )


def test_single_asset_long_flat_sits_out_a_downtrend() -> None:
    """In a sustained downtrend the long/flat rule should keep the book mostly
    in cash, so it loses far less than a buy-and-hold single long."""
    prices = _wide_prices(drift=-0.004)
    trend = _run("single_asset_trend_setup", prices)
    hold = _run("synthetic_long_allocation", prices, weighting="equal")
    assert trend["metrics"]["max_drawdown"] >= hold["metrics"]["max_drawdown"]


@pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"),
    reason="needs Postgres (DATABASE_URL); live integration check",
)
@pytest.mark.parametrize("template_id", ALL_FAMILIES)
def test_live_run_per_family(template_id: str) -> None:
    from agent_invest_scripts._lib import data

    wide = data.daily_prices().pivot(index="date", columns="coin_id", values="price")
    wide.index = pd.to_datetime(wide.index)
    coins = ["bitcoin", "ethereum", "solana", "avalanche-2", "chainlink"]
    window = (date(2024, 1, 1), date(2025, 6, 1))
    candidate = {
        "candidate_id": "c1",
        "thesis": {"objective": "balanced", "primary_factors": []},
    }
    config = _config(template_id)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        payload = to_dict(
            run_recipe(
                TEMPLATES[template_id],
                _universe(coins),
                wide,
                config,
                window,
                candidate=candidate,
            )
        )
    assert set(payload) == _CONTRACT_KEYS
    assert np.isfinite(payload["metrics"]["max_drawdown"])
