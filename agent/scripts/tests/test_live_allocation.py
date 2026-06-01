"""Phase 3 parity guarantee: the live allocator never rewrites history.

The live allocator runs the SAME recipe each period over data through "today"
and reads the latest rebalance (``holdings_history[-1]``). The keeper calls it
repeatedly as new bars arrive, so the property that makes it safe is CAUSALITY:
extending the data window forward must never change the weights of any rebalance
that already happened. If that holds, the sequence of live targets the keeper
executes is exactly the sequence the backtest produced -- no drift, no peeking.

We assert it directly: run a recipe over a short data window and again over a
longer one, then check every rebalance in the short run appears with identical
weights in the long run.

(Note: a run ending *exactly* at date D suppresses a rebalance on the terminal
bar -- bt only records a rebalance once it can hold the position into a
subsequent bar. So ``holdings_history[-1]`` of a to-today run is the most recent
*completed* scheduled rebalance, i.e. the strategy's current standing target on
closed bars. That is the correct thing to execute; the WHEN-to-act decision is
the separate trigger step, 3.2.)
"""

from __future__ import annotations

import warnings
from datetime import date

import numpy as np
import pandas as pd
import pytest

from agent_invest_scripts._lib.backtest.bt_templates import TEMPLATES, run_recipe
from agent_invest_scripts._lib.backtest.live_allocation import (
    select_live_rebalance,
    to_live_allocation,
)
from agent_invest_scripts._lib.backtest.result import to_dict

_COINS = ["bitcoin", "ethereum", "solana", "avalanche-2", "chainlink", "uniswap"]


def _wide_prices(*, days: int = 700, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    index = pd.date_range("2022-01-01", periods=days, freq="D")
    frame: dict[str, np.ndarray] = {}
    for offset, coin in enumerate(_COINS):
        rets = rng.normal(0.0005 + offset * 0.0002, 0.03, days)
        frame[coin] = 100 * np.exp(np.cumsum(rets))
    out = pd.DataFrame(frame, index=index)
    out["usd-coin"] = 1.0
    return out


def _universe(coins: list[str]) -> pd.DataFrame:
    return pd.DataFrame([{"coin_id": c, "rank": i + 1} for i, c in enumerate(coins)])


def _config(template_id: str) -> dict:
    config: dict = {"select_top": 4, "weighting": "equal"}
    if template_id in ("core_satellite_allocation", "barbell_allocation"):
        config["core_weight"] = 0.7
    if template_id == "barbell_allocation":
        config["sleeve_cap"] = 0.05
    return config


def _run(template_id: str, prices: pd.DataFrame, window: tuple[date, date]) -> dict:
    candidate = {
        "candidate_id": "c1",
        "thesis": {"objective": "balanced", "primary_factors": []},
    }
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = run_recipe(
            TEMPLATES[template_id],
            _universe(_COINS),
            prices,
            _config(template_id),
            window,
            candidate=candidate,
        )
    return to_dict(result)


# A static, a state-dependent, and two asset-rotating families -- the rotators
# are where a naive re-implementation would drift, so they matter most.
_PARITY_FAMILIES = [
    "synthetic_long_allocation",
    "periodic_rebalanced_allocation",
    "volatility_targeted_exposure",
    "relative_momentum_rotation",
    "trend_following_long_neutral",
]


def _hist_map(result: dict) -> dict[date, dict[str, float]]:
    out: dict[date, dict[str, float]] = {}
    for entry in result["allocation_metrics"]["holdings_history"]:
        d = entry["date"]
        d = d if isinstance(d, date) else date.fromisoformat(str(d)[:10])
        out[d] = {
            str(k): float(v)
            for k, v in entry["weights"].items()
            if abs(float(v)) > 1e-6
        }
    return out


@pytest.mark.parametrize("template_id", _PARITY_FAMILIES)
def test_forward_extension_never_rewrites_past_rebalances(template_id: str) -> None:
    prices = _wide_prices()
    start = prices.index[0].date()

    full = _hist_map(_run(template_id, prices, (start, prices.index[-1].date())))
    shorter_prices = prices.iloc[:-60]
    shorter = _hist_map(
        _run(template_id, shorter_prices, (start, shorter_prices.index[-1].date()))
    )

    assert shorter, f"{template_id} never rebalanced"
    for d, weights in shorter.items():
        assert d in full, f"{template_id}: rebalance {d} vanished when data extended"
        assert set(weights) == set(full[d]), (
            f"{template_id}: asset set changed at {d} when data extended"
        )
        for coin, w in weights.items():
            assert full[d][coin] == pytest.approx(w, abs=1e-6), (
                f"{template_id}: weight for {coin} at {d} changed when data extended"
            )


@pytest.mark.parametrize("template_id", _PARITY_FAMILIES)
def test_live_allocation_returns_latest_completed_rebalance(template_id: str) -> None:
    prices = _wide_prices()
    start = prices.index[0].date()
    full = _run(template_id, prices, (start, prices.index[-1].date()))
    history = full["allocation_metrics"]["holdings_history"]

    alloc = to_live_allocation(history)
    last = history[-1]
    last_date = last["date"]
    last_date = (
        last_date
        if isinstance(last_date, date)
        else date.fromisoformat(str(last_date)[:10])
    )
    assert alloc["rebalance_date"] == last_date.isoformat()
    expected = {
        str(k): float(v)
        for k, v in last["weights"].items()
        if abs(float(v)) > 1e-6
    }
    actual = {leg["coin_id"]: leg["weight"] for leg in alloc["weights"]}
    assert actual == pytest.approx(expected, abs=1e-9)


def test_select_live_rebalance_picks_latest_on_or_before_cutoff() -> None:
    history = [
        {"date": "2024-01-01", "weights": {"bitcoin": 1.0}},
        {"date": "2024-02-01", "weights": {"ethereum": 1.0}},
        {"date": "2024-03-01", "weights": {"solana": 1.0}},
    ]
    assert select_live_rebalance(history, as_of="2024-02-15")["date"] == "2024-02-01"
    assert select_live_rebalance(history, as_of="2024-03-01")["date"] == "2024-03-01"
    assert select_live_rebalance(history)["date"] == "2024-03-01"


def test_select_live_rebalance_raises_before_first_rebalance() -> None:
    history = [{"date": "2024-02-01", "weights": {"bitcoin": 1.0}}]
    with pytest.raises(ValueError):
        select_live_rebalance(history, as_of="2024-01-01")
    with pytest.raises(ValueError):
        select_live_rebalance([], as_of="2024-01-01")


def test_to_live_allocation_tags_sides_and_summaries() -> None:
    history = [
        {"date": "2024-03-01", "weights": {"bitcoin": 0.6, "ethereum": -0.2, "dust": 1e-9}},
    ]
    alloc = to_live_allocation(history)
    assert alloc["rebalance_date"] == "2024-03-01"
    assert [leg["coin_id"] for leg in alloc["weights"]] == ["bitcoin", "ethereum"]
    assert alloc["weights"][0]["side"] == "long"
    assert alloc["weights"][1]["side"] == "short"
    assert alloc["net_weight"] == pytest.approx(0.4)
    assert alloc["gross_weight"] == pytest.approx(0.8)
    assert alloc["cash_weight"] == pytest.approx(0.6)
