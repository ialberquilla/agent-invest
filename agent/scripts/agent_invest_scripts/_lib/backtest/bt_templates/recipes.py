"""One ``bt`` recipe per long-only strategy family.

Each builder takes the resolved universe (ranked, already limited to
``select_top``), the wide price frame, the candidate config, and the window, and
returns a runnable ``bt.Strategy``. The runner slices prices to the window and
runs the strategy; nothing here touches the DB.

Phase 1 ships the eight long-only families. Phase 2 adds the five short/hedge
families (negative weights), which `select_templates` only shortlists when the
thesis opts into shorts (the SHORTS gate).
"""

from __future__ import annotations

import bt
import pandas as pd

from .base import Recipe, TemplateMetadata

# BTC/ETH are the structural "core" for core-satellite / barbell. If a basket
# has neither (rare), the top-ranked coin stands in as the core.
CORE_COINS = ("bitcoin", "ethereum")

_DEFAULT_UNIVERSE = {
    "selector": "top_n_by_mcap",
    "n": 50,
    "market_cap_floor_usd": 100_000_000,
}

# Slot fragments composed into per-family schemas.
_SELECT_TOP = {"type": "int", "required": True, "min": 1, "max": 50}
_WEIGHTING = {"type": "registry", "registry": "weighting_schemes", "required": True}
_REBALANCE = {"type": "registry", "registry": "rebalance_triggers"}
_CORE_WEIGHT = {"type": "float", "min": 0.0, "max": 1.0}
_SLEEVE_CAP = {"type": "float", "min": 0.0, "max": 1.0}
_HEDGE_WEIGHT = {"type": "float", "min": 0.0, "max": 2.0}
_DRAWDOWN_THRESHOLD = {"type": "float", "min": 0.0, "max": 1.0}
_SMA_LOOKBACK = {"type": "int", "min": 2, "max": 400}
_TARGET_COIN_ID = {"type": "string"}
_LONG_COIN_ID = {"type": "string", "required": True}
_SHORT_COIN_ID = {"type": "string", "required": True}
_HEDGE_RATIO = {"type": "float", "min": 0.0, "max": 2.0}


# --- shared algo helpers -----------------------------------------------------


def _coins(universe: pd.DataFrame, config: dict) -> list[str]:
    if "coin_id" not in universe.columns:
        raise ValueError("universe frame must include a coin_id column")
    selected = universe["coin_id"].head(int(config["select_top"]))
    coins = [str(c) for c in selected.tolist()]
    if not coins:
        raise ValueError("universe resolved to zero coins")
    return coins


def _run_algo(trigger: str | None) -> bt.algos.Algo:
    match trigger:
        case None | "none":
            return bt.algos.RunOnce()
        case "periodic_30d":
            return bt.algos.RunMonthly()
        case "periodic_90d":
            return bt.algos.RunQuarterly()
        case "threshold_drift_10pct":
            return bt.algos.RunIfOutOfBounds(0.10)
        case other:
            raise ValueError(f"unsupported rebalance_trigger: {other!r}")


def _weigh(
    scheme: str,
    coins: list[str],
    universe: pd.DataFrame,
    prices: pd.DataFrame,
    window: tuple,
) -> bt.algos.Algo:
    # `cap` falls back to equal: the ranked universe doesn't carry market_cap as
    # a column (it lives nested in factor_values), the same fallback the legacy
    # engine used.
    match scheme:
        case "equal" | "cap":
            return bt.algos.WeighEqually()
        case "vol_inverse":
            # bt's WeighInvVol estimates volatility from a trailing lookback,
            # which doesn't exist at the first rebalance once prices are sliced
            # to the window -> it returns empty weights -> the strategy holds
            # 100% cash and the equity curve is flat. Instead pin static
            # inverse-volatility weights computed from the window's returns,
            # the same static-weight approach `ranking_proportional` uses.
            return bt.algos.WeighSpecified(
                **_inverse_vol_weights(coins, prices, window)
            )
        case "ranking_proportional":
            return bt.algos.WeighSpecified(**_ranking_weights(coins, universe))
        case other:
            raise ValueError(f"unsupported weighting: {other!r}")


def _inverse_vol_weights(
    coins: list[str], prices: pd.DataFrame, window: tuple
) -> dict[str, float]:
    """Static inverse-volatility weights over the backtest window.

    Each coin's weight is proportional to 1 / (stddev of its daily returns)
    within the window, normalized to sum to 1. Coins with zero/undefined
    volatility (constant or too-short series) drop to a 0 weight. Falls back
    to equal weights when nothing has a usable volatility."""
    frame = _float_prices(prices, coins)
    frame.index = pd.to_datetime(frame.index)
    mask = (frame.index.date >= window[0]) & (frame.index.date <= window[1])
    returns = frame[mask].pct_change().dropna()
    vol = returns.std()
    inv = (1.0 / vol).replace([float("inf"), float("-inf")], pd.NA).dropna()
    inv = inv[inv > 0]
    if inv.empty:
        return {coin: 1.0 / len(coins) for coin in coins}
    total = float(inv.sum())
    return {coin: float(inv.get(coin, 0.0)) / total for coin in coins}


def _ranking_weights(coins: list[str], universe: pd.DataFrame) -> dict[str, float]:
    if "rank" not in universe.columns:
        return {coin: 1.0 / len(coins) for coin in coins}
    by_coin = universe.set_index("coin_id")
    ranks = {
        coin: float(by_coin.loc[coin, "rank"])
        if coin in by_coin.index and pd.notna(by_coin.loc[coin, "rank"])
        else 0.0
        for coin in coins
    }
    max_rank = max(ranks.values(), default=0.0)
    raw = {coin: max(max_rank - rank + 1.0, 0.0) for coin, rank in ranks.items()}
    total = sum(raw.values())
    if total <= 0:
        return {coin: 1.0 / len(coins) for coin in coins}
    return {coin: value / total for coin, value in raw.items()}


def _float_prices(prices: pd.DataFrame, coins: list[str]) -> pd.DataFrame:
    return prices[coins].apply(pd.to_numeric, errors="coerce").astype(float).ffill()


# --- builders ----------------------------------------------------------------


def _synthetic_long(universe, prices, config, window) -> bt.Strategy:
    coins = _coins(universe, config)
    return bt.Strategy(
        "synthetic_long_allocation",
        [
            bt.algos.RunOnce(),
            bt.algos.SelectThese(coins),
            _weigh(config["weighting"], coins, universe, prices, window),
            bt.algos.Rebalance(),
        ],
    )


def _periodic_rebalanced(universe, prices, config, window) -> bt.Strategy:
    coins = _coins(universe, config)
    return bt.Strategy(
        "periodic_rebalanced_allocation",
        [
            _run_algo(config.get("rebalance_trigger", "periodic_30d")),
            bt.algos.SelectThese(coins),
            _weigh(config["weighting"], coins, universe, prices, window),
            bt.algos.Rebalance(),
        ],
    )


def _threshold_rebalanced(universe, prices, config, window) -> bt.Strategy:
    coins = _coins(universe, config)
    return bt.Strategy(
        "threshold_rebalanced_allocation",
        [
            bt.algos.RunIfOutOfBounds(0.10),
            bt.algos.SelectThese(coins),
            _weigh(config["weighting"], coins, universe, prices, window),
            bt.algos.Rebalance(),
        ],
    )


def _core_satellite(universe, prices, config, window) -> bt.Strategy:
    coins = _coins(universe, config)
    weights = _core_satellite_weights(coins, float(config.get("core_weight", 0.7)))
    return bt.Strategy(
        "core_satellite_allocation",
        [
            _run_algo(config.get("rebalance_trigger", "periodic_90d")),
            bt.algos.SelectThese(list(weights)),
            bt.algos.WeighSpecified(**weights),
            bt.algos.Rebalance(),
        ],
    )


def _core_satellite_weights(coins: list[str], core_weight: float) -> dict[str, float]:
    core = [coin for coin in coins if coin in CORE_COINS] or coins[:1]
    sats = [coin for coin in coins if coin not in core]
    if not sats:
        return {coin: 1.0 / len(core) for coin in core}
    weights = {coin: core_weight / len(core) for coin in core}
    sleeve = (1.0 - core_weight) / len(sats)
    for coin in sats:
        weights[coin] = sleeve
    return weights


def _barbell(universe, prices, config, window) -> bt.Strategy:
    coins = _coins(universe, config)
    weights = _barbell_weights(
        coins,
        core_weight=float(config.get("core_weight", 0.85)),
        sleeve_cap=float(config.get("sleeve_cap", 0.05)),
    )
    return bt.Strategy(
        "barbell_allocation",
        [
            _run_algo(config.get("rebalance_trigger", "periodic_90d")),
            bt.algos.SelectThese(list(weights)),
            bt.algos.WeighSpecified(**weights),
            bt.algos.Rebalance(),
        ],
    )


def _barbell_weights(
    coins: list[str], *, core_weight: float, sleeve_cap: float
) -> dict[str, float]:
    core = [coin for coin in coins if coin in CORE_COINS] or coins[:1]
    sats = [coin for coin in coins if coin not in core]
    if not sats:
        return {coin: 1.0 / len(core) for coin in core}
    sleeve_total = 1.0 - core_weight
    per_sat = min(sleeve_cap, sleeve_total / len(sats))
    # Any speculative weight clipped by the cap returns to the safe core, so the
    # book stays fully invested and the weights sum to 1.0 by construction.
    core_total = core_weight + (sleeve_total - per_sat * len(sats))
    weights = {coin: core_total / len(core) for coin in core}
    for coin in sats:
        weights[coin] = per_sat
    return weights


def _volatility_targeted(universe, prices, config, window) -> bt.Strategy:
    coins = _coins(universe, config)
    return bt.Strategy(
        "volatility_targeted_exposure",
        [
            _run_algo(config.get("rebalance_trigger", "periodic_30d")),
            bt.algos.SelectThese(coins),
            bt.algos.WeighInvVol(),
            bt.algos.Rebalance(),
        ],
    )


def _relative_momentum_rotation(universe, prices, config, window) -> bt.Strategy:
    coins = _coins(universe, config)
    # Rotate into the strongest half of the candidate pool each rebalance.
    rotate_n = max(1, round(len(coins) / 2))
    return bt.Strategy(
        "relative_momentum_rotation",
        [
            _run_algo(config.get("rebalance_trigger", "periodic_30d")),
            bt.algos.SelectThese(coins),
            bt.algos.SelectMomentum(n=rotate_n, lookback=pd.DateOffset(months=3)),
            bt.algos.WeighEqually(),
            bt.algos.Rebalance(),
        ],
    )


def _trend_following_long_neutral(universe, prices, config, window) -> bt.Strategy:
    coins = _coins(universe, config)
    frame = _float_prices(prices, coins)
    sma = frame.rolling(50, min_periods=50).mean()
    signal = frame > sma
    return bt.Strategy(
        "trend_following_long_neutral",
        [
            bt.algos.RunWeekly(),
            bt.algos.SelectWhere(signal),
            bt.algos.WeighEqually(),
            bt.algos.Rebalance(),
        ],
    )


# --- single-asset builder ----------------------------------------------------
#
# One market, long/flat on an explicit moving-average trend signal: hold 100%
# while price is above its SMA, otherwise sit in cash. This is the single_asset
# strategy_mode shape -- a tactical setup rather than a basket. The coin is the
# thesis target_coin_id when given, else the top-ranked coin in the resolved
# (select_top=1) universe.


def _single_asset_coin(universe: pd.DataFrame, config: dict) -> list[str]:
    if "coin_id" not in universe.columns:
        raise ValueError("universe frame must include a coin_id column")
    available = [str(c) for c in universe["coin_id"].tolist()]
    if not available:
        raise ValueError("universe resolved to zero coins")
    target = config.get("target_coin_id")
    if target is not None:
        if str(target) not in available:
            raise ValueError(
                f"target_coin_id {target!r} is not in the resolved universe"
            )
        return [str(target)]
    return available[:1]


def _single_asset_trend_setup(universe, prices, config, window) -> bt.Strategy:
    coins = _single_asset_coin(universe, config)
    frame = _float_prices(prices, coins)
    lookback = int(config.get("sma_lookback", 50))
    sma = frame.rolling(lookback, min_periods=lookback).mean()
    signal = frame > sma
    return bt.Strategy(
        "single_asset_trend_setup",
        [
            bt.algos.RunWeekly(),
            bt.algos.SelectWhere(signal),
            bt.algos.WeighEqually(),
            bt.algos.Rebalance(),
        ],
    )


# --- short / hedge builders (Phase 2, behind the SHORTS gate) ----------------
#
# These produce negative target weights. bt shorts natively: a negative
# `WeighSpecified`/`WeighTarget` weight opens a short leg under `Rebalance`. We
# size gross by construction (no `LimitWeights`, whose ffn backend assumes
# weights sum to 1.0 and only caps positives). No borrow/funding cost is
# modelled -- a research simplification, documented in the migration plan.


def _core_and_sats(coins: list[str]) -> tuple[list[str], list[str]]:
    core = [coin for coin in coins if coin in CORE_COINS] or coins[:1]
    sats = [coin for coin in coins if coin not in core]
    return core, sats


def _hedged_weights(coins: list[str], hedge_weight: float) -> dict[str, float]:
    """Long the satellites equally (gross 1.0), short the BTC/ETH core to hedge
    market beta. With no satellites there is nothing to hedge against, so the
    book is a plain long core."""
    core, sats = _core_and_sats(coins)
    if not sats:
        return {coin: 1.0 / len(core) for coin in core}
    weights = {coin: 1.0 / len(sats) for coin in sats}
    for coin in core:
        weights[coin] = -hedge_weight / len(core)
    return weights


def _partial_hedge_overlay(universe, prices, config, window) -> bt.Strategy:
    coins = _coins(universe, config)
    hedge_weight = float(config.get("hedge_weight", 0.3))
    weights = _hedged_weights(coins, hedge_weight=hedge_weight)
    return bt.Strategy(
        "partial_hedge_overlay",
        [
            _run_algo(config.get("rebalance_trigger", "periodic_30d")),
            bt.algos.SelectThese(list(weights)),
            bt.algos.WeighSpecified(**weights),
            bt.algos.Rebalance(),
        ],
    )


def _beta_hedged_alt_exposure(universe, prices, config, window) -> bt.Strategy:
    coins = _coins(universe, config)
    # A larger hedge than the partial overlay: size the short to strip most of
    # the BTC/ETH beta out of the alt book.
    hedge_weight = float(config.get("hedge_weight", 0.5))
    weights = _hedged_weights(coins, hedge_weight=hedge_weight)
    return bt.Strategy(
        "beta_hedged_alt_exposure",
        [
            _run_algo(config.get("rebalance_trigger", "periodic_30d")),
            bt.algos.SelectThese(list(weights)),
            bt.algos.WeighSpecified(**weights),
            bt.algos.Rebalance(),
        ],
    )


def _explicit_pair_trade(universe, prices, config, window) -> bt.Strategy:
    """Long an explicit coin, short another, with an optional hedge ratio sizing
    the short leg. Unlike relative_value_pair_trade (which longs the top-ranked
    name and shorts the runner-up), the two legs are named by the thesis."""
    long_coin = config.get("long_coin_id")
    short_coin = config.get("short_coin_id")
    if not long_coin or not short_coin:
        raise ValueError(
            "explicit_pair_trade requires long_coin_id and short_coin_id"
        )
    if str(long_coin) == str(short_coin):
        raise ValueError("long_coin_id and short_coin_id must differ")
    if "coin_id" not in universe.columns:
        raise ValueError("universe frame must include a coin_id column")
    available = {str(c) for c in universe["coin_id"].tolist()}
    for coin in (long_coin, short_coin):
        if str(coin) not in available:
            raise ValueError(f"{coin!r} is not in the resolved universe")
    hedge_ratio = float(config.get("hedge_ratio", 1.0))
    weights = {str(long_coin): 1.0, str(short_coin): -hedge_ratio}
    return bt.Strategy(
        "explicit_pair_trade",
        [
            bt.algos.RunMonthly(),
            bt.algos.SelectThese(list(weights)),
            bt.algos.WeighSpecified(**weights),
            bt.algos.Rebalance(),
        ],
    )


def _relative_value_pair_trade(universe, prices, config, window) -> bt.Strategy:
    coins = _coins(universe, config)
    if len(coins) < 2:
        raise ValueError("relative_value_pair_trade requires select_top >= 2")
    # Long the top-ranked name, short the runner-up to express relative value.
    weights = {coins[0]: 0.5, coins[1]: -0.5}
    return bt.Strategy(
        "relative_value_pair_trade",
        [
            bt.algos.RunMonthly(),
            bt.algos.SelectThese(list(weights)),
            bt.algos.WeighSpecified(**weights),
            bt.algos.Rebalance(),
        ],
    )


def _trend_following_long_short(universe, prices, config, window) -> bt.Strategy:
    coins = _coins(universe, config)
    weights = _trend_long_short_weights(prices, coins)
    return bt.Strategy(
        "trend_following_long_short",
        [
            bt.algos.RunWeekly(),
            bt.algos.SelectThese(coins),
            bt.algos.WeighTarget(weights),
            bt.algos.Rebalance(),
        ],
    )


def _trend_long_short_weights(prices, coins: list[str]) -> pd.DataFrame:
    """+1 long names above their 50d SMA, -1 short names below; normalised so
    gross exposure is 1.0 each day (flat when no name has a 50d history)."""
    frame = _float_prices(prices, coins)
    frame.index = pd.to_datetime(frame.index)
    sma = frame.rolling(50, min_periods=50).mean()
    sign = pd.DataFrame(0.0, index=frame.index, columns=coins)
    sign[frame > sma] = 1.0
    sign[frame < sma] = -1.0
    gross = sign.abs().sum(axis=1)
    return sign.div(gross.where(gross > 0), axis=0).fillna(0.0)


def _drawdown_based_hedge(universe, prices, config, window) -> bt.Strategy:
    coins = _coins(universe, config)
    weights = _drawdown_hedge_weights(
        prices,
        coins,
        threshold=float(config.get("drawdown_threshold", 0.15)),
        hedge_weight=float(config.get("hedge_weight", 0.5)),
    )
    return bt.Strategy(
        "drawdown_based_hedge",
        [
            bt.algos.RunWeekly(),
            bt.algos.SelectThese(coins),
            bt.algos.WeighTarget(weights),
            bt.algos.Rebalance(),
        ],
    )


def _drawdown_hedge_weights(
    prices, coins: list[str], *, threshold: float, hedge_weight: float
) -> pd.DataFrame:
    """Equal-weight long book that overlays a short BTC/ETH hedge once the
    book's own drawdown crosses ``threshold``. The drawdown is measured on a
    buy-and-hold reference basket (no look-ahead -- it uses only past prices)."""
    frame = _float_prices(prices, coins)
    frame.index = pd.to_datetime(frame.index)
    core, _ = _core_and_sats(coins)
    base = 1.0 / len(coins)
    basket = (frame / frame.iloc[0]).mean(axis=1)
    drawdown = basket / basket.cummax() - 1.0
    hedged = drawdown < -threshold
    weights = pd.DataFrame(base, index=frame.index, columns=coins)
    for coin in core:
        weights.loc[hedged, coin] = base - hedge_weight / len(core)
    return weights


# --- registry ----------------------------------------------------------------


def _recipe(
    id: str,
    builder,
    *,
    composite_formula: str,
    min_history_days: int,
    slot_schema: dict,
    preferred_factors: list[str],
) -> Recipe:
    return Recipe(
        METADATA=TemplateMetadata(
            id=id,
            category="allocation",
            preferred_factors=preferred_factors,
            default_universe=dict(_DEFAULT_UNIVERSE),
            min_history_days=min_history_days,
            composite_formula=composite_formula,
            slot_schema=slot_schema,
        ),
        _builder=builder,
    )


RECIPES: dict[str, Recipe] = {
    "synthetic_long_allocation": _recipe(
        "synthetic_long_allocation",
        _synthetic_long,
        composite_formula="long_horizon_composite",
        min_history_days=0,
        slot_schema={"select_top": _SELECT_TOP, "weighting": _WEIGHTING},
        preferred_factors=["return_365d", "recovery_rate"],
    ),
    "periodic_rebalanced_allocation": _recipe(
        "periodic_rebalanced_allocation",
        _periodic_rebalanced,
        composite_formula="long_horizon_composite",
        min_history_days=0,
        slot_schema={
            "select_top": _SELECT_TOP,
            "weighting": _WEIGHTING,
            "rebalance_trigger": _REBALANCE,
        },
        preferred_factors=["sharpe_365d", "recovery_rate"],
    ),
    "threshold_rebalanced_allocation": _recipe(
        "threshold_rebalanced_allocation",
        _threshold_rebalanced,
        composite_formula="long_horizon_composite",
        min_history_days=0,
        slot_schema={
            "select_top": _SELECT_TOP,
            "weighting": _WEIGHTING,
            "rebalance_trigger": _REBALANCE,
        },
        preferred_factors=["sharpe_365d", "recovery_rate"],
    ),
    "core_satellite_allocation": _recipe(
        "core_satellite_allocation",
        _core_satellite,
        composite_formula="long_horizon_composite",
        min_history_days=0,
        slot_schema={
            "select_top": _SELECT_TOP,
            "weighting": _WEIGHTING,
            "rebalance_trigger": _REBALANCE,
            "core_weight": _CORE_WEIGHT,
        },
        preferred_factors=["return_365d", "recovery_rate"],
    ),
    "barbell_allocation": _recipe(
        "barbell_allocation",
        _barbell,
        composite_formula="long_horizon_composite",
        min_history_days=0,
        slot_schema={
            "select_top": _SELECT_TOP,
            "weighting": _WEIGHTING,
            "rebalance_trigger": _REBALANCE,
            "core_weight": _CORE_WEIGHT,
            "sleeve_cap": _SLEEVE_CAP,
        },
        preferred_factors=["return_365d", "max_drawdown"],
    ),
    "volatility_targeted_exposure": _recipe(
        "volatility_targeted_exposure",
        _volatility_targeted,
        composite_formula="vol_adjusted_return",
        min_history_days=90,
        slot_schema={
            "select_top": _SELECT_TOP,
            "weighting": _WEIGHTING,
            "rebalance_trigger": _REBALANCE,
        },
        # "low_volatility" is a rank_universe sort mode, NOT a ranking-factor
        # id, so it can't lead preferred_factors: _default_ranking feeds [0]
        # straight into the ranking_factors registry, which would reject it and
        # fail every batch carrying this family. sharpe_365d is the valid,
        # vol-adjusted selector (high = best risk-adjusted) for this family.
        preferred_factors=["sharpe_365d", "volatility_180d"],
    ),
    "relative_momentum_rotation": _recipe(
        "relative_momentum_rotation",
        _relative_momentum_rotation,
        composite_formula="trade_count_aware_sharpe",
        min_history_days=180,
        slot_schema={
            "select_top": _SELECT_TOP,
            "weighting": _WEIGHTING,
            "rebalance_trigger": _REBALANCE,
        },
        preferred_factors=["roc_90d", "rs_vs_btc"],
    ),
    "trend_following_long_neutral": _recipe(
        "trend_following_long_neutral",
        _trend_following_long_neutral,
        composite_formula="trade_count_aware_sharpe",
        min_history_days=120,
        slot_schema={"select_top": _SELECT_TOP, "weighting": _WEIGHTING},
        preferred_factors=["pct_above_sma_200d", "roc_90d"],
    ),
    # --- short / hedge families (Phase 2; only proposed when shorts opted in) -
    "partial_hedge_overlay": _recipe(
        "partial_hedge_overlay",
        _partial_hedge_overlay,
        composite_formula="vol_adjusted_return",
        min_history_days=0,
        slot_schema={
            "select_top": _SELECT_TOP,
            "weighting": _WEIGHTING,
            "rebalance_trigger": _REBALANCE,
            "hedge_weight": _HEDGE_WEIGHT,
        },
        preferred_factors=["return_365d", "max_drawdown_365d"],
    ),
    "beta_hedged_alt_exposure": _recipe(
        "beta_hedged_alt_exposure",
        _beta_hedged_alt_exposure,
        composite_formula="vol_adjusted_return",
        min_history_days=90,
        slot_schema={
            "select_top": _SELECT_TOP,
            "weighting": _WEIGHTING,
            "rebalance_trigger": _REBALANCE,
            "hedge_weight": _HEDGE_WEIGHT,
        },
        preferred_factors=["rs_vs_btc", "sharpe_365d"],
    ),
    "relative_value_pair_trade": _recipe(
        "relative_value_pair_trade",
        _relative_value_pair_trade,
        composite_formula="trade_count_aware_sharpe",
        min_history_days=0,
        slot_schema={"select_top": _SELECT_TOP, "weighting": _WEIGHTING},
        preferred_factors=["rs_vs_btc"],
    ),
    "trend_following_long_short": _recipe(
        "trend_following_long_short",
        _trend_following_long_short,
        composite_formula="trade_count_aware_sharpe",
        min_history_days=120,
        slot_schema={"select_top": _SELECT_TOP, "weighting": _WEIGHTING},
        preferred_factors=["roc_90d", "pct_above_sma_200d"],
    ),
    "drawdown_based_hedge": _recipe(
        "drawdown_based_hedge",
        _drawdown_based_hedge,
        composite_formula="vol_adjusted_return",
        min_history_days=90,
        slot_schema={
            "select_top": _SELECT_TOP,
            "weighting": _WEIGHTING,
            "hedge_weight": _HEDGE_WEIGHT,
            "drawdown_threshold": _DRAWDOWN_THRESHOLD,
        },
        preferred_factors=["recovery_rate", "max_drawdown_365d"],
    ),
    # --- explicit pair trade (pair_trade strategy_mode, REQUIRES SHORTS) -----
    "explicit_pair_trade": _recipe(
        "explicit_pair_trade",
        _explicit_pair_trade,
        composite_formula="trade_count_aware_sharpe",
        min_history_days=0,
        slot_schema={
            "long_coin_id": _LONG_COIN_ID,
            "short_coin_id": _SHORT_COIN_ID,
            "hedge_ratio": _HEDGE_RATIO,
        },
        preferred_factors=["rs_vs_btc"],
    ),
    # --- single-asset family (single_asset strategy_mode) --------------------
    # One market, long/flat on an SMA trend signal. No select_top/weighting
    # slots: the book is one position at 100% (or flat). The candidate still
    # carries select_top=1 at the job level to bound universe ranking.
    "single_asset_trend_setup": _recipe(
        "single_asset_trend_setup",
        _single_asset_trend_setup,
        composite_formula="trade_count_aware_sharpe",
        min_history_days=50,
        slot_schema={
            "target_coin_id": _TARGET_COIN_ID,
            "sma_lookback": _SMA_LOOKBACK,
        },
        preferred_factors=["pct_above_sma_200d", "roc_90d"],
    ),
}
