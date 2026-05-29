from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypedDict

from agent_invest_scripts._lib.backtest.composite_scorers import SCORERS
from agent_invest_scripts._lib.backtest.factors import FACTORS
from agent_invest_scripts._lib.backtest.signal_indicators import SIGNAL_INDICATORS


class RegistryEntry(TypedDict, total=False):
    id: str
    params_schema: dict[str, Any]
    metadata: dict[str, Any]


def _schema(
    properties: dict[str, Any] | None = None, required: list[str] | None = None
) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties or {},
        "required": required or [],
        "additionalProperties": False,
    }


def _stub(
    id: str,
    metadata: dict[str, Any] | None = None,
    params_schema: dict[str, Any] | None = None,
) -> RegistryEntry:
    entry: RegistryEntry = {"id": id, "params_schema": params_schema or _schema()}
    if metadata:
        entry["metadata"] = metadata
    return entry


def _ranking_factors() -> list[RegistryEntry]:
    return [
        {
            "id": factor.id,
            "params_schema": _schema(),
            "metadata": {
                "category": factor.category,
                "description": factor.description,
                "formula": factor.formula,
                "window_days": factor.window_days,
                "units": factor.units,
                "direction_better": factor.direction_better,
                "min_history_days": factor.min_history_days,
                "sample_size_rule": factor.sample_size_rule,
            },
        }
        for factor in FACTORS.values()
    ]


def _composite_scorers() -> list[RegistryEntry]:
    return [_stub(id=scorer_id) for scorer_id in SCORERS]


def _signal_indicators() -> list[RegistryEntry]:
    return [
        {"id": indicator.ID, "params_schema": indicator.params_schema}
        for indicator in SIGNAL_INDICATORS.values()
    ]


_FILTERS = [
    _stub(
        "min_history_days",
        {"description": "Require each asset to have at least N days of price history."},
        _schema({"days": {"type": "integer", "minimum": 1}}, ["days"]),
    ),
    _stub(
        "max_drawdown_full_le",
        {
            "description": (
                "Require full-history max drawdown to be less than or equal to a "
                "threshold."
            )
        },
        _schema({"threshold": {"type": "number", "maximum": 0}}, ["threshold"]),
    ),
    _stub(
        "correlation_prune",
        {"description": "Prune assets above a pairwise correlation threshold."},
        _schema(
            {
                "threshold": {"type": "number", "minimum": -1, "maximum": 1},
                "window_days": {"type": "integer", "minimum": 2},
            },
        ),
    ),
    _stub(
        "market_cap_floor",
        {"description": "Require market cap above a USD floor."},
        _schema({"usd": {"type": "number", "minimum": 0}}, ["usd"]),
    ),
    _stub("exclude_stablecoins", {"description": "Exclude stablecoin assets."}),
    _stub("exclude_wrapped", {"description": "Exclude wrapped-token assets."}),
    _stub(
        "volume_floor",
        {"description": "Require average daily USD volume above a floor."},
        _schema({"usd": {"type": "number", "minimum": 0}}, ["usd"]),
    ),
    _stub(
        "sector_in",
        {"description": "Keep assets assigned to one of the provided sectors."},
        _schema(
            {"sectors": {"type": "array", "items": {"type": "string"}, "minItems": 1}},
            ["sectors"],
        ),
    ),
    _stub(
        "sector_not_in",
        {"description": "Exclude assets assigned to any provided sector."},
        _schema(
            {"sectors": {"type": "array", "items": {"type": "string"}, "minItems": 1}},
            ["sectors"],
        ),
    ),
]

_WEIGHTING_SCHEMES = [
    _stub(id) for id in ("equal", "cap", "vol_inverse", "ranking_proportional")
]

_WINDOW_SELECTORS = [
    _stub(id) for id in ("latest", "with_btc_drawdown", "full_history")
]

_REBALANCE_TRIGGERS = [
    _stub(id)
    for id in ("none", "periodic_30d", "periodic_90d", "threshold_drift_10pct")
]

_EXIT_RULES = [
    _stub(id) for id in ("signal_reverse", "trailing_stop", "fixed_target", "time_stop")
]

_UNIVERSE_SELECTORS = [
    _stub(id)
    for id in (
        "top_n_by_mcap",
        "top_n_by_volume",
        "hand_picked",
        "sector_filtered",
        "liquidity_floor",
    )
]

_REGISTRIES: dict[str, Callable[[], list[RegistryEntry]]] = {
    "ranking_factors": _ranking_factors,
    "filters": lambda: list(_FILTERS),
    "weighting_schemes": lambda: list(_WEIGHTING_SCHEMES),
    "signal_indicators": _signal_indicators,
    "window_selectors": lambda: list(_WINDOW_SELECTORS),
    "rebalance_triggers": lambda: list(_REBALANCE_TRIGGERS),
    "exit_rules": lambda: list(_EXIT_RULES),
    "composite_scorers": _composite_scorers,
    "universe_selectors": lambda: list(_UNIVERSE_SELECTORS),
}

REGISTRY_NAMES = tuple(_REGISTRIES)


def list_registry(name: str) -> list[RegistryEntry]:
    try:
        return _REGISTRIES[name]()
    except KeyError as error:
        valid = ", ".join(REGISTRY_NAMES)
        raise ValueError(
            f"Unknown registry '{name}'. Valid registries: {valid}"
        ) from error


__all__ = ["REGISTRY_NAMES", "RegistryEntry", "list_registry"]
