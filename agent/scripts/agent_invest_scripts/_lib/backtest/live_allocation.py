"""Extract a strategy's *live* target weights from a backtest's
``holdings_history``.

Phase 3 of plans/integrate_contracts.md. The live allocator never reimplements
selection/weighting: it runs the SAME ``bt`` recipe (via run_candidate_batch's
per-candidate path) over data through ``as_of`` and reads the latest rebalance.
Because ``bt`` is causal (a rebalance on date D only uses prices <= D),
truncating the window at D reproduces exactly the weights the full backtest
computed at D -- the parity guarantee tested in test_live_allocation.py.

This module is the pure extraction step shared by the CLI and the parity test.
"""

from __future__ import annotations

from datetime import date
from typing import Any

_DUST = 1e-6


def _to_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    raise ValueError(f"unparseable rebalance date: {value!r}")


def select_live_rebalance(
    holdings_history: list[dict[str, Any]], as_of: Any | None = None
) -> dict[str, Any]:
    """Most recent rebalance entry with date <= ``as_of`` (or the final entry
    when ``as_of`` is None). ``holdings_history`` is the ascending-by-date list
    of ``{"date", "weights"}`` the runner emits."""
    if not holdings_history:
        raise ValueError("holdings_history is empty; strategy never traded")
    if as_of is None:
        return holdings_history[-1]
    cutoff = _to_date(as_of)
    eligible = [h for h in holdings_history if _to_date(h["date"]) <= cutoff]
    if not eligible:
        raise ValueError(f"no rebalance on or before {cutoff.isoformat()}")
    return eligible[-1]


def to_live_allocation(
    holdings_history: list[dict[str, Any]], as_of: Any | None = None
) -> dict[str, Any]:
    """Shape the latest rebalance into an executable target: per-coin weight +
    side (sign), plus net/gross/cash summaries. Long-only cash is
    ``1 - net_weight``; for long/short, ``net_weight`` is the directional sum
    and ``gross_weight`` the leverage-relevant total."""
    entry = select_live_rebalance(holdings_history, as_of)
    legs: list[dict[str, Any]] = []
    for coin_id, raw in entry["weights"].items():
        weight = float(raw)
        if abs(weight) <= _DUST:
            continue
        legs.append(
            {
                "coin_id": str(coin_id),
                "weight": weight,
                "side": "short" if weight < 0 else "long",
            }
        )
    legs.sort(key=lambda leg: leg["weight"], reverse=True)
    net = sum(leg["weight"] for leg in legs)
    gross = sum(abs(leg["weight"]) for leg in legs)
    return {
        "as_of": _to_date(as_of).isoformat() if as_of is not None else None,
        "rebalance_date": _to_date(entry["date"]).isoformat(),
        "weights": legs,
        "net_weight": net,
        "gross_weight": gross,
        "cash_weight": 1.0 - net,
    }
