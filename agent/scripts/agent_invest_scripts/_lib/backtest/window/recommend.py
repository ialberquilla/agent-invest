"""Deterministic backtest window recommendations."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import polars as pl

_BTC_COIN_ID = "bitcoin"
_MIN_WINDOW_DAYS = 1460
_EPSILON = 1e-12


def recommend_backtest_window(
    prices: pl.DataFrame,
    *,
    coin_ids: list[str],
    horizon_days: int,
    require_drawdown_pct: float = 0.30,
) -> dict[str, Any]:
    """Return a deterministic backtest window for the requested coin set."""
    if horizon_days <= 0:
        raise ValueError("--horizon-days must be positive")
    if require_drawdown_pct <= 0 or require_drawdown_pct >= 1:
        raise ValueError("--require-drawdown-pct must be between 0 and 1")

    normalized_coin_ids = _normalize_coin_ids(coin_ids)
    frame = _normalize_prices(prices)
    constraints = _history_constraints(frame, normalized_coin_ids)
    btc_constraint = _history_constraints(frame, [_BTC_COIN_ID])["coins"][_BTC_COIN_ID]

    intersection_start = max(
        date.fromisoformat(coin["first_price_date"])
        for coin in constraints["coins"].values()
    )
    intersection_end = min(
        date.fromisoformat(coin["last_price_date"])
        for coin in constraints["coins"].values()
    )
    if intersection_start > intersection_end:
        raise ValueError("coin histories do not overlap")

    target_window_length = max(2 * horizon_days, _MIN_WINDOW_DAYS)
    start = max(
        intersection_start, intersection_end - timedelta(days=target_window_length)
    )

    drawdowns = _btc_drawdowns(
        frame,
        start=start,
        end=intersection_end,
        require_drawdown_pct=require_drawdown_pct,
    )
    relaxed = False
    if not drawdowns:
        widest_start = max(
            intersection_start, date.fromisoformat(btc_constraint["first_price_date"])
        )
        if widest_start < start:
            start = widest_start
            drawdowns = _btc_drawdowns(
                frame,
                start=start,
                end=intersection_end,
                require_drawdown_pct=require_drawdown_pct,
            )
        relaxed = not drawdowns

    window_days = (intersection_end - start).days
    return {
        "start": start.isoformat(),
        "end": intersection_end.isoformat(),
        "rationale": _rationale(
            target_window_length=target_window_length,
            require_drawdown_pct=require_drawdown_pct,
            covered_drawdowns=drawdowns,
            relaxed=relaxed,
            bounded_by_history=start == intersection_start,
        ),
        "covered_drawdowns": drawdowns,
        "history_constraints": {
            "intersection_start": intersection_start.isoformat(),
            "intersection_end": intersection_end.isoformat(),
            "target_window_length_days": target_window_length,
            "window_length_days": window_days,
            "limiting_coin": constraints["limiting_coin"],
            "coins": constraints["coins"],
        },
    }


def _normalize_coin_ids(coin_ids: list[str]) -> list[str]:
    normalized = []
    seen = set()
    for coin_id in coin_ids:
        value = coin_id.strip()
        if not value or value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    if not normalized:
        raise ValueError("--coin-ids must include at least one coin ID")
    return normalized


def _normalize_prices(prices: pl.DataFrame) -> pl.DataFrame:
    if not {"date", "coin_id", "price"} <= set(prices.columns):
        raise ValueError("daily_prices must include date, coin_id, and price columns")
    return (
        prices.with_columns(
            pl.col("date").cast(pl.Date),
            pl.col("coin_id").cast(pl.String),
            pl.col("price").cast(pl.Float64),
        )
        .filter(pl.col("price").is_not_null() & (pl.col("price") > 0))
        .select("date", "coin_id", "price")
        .sort(["coin_id", "date"])
    )


def _history_constraints(frame: pl.DataFrame, coin_ids: list[str]) -> dict[str, Any]:
    grouped = frame.group_by("coin_id").agg(
        pl.col("date").min().alias("first_price_date"),
        pl.col("date").max().alias("last_price_date"),
        pl.len().alias("price_days"),
    )
    rows = {row["coin_id"]: row for row in grouped.to_dicts()}
    missing = sorted(set(coin_ids) - set(rows))
    if missing:
        raise ValueError(f"coin_id(s) not found in daily_prices: {', '.join(missing)}")

    coins = {
        coin_id: {
            "first_price_date": rows[coin_id]["first_price_date"].isoformat(),
            "last_price_date": rows[coin_id]["last_price_date"].isoformat(),
            "price_days": rows[coin_id]["price_days"],
        }
        for coin_id in coin_ids
    }
    limiting_coin = max(
        coin_ids, key=lambda coin_id: coins[coin_id]["first_price_date"]
    )
    return {"limiting_coin": limiting_coin, "coins": coins}


def _btc_drawdowns(
    frame: pl.DataFrame,
    *,
    start: date,
    end: date,
    require_drawdown_pct: float,
) -> list[dict[str, Any]]:
    rows = (
        frame.filter(
            (pl.col("coin_id") == _BTC_COIN_ID)
            & (pl.col("date") >= start)
            & (pl.col("date") <= end)
        )
        .select("date", "price")
        .sort("date")
        .to_dicts()
    )
    if not rows:
        raise ValueError(
            "bitcoin price history is required for drawdown window selection"
        )

    peak_date = rows[0]["date"]
    peak_price = rows[0]["price"]
    open_episode: dict[str, Any] | None = None
    episodes: list[dict[str, Any]] = []
    for row in rows:
        day = row["date"]
        price = row["price"]
        if price > peak_price:
            peak_date = day
            peak_price = price
            open_episode = None
            continue

        drawdown_pct = price / peak_price - 1.0
        if drawdown_pct <= -require_drawdown_pct + _EPSILON:
            if open_episode is None:
                open_episode = {
                    "asset": _BTC_COIN_ID,
                    "peak_date": peak_date,
                    "trough_date": day,
                    "drawdown_pct": drawdown_pct,
                }
                episodes.append(open_episode)
            elif drawdown_pct < open_episode["drawdown_pct"]:
                open_episode["trough_date"] = day
                open_episode["drawdown_pct"] = drawdown_pct

    return [
        {
            "asset": episode["asset"],
            "peak_date": episode["peak_date"].isoformat(),
            "trough_date": episode["trough_date"].isoformat(),
            "drawdown_pct": round(episode["drawdown_pct"], 6),
        }
        for episode in episodes
    ]


def _rationale(
    *,
    target_window_length: int,
    require_drawdown_pct: float,
    covered_drawdowns: list[dict[str, Any]],
    relaxed: bool,
    bounded_by_history: bool,
) -> str:
    if covered_drawdowns:
        suffix = (
            " after expanding to the available common history"
            if bounded_by_history
            else ""
        )
        return (
            f"Selected the latest common-history window targeting at least "
            f"{target_window_length} days and covering a BTC drawdown of at least "
            f"{require_drawdown_pct:.0%}{suffix}."
        )
    if relaxed:
        return (
            f"Selected the widest common-history window available, but relaxed the "
            f"BTC drawdown requirement because no drawdown of at least "
            f"{require_drawdown_pct:.0%} is covered by this coin set and horizon."
        )
    return (
        f"Selected the latest common-history window targeting at least "
        f"{target_window_length} days."
    )
