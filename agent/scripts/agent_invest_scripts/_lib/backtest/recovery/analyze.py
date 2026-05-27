from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from statistics import median
from typing import Any

import polars as pl

_EPSILON = 1e-12


@dataclass(slots=True)
class _OpenDrawdown:
    peak_date: date
    peak_price: float
    trough_date: date
    trough_price: float
    lower_peak_date: date | None = None
    lower_peak_price: float | None = None


def analyze_price_series(
    series: pl.DataFrame,
    *,
    horizon_days: int,
    min_drawdown_pct: float = 0.20,
    as_of: date | None = None,
    coin_id: str | None = None,
) -> dict[str, Any]:
    if horizon_days <= 0:
        raise ValueError("--horizon-days must be positive")
    if min_drawdown_pct <= 0 or min_drawdown_pct >= 1:
        raise ValueError("--min-drawdown-pct must be between 0 and 1")

    frame = _normalize_series(series)
    if as_of is not None:
        frame = frame.filter(pl.col("date") <= as_of)
    if frame.is_empty():
        raise ValueError("price series has no rows on or before as_of")

    rows = frame.to_dicts()
    effective_as_of = rows[-1]["date"]
    prices = [(row["date"], float(row["price"])) for row in rows]
    episodes = _drawdown_episodes(prices, min_drawdown_pct=min_drawdown_pct)
    recovered = [
        episode for episode in episodes if episode["recovery_date"] is not None
    ]
    n_unrecovered = sum(
        1
        for episode in episodes
        if episode["recovery_date"] is None and episode["status"] == "unrecovered"
    )
    n_in_progress = sum(
        1
        for episode in episodes
        if episode["recovery_date"] is None and episode["status"] == "in_progress"
    )
    recovery_days = [
        int(episode["peak_to_trough_days"] + episode["trough_to_recovery_days"])
        for episode in recovered
    ]
    n_episodes = len(episodes)
    has_episode_sample = n_episodes >= 3

    rolling_returns = _rolling_horizon_returns(prices, horizon_days=horizon_days)
    n_windows = len(rolling_returns)
    has_window_sample = n_windows >= 365

    latest_date, latest_price = prices[-1]
    ath_date, ath_price = max(prices, key=lambda item: item[1])
    sma_200 = (
        _sma([price for _, price in prices[-200:]]) if len(prices) >= 200 else None
    )
    horizon_recovered = [
        episode
        for episode in recovered
        if int(episode["peak_to_trough_days"] + episode["trough_to_recovery_days"])
        <= horizon_days
    ]

    output: dict[str, Any] = {
        "coverage": {
            "first_price_date": prices[0][0].isoformat(),
            "last_price_date": effective_as_of.isoformat(),
            "history_days": (prices[-1][0] - prices[0][0]).days + 1,
        },
        "current_state": {
            "current_drawdown_from_ath": _round(latest_price / ath_price - 1.0),
            "days_since_ath": (latest_date - ath_date).days,
            "currently_above_sma_200d": None
            if sma_200 is None
            else latest_price >= sma_200,
        },
        "drawdown_episodes": [_public_episode(episode) for episode in episodes],
        "recovery_stats": {
            "n_episodes": n_episodes,
            "n_recovered": len(recovered),
            "n_unrecovered": n_unrecovered,
            "n_in_progress": n_in_progress,
            "recovery_rate": _round(len(recovered) / n_episodes)
            if has_episode_sample
            else None,
            "median_recovery_days": int(median(recovery_days))
            if has_episode_sample and recovery_days
            else None,
            "p90_recovery_days": _percentile_int(recovery_days, 0.9)
            if has_episode_sample and recovery_days
            else None,
            "max_recovery_days": max(recovery_days)
            if has_episode_sample and recovery_days
            else None,
        },
        "rolling_horizon_returns": {
            "n_windows": n_windows,
            "median": _round(
                _percentile([value for _, _, value in rolling_returns], 0.5)
            )
            if has_window_sample
            else None,
            "p10": _round(_percentile([value for _, _, value in rolling_returns], 0.1))
            if has_window_sample
            else None,
            "p90": _round(_percentile([value for _, _, value in rolling_returns], 0.9))
            if has_window_sample
            else None,
            "pct_negative": _round(
                sum(1 for _, _, value in rolling_returns if value < 0) / n_windows
            )
            if has_window_sample and n_windows
            else None,
            "worst_window": _worst_window(rolling_returns) if rolling_returns else None,
        },
        "horizon_verdict": {
            "pct_drawdowns_recovered_within_horizon": _round(
                len(horizon_recovered) / n_episodes
            )
            if has_episode_sample
            else None,
            "pct_horizon_windows_positive": _round(
                sum(1 for _, _, value in rolling_returns if value > 0) / n_windows
            )
            if has_window_sample and n_windows
            else None,
            "worst_horizon_loss": _round(min(value for _, _, value in rolling_returns))
            if has_window_sample and rolling_returns
            else None,
        },
        "survivorship_warning": (datetime.now(UTC).date() - prices[0][0]).days > 365,
    }
    if coin_id is not None:
        output = {"coin_id": coin_id, **output}
    return output


def _normalize_series(series: pl.DataFrame) -> pl.DataFrame:
    if not {"date", "price"} <= set(series.columns):
        raise ValueError("price series must include date and price columns")
    return (
        series.select(pl.col("date").cast(pl.Date), pl.col("price").cast(pl.Float64))
        .drop_nulls(["date", "price"])
        .filter(pl.col("price") > 0)
        .sort("date")
    )


def _drawdown_episodes(
    prices: list[tuple[date, float]], *, min_drawdown_pct: float
) -> list[dict[str, Any]]:
    peak_date, peak_price = prices[0]
    open_drawdown: _OpenDrawdown | None = None
    episodes: list[dict[str, Any]] = []

    for day, price in prices[1:]:
        if open_drawdown is None:
            if price >= peak_price:
                peak_date, peak_price = day, price
                continue
            drawdown = price / peak_price - 1.0
            if drawdown <= -min_drawdown_pct + _EPSILON:
                open_drawdown = _OpenDrawdown(peak_date, peak_price, day, price)
            continue

        if price >= open_drawdown.peak_price:
            episodes.append(
                _episode(open_drawdown, recovery_date=day, status="recovered")
            )
            peak_date, peak_price = day, price
            open_drawdown = None
            continue

        if (
            open_drawdown.lower_peak_price is not None
            and price / open_drawdown.lower_peak_price - 1.0
            <= -min_drawdown_pct + _EPSILON
        ):
            episodes.append(
                _episode(open_drawdown, recovery_date=None, status="unrecovered")
            )
            open_drawdown = _OpenDrawdown(
                open_drawdown.lower_peak_date or day,
                open_drawdown.lower_peak_price,
                day,
                price,
            )
            continue

        if price < open_drawdown.trough_price:
            open_drawdown.trough_date = day
            open_drawdown.trough_price = price
            continue

        if price > open_drawdown.trough_price and (
            open_drawdown.lower_peak_price is None
            or price >= open_drawdown.lower_peak_price
        ):
            open_drawdown.lower_peak_date = day
            open_drawdown.lower_peak_price = price
            continue

    if open_drawdown is not None:
        episodes.append(
            _episode(open_drawdown, recovery_date=None, status="in_progress")
        )
    return episodes


def _episode(
    drawdown: _OpenDrawdown, *, recovery_date: date | None, status: str
) -> dict[str, Any]:
    return {
        "peak_date": drawdown.peak_date,
        "peak_price": drawdown.peak_price,
        "trough_date": drawdown.trough_date,
        "trough_price": drawdown.trough_price,
        "drawdown_pct": drawdown.trough_price / drawdown.peak_price - 1.0,
        "recovery_date": recovery_date,
        "peak_to_trough_days": (drawdown.trough_date - drawdown.peak_date).days,
        "trough_to_recovery_days": None
        if recovery_date is None
        else (recovery_date - drawdown.trough_date).days,
        "status": status,
    }


def _public_episode(episode: dict[str, Any]) -> dict[str, Any]:
    return {
        "peak_date": episode["peak_date"].isoformat(),
        "peak_price": _round(episode["peak_price"]),
        "trough_date": episode["trough_date"].isoformat(),
        "trough_price": _round(episode["trough_price"]),
        "drawdown_pct": _round(episode["drawdown_pct"]),
        "recovery_date": None
        if episode["recovery_date"] is None
        else episode["recovery_date"].isoformat(),
        "peak_to_trough_days": episode["peak_to_trough_days"],
        "trough_to_recovery_days": episode["trough_to_recovery_days"],
    }


def _rolling_horizon_returns(
    prices: list[tuple[date, float]], *, horizon_days: int
) -> list[tuple[date, date, float]]:
    windows: list[tuple[date, date, float]] = []
    for start_index in range(0, len(prices) - horizon_days):
        start_date, start_price = prices[start_index]
        end_date, end_price = prices[start_index + horizon_days]
        windows.append((start_date, end_date, end_price / start_price - 1.0))
    return windows


def _worst_window(windows: list[tuple[date, date, float]]) -> dict[str, Any]:
    start, end, value = min(windows, key=lambda item: item[2])
    return {"start": start.isoformat(), "end": end.isoformat(), "return": _round(value)}


def _sma(values: list[float]) -> float:
    return sum(values) / len(values)


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _percentile_int(values: list[int], q: float) -> int | None:
    value = _percentile([float(item) for item in values], q)
    return None if value is None else int(round(value))


def _round(value: float | None) -> float | None:
    return None if value is None else round(float(value), 6)
