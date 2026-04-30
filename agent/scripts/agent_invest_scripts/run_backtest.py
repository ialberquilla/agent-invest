"""CLI entrypoint for running JSON-specified backtests."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

import polars as pl

from agent_invest_scripts._lib import (
    daily_prices,
    print_json,
    universe_history,
)
from agent_invest_scripts._lib.backtest import (
    TradingCostModel,
    run_cross_sectional_momentum_backtest,
)
from agent_invest_scripts._lib.cli import (
    add_timeout_argument,
    fail_json,
    resolve_timeout_seconds,
    script_timeout,
)
from agent_invest_scripts._lib.signals.regimes import btc_above_moving_average

_DEFAULT_SIGNAL_TYPE = "cross_sectional_momentum"


class JsonArgumentParser(argparse.ArgumentParser):
    """Argument parser that emits JSON-formatted stderr errors."""

    def error(self, message: str) -> None:
        print_json({"error": message}, stream=sys.stderr)
        raise SystemExit(2)


def build_parser() -> JsonArgumentParser:
    parser = JsonArgumentParser(description="Run a backtest from a JSON strategy spec.")
    parser.add_argument(
        "--spec",
        required=True,
        help="JSON strategy spec for the backtest run",
    )
    add_timeout_argument(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        with script_timeout(resolve_timeout_seconds(args.timeout_seconds)):
            spec = _parse_spec(args.spec)
            payload = _run_backtest(spec)
    except Exception as error:
        fail_json(str(error), error_type=type(error).__name__)

    print_json(payload)
    return 0


def _parse_spec(raw_spec: str) -> dict[str, Any]:
    try:
        spec = json.loads(raw_spec)
    except json.JSONDecodeError as error:
        raise ValueError("--spec must be valid JSON") from error

    if not isinstance(spec, dict):
        raise ValueError("--spec must decode to a JSON object")

    return spec


def _run_backtest(spec: Mapping[str, Any]) -> dict[str, Any]:
    signal_type = _read_string(
        spec,
        "signal_type",
        default=_DEFAULT_SIGNAL_TYPE,
        location="spec.signal_type",
    )
    if signal_type != _DEFAULT_SIGNAL_TYPE:
        raise ValueError(f"unsupported signal_type: {signal_type}")

    prices = _load_prices_frame(spec)
    universe = _resolve_universe(spec)
    regime_frame = _build_regime_frame(spec, prices)
    result = run_cross_sectional_momentum_backtest(
        prices,
        universe=universe,
        lookback_days=_read_int(
            spec,
            "lookback_days",
            default=60,
            minimum=1,
            location="spec.lookback_days",
        ),
        top_k=_read_int(
            spec,
            "top_k",
            default=10,
            minimum=1,
            location="spec.top_k",
        ),
        rebalance_frequency=_read_string(
            spec,
            "rebalance_frequency",
            default="weekly",
            location="spec.rebalance_frequency",
        ),
        cost_model=_build_cost_model(spec.get("costs")),
        initial_capital_usd=_read_float(
            spec,
            "initial_capital_usd",
            default=1000.0,
            minimum=0.0,
            location="spec.initial_capital_usd",
        ),
        skip_days=_read_int(
            spec,
            "skip_days",
            default=0,
            minimum=0,
            location="spec.skip_days",
        ),
        regime_frame=regime_frame,
    )
    return {
        "metrics": result.summary,
        "equity_curve": _equity_curve_points(result.performance),
    }


def _load_prices_frame(spec: Mapping[str, Any]) -> pl.DataFrame:
    frame = pl.from_pandas(daily_prices())
    _require_columns(frame, {"date", "coin_id", "price"}, dataset_name="daily_prices")
    frame = _cast_date_columns(frame, "date").select(
        pl.col("date"),
        pl.col("coin_id").cast(pl.String),
        pl.col("price").cast(pl.Float64),
    )

    date_range = spec.get("dates")
    if date_range is None:
        return frame.sort(["date", "coin_id"])

    if not isinstance(date_range, Mapping):
        raise ValueError("spec.dates must be an object")

    start_date = _read_optional_date(date_range, "start", location="spec.dates.start")
    end_date = _read_optional_date(date_range, "end", location="spec.dates.end")
    if start_date and end_date and start_date > end_date:
        raise ValueError("spec.dates.start must be on or before spec.dates.end")

    if start_date is not None:
        frame = frame.filter(pl.col("date") >= pl.lit(start_date))
    if end_date is not None:
        frame = frame.filter(pl.col("date") <= pl.lit(end_date))
    if frame.is_empty():
        raise ValueError("No price rows matched spec.dates")

    return frame.sort(["date", "coin_id"])


def _resolve_universe(spec: Mapping[str, Any]) -> list[str] | None:
    raw_universe = spec.get("universe")
    if raw_universe is not None:
        if not isinstance(raw_universe, list) or not raw_universe:
            raise ValueError("spec.universe must be a non-empty array of coin ids")
        universe = [
            coin_id.strip()
            for coin_id in raw_universe
            if isinstance(coin_id, str) and coin_id.strip()
        ]
        if len(universe) != len(raw_universe):
            raise ValueError("spec.universe must contain only non-empty strings")
        return universe

    frame = pl.from_pandas(universe_history())
    _require_columns(frame, {"coin_id"}, dataset_name="universe_history")
    frame = _cast_date_columns(frame, "snapshot_date").with_columns(
        pl.col("coin_id").cast(pl.String)
    )

    if "included" not in frame.columns or "snapshot_date" not in frame.columns:
        return sorted(frame.get_column("coin_id").unique().to_list())

    included = frame.filter(pl.col("included"))
    if included.is_empty():
        return sorted(frame.get_column("coin_id").unique().to_list())

    latest_snapshot = included.get_column("snapshot_date").max()
    latest = included.filter(pl.col("snapshot_date") == latest_snapshot)
    if "market_cap_rank" in latest.columns:
        latest = latest.sort("market_cap_rank")
    else:
        latest = latest.sort("coin_id")
    return latest.get_column("coin_id").to_list()


def _build_regime_frame(
    spec: Mapping[str, Any], prices_long: pl.DataFrame
) -> pl.DataFrame | None:
    raw_regime = spec.get("regime")
    if raw_regime is None:
        return None
    if not isinstance(raw_regime, Mapping):
        raise ValueError("spec.regime must be an object")

    regime_type = _read_string(
        raw_regime,
        "type",
        default="btc_above_moving_average",
        location="spec.regime.type",
    )
    if regime_type != "btc_above_moving_average":
        raise ValueError(f"unsupported regime.type: {regime_type}")

    prices_wide = _wide_prices(prices_long)
    return btc_above_moving_average(
        prices_wide,
        coin_id=_read_string(
            raw_regime,
            "coin_id",
            default="bitcoin",
            location="spec.regime.coin_id",
        ),
        window_days=_read_int(
            raw_regime,
            "window_days",
            default=200,
            minimum=1,
            location="spec.regime.window_days",
        ),
    )


def _build_cost_model(raw_costs: Any) -> TradingCostModel:
    if raw_costs is None:
        return TradingCostModel()
    if not isinstance(raw_costs, Mapping):
        raise ValueError("spec.costs must be an object")

    return TradingCostModel(
        protocol_bps=_read_float(
            raw_costs,
            "protocol_bps",
            default=2.0,
            minimum=0.0,
            location="spec.costs.protocol_bps",
        ),
        widget_bps=_read_float(
            raw_costs,
            "widget_bps",
            default=70.0,
            minimum=0.0,
            location="spec.costs.widget_bps",
        ),
        slippage_bps=_read_float(
            raw_costs,
            "slippage_bps",
            default=30.0,
            minimum=0.0,
            location="spec.costs.slippage_bps",
        ),
        gas_usd_per_swap=_read_float(
            raw_costs,
            "gas_usd_per_swap",
            default=1.0,
            minimum=0.0,
            location="spec.costs.gas_usd_per_swap",
        ),
    )


def _equity_curve_points(performance: pl.DataFrame) -> list[dict[str, Any]]:
    return performance.select(
        pl.col("date").dt.strftime("%Y-%m-%d").alias("date"),
        pl.col("equity"),
        pl.col("equity_usd"),
    ).to_dicts()


def _wide_prices(prices_long: pl.DataFrame) -> pl.DataFrame:
    return (
        prices_long.select("date", "coin_id", "price")
        .sort(["date", "coin_id"])
        .pivot(values="price", index="date", on="coin_id", aggregate_function="last")
        .sort("date")
    )


def _cast_date_columns(frame: pl.DataFrame, *columns: str) -> pl.DataFrame:
    existing_columns = [column for column in columns if column in frame.columns]
    if not existing_columns:
        return frame
    return frame.with_columns(
        [pl.col(column).cast(pl.Date) for column in existing_columns]
    )


def _require_columns(
    frame: pl.DataFrame, required_columns: set[str], *, dataset_name: str
) -> None:
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        missing_list = ", ".join(missing_columns)
        raise ValueError(
            f"{dataset_name} is missing required column(s): {missing_list}"
        )


def _read_string(
    payload: Mapping[str, Any],
    key: str,
    *,
    default: str | None = None,
    location: str,
) -> str:
    value = payload.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{location} must be a non-empty string")
    return value.strip()


def _read_int(
    payload: Mapping[str, Any],
    key: str,
    *,
    default: int | None = None,
    minimum: int | None = None,
    location: str,
) -> int:
    value = payload.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{location} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{location} must be >= {minimum}")
    return value


def _read_float(
    payload: Mapping[str, Any],
    key: str,
    *,
    default: float | None = None,
    minimum: float | None = None,
    location: str,
) -> float:
    value = payload.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{location} must be a number")

    numeric_value = float(value)
    if minimum is not None and numeric_value < minimum:
        raise ValueError(f"{location} must be >= {minimum}")
    return numeric_value


def _read_optional_date(
    payload: Mapping[str, Any], key: str, *, location: str
) -> date | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{location} must be an ISO date string")

    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{location} must be an ISO date string") from error


if __name__ == "__main__":
    raise SystemExit(main())
