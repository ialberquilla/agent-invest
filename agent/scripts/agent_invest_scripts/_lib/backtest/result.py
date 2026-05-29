from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

import pandas as pd


@dataclass(slots=True)
class BacktestMetrics:
    total_return: float
    cagr: float
    period_return: float
    volatility: float
    max_drawdown: float
    max_drawdown_duration_days: int
    sharpe: float
    sortino: float
    calmar: float
    total_fees_paid: float
    total_slippage: float
    return_365d: float | None = None
    recovery_rate: float | None = None
    pct_horizon_windows_positive: float | None = None
    n_trades: int | None = None


@dataclass(slots=True)
class RobustnessSignals:
    n_trades: int | None
    n_rebalances: int | None
    duration_days: int
    sample_size_warning: bool
    half_consistency_score: float
    half_consistency_warning: bool
    top_3_trades_pct_of_pnl: float | None
    top_3_months_pct_of_pnl: float | None
    concentration_warning: bool
    worst_180d_return: float
    worst_90d_drawdown: float
    worst_window_warning: bool
    correlation_to_benchmark: float
    beta_to_benchmark: float
    excess_return_t_stat: float
    benchmark_coupling_warning: bool
    significance_warning: bool
    survivorship_warning: bool


@dataclass(slots=True)
class Trade:
    coin_id: str
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    pnl_pct: float
    hold_days: int
    exit_reason: str


@dataclass(slots=True)
class TacticalMetrics:
    trades: list[Trade]
    n_trades: int
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    avg_hold_days: float
    max_consecutive_losses: int


@dataclass(slots=True)
class Rebalance:
    date: date
    weights: dict[str, float]
    turnover_pct: float
    cost_paid: float


@dataclass(slots=True)
class AllocationMetrics:
    rebalances: list[Rebalance]
    avg_turnover_per_rebalance: float
    max_single_weight: float
    holdings_history: list[dict[str, Any]]


@dataclass(slots=True)
class DrawdownEpisode:
    peak_date: date
    trough_date: date
    drawdown_pct: float
    recovery_date: date | None
    peak_to_trough_days: int
    trough_to_recovery_days: int | None


@dataclass(slots=True)
class BacktestResult:
    candidate_id: str
    template_id: str
    config: dict[str, Any]
    window: tuple[date, date]
    equity_curve: pd.Series
    benchmark_curve: pd.Series
    drawdown_episodes: list[DrawdownEpisode]
    metrics: BacktestMetrics
    robustness: RobustnessSignals
    composite_score: float
    allocation_metrics: AllocationMetrics | None
    tactical_metrics: TacticalMetrics | None


def to_dict(result: BacktestResult) -> dict[str, Any]:
    return {
        "candidate_id": result.candidate_id,
        "template_id": result.template_id,
        "config": result.config,
        "window": {
            "start": _date_to_string(result.window[0]),
            "end": _date_to_string(result.window[1]),
        },
        "equity_curve": _series_to_records(result.equity_curve, include_drawdown=True),
        "benchmark_curve": _series_to_records(
            result.benchmark_curve,
            include_drawdown="drawdown_pct" in _series_columns(result.benchmark_curve),
        ),
        "drawdown_episodes": [
            _dataclass_to_json_dict(episode) for episode in result.drawdown_episodes
        ],
        "metrics": asdict(result.metrics),
        "robustness": asdict(result.robustness),
        "composite_score": result.composite_score,
        "allocation_metrics": _allocation_metrics_to_dict(result.allocation_metrics),
        "tactical_metrics": _tactical_metrics_to_dict(result.tactical_metrics),
    }


def from_dict(data: dict[str, Any]) -> BacktestResult:
    window = data["window"]
    return BacktestResult(
        candidate_id=data["candidate_id"],
        template_id=data["template_id"],
        config=data["config"],
        window=(_parse_date(window["start"]), _parse_date(window["end"])),
        equity_curve=_records_to_series(data["equity_curve"]),
        benchmark_curve=_records_to_series(data["benchmark_curve"]),
        drawdown_episodes=[
            _drawdown_episode_from_dict(item) for item in data["drawdown_episodes"]
        ],
        metrics=BacktestMetrics(**data["metrics"]),
        robustness=RobustnessSignals(**data["robustness"]),
        composite_score=data["composite_score"],
        allocation_metrics=_allocation_metrics_from_dict(data["allocation_metrics"]),
        tactical_metrics=_tactical_metrics_from_dict(data["tactical_metrics"]),
    )


def _series_to_records(
    series: pd.Series, *, include_drawdown: bool
) -> list[dict[str, Any]]:
    frame = (
        series.to_frame(name="value") if series.name != "value" else series.to_frame()
    )
    records = []
    drawdowns = (
        _drawdowns(series)
        if include_drawdown and "drawdown_pct" not in frame.columns
        else None
    )
    for index, row in frame.iterrows():
        record = {"date": _date_to_string(index), "value": float(row["value"])}
        if "drawdown_pct" in frame.columns:
            record["drawdown_pct"] = float(row["drawdown_pct"])
        elif drawdowns is not None:
            record["drawdown_pct"] = float(drawdowns.loc[index])
        records.append(record)
    return records


def _records_to_series(records: list[dict[str, Any]]) -> pd.Series:
    values = [record["value"] for record in records]
    index = pd.Index([_parse_date(record["date"]) for record in records], name="date")
    return pd.Series(values, index=index, name="value", dtype="float64")


def _drawdowns(series: pd.Series) -> pd.Series:
    values = series.astype("float64")
    return values / values.cummax() - 1.0


def _series_columns(series: pd.Series) -> set[str]:
    if isinstance(series, pd.DataFrame):
        return set(series.columns)
    return {series.name} if series.name is not None else set()


def _allocation_metrics_to_dict(
    metrics: AllocationMetrics | None,
) -> dict[str, Any] | None:
    if metrics is None:
        return None
    return {
        "rebalances": [
            _dataclass_to_json_dict(rebalance) for rebalance in metrics.rebalances
        ],
        "avg_turnover_per_rebalance": metrics.avg_turnover_per_rebalance,
        "max_single_weight": metrics.max_single_weight,
        "holdings_history": [_convert_dates(item) for item in metrics.holdings_history],
    }


def _allocation_metrics_from_dict(
    data: dict[str, Any] | None,
) -> AllocationMetrics | None:
    if data is None:
        return None
    return AllocationMetrics(
        rebalances=[
            Rebalance(
                date=_parse_date(item["date"]),
                weights=item["weights"],
                turnover_pct=item["turnover_pct"],
                cost_paid=item["cost_paid"],
            )
            for item in data["rebalances"]
        ],
        avg_turnover_per_rebalance=data["avg_turnover_per_rebalance"],
        max_single_weight=data["max_single_weight"],
        holdings_history=[_parse_dates(item) for item in data["holdings_history"]],
    )


def _tactical_metrics_to_dict(metrics: TacticalMetrics | None) -> dict[str, Any] | None:
    if metrics is None:
        return None
    data = asdict(metrics)
    data["trades"] = [_dataclass_to_json_dict(trade) for trade in metrics.trades]
    return data


def _tactical_metrics_from_dict(data: dict[str, Any] | None) -> TacticalMetrics | None:
    if data is None:
        return None
    return TacticalMetrics(
        trades=[
            Trade(
                coin_id=item["coin_id"],
                entry_date=_parse_date(item["entry_date"]),
                exit_date=_parse_date(item["exit_date"]),
                entry_price=item["entry_price"],
                exit_price=item["exit_price"],
                pnl_pct=item["pnl_pct"],
                hold_days=item["hold_days"],
                exit_reason=item["exit_reason"],
            )
            for item in data["trades"]
        ],
        n_trades=data["n_trades"],
        win_rate=data["win_rate"],
        avg_win=data["avg_win"],
        avg_loss=data["avg_loss"],
        profit_factor=data["profit_factor"],
        avg_hold_days=data["avg_hold_days"],
        max_consecutive_losses=data["max_consecutive_losses"],
    )


def _drawdown_episode_from_dict(data: dict[str, Any]) -> DrawdownEpisode:
    return DrawdownEpisode(
        peak_date=_parse_date(data["peak_date"]),
        trough_date=_parse_date(data["trough_date"]),
        drawdown_pct=data["drawdown_pct"],
        recovery_date=_parse_date(data["recovery_date"])
        if data["recovery_date"] is not None
        else None,
        peak_to_trough_days=data["peak_to_trough_days"],
        trough_to_recovery_days=data["trough_to_recovery_days"],
    )


def _dataclass_to_json_dict(value: Any) -> dict[str, Any]:
    return _convert_dates(asdict(value))


def _convert_dates(value: Any) -> Any:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _convert_dates(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_convert_dates(item) for item in value]
    return value


def _parse_dates(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: (_parse_date(item) if key.endswith("date") else _parse_dates(item))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_parse_dates(item) for item in value]
    return value


def _date_to_string(value: Any) -> str:
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)
