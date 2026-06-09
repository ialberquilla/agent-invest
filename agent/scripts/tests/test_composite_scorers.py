import math

from agent_invest_scripts._lib.backtest.composite_scorers import SCORERS
from agent_invest_scripts._lib.backtest.result import BacktestMetrics


def test_scorer_registry_contains_initial_scorers() -> None:
    assert set(SCORERS) == {
        "long_horizon_composite",
        "trade_count_aware_sharpe",
        "vol_adjusted_return",
    }


def test_long_horizon_composite_returns_weighted_scalar() -> None:
    metrics = _metrics(
        return_365d=0.6,
        recovery_rate=0.75,
        pct_horizon_windows_positive=0.9,
    )

    score = SCORERS["long_horizon_composite"].score(
        metrics,
        [
            {"factor": "return_365d", "direction": "high", "weight": 2.0},
            {"factor": "recovery_rate", "direction": "high", "weight": 1.0},
            {
                "factor": "pct_horizon_windows_positive",
                "direction": "high",
                "weight": 1.0,
            },
        ],
    )

    assert math.isfinite(score)
    assert score == 0.7125


def test_trade_count_aware_sharpe_penalizes_small_trade_counts() -> None:
    metrics = _metrics(sharpe=1.5, n_trades=10)

    score = SCORERS["trade_count_aware_sharpe"].score(
        metrics,
        [{"factor": "sharpe", "direction": "high", "weight": 3.0}],
    )

    assert math.isfinite(score)
    assert score == 0.5


def test_trade_count_aware_sharpe_returns_zero_without_trades() -> None:
    scorer = SCORERS["trade_count_aware_sharpe"]

    assert scorer.score(_metrics(n_trades=None), []) == 0.0
    assert scorer.score(_metrics(n_trades=0), []) == 0.0


def test_vol_adjusted_return_returns_cagr_over_volatility_floor() -> None:
    metrics = _metrics(cagr=0.24, volatility=0.005)

    score = SCORERS["vol_adjusted_return"].score(
        metrics,
        [{"factor": "cagr", "direction": "high", "weight": 4.0}],
    )

    assert math.isfinite(score)
    assert score == 24.0


def _metrics(**overrides: float | int | None) -> BacktestMetrics:
    values = {
        "total_return": 0.3,
        "cagr": 0.2,
        "period_return": 0.3,
        "volatility": 0.4,
        "max_drawdown": -0.25,
        "max_drawdown_duration_days": 30,
        "sharpe": 1.2,
        "sortino": 1.4,
        "calmar": 0.8,
        "total_fees_paid": 10.0,
        "total_slippage": 2.0,
        "return_365d": 0.2,
        "recovery_rate": 0.5,
        "pct_horizon_windows_positive": 0.6,
        "n_trades": 30,
    }
    values.update(overrides)
    return BacktestMetrics(**values)
