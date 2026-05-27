from datetime import date

import pandas as pd

from agent_invest_scripts._lib.backtest import BacktestResult
from agent_invest_scripts._lib.backtest.result import (
    AllocationMetrics,
    BacktestMetrics,
    DrawdownEpisode,
    Rebalance,
    RobustnessSignals,
    TacticalMetrics,
    Trade,
    from_dict,
    to_dict,
)


def test_allocation_result_round_trip_preserves_allocation_metrics() -> None:
    result = _base_result(
        allocation_metrics=AllocationMetrics(
            rebalances=[
                Rebalance(
                    date=date(2024, 1, 8),
                    weights={"btc": 0.7, "eth": 0.3},
                    turnover_pct=0.2,
                    cost_paid=1.5,
                )
            ],
            avg_turnover_per_rebalance=0.2,
            max_single_weight=0.7,
            holdings_history=[
                {"date": date(2024, 1, 8), "weights": {"btc": 0.7, "eth": 0.3}}
            ],
        ),
        tactical_metrics=None,
    )

    encoded = to_dict(result)
    decoded = from_dict(encoded)

    assert encoded["equity_curve"] == [
        {"date": "2024-01-01", "value": 1.0, "drawdown_pct": 0.0},
        {"date": "2024-01-02", "value": 0.9, "drawdown_pct": -0.09999999999999998},
        {"date": "2024-01-03", "value": 1.1, "drawdown_pct": 0.0},
    ]
    assert decoded.equity_curve.index.to_list() == [
        date(2024, 1, 1),
        date(2024, 1, 2),
        date(2024, 1, 3),
    ]
    assert decoded.robustness.sample_size_warning is True
    assert decoded.allocation_metrics is not None
    assert decoded.tactical_metrics is None
    assert (decoded.allocation_metrics is None) != (decoded.tactical_metrics is None)
    assert decoded.allocation_metrics.holdings_history[0]["date"] == date(2024, 1, 8)


def test_tactical_result_round_trip_preserves_nullable_category_metrics() -> None:
    result = _base_result(
        allocation_metrics=None,
        tactical_metrics=TacticalMetrics(
            trades=[
                Trade(
                    coin_id="btc",
                    entry_date=date(2024, 1, 1),
                    exit_date=date(2024, 1, 3),
                    entry_price=100.0,
                    exit_price=110.0,
                    pnl_pct=0.1,
                    hold_days=2,
                    exit_reason="signal_reverse",
                )
            ],
            n_trades=1,
            win_rate=1.0,
            avg_win=0.1,
            avg_loss=0.0,
            profit_factor=1.0,
            avg_hold_days=2.0,
            max_consecutive_losses=0,
        ),
    )

    decoded = from_dict(to_dict(result))

    assert decoded.allocation_metrics is None
    assert decoded.tactical_metrics is not None
    assert (decoded.allocation_metrics is None) != (decoded.tactical_metrics is None)
    assert decoded.tactical_metrics.trades[0].entry_date == date(2024, 1, 1)
    assert decoded.robustness.n_trades == 1


def test_backtest_result_imports_from_backtest_package() -> None:
    assert BacktestResult.__name__ == "BacktestResult"


def _base_result(
    *,
    allocation_metrics: AllocationMetrics | None,
    tactical_metrics: TacticalMetrics | None,
) -> BacktestResult:
    return BacktestResult(
        candidate_id="candidate-1",
        template_id="template-1",
        config={"lookback_days": 30},
        window=(date(2024, 1, 1), date(2024, 1, 3)),
        equity_curve=pd.Series(
            [1.0, 0.9, 1.1],
            index=pd.Index(
                [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)], name="date"
            ),
            name="value",
        ),
        benchmark_curve=pd.Series(
            [1.0, 1.01, 1.02],
            index=pd.Index(
                [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)], name="date"
            ),
            name="value",
        ),
        drawdown_episodes=[
            DrawdownEpisode(
                peak_date=date(2024, 1, 1),
                trough_date=date(2024, 1, 2),
                drawdown_pct=-0.1,
                recovery_date=date(2024, 1, 3),
                peak_to_trough_days=1,
                trough_to_recovery_days=1,
            )
        ],
        metrics=BacktestMetrics(
            total_return=0.1,
            cagr=0.1,
            period_return=0.1,
            volatility=0.2,
            max_drawdown=-0.1,
            max_drawdown_duration_days=1,
            sharpe=1.2,
            sortino=1.3,
            calmar=1.0,
            total_fees_paid=2.0,
            total_slippage=0.5,
        ),
        robustness=RobustnessSignals(
            n_trades=1 if tactical_metrics is not None else None,
            n_rebalances=1 if allocation_metrics is not None else None,
            duration_days=2,
            sample_size_warning=True,
            half_consistency_score=0.5,
            half_consistency_warning=False,
            top_3_trades_pct_of_pnl=0.4 if tactical_metrics is not None else None,
            top_3_months_pct_of_pnl=0.5 if allocation_metrics is not None else None,
            concentration_warning=False,
            worst_180d_return=-0.2,
            worst_90d_drawdown=-0.1,
            worst_window_warning=False,
            correlation_to_benchmark=0.8,
            beta_to_benchmark=1.1,
            excess_return_t_stat=1.6,
            benchmark_coupling_warning=False,
            significance_warning=False,
            survivorship_warning=False,
        ),
        composite_score=0.75,
        allocation_metrics=allocation_metrics,
        tactical_metrics=tactical_metrics,
    )
