import polars as pl
import pytest

from agent_invest_scripts._lib.backtest import calculate_summary_metrics


def test_calculate_summary_metrics_returns_expected_keys() -> None:
    frame = pl.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
            "gross_return": [0.0, 0.01, -0.02, 0.03],
            "net_return": [0.0, 0.009, -0.021, 0.029],
            "turnover": [0.0, 0.5, 0.0, 0.25],
            "trading_cost": [0.0, 0.001, 0.0, 0.001],
            "holdings_count": [0, 2, 2, 2],
            "equity": [1.0, 1.009, 0.987811, 1.016457519],
        }
    ).with_columns(pl.col("date").str.to_date())

    metrics = calculate_summary_metrics(frame)

    assert metrics["cagr"] != 0
    assert metrics["max_drawdown"] <= 0
    assert "sharpe_ratio" in metrics
    assert "best_month" in metrics


def test_calculate_summary_metrics_uses_geometric_sharpe() -> None:
    frame = pl.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-31", "2024-03-31"],
            "net_return": [0.0, 0.2, -0.05],
            "turnover": [0.0, 0.0, 0.0],
            "holdings_count": [1, 1, 1],
            "equity": [1.0, 1.2, 1.14],
        }
    ).with_columns(pl.col("date").str.to_date())

    metrics = calculate_summary_metrics(frame)
    expected_cagr = (1.14 / 1.0) ** (365 / 90) - 1
    expected_volatility = pl.Series([0.0, 0.2, -0.05]).std(ddof=0) * (365**0.5)

    assert metrics["sharpe_ratio"] == pytest.approx(
        expected_cagr / expected_volatility, abs=1e-6
    )


def test_calculate_summary_metrics_reports_period_return_separately() -> None:
    frame = pl.DataFrame(
        {
            "date": ["2024-01-01", "2024-03-31"],
            "net_return": [0.0, 0.1],
            "turnover": [0.0, 0.0],
            "holdings_count": [1, 1],
            "equity": [1.0, 1.1],
        }
    ).with_columns(pl.col("date").str.to_date())

    metrics = calculate_summary_metrics(frame)

    assert metrics["period_return"] == pytest.approx(0.1)
    assert metrics["total_return"] == pytest.approx(metrics["period_return"])
    assert metrics["cagr"] == pytest.approx(1.1 ** (365 / 90) - 1)
    assert metrics["cagr"] > metrics["period_return"]
