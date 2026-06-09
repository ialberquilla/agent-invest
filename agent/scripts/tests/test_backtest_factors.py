from __future__ import annotations

import math

import pandas as pd

from agent_invest_scripts._lib.backtest.factors import (
    FACTORS,
    SECTOR_TAGS_PATH,
    FactorMetadata,
)

EXPECTED_FACTOR_IDS = {
    "return_90d",
    "return_180d",
    "return_365d",
    "return_730d",
    "roc_7d",
    "roc_30d",
    "roc_90d",
    "12_minus_1_momentum",
    "momentum_acceleration",
    "volatility_90d",
    "volatility_180d",
    "volatility_365d",
    "atr_30d",
    "downside_deviation_180d",
    "sharpe_180d",
    "sharpe_365d",
    "sortino_365d",
    "calmar_365d",
    "max_drawdown_365d",
    "max_drawdown_full",
    "current_drawdown_from_ath",
    "time_underwater_pct",
    "days_since_ath",
    "recovery_rate",
    "median_recovery_days",
    "p90_recovery_days",
    "pct_horizon_windows_positive",
    "worst_horizon_loss",
    "n_drawdown_episodes",
    "pct_above_sma_50d",
    "pct_above_sma_200d",
    "sma_50_slope",
    "adx_30d",
    "golden_cross_active",
    "current_z_score",
    "bollinger_pct_b",
    "rsi_14d",
    "mean_reversion_halflife",
    "range_bound_score",
    "n_day_high_proximity",
    "consolidation_score",
    "days_since_n_day_high",
    "volume_surge_ratio",
    "rs_vs_btc",
    "rs_vs_eth",
    "beta_to_btc",
    "correlation_to_btc_90d",
    "idiosyncratic_vol",
    "avg_daily_volume_usd_30d",
    "liquidity_score",
    "bid_ask_spread_estimate",
    "market_cap_usd",
    "market_cap_rank",
    "circulating_supply_pct",
    "history_days",
    "days_since_listing",
}


def _prices(days: int = 800) -> pd.DataFrame:
    dates = pd.date_range("2022-01-01", periods=days, freq="D")
    wave = [math.sin(i / 11) * 4 for i in range(days)]
    price = pd.Series([100 + i * 0.08 + wave[i] for i in range(days)])
    return pd.DataFrame(
        {
            "date": dates,
            "price": price,
            "high": price * 1.02,
            "low": price * 0.98,
            "volume": [1_000 + i % 50 for i in range(days)],
            "volume_usd": [100_000 + i * 10 for i in range(days)],
            "btc_price": pd.Series(
                [200 + i * 0.05 + math.sin(i / 17) for i in range(days)]
            ),
            "eth_price": pd.Series(
                [50 + i * 0.04 + math.sin(i / 13) for i in range(days)]
            ),
            "market_cap_usd": [1_000_000_000 + i for i in range(days)],
            "market_cap_rank": [10 for _ in range(days)],
            "circulating_supply": [8_000_000 for _ in range(days)],
            "max_supply": [10_000_000 for _ in range(days)],
        }
    )


def test_catalog_contains_all_factor_ids_and_metadata() -> None:
    assert set(FACTORS) == EXPECTED_FACTOR_IDS
    assert all(isinstance(factor, FactorMetadata) for factor in FACTORS.values())
    assert all(factor.min_history_days >= 1 for factor in FACTORS.values())
    assert all(
        factor.sector_tags_path == SECTOR_TAGS_PATH for factor in FACTORS.values()
    )
    assert SECTOR_TAGS_PATH.exists()


def test_factors_obey_min_history_days() -> None:
    short = _prices(10)

    assert FACTORS["return_90d"].compute(short) is None
    assert FACTORS["return_90d"].compute_with_flags(short) == (None, True)


def test_compute_with_flags_surfaces_low_sample() -> None:
    value, low_sample = FACTORS["return_90d"].compute_with_flags(_prices(90))

    assert value is not None
    assert low_sample is False


def test_factor_computations_cover_each_category() -> None:
    prices = _prices()
    prices.attrs["recovery_report"] = {
        "n_drawdown_episodes": 4,
        "recovery_rate": 0.75,
        "n_windows": 400,
        "pct_horizon_windows_positive": 0.8,
    }

    ids = [
        "return_90d",
        "volatility_90d",
        "sharpe_180d",
        "max_drawdown_full",
        "recovery_rate",
        "pct_above_sma_50d",
        "current_z_score",
        "range_bound_score",
        "n_day_high_proximity",
        "rs_vs_btc",
        "avg_daily_volume_usd_30d",
        "market_cap_usd",
        "history_days",
    ]

    for factor_id in ids:
        assert FACTORS[factor_id].compute(prices) is not None, factor_id


def test_recovery_sample_size_rules() -> None:
    prices = _prices()
    prices.attrs["recovery_report"] = {
        "n_drawdown_episodes": 2,
        "recovery_rate": 1.0,
        "n_windows": 100,
        "pct_horizon_windows_positive": 1.0,
    }

    assert FACTORS["recovery_rate"].compute(prices) is None
    assert FACTORS["pct_horizon_windows_positive"].compute(prices) is None
    assert FACTORS["recovery_rate"].compute_with_flags(prices) == (None, True)


def test_mean_reversion_halflife_null_when_beta_non_stationary() -> None:
    prices = pd.DataFrame(
        {
            "date": pd.date_range("2022-01-01", periods=220, freq="D"),
            "price": [float(i + 1) for i in range(220)],
        }
    )

    assert FACTORS["mean_reversion_halflife"].compute(prices) is None


def test_range_bound_score_uses_180d_path_length() -> None:
    prices = pd.DataFrame(
        {
            "date": pd.date_range("2022-01-01", periods=180, freq="D"),
            "price": [100 if i % 2 == 0 else 110 for i in range(180)],
        }
    )

    assert FACTORS["range_bound_score"].compute(prices) == 1.0 - 10.0 / 1790.0
