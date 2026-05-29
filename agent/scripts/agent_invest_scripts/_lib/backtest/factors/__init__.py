from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable

import pandas as pd

TRADING_DAYS_PER_YEAR = 365.0
SECTOR_TAGS_PATH = Path(__file__).with_name("sector_tags.json")

ComputeFn = Callable[[pd.DataFrame, date | None], float | None]


@dataclass(frozen=True)
class FactorMetadata:
    id: str
    category: str
    description: str
    formula: str
    window_days: int | None
    units: str
    direction_better: str | None
    min_history_days: int
    sample_size_rule: str | None
    compute: ComputeFn
    sector_tags_path: Path | None = None

    def compute_with_flags(
        self, prices: pd.DataFrame, as_of: date | None = None
    ) -> tuple[float | None, bool]:
        history_days = _history_days(prices, as_of)
        value = self.compute(prices, as_of)
        low_sample = history_days < self.min_history_days
        report = prices.attrs.get("recovery_report", {})
        if self.id == "recovery_rate" and report.get("n_drawdown_episodes", 0) < 3:
            low_sample = True
        if (
            self.id == "pct_horizon_windows_positive"
            and report.get("n_windows", 0) < 365
        ):
            low_sample = True
        return value, low_sample

    @property
    def window_days_required(self) -> int:
        return self.window_days or self.min_history_days


def _factor(
    id: str,
    category: str,
    description: str,
    formula: str,
    window_days: int | None,
    units: str,
    direction_better: str | None,
    min_history_days: int,
    sample_size_rule: str | None,
    compute: ComputeFn,
) -> FactorMetadata:
    return FactorMetadata(
        id=id,
        category=category,
        description=description,
        formula=formula,
        window_days=window_days,
        units=units,
        direction_better=direction_better,
        min_history_days=min_history_days,
        sample_size_rule=sample_size_rule,
        compute=lambda prices, as_of=None: _guard(
            prices, as_of, min_history_days, compute
        ),
        sector_tags_path=SECTOR_TAGS_PATH,
    )


def _guard(
    prices: pd.DataFrame, as_of: date | None, min_history_days: int, compute: ComputeFn
) -> float | None:
    frame = _frame(prices, as_of)
    if _history_days(frame) < min_history_days:
        return None
    value = compute(frame, None)
    if value is None or not math.isfinite(float(value)):
        return None
    return float(value)


def _frame(prices: pd.DataFrame, as_of: date | None = None) -> pd.DataFrame:
    if "date" not in prices or "price" not in prices:
        raise ValueError("prices must include date and price columns")
    frame = prices.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.sort_values("date")
    if as_of is not None:
        frame = frame[frame["date"].dt.date <= as_of]
    return frame[frame["price"].notna()]


def _history_days(prices: pd.DataFrame, as_of: date | None = None) -> int:
    frame = _frame(prices, as_of) if "date" in prices and "price" in prices else prices
    if frame.empty:
        return 0
    return int((frame["date"].iloc[-1] - frame["date"].iloc[0]).days) + 1


def _series(frame: pd.DataFrame, column: str = "price") -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce").dropna().astype(float)


def _window(frame: pd.DataFrame, days: int) -> pd.DataFrame:
    cutoff = frame["date"].iloc[-1] - pd.Timedelta(days=days - 1)
    return frame[frame["date"] >= cutoff]


def _return(days: int) -> ComputeFn:
    def compute(frame: pd.DataFrame, _: date | None) -> float | None:
        w = _window(frame, days)
        price = _series(w)
        return (
            price.iloc[-1] / price.iloc[0] - 1.0
            if len(price) >= 2 and price.iloc[0]
            else None
        )

    return compute


def _daily_returns(frame: pd.DataFrame, days: int | None = None) -> pd.Series:
    source = _window(frame, days) if days else frame
    return _series(source).pct_change().dropna()


def _vol(days: int) -> ComputeFn:
    return lambda frame, _: _daily_returns(frame, days).std(ddof=0) * math.sqrt(
        TRADING_DAYS_PER_YEAR
    )


def _downside(frame: pd.DataFrame, _: date | None) -> float | None:
    returns = _daily_returns(frame, 180)
    downside = returns[returns < 0]
    return (
        downside.std(ddof=0) * math.sqrt(TRADING_DAYS_PER_YEAR)
        if len(downside)
        else None
    )


def _atr_30d(frame: pd.DataFrame, _: date | None) -> float | None:
    w = _window(frame, 30)
    high = _series(w, "high") if "high" in w else _series(w)
    low = _series(w, "low") if "low" in w else _series(w)
    close = _series(w)
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    return tr.dropna().mean()


def _cagr(price: pd.Series, days: int) -> float | None:
    if len(price) < 2 or price.iloc[0] <= 0 or price.iloc[-1] <= 0:
        return None
    return (price.iloc[-1] / price.iloc[0]) ** (TRADING_DAYS_PER_YEAR / days) - 1.0


def _sharpe(days: int) -> ComputeFn:
    def compute(frame: pd.DataFrame, _: date | None) -> float | None:
        price = _series(_window(frame, days))
        vol = _daily_returns(frame, days).std(ddof=0) * math.sqrt(TRADING_DAYS_PER_YEAR)
        cagr = _cagr(price, days)
        return cagr / vol if cagr is not None and vol else None

    return compute


def _sortino(frame: pd.DataFrame, _: date | None) -> float | None:
    returns = _daily_returns(frame, 365)
    downside = returns[returns < 0]
    dev = (
        downside.std(ddof=0) * math.sqrt(TRADING_DAYS_PER_YEAR)
        if len(downside)
        else 0.0
    )
    cagr = _cagr(_series(_window(frame, 365)), 365)
    return cagr / dev if cagr is not None and dev else None


def _drawdown(frame: pd.DataFrame, days: int | None = None) -> pd.Series:
    price = _series(_window(frame, days) if days else frame)
    return price / price.cummax() - 1.0


def _max_drawdown(days: int | None) -> ComputeFn:
    return lambda frame, _: _drawdown(frame, days).min()


def _calmar(frame: pd.DataFrame, _: date | None) -> float | None:
    cagr = _cagr(_series(_window(frame, 365)), 365)
    mdd = _drawdown(frame, 365).min()
    return cagr / abs(mdd) if cagr is not None and mdd < 0 else None


def _recovery_value(key: str) -> ComputeFn:
    def compute(frame: pd.DataFrame, _: date | None) -> float | None:
        report = frame.attrs.get("recovery_report", {})
        episodes = report.get("n_drawdown_episodes")
        if key == "recovery_rate" and (episodes is None or episodes < 3):
            return None
        if key == "pct_horizon_windows_positive" and report.get("n_windows", 0) < 365:
            return None
        return report.get(key)

    return compute


def _pct_above_sma(days: int) -> ComputeFn:
    def compute(frame: pd.DataFrame, _: date | None) -> float | None:
        price = _series(_window(frame, days))
        sma = price.mean()
        return price.iloc[-1] / sma - 1.0 if sma else None

    return compute


def _sma_50_slope(frame: pd.DataFrame, _: date | None) -> float | None:
    price = _series(frame)
    sma = price.rolling(50).mean().dropna().tail(50)
    if len(sma) < 2 or sma.mean() == 0:
        return None
    slope = (sma.iloc[-1] - sma.iloc[0]) / (len(sma) - 1)
    return slope / sma.mean()


def _adx_30d(frame: pd.DataFrame, _: date | None) -> float | None:
    w = _window(frame, 31)
    high = _series(w, "high") if "high" in w else _series(w)
    low = _series(w, "low") if "low" in w else _series(w)
    close = _series(w)
    up = high.diff()
    down = -low.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    tr = pd.concat(
        [(high - low), (high - close.shift(1)).abs(), (low - close.shift(1)).abs()],
        axis=1,
    ).max(axis=1)
    plus_di = 100 * plus_dm.rolling(30).sum() / tr.rolling(30).sum()
    minus_di = 100 * minus_dm.rolling(30).sum() / tr.rolling(30).sum()
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di)) * 100
    return dx.dropna().iloc[-1] / 100.0 if len(dx.dropna()) else None


def _golden_cross(frame: pd.DataFrame, _: date | None) -> float | None:
    price = _series(frame)
    return float(price.tail(50).mean() > price.tail(200).mean())


def _z_score(frame: pd.DataFrame, _: date | None) -> float | None:
    price = _series(_window(frame, 30))
    std = price.std(ddof=0)
    return (price.iloc[-1] - price.mean()) / std if std else None


def _bollinger(frame: pd.DataFrame, _: date | None) -> float | None:
    price = _series(_window(frame, 20))
    std = price.std(ddof=0)
    lower = price.mean() - 2 * std
    upper = price.mean() + 2 * std
    return (price.iloc[-1] - lower) / (upper - lower) if upper != lower else None


def _rsi(frame: pd.DataFrame, _: date | None) -> float | None:
    delta = _series(_window(frame, 15)).diff().dropna()
    gains = delta.clip(lower=0).mean()
    losses = -delta.clip(upper=0).mean()
    if losses == 0:
        return 1.0
    return (100 - (100 / (1 + gains / losses))) / 100.0


def _halflife(frame: pd.DataFrame, _: date | None) -> float | None:
    price = _series(frame)
    residual = (price - price.rolling(90).mean()).dropna().tail(90)
    if len(residual) < 3:
        return None
    x = residual.shift(1).dropna()
    y = residual.loc[x.index]
    beta = x.cov(y) / x.var() if x.var() else None
    if beta is None or beta <= 0 or beta >= 1:
        return None
    return -math.log(2) / math.log(beta)


def _range_bound(frame: pd.DataFrame, _: date | None) -> float | None:
    price = _series(_window(frame, 180))
    path = price.diff().abs().sum()
    return 1.0 - abs(price.iloc[-1] - price.iloc[0]) / path if path else None


def _high_proximity(frame: pd.DataFrame, _: date | None) -> float | None:
    w = _window(frame, 60)
    high = _series(w, "high") if "high" in w else _series(w)
    return _series(w).iloc[-1] / high.max() if high.max() else None


def _consolidation(frame: pd.DataFrame, _: date | None) -> float | None:
    price = _series(_window(frame, 60))
    return 1.0 - (price.max() - price.min()) / price.mean() if price.mean() else None


def _days_since_high(frame: pd.DataFrame, _: date | None) -> float | None:
    price = _series(_window(frame, 60))
    return float(len(price) - 1 - price.reset_index(drop=True).idxmax())


def _volume_surge(frame: pd.DataFrame, _: date | None) -> float | None:
    if "volume" not in frame:
        return None
    volume = _series(_window(frame, 30), "volume")
    avg = volume.iloc[:-1].mean()
    return volume.iloc[-1] / avg if avg else None


def _benchmark_return(frame: pd.DataFrame, column: str) -> float | None:
    if column not in frame:
        return None
    w = _window(frame[["date", column]].rename(columns={column: "price"}), 90)
    price = _series(w)
    return (
        price.iloc[-1] / price.iloc[0] - 1.0
        if len(price) >= 2 and price.iloc[0]
        else None
    )


def _rs(column: str) -> ComputeFn:
    return (
        lambda frame, _: _return(90)(frame, None) - _benchmark_return(frame, column)
        if _benchmark_return(frame, column) is not None
        else None
    )


def _beta_to_btc(frame: pd.DataFrame, _: date | None) -> float | None:
    if "btc_price" not in frame:
        return None
    w = _window(frame, 365)
    asset = _series(w).pct_change()
    btc = _series(w, "btc_price").pct_change()
    joined = pd.concat([asset, btc], axis=1).dropna()
    variance = joined.iloc[:, 1].var(ddof=0)
    return joined.iloc[:, 0].cov(joined.iloc[:, 1]) / variance if variance else None


def _corr_btc(frame: pd.DataFrame, _: date | None) -> float | None:
    if "btc_price" not in frame:
        return None
    w = _window(frame, 90)
    joined = pd.concat(
        [_series(w).pct_change(), _series(w, "btc_price").pct_change()], axis=1
    ).dropna()
    return joined.iloc[:, 0].corr(joined.iloc[:, 1]) if len(joined) >= 2 else None


def _idio_vol(frame: pd.DataFrame, _: date | None) -> float | None:
    beta = _beta_to_btc(frame, None)
    if beta is None or "btc_price" not in frame:
        return None
    w = _window(frame, 365)
    residual = _series(w).pct_change() - beta * _series(w, "btc_price").pct_change()
    return residual.dropna().std(ddof=0) * math.sqrt(TRADING_DAYS_PER_YEAR)


def _avg_volume_usd(frame: pd.DataFrame, _: date | None) -> float | None:
    w = _window(frame, 30)
    if "volume_usd" in w:
        return _series(w, "volume_usd").mean()
    if "volume" in w:
        return (_series(w, "volume") * _series(w)).mean()
    return None


def _liquidity(frame: pd.DataFrame, _: date | None) -> float | None:
    avg = _avg_volume_usd(frame, None)
    return min(9.0, max(4.0, math.log10(avg))) if avg and avg > 0 else None


def _spread(frame: pd.DataFrame, _: date | None) -> float | None:
    returns = _daily_returns(frame, 30)
    cov = returns.iloc[1:].cov(returns.shift(1).dropna()) if len(returns) > 2 else None
    return 2 * math.sqrt(max(0.0, -cov)) * 10_000 if cov is not None else None


def _latest_col(column: str) -> ComputeFn:
    return (
        lambda frame, _: _series(frame, column).iloc[-1]
        if column in frame and len(_series(frame, column))
        else None
    )


def _circulating(frame: pd.DataFrame, _: date | None) -> float | None:
    if "circulating_supply" not in frame or "max_supply" not in frame:
        return None
    max_supply = _series(frame, "max_supply").iloc[-1]
    return (
        _series(frame, "circulating_supply").iloc[-1] / max_supply
        if max_supply
        else None
    )


def _days_since_listing(frame: pd.DataFrame, _: date | None) -> float | None:
    return float((frame["date"].iloc[-1] - frame["date"].iloc[0]).days)


def _momentum_12_1(frame: pd.DataFrame, _: date | None) -> float | None:
    end = frame["date"].iloc[-1] - pd.Timedelta(days=30)
    start = frame["date"].iloc[-1] - pd.Timedelta(days=365)
    w = frame[(frame["date"] >= start) & (frame["date"] <= end)]
    price = _series(w)
    return (
        price.iloc[-1] / price.iloc[0] - 1.0
        if len(price) >= 2 and price.iloc[0]
        else None
    )


def _momentum_accel(frame: pd.DataFrame, _: date | None) -> float | None:
    r30 = _return(30)(frame, None)
    r90 = _return(90)(frame, None)
    return r30 - r90 if r30 is not None and r90 is not None else None


FACTORS: dict[str, FactorMetadata] = {
    "return_90d": _factor(
        "return_90d",
        "return",
        "90 day return",
        "price[-1] / price[-90] - 1",
        90,
        "pct",
        "high",
        90,
        None,
        _return(90),
    ),
    "return_180d": _factor(
        "return_180d",
        "return",
        "180 day return",
        "price[-1] / price[-180] - 1",
        180,
        "pct",
        "high",
        180,
        None,
        _return(180),
    ),
    "return_365d": _factor(
        "return_365d",
        "return",
        "365 day return",
        "price[-1] / price[-365] - 1",
        365,
        "pct",
        "high",
        365,
        None,
        _return(365),
    ),
    "return_730d": _factor(
        "return_730d",
        "return",
        "730 day return",
        "price[-1] / price[-730] - 1",
        730,
        "pct",
        "high",
        730,
        None,
        _return(730),
    ),
    "roc_7d": _factor(
        "roc_7d",
        "return",
        "7 day rate of change",
        "price[-1] / price[-7] - 1",
        7,
        "pct",
        "high",
        7,
        None,
        _return(7),
    ),
    "roc_30d": _factor(
        "roc_30d",
        "return",
        "30 day rate of change",
        "price[-1] / price[-30] - 1",
        30,
        "pct",
        "high",
        30,
        None,
        _return(30),
    ),
    "roc_90d": _factor(
        "roc_90d",
        "return",
        "90 day rate of change",
        "price[-1] / price[-90] - 1",
        90,
        "pct",
        "high",
        90,
        None,
        _return(90),
    ),
    "12_minus_1_momentum": _factor(
        "12_minus_1_momentum",
        "return",
        "12 minus 1 momentum",
        "return(t-365 -> t-30)",
        335,
        "pct",
        "high",
        365,
        None,
        _momentum_12_1,
    ),
    "momentum_acceleration": _factor(
        "momentum_acceleration",
        "return",
        "Momentum acceleration",
        "roc_30d - roc_90d",
        90,
        "pct",
        "high",
        90,
        None,
        _momentum_accel,
    ),
    "volatility_90d": _factor(
        "volatility_90d",
        "risk",
        "90 day annualized volatility",
        "annualized stdev daily returns",
        90,
        "ratio",
        "low",
        90,
        None,
        _vol(90),
    ),
    "volatility_180d": _factor(
        "volatility_180d",
        "risk",
        "180 day annualized volatility",
        "annualized stdev daily returns",
        180,
        "ratio",
        "low",
        180,
        None,
        _vol(180),
    ),
    "volatility_365d": _factor(
        "volatility_365d",
        "risk",
        "365 day annualized volatility",
        "annualized stdev daily returns",
        365,
        "ratio",
        "low",
        365,
        None,
        _vol(365),
    ),
    "atr_30d": _factor(
        "atr_30d",
        "risk",
        "30 day average true range",
        "average true range",
        30,
        "usd",
        "low",
        30,
        None,
        _atr_30d,
    ),
    "downside_deviation_180d": _factor(
        "downside_deviation_180d",
        "risk",
        "180 day downside deviation",
        "annualized stdev negative daily returns",
        180,
        "ratio",
        "low",
        180,
        None,
        _downside,
    ),
    "sharpe_180d": _factor(
        "sharpe_180d",
        "risk_adjusted",
        "180 day geometric Sharpe",
        "cagr / volatility",
        180,
        "ratio",
        "high",
        180,
        None,
        _sharpe(180),
    ),
    "sharpe_365d": _factor(
        "sharpe_365d",
        "risk_adjusted",
        "365 day geometric Sharpe",
        "cagr / volatility",
        365,
        "ratio",
        "high",
        365,
        None,
        _sharpe(365),
    ),
    "sortino_365d": _factor(
        "sortino_365d",
        "risk_adjusted",
        "365 day Sortino",
        "cagr / downside deviation",
        365,
        "ratio",
        "high",
        365,
        None,
        _sortino,
    ),
    "calmar_365d": _factor(
        "calmar_365d",
        "risk_adjusted",
        "365 day Calmar",
        "cagr / abs(max_drawdown)",
        365,
        "ratio",
        "high",
        365,
        None,
        _calmar,
    ),
    "max_drawdown_365d": _factor(
        "max_drawdown_365d",
        "drawdown",
        "365 day max drawdown",
        "worst peak-to-trough",
        365,
        "pct",
        "high",
        365,
        None,
        _max_drawdown(365),
    ),
    "max_drawdown_full": _factor(
        "max_drawdown_full",
        "drawdown",
        "Full history max drawdown",
        "worst peak-to-trough",
        None,
        "pct",
        "high",
        2,
        None,
        _max_drawdown(None),
    ),
    "current_drawdown_from_ath": _factor(
        "current_drawdown_from_ath",
        "drawdown",
        "Current drawdown from ATH",
        "price[-1] / max(price) - 1",
        None,
        "pct",
        None,
        2,
        None,
        lambda f, _: _series(f).iloc[-1] / _series(f).max() - 1.0,
    ),
    "time_underwater_pct": _factor(
        "time_underwater_pct",
        "drawdown",
        "Time underwater",
        "fraction of days below previous peak",
        None,
        "pct",
        "low",
        2,
        None,
        lambda f, _: float((_series(f) < _series(f).cummax()).mean()),
    ),
    "days_since_ath": _factor(
        "days_since_ath",
        "drawdown",
        "Days since all-time high",
        "days since last ATH",
        None,
        "days",
        None,
        2,
        None,
        lambda f, _: float(
            len(_series(f)) - 1 - _series(f).reset_index(drop=True).idxmax()
        ),
    ),
    "recovery_rate": _factor(
        "recovery_rate",
        "recovery",
        "Recovery rate",
        "n_recovered / n_episodes",
        None,
        "ratio",
        "high",
        1,
        "null when n_drawdown_episodes < 3",
        _recovery_value("recovery_rate"),
    ),
    "median_recovery_days": _factor(
        "median_recovery_days",
        "recovery",
        "Median recovery days",
        "median peak-to-peak duration",
        None,
        "days",
        "low",
        1,
        None,
        _recovery_value("median_recovery_days"),
    ),
    "p90_recovery_days": _factor(
        "p90_recovery_days",
        "recovery",
        "P90 recovery days",
        "90th percentile peak-to-peak duration",
        None,
        "days",
        "low",
        1,
        None,
        _recovery_value("p90_recovery_days"),
    ),
    "pct_horizon_windows_positive": _factor(
        "pct_horizon_windows_positive",
        "recovery",
        "Positive horizon windows",
        "positive rolling horizon windows",
        None,
        "ratio",
        "high",
        1,
        "null when n_windows < 365",
        _recovery_value("pct_horizon_windows_positive"),
    ),
    "worst_horizon_loss": _factor(
        "worst_horizon_loss",
        "recovery",
        "Worst horizon loss",
        "worst rolling horizon return",
        None,
        "pct",
        "high",
        1,
        None,
        _recovery_value("worst_horizon_loss"),
    ),
    "n_drawdown_episodes": _factor(
        "n_drawdown_episodes",
        "recovery",
        "Drawdown episode count",
        "count episodes >= min_drawdown_pct",
        None,
        "count",
        None,
        1,
        None,
        _recovery_value("n_drawdown_episodes"),
    ),
    "pct_above_sma_50d": _factor(
        "pct_above_sma_50d",
        "trend",
        "Percent above SMA 50",
        "(price - sma_50) / sma_50",
        50,
        "pct",
        "high",
        50,
        None,
        _pct_above_sma(50),
    ),
    "pct_above_sma_200d": _factor(
        "pct_above_sma_200d",
        "trend",
        "Percent above SMA 200",
        "(price - sma_200) / sma_200",
        200,
        "pct",
        "high",
        200,
        None,
        _pct_above_sma(200),
    ),
    "sma_50_slope": _factor(
        "sma_50_slope",
        "trend",
        "SMA 50 slope",
        "linear slope of SMA(50) over 50d",
        99,
        "pct/day",
        "high",
        99,
        None,
        _sma_50_slope,
    ),
    "adx_30d": _factor(
        "adx_30d",
        "trend",
        "30 day ADX",
        "average directional index",
        30,
        "ratio",
        "high",
        31,
        None,
        _adx_30d,
    ),
    "golden_cross_active": _factor(
        "golden_cross_active",
        "trend",
        "Golden cross active",
        "1 if sma_50 > sma_200 else 0",
        200,
        "bool",
        "high",
        200,
        None,
        _golden_cross,
    ),
    "current_z_score": _factor(
        "current_z_score",
        "mean_reversion",
        "Current z-score",
        "(price - mean) / stdev",
        30,
        "z_score",
        None,
        30,
        None,
        _z_score,
    ),
    "bollinger_pct_b": _factor(
        "bollinger_pct_b",
        "mean_reversion",
        "Bollinger percent B",
        "(price - lower_band) / (upper_band - lower_band)",
        20,
        "ratio",
        None,
        20,
        None,
        _bollinger,
    ),
    "rsi_14d": _factor(
        "rsi_14d",
        "mean_reversion",
        "14 day RSI",
        "relative strength index",
        14,
        "ratio",
        None,
        15,
        None,
        _rsi,
    ),
    "mean_reversion_halflife": _factor(
        "mean_reversion_halflife",
        "mean_reversion",
        "Mean reversion half-life",
        "-ln(2) / ln(beta)",
        90,
        "days",
        "low",
        180,
        "null when AR(1) beta >= 1",
        _halflife,
    ),
    "range_bound_score": _factor(
        "range_bound_score",
        "mean_reversion",
        "Range bound score",
        "1 - abs(net_drift) / sum(abs(daily moves))",
        180,
        "ratio",
        "high",
        180,
        None,
        _range_bound,
    ),
    "n_day_high_proximity": _factor(
        "n_day_high_proximity",
        "breakout",
        "N-day high proximity",
        "price / max(high, last n days)",
        60,
        "ratio",
        "high",
        60,
        None,
        _high_proximity,
    ),
    "consolidation_score": _factor(
        "consolidation_score",
        "breakout",
        "Consolidation score",
        "1 - (max - min) / sma_close",
        60,
        "ratio",
        "high",
        60,
        None,
        _consolidation,
    ),
    "days_since_n_day_high": _factor(
        "days_since_n_day_high",
        "breakout",
        "Days since N-day high",
        "days since last N-day high",
        60,
        "days",
        "low",
        60,
        None,
        _days_since_high,
    ),
    "volume_surge_ratio": _factor(
        "volume_surge_ratio",
        "breakout",
        "Volume surge ratio",
        "vol[today] / avg(vol, 30d)",
        30,
        "ratio",
        "high",
        30,
        None,
        _volume_surge,
    ),
    "rs_vs_btc": _factor(
        "rs_vs_btc",
        "relative_strength",
        "Relative strength vs BTC",
        "return_coin_90d - return_btc_90d",
        90,
        "pct",
        "high",
        90,
        None,
        _rs("btc_price"),
    ),
    "rs_vs_eth": _factor(
        "rs_vs_eth",
        "relative_strength",
        "Relative strength vs ETH",
        "return_coin_90d - return_eth_90d",
        90,
        "pct",
        "high",
        90,
        None,
        _rs("eth_price"),
    ),
    "beta_to_btc": _factor(
        "beta_to_btc",
        "relative_strength",
        "Beta to BTC",
        "OLS beta daily returns vs BTC",
        365,
        "ratio",
        None,
        365,
        None,
        _beta_to_btc,
    ),
    "correlation_to_btc_90d": _factor(
        "correlation_to_btc_90d",
        "relative_strength",
        "90 day correlation to BTC",
        "Pearson correlation",
        90,
        "ratio",
        "low",
        90,
        None,
        _corr_btc,
    ),
    "idiosyncratic_vol": _factor(
        "idiosyncratic_vol",
        "relative_strength",
        "Idiosyncratic volatility",
        "vol residuals after BTC regression",
        365,
        "ratio",
        None,
        365,
        None,
        _idio_vol,
    ),
    "avg_daily_volume_usd_30d": _factor(
        "avg_daily_volume_usd_30d",
        "volume",
        "Average daily USD volume",
        "mean USD volume",
        30,
        "usd",
        "high",
        30,
        None,
        _avg_volume_usd,
    ),
    "liquidity_score": _factor(
        "liquidity_score",
        "volume",
        "Liquidity score",
        "log10(avg_daily_volume_usd_30d) clipped [4, 9]",
        30,
        "ratio",
        "high",
        30,
        None,
        _liquidity,
    ),
    "bid_ask_spread_estimate": _factor(
        "bid_ask_spread_estimate",
        "volume",
        "Bid-ask spread estimate",
        "Roll estimator over daily returns",
        30,
        "bps",
        "low",
        30,
        None,
        _spread,
    ),
    "market_cap_usd": _factor(
        "market_cap_usd",
        "size",
        "Market cap USD",
        "latest market cap",
        None,
        "usd",
        "high",
        1,
        None,
        _latest_col("market_cap_usd"),
    ),
    "market_cap_rank": _factor(
        "market_cap_rank",
        "size",
        "Market cap rank",
        "source-defined rank",
        None,
        "count",
        "low",
        1,
        None,
        _latest_col("market_cap_rank"),
    ),
    "circulating_supply_pct": _factor(
        "circulating_supply_pct",
        "size",
        "Circulating supply percent",
        "circulating / max_supply",
        None,
        "ratio",
        None,
        1,
        None,
        _circulating,
    ),
    "history_days": _factor(
        "history_days",
        "maturity",
        "History days",
        "days of price data",
        None,
        "days",
        "high",
        1,
        None,
        lambda f, _: float(_history_days(f)),
    ),
    "days_since_listing": _factor(
        "days_since_listing",
        "maturity",
        "Days since listing",
        "days since first observed price",
        None,
        "days",
        "high",
        1,
        None,
        _days_since_listing,
    ),
}
