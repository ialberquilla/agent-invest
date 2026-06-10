from __future__ import annotations

from .balanced_5050 import BENCHMARK as BALANCED_5050
from .base import Benchmark, Objective
from .btc_hodl import BENCHMARK as BTC_HODL
from .staked_eth_proxy import BENCHMARK as STAKED_ETH_PROXY
from .usdc import BENCHMARK as USDC


BENCHMARKS: dict[str, Benchmark] = {
    BTC_HODL.ID: BTC_HODL,
    BALANCED_5050.ID: BALANCED_5050,
    USDC.ID: USDC,
    STAKED_ETH_PROXY.ID: STAKED_ETH_PROXY,
}


def benchmark_for(objective: Objective) -> Benchmark:
    for benchmark in BENCHMARKS.values():
        if benchmark.OBJECTIVE == objective:
            return benchmark
    raise ValueError(f"unsupported benchmark objective: {objective}")


def benchmark_coin_ids(objective: Objective) -> tuple[str, ...]:
    """Coins the objective's benchmark needs price history for.

    The window selector folds these into the common-history intersection so a
    backtest window can never start before the benchmark's own data exists
    (otherwise the benchmark curve has leading gaps and the run fails).
    """
    return benchmark_for(objective).REQUIRED_COIN_IDS


__all__ = [
    "BALANCED_5050",
    "BENCHMARKS",
    "BTC_HODL",
    "Benchmark",
    "Objective",
    "STAKED_ETH_PROXY",
    "USDC",
    "benchmark_coin_ids",
    "benchmark_for",
]
