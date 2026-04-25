"""Ingestion-only modules for scheduled dataset refresh jobs.

These jobs own the parquet snapshots under ``datasets/``. Universe snapshots are
appended by date with same-day reruns replacing the prior snapshot. Time-series
datasets are upserted by ``(coin_id, date)`` so reruns heal partial failures.
"""

from .coingecko import CoinGeckoClient

__all__ = ["CoinGeckoClient"]
