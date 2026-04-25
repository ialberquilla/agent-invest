"""Ingestion-only modules for scheduled dataset refresh jobs.

The agent never imports from this package; it exists for out-of-band dataset
refresh tasks only. Universe snapshots are appended by date with same-day
reruns replacing the prior snapshot, while time-series datasets are upserted by
``(coin_id, date)`` so reruns heal partial failures.
"""

from .coingecko import CoinGeckoClient

__all__ = ["CoinGeckoClient"]
