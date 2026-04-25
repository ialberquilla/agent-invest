"""Ingestion-only modules for scheduled refresh jobs.

The agent never imports from this package; it exists for out-of-band dataset
refresh tasks only.
"""

from .coingecko import CoinGeckoClient

__all__ = ["CoinGeckoClient"]
