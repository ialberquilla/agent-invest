"""Common utilities shared across pond3r-portfolio Python CLIs."""

from __future__ import annotations

from typing import Any

__all__ = [
    "asset_universe",
    "asset_universe_features",
    "compute_universe_features_as_of",
    "daily_prices",
    "fail",
    "print_json",
    "storage_root",
]


def __getattr__(name: str) -> Any:
    if name in {"fail", "print_json"}:
        from . import cli

        return getattr(cli, name)
    if name in {"asset_universe", "asset_universe_features", "daily_prices"}:
        from . import data

        return getattr(data, name)
    if name == "compute_universe_features_as_of":
        from .features import compute_universe_features_as_of

        return compute_universe_features_as_of
    if name == "storage_root":
        from .storage import storage_root

        return storage_root
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
