"""Stable helpers exposed to generated research code."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt


_ARTIFACTS_DIR: Path | None = None
_ARTIFACTS: list[dict[str, Any]] = []


def configure_artifacts(path: str | Path) -> None:
    global _ARTIFACTS_DIR, _ARTIFACTS
    _ARTIFACTS_DIR = Path(path)
    _ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    _ARTIFACTS = []


def save_chart(name: str, figure: Any | None = None) -> str:
    """Save a matplotlib chart into the research scratch directory."""
    if _ARTIFACTS_DIR is None:
        raise RuntimeError("research_api artifacts are not configured")
    safe_name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in name).strip("._")
    if not safe_name:
        safe_name = "chart.png"
    if not safe_name.lower().endswith(".png"):
        safe_name = f"{safe_name}.png"
    path = _ARTIFACTS_DIR / safe_name
    (figure or plt.gcf()).savefig(path, bbox_inches="tight")
    _ARTIFACTS.append({"name": safe_name, "path": str(path), "bytes": path.stat().st_size})
    return str(path)


def artifacts() -> list[dict[str, Any]]:
    return list(_ARTIFACTS)


__all__ = ["artifacts", "configure_artifacts", "save_chart"]
