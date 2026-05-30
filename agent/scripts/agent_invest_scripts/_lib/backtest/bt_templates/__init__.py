"""``bt``-backed strategy templates.

``TEMPLATES`` mirrors the old ``templates/__init__.py`` registry shape (id ->
object with ``METADATA`` / ``validate_config`` / ``build``) so callers swap the
import path only. ``build`` now returns a ``bt.Strategy``; ``runner.run_recipe``
turns a run into the existing ``BacktestResult`` contract.
"""

from __future__ import annotations

from .base import Recipe, TemplateMetadata, validate_against_slot_schema
from .recipes import RECIPES
from .runner import run_recipe

TEMPLATES: dict[str, Recipe] = dict(RECIPES)

__all__ = [
    "Recipe",
    "TEMPLATES",
    "TemplateMetadata",
    "run_recipe",
    "validate_against_slot_schema",
]
