from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Literal, Protocol, TypedDict

import pandas as pd


@dataclass(frozen=True)
class TemplateMetadata:
    id: str
    category: Literal["allocation", "tactical"]
    preferred_factors: list[str]
    default_universe: dict
    min_history_days: int
    composite_formula: str
    slot_schema: dict


class AllocationPlan(TypedDict):
    """Output of allocation templates. Consumed by the backtest engine."""

    holdings: dict[date, dict[str, float]]
    rebalance_dates: list[date]


class SignalPlan(TypedDict):
    """Output of tactical templates. Consumed by the backtest engine."""

    signals: dict[str, pd.Series]
    sizing: dict[str, float]
    exit_rule: str


class BaseTemplate(Protocol):
    METADATA: TemplateMetadata

    def validate_config(self, config: dict) -> None:
        """Raise ValueError if config doesn't satisfy METADATA.slot_schema."""
        ...

    def build(
        self,
        universe: pd.DataFrame,
        prices: dict[str, pd.DataFrame],
        config: dict,
        window: tuple[date, date],
    ) -> AllocationPlan | SignalPlan:
        """Allocation templates return AllocationPlan; tactical return SignalPlan."""
        ...


def validate_against_slot_schema(config: dict, slot_schema: dict) -> None:
    """Validate a template config against the plan's lightweight slot schema."""
    properties = slot_schema.get("properties", slot_schema)
    required = set(slot_schema.get("required", ()))

    for key in config:
        if key not in properties:
            raise ValueError(f"Unknown config key: {key}")

    for key, schema in properties.items():
        if schema.get("required", False):
            required.add(key)

    for key in required:
        if key not in config:
            raise ValueError(f"Missing required config key: {key}")

    for key, value in config.items():
        schema = properties[key]
        _validate_slot_value(key, value, schema)


def _validate_slot_value(key: str, value: Any, schema: dict) -> None:
    expected_type = schema.get("type")

    if expected_type == "int":
        if type(value) is not int:
            raise ValueError(f"Invalid type for config key {key}: expected int")
        _validate_numeric_range(key, value, schema)
    elif expected_type == "float":
        if type(value) not in (int, float):
            raise ValueError(f"Invalid type for config key {key}: expected float")
        _validate_numeric_range(key, float(value), schema)
    elif expected_type == "registry":
        if not isinstance(value, str):
            raise ValueError(f"Invalid type for config key {key}: expected registry ID")
    elif expected_type == "string":
        if not isinstance(value, str):
            raise ValueError(f"Invalid type for config key {key}: expected string")
    elif expected_type == "object":
        if not isinstance(value, dict):
            raise ValueError(f"Invalid type for config key {key}: expected object")
    else:
        raise ValueError(
            f"Unsupported slot schema type for config key {key}: {expected_type}"
        )


def _validate_numeric_range(key: str, value: int | float, schema: dict) -> None:
    minimum = schema.get("min")
    maximum = schema.get("max")

    if minimum is not None and value < minimum:
        raise ValueError(f"Config key {key} is below minimum {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"Config key {key} is above maximum {maximum}")
