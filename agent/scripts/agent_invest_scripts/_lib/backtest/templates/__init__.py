from .base import (
    AllocationPlan,
    BaseTemplate,
    SignalPlan,
    TemplateMetadata,
    validate_against_slot_schema,
)
from .breakout import Breakout
from .buy_and_hold import BuyAndHold
from .dual_momentum import DualMomentum
from .mean_reversion import MeanReversion
from .momentum import Momentum
from .periodic_rebalance import PeriodicRebalance
from .trend_following import TrendFollowing

TEMPLATES: dict[str, BaseTemplate] = {
    "buy_and_hold": BuyAndHold(),
    "periodic_rebalance": PeriodicRebalance(),
    "momentum": Momentum(),
    "dual_momentum": DualMomentum(),
    "trend_following": TrendFollowing(),
    "mean_reversion": MeanReversion(),
    "breakout": Breakout(),
}

__all__ = [
    "AllocationPlan",
    "BaseTemplate",
    "Breakout",
    "BuyAndHold",
    "DualMomentum",
    "MeanReversion",
    "Momentum",
    "PeriodicRebalance",
    "SignalPlan",
    "TEMPLATES",
    "TemplateMetadata",
    "TrendFollowing",
    "validate_against_slot_schema",
]
