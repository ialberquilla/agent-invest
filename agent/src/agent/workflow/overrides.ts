// Deterministic application of StrategyRunOverrides onto an interpreted
// Thesis. This is the "edit the thesis, then re-validate" contract from
// plans/workflow_backtest_improvements.md: overrides never bypass
// validation. An override set that produces an infeasible thesis throws
// OverrideValidationError, which the controller surfaces as the run's
// failure reason rather than silently producing a degenerate run.

import {
  ThesisValidationError,
  validateThesis,
  type StrategyRunOverrides,
  type Thesis,
} from "./state.ts";

export class OverrideValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "OverrideValidationError";
  }
}

// True when at least one override field is set. Lets the controller skip
// the merge+revalidate entirely on a cold run (no overrides), keeping the
// common path identical to pre-PR1 behavior.
export function hasOverrides(
  overrides: StrategyRunOverrides | undefined,
): overrides is StrategyRunOverrides {
  if (!overrides) return false;
  return Object.values(overrides).some((value) => value !== undefined);
}

// Merge overrides onto a copy of the thesis and re-validate. Each field
// falls back to the interpreted value when the override is absent.
// Optional universe_hints fields (top_skip, hand_picked_coin_ids) are
// only written when explicitly provided so an override can't accidentally
// clear them.
export function applyOverrides(
  thesis: Thesis,
  overrides: StrategyRunOverrides,
): Thesis {
  const next: Thesis = {
    ...thesis,
    horizon_days: overrides.horizon_days ?? thesis.horizon_days,
    rebalance_frequency:
      overrides.rebalance_frequency ?? thesis.rebalance_frequency,
    strategy_mode: overrides.strategy_mode ?? thesis.strategy_mode,
    allowed_sides: overrides.allowed_sides ?? thesis.allowed_sides,
    ...(overrides.target_coin_id !== undefined
      ? { target_coin_id: overrides.target_coin_id }
      : {}),
    ...(overrides.long_coin_ids !== undefined
      ? { long_coin_ids: overrides.long_coin_ids }
      : {}),
    ...(overrides.short_coin_ids !== undefined
      ? { short_coin_ids: overrides.short_coin_ids }
      : {}),
    constraints: {
      ...thesis.constraints,
      asset_count_min:
        overrides.asset_count_min ?? thesis.constraints.asset_count_min,
      asset_count_max:
        overrides.asset_count_max ?? thesis.constraints.asset_count_max,
      max_weight_per_asset:
        overrides.max_weight_per_asset ??
        thesis.constraints.max_weight_per_asset,
      max_cash_weight:
        overrides.max_cash_weight ?? thesis.constraints.max_cash_weight,
      max_drawdown: overrides.max_drawdown ?? thesis.constraints.max_drawdown,
    },
    universe_hints: {
      ...thesis.universe_hints,
      top_n: overrides.top_n ?? thesis.universe_hints.top_n,
      exclude_stablecoins:
        overrides.exclude_stablecoins ??
        thesis.universe_hints.exclude_stablecoins,
      exclude_wrapped:
        overrides.exclude_wrapped ?? thesis.universe_hints.exclude_wrapped,
      ...(overrides.top_skip !== undefined
        ? { top_skip: overrides.top_skip }
        : {}),
      ...(overrides.hand_picked_coin_ids !== undefined
        ? { hand_picked_coin_ids: overrides.hand_picked_coin_ids }
        : {}),
    },
  };

  try {
    validateThesis(next);
  } catch (error) {
    const reason =
      error instanceof ThesisValidationError
        ? error.message
        : error instanceof Error
          ? error.message
          : String(error);
    throw new OverrideValidationError(
      `overrides produced an infeasible thesis: ${reason}`,
    );
  }

  return next;
}
