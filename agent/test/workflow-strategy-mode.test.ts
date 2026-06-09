import assert from "node:assert/strict";
import test from "node:test";

import {
  DEFAULT_ALLOWED_SIDES,
  DEFAULT_EXECUTION_MODE,
  DEFAULT_STRATEGY_MODE,
  ThesisValidationError,
  resolveAllowedSides,
  resolveExecutionMode,
  resolveStrategyMode,
  validateThesis,
  type Thesis,
} from "../src/agent/workflow/state.ts";

function baseThesis(overrides: Partial<Thesis> = {}): Thesis {
  return {
    objective: "balanced_growth",
    horizon_days: 365,
    weight_mode: "percentage",
    universe_hints: {
      top_n: 25,
      exclude_stablecoins: true,
      exclude_wrapped: true,
    },
    constraints: {
      max_weight_per_asset: 0.2,
      max_cash_weight: 0.1,
      max_drawdown: 0.35,
      asset_count_min: 5,
      asset_count_max: 10,
    },
    rebalance_frequency: "monthly",
    interpretation_notes: "base",
    ...overrides,
  };
}

test("resolve* helpers return defaults when mode fields are unset", () => {
  const thesis = baseThesis();
  assert.equal(resolveStrategyMode(thesis), DEFAULT_STRATEGY_MODE);
  assert.equal(resolveAllowedSides(thesis), DEFAULT_ALLOWED_SIDES);
  assert.equal(resolveExecutionMode(thesis), DEFAULT_EXECUTION_MODE);
  assert.equal(DEFAULT_STRATEGY_MODE, "basket_allocation");
  assert.equal(DEFAULT_ALLOWED_SIDES, "long_only");
});

test("resolve* helpers return the set value when present", () => {
  const thesis = baseThesis({
    strategy_mode: "momentum_rotation",
    allowed_sides: "long_short",
    execution_mode: "wallet_direct",
  });
  assert.equal(resolveStrategyMode(thesis), "momentum_rotation");
  assert.equal(resolveAllowedSides(thesis), "long_short");
  assert.equal(resolveExecutionMode(thesis), "wallet_direct");
});

test("single_asset relaxes the basket coverage rule for a 1-asset book", () => {
  // 1 asset at a 20% per-asset cap would FAIL the basket coverage rule
  // (0.2 < 0.9), but single_asset fills the whole book so it is feasible.
  const thesis = baseThesis({
    strategy_mode: "single_asset",
    constraints: {
      max_weight_per_asset: 0.2,
      max_cash_weight: 0,
      max_drawdown: 0.5,
      asset_count_min: 1,
      asset_count_max: 1,
    },
    target_coin_id: "bitcoin",
  });
  assert.doesNotThrow(() => validateThesis(thesis));
});

test("single_asset requires the asset count to actually be 1", () => {
  const thesis = baseThesis({
    strategy_mode: "single_asset",
    constraints: { ...baseThesis().constraints, asset_count_min: 1, asset_count_max: 5 },
  });
  assert.throws(
    () => validateThesis(thesis),
    (error: unknown) => {
      assert.ok(error instanceof ThesisValidationError);
      assert.match(error.message, /single_asset/);
      return true;
    },
  );
});

test("basket modes still enforce the coverage inequality", () => {
  // Default (basket) thesis with 1 asset at 20% cap and 10% cash is infeasible.
  const thesis = baseThesis({
    constraints: { ...baseThesis().constraints, asset_count_min: 1, asset_count_max: 1 },
  });
  assert.throws(() => validateThesis(thesis), ThesisValidationError);
});

test("validateThesis rejects an unknown strategy_mode", () => {
  const thesis = baseThesis();
  (thesis as unknown as Record<string, unknown>).strategy_mode = "moon";
  assert.throws(() => validateThesis(thesis), ThesisValidationError);
});

test("validateThesis accepts explicit leg fields", () => {
  const thesis = baseThesis({
    strategy_mode: "pair_trade",
    allowed_sides: "long_short",
    constraints: { ...baseThesis().constraints, asset_count_min: 2, asset_count_max: 2, max_weight_per_asset: 0.6 },
    long_coin_ids: ["ethereum"],
    short_coin_ids: ["bitcoin"],
  });
  assert.doesNotThrow(() => validateThesis(thesis));
});
