import assert from "node:assert/strict";
import test from "node:test";

import {
  applyOverrides,
  hasOverrides,
  OverrideValidationError,
} from "../src/agent/workflow/overrides.ts";
import type { Thesis } from "../src/agent/workflow/state.ts";

const BASE_THESIS: Thesis = {
  objective: "balanced_growth",
  horizon_days: 365,
  weight_mode: "percentage",
  universe_hints: {
    top_n: 25,
    market_cap_min_usd: 1_000_000_000,
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
};

test("hasOverrides is false for undefined and all-undefined objects", () => {
  assert.equal(hasOverrides(undefined), false);
  assert.equal(hasOverrides({}), false);
  assert.equal(hasOverrides({ top_n: undefined }), false);
  assert.equal(hasOverrides({ asset_count_max: 4 }), true);
});

test("applyOverrides merges constraint, horizon and universe fields", () => {
  // A "fewer assets" rerun must also give the weight cap enough room to
  // cover the non-cash portion across the smaller count (0.4 * 3 >= 0.9).
  const next = applyOverrides(BASE_THESIS, {
    asset_count_min: 3,
    asset_count_max: 4,
    max_weight_per_asset: 0.4,
    max_drawdown: 0.2,
    horizon_days: 180,
    rebalance_frequency: "weekly",
    top_n: 10,
    top_skip: 2,
    exclude_stablecoins: false,
  });

  assert.equal(next.constraints.asset_count_min, 3);
  assert.equal(next.constraints.asset_count_max, 4);
  assert.equal(next.constraints.max_weight_per_asset, 0.4);
  assert.equal(next.constraints.max_drawdown, 0.2);
  assert.equal(next.horizon_days, 180);
  assert.equal(next.rebalance_frequency, "weekly");
  assert.equal(next.universe_hints.top_n, 10);
  assert.equal(next.universe_hints.top_skip, 2);
  assert.equal(next.universe_hints.exclude_stablecoins, false);
  // Untouched fields are preserved.
  assert.equal(next.constraints.max_cash_weight, 0.1);
  assert.equal(next.universe_hints.exclude_wrapped, true);
});

test("applyOverrides does not mutate the input thesis", () => {
  const snapshot = JSON.stringify(BASE_THESIS);
  applyOverrides(BASE_THESIS, { max_drawdown: 0.25, top_n: 12 });
  assert.equal(JSON.stringify(BASE_THESIS), snapshot);
});

test("applyOverrides re-validates and rejects an infeasible thesis", () => {
  // asset_count_min=1 with the inherited 20% weight cap and 10% cash
  // floor can't fill the non-cash portion: 0.2 * 1 < 1 - 0.1.
  assert.throws(
    () => applyOverrides(BASE_THESIS, { asset_count_min: 1 }),
    (error: unknown) => {
      assert.ok(error instanceof OverrideValidationError);
      assert.match(error.message, /infeasible thesis/);
      return true;
    },
  );
});

test("applyOverrides allows single-asset when the weight cap is raised too", () => {
  const next = applyOverrides(BASE_THESIS, {
    asset_count_min: 1,
    asset_count_max: 1,
    max_weight_per_asset: 1,
    max_cash_weight: 0,
  });
  assert.equal(next.constraints.asset_count_min, 1);
  assert.equal(next.constraints.asset_count_max, 1);
});

test("applyOverrides keeps hand_picked length within asset-count bounds", () => {
  assert.throws(
    () =>
      applyOverrides(BASE_THESIS, {
        hand_picked_coin_ids: ["bitcoin", "ethereum"],
      }),
    OverrideValidationError,
  );
  const ok = applyOverrides(BASE_THESIS, {
    asset_count_min: 2,
    asset_count_max: 2,
    max_weight_per_asset: 0.5,
    max_cash_weight: 0,
    hand_picked_coin_ids: ["bitcoin", "ethereum"],
  });
  assert.deepEqual(ok.universe_hints.hand_picked_coin_ids, [
    "bitcoin",
    "ethereum",
  ]);
});
