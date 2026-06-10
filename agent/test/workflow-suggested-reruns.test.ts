import assert from "node:assert/strict";
import test from "node:test";

import { applyOverrides } from "../src/agent/workflow/overrides.ts";
import { buildSuggestedReruns } from "../src/agent/workflow/suggested-reruns.ts";
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

test("every suggested rerun is feasible against the source thesis", () => {
  const suggestions = buildSuggestedReruns(BASE_THESIS);
  assert.ok(suggestions.length > 0);
  for (const s of suggestions) {
    // Must not throw -- feasibility is guaranteed by construction.
    assert.doesNotThrow(() => applyOverrides(BASE_THESIS, s.overrides));
    assert.ok(s.label.length > 0);
    assert.ok(s.rationale.length > 0);
  }
});

test("fewer-assets suggestion pairs a smaller count with weight-cap room", () => {
  const fewer = buildSuggestedReruns(BASE_THESIS).find(
    (s) => s.label === "Fewer assets",
  );
  assert.ok(fewer, "expected a Fewer assets suggestion");
  const min = fewer.overrides.asset_count_min ?? 0;
  const cap = fewer.overrides.max_weight_per_asset ?? 0;
  assert.ok(min < BASE_THESIS.constraints.asset_count_min);
  // The paired cap must cover the non-cash portion across the new count.
  assert.ok(cap * min >= 1 - BASE_THESIS.constraints.max_cash_weight - 1e-9);
});

test("offers more assets only when the universe has room", () => {
  const roomy = buildSuggestedReruns(BASE_THESIS);
  assert.ok(roomy.some((s) => s.label === "More assets"));

  const capped: Thesis = {
    ...BASE_THESIS,
    universe_hints: { ...BASE_THESIS.universe_hints, top_n: 10 },
    constraints: { ...BASE_THESIS.constraints, asset_count_max: 10 },
  };
  assert.ok(!buildSuggestedReruns(capped).some((s) => s.label === "More assets"));
});

test("suggests excluding stablecoins only when they are eligible", () => {
  assert.ok(
    !buildSuggestedReruns(BASE_THESIS).some(
      (s) => s.label === "Exclude stablecoins",
    ),
  );
  const withStables: Thesis = {
    ...BASE_THESIS,
    universe_hints: { ...BASE_THESIS.universe_hints, exclude_stablecoins: false },
  };
  assert.ok(
    buildSuggestedReruns(withStables).some(
      (s) => s.label === "Exclude stablecoins",
    ),
  );
});

test("rebalance suggestion steps to a less-frequent cadence and stops at quarterly", () => {
  const monthly = buildSuggestedReruns(BASE_THESIS).find((s) =>
    s.label.startsWith("Rebalance"),
  );
  assert.equal(monthly?.overrides.rebalance_frequency, "quarterly");

  const quarterly: Thesis = { ...BASE_THESIS, rebalance_frequency: "quarterly" };
  assert.ok(
    !buildSuggestedReruns(quarterly).some((s) => s.label.startsWith("Rebalance")),
  );
});

test("does not suggest lowering drawdown when already tight", () => {
  const tight: Thesis = {
    ...BASE_THESIS,
    constraints: { ...BASE_THESIS.constraints, max_drawdown: 0.12 },
  };
  assert.ok(
    !buildSuggestedReruns(tight).some((s) => s.label === "Lower max drawdown"),
  );
});
