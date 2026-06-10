import assert from "node:assert/strict";
import test from "node:test";

import { buildRotationProposal } from "../src/agent/workflow/steps/propose_candidates.ts";
import {
  buildBatchInput,
  thesisForValidate,
} from "../src/agent/workflow/steps/run_and_validate.ts";
import { selectTemplates } from "../src/agent/workflow/steps/select_templates.ts";
import { buildSuggestedReruns } from "../src/agent/workflow/suggested-reruns.ts";
import { createSilentStepLogger } from "../src/agent/workflow/logging.ts";
import type { LLMClient } from "../src/agent/workflow/llm.ts";
import {
  LONG_SHORT_FAMILY,
  MOMENTUM_ROTATION_FAMILY,
  validateProposal,
  validateThesis,
  type Thesis,
  type Universe,
  type Window,
} from "../src/agent/workflow/state.ts";

function rotationThesis(overrides: Partial<Thesis> = {}): Thesis {
  return {
    objective: "growth",
    horizon_days: 180,
    weight_mode: "percentage",
    universe_hints: { top_n: 25, exclude_stablecoins: true, exclude_wrapped: true },
    constraints: {
      max_weight_per_asset: 0.2,
      max_cash_weight: 0,
      max_drawdown: 0.5,
      asset_count_min: 6,
      asset_count_max: 9,
    },
    rebalance_frequency: "monthly",
    interpretation_notes: "rotation",
    strategy_mode: "long_short_portfolio",
    allowed_sides: "long_short",
    ...overrides,
  };
}

const UNIVERSE: Universe = {
  coin_ids: ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
  source: "rank_universe",
  effective_filters: { top_n: 10, exclude_stablecoins: true, exclude_wrapped: true },
};

const WINDOW = {
  start: "2022-01-01",
  end: "2024-01-01",
  horizon_days: 180,
  effective: {
    window_length_days: 730,
    target_window_length_days: 1460,
    covered_drawdowns_count: 1,
  },
} as Window;

const throwingLLM: LLMClient = {
  async complete() {
    throw new Error("LLM must not be called for a forced rotation mode");
  },
};

test("feasibility requires a pool and skips the long-basket coverage rule", () => {
  // long_short_portfolio needs >= 4 names; a 0.2 cap that would fail the
  // long-basket coverage rule (0.2 * 6 = 1.2 >= 1, ok here) is irrelevant.
  assert.doesNotThrow(() => validateThesis(rotationThesis()));
  assert.throws(
    () =>
      validateThesis(
        rotationThesis({
          constraints: { ...rotationThesis().constraints, asset_count_min: 3 },
        }),
      ),
    /requires asset_count_min >= 4/,
  );
  // momentum_rotation only needs >= 3.
  assert.doesNotThrow(() =>
    validateThesis(
      rotationThesis({
        strategy_mode: "momentum_rotation",
        allowed_sides: "long_flat",
        constraints: { ...rotationThesis().constraints, asset_count_min: 3 },
      }),
    ),
  );
});

test("select_templates forces the rotation family by mode without the LLM", async () => {
  const ls = await selectTemplates(
    { run_id: "r", thesis: rotationThesis() },
    { llm: throwingLLM, logger: createSilentStepLogger() },
  );
  assert.deepEqual(
    ls.delta.template_selection.selected.map((s) => s.family),
    [LONG_SHORT_FAMILY],
  );

  const mr = await selectTemplates(
    {
      run_id: "r",
      thesis: rotationThesis({ strategy_mode: "momentum_rotation", allowed_sides: "long_flat" }),
    },
    { llm: throwingLLM, logger: createSilentStepLogger() },
  );
  assert.deepEqual(
    mr.delta.template_selection.selected.map((s) => s.family),
    [MOMENTUM_ROTATION_FAMILY],
  );
});

test("buildRotationProposal sweeps the momentum window over a capped pool", () => {
  const thesis = rotationThesis();
  const proposal = buildRotationProposal(
    { run_id: "r", thesis, universe: UNIVERSE, window: WINDOW, attempts: [] },
    LONG_SHORT_FAMILY,
  );
  assert.equal(proposal.candidates.length, 3);
  for (const c of proposal.candidates) {
    assert.equal(c.template_id, LONG_SHORT_FAMILY);
    // pool = min(asset_count_max=9, universe=10) = 9.
    assert.equal(c.select_top, 9);
    assert.ok(typeof c.momentum_lookback === "number");
  }
  assert.doesNotThrow(() =>
    validateProposal(proposal, { thesis, universe: UNIVERSE }),
  );
});

test("rotation batch config carries momentum_lookback, not weighting", () => {
  const thesis = rotationThesis();
  const proposal = buildRotationProposal(
    { run_id: "r", thesis, universe: UNIVERSE, window: WINDOW, attempts: [] },
    LONG_SHORT_FAMILY,
  );
  const batch = buildBatchInput(
    { run_id: "r", thesis, universe: UNIVERSE, window: WINDOW, proposal, attempts: [] },
    1,
  );
  const entry = batch.candidates[0] as {
    select_top: number;
    config: Record<string, unknown>;
  };
  assert.equal(entry.select_top, 9);
  assert.equal(entry.config.weighting, undefined);
  assert.equal(entry.config.momentum_lookback, 30);
  assert.equal(entry.config.rebalance_trigger, "periodic_30d");
});

test("momentum_rotation drops the long-only weight/asset-count rules at validate", () => {
  const thesis = rotationThesis({
    strategy_mode: "momentum_rotation",
    allowed_sides: "long_flat",
  });
  const sent = thesisForValidate(thesis) as { constraints: Record<string, unknown> };
  assert.equal(sent.constraints.max_weight_per_asset, undefined);
  assert.equal(sent.constraints.asset_count_min, undefined);
  assert.ok((sent.constraints.max_drawdown as number) < 0);
});

test("suggested reruns offer a long/short pivot for non-long-short books", () => {
  const basket: Thesis = rotationThesis({
    strategy_mode: undefined,
    allowed_sides: undefined,
    constraints: {
      max_weight_per_asset: 0.2,
      max_cash_weight: 0.1,
      max_drawdown: 0.35,
      asset_count_min: 5,
      asset_count_max: 10,
    },
  });
  assert.ok(
    buildSuggestedReruns(basket).some((s) => s.label === "Try long/short momentum"),
  );
  assert.ok(
    !buildSuggestedReruns(rotationThesis()).some(
      (s) => s.label === "Try long/short momentum",
    ),
  );
});
