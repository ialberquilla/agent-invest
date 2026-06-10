import assert from "node:assert/strict";
import test from "node:test";

import { buildSingleAssetProposal } from "../src/agent/workflow/steps/propose_candidates.ts";
import { buildBatchInput } from "../src/agent/workflow/steps/run_and_validate.ts";
import { selectTemplates } from "../src/agent/workflow/steps/select_templates.ts";
import { selectUniverse } from "../src/agent/workflow/steps/select_universe.ts";
import { buildSuggestedReruns } from "../src/agent/workflow/suggested-reruns.ts";
import { createSilentStepLogger } from "../src/agent/workflow/logging.ts";
import type { LLMClient } from "../src/agent/workflow/llm.ts";
import {
  validateProposal,
  type Thesis,
  type Universe,
  type Window,
} from "../src/agent/workflow/state.ts";

function singleAssetThesis(overrides: Partial<Thesis> = {}): Thesis {
  return {
    objective: "growth",
    horizon_days: 180,
    weight_mode: "percentage",
    universe_hints: {
      top_n: 25,
      exclude_stablecoins: true,
      exclude_wrapped: true,
    },
    constraints: {
      max_weight_per_asset: 1,
      max_cash_weight: 0,
      max_drawdown: 0.5,
      asset_count_min: 1,
      asset_count_max: 1,
    },
    rebalance_frequency: "weekly",
    interpretation_notes: "single asset",
    strategy_mode: "single_asset",
    target_coin_id: "bitcoin",
    ...overrides,
  };
}

const UNIVERSE: Universe = {
  coin_ids: ["bitcoin"],
  source: "hand_picked",
  effective_filters: { top_n: 1, exclude_stablecoins: true, exclude_wrapped: true },
};

const WINDOW: Window = {
  start: "2022-01-01",
  end: "2024-01-01",
  horizon_days: 180,
  effective: {
    window_length_days: 730,
    target_window_length_days: 1460,
    covered_drawdowns_count: 1,
  },
} as Window;

// LLM that throws if ever called -- single_asset must not hit the model.
const throwingLLM: LLMClient = {
  async complete() {
    throw new Error("LLM must not be called for single_asset");
  },
};

test("select_templates forces the single-asset family without calling the LLM", async () => {
  const result = await selectTemplates(
    { run_id: "sa", thesis: singleAssetThesis() },
    { llm: throwingLLM, logger: createSilentStepLogger() },
  );
  assert.deepEqual(
    result.delta.template_selection.selected.map((s) => s.family),
    ["single_asset_trend_setup"],
  );
});

test("select_universe resolves a single-asset target to a one-coin universe", async () => {
  const result = await selectUniverse(
    {
      run_id: "sa",
      thesis: singleAssetThesis({ target_coin_id: "ethereum" }),
    },
    {
      // rankUniverse must not be needed when a target is set.
      rankUniverse: async () => {
        throw new Error("rank_universe must not be called for a pinned target");
      },
      logger: createSilentStepLogger(),
    },
  );
  assert.deepEqual(result.delta.universe.coin_ids, ["ethereum"]);
  assert.equal(result.delta.universe.source, "hand_picked");
});

test("buildSingleAssetProposal sweeps the SMA window on the target coin", () => {
  const thesis = singleAssetThesis();
  const proposal = buildSingleAssetProposal({
    run_id: "sa",
    thesis,
    universe: UNIVERSE,
    window: WINDOW,
    attempts: [],
  });

  assert.equal(proposal.candidates.length, 3);
  for (const c of proposal.candidates) {
    assert.equal(c.template_id, "single_asset_trend_setup");
    assert.equal(c.select_top, 1);
    assert.equal(c.target_coin_id, "bitcoin");
    assert.ok(typeof c.sma_lookback === "number");
  }
  // The sweep is distinct lookbacks.
  const lookbacks = proposal.candidates.map((c) => c.sma_lookback);
  assert.equal(new Set(lookbacks).size, lookbacks.length);
  // And it is a valid proposal for this thesis/universe.
  assert.doesNotThrow(() =>
    validateProposal(proposal, { thesis, universe: UNIVERSE }),
  );
});

test("buildSingleAssetProposal falls back to the top universe coin without a target", () => {
  const thesis = singleAssetThesis({ target_coin_id: undefined });
  const proposal = buildSingleAssetProposal({
    run_id: "sa",
    thesis,
    universe: { ...UNIVERSE, coin_ids: ["solana"] },
    window: WINDOW,
    attempts: [],
  });
  assert.ok(proposal.candidates.every((c) => c.target_coin_id === "solana"));
});

test("run_and_validate batch config for single_asset omits weighting/select_top", () => {
  const thesis = singleAssetThesis();
  const proposal = buildSingleAssetProposal({
    run_id: "sa",
    thesis,
    universe: UNIVERSE,
    window: WINDOW,
    attempts: [],
  });
  const batch = buildBatchInput(
    { run_id: "sa", thesis, universe: UNIVERSE, window: WINDOW, proposal, attempts: [] },
    1,
  );
  const entry = batch.candidates[0] as {
    select_top: number;
    config: Record<string, unknown>;
  };
  assert.equal(entry.select_top, 1);
  assert.equal(entry.config.weighting, undefined);
  assert.equal(entry.config.sma_lookback, 20);
  assert.equal(entry.config.target_coin_id, "bitcoin");
});

test("suggested reruns offer a single-asset pivot for basket runs only", () => {
  const basket: Thesis = {
    ...singleAssetThesis(),
    strategy_mode: undefined,
    target_coin_id: undefined,
    constraints: {
      max_weight_per_asset: 0.2,
      max_cash_weight: 0.1,
      max_drawdown: 0.35,
      asset_count_min: 5,
      asset_count_max: 10,
    },
  };
  const basketSuggestions = buildSuggestedReruns(basket).map((s) => s.label);
  assert.ok(basketSuggestions.includes("Try as single-asset setup"));

  const single = buildSuggestedReruns(singleAssetThesis()).map((s) => s.label);
  assert.ok(!single.includes("Try as single-asset setup"));
});
