import assert from "node:assert/strict";
import test from "node:test";

import { buildPairTradeProposal } from "../src/agent/workflow/steps/propose_candidates.ts";
import {
  buildBatchInput,
  thesisForValidate,
} from "../src/agent/workflow/steps/run_and_validate.ts";
import { selectTemplates } from "../src/agent/workflow/steps/select_templates.ts";
import { selectUniverse } from "../src/agent/workflow/steps/select_universe.ts";
import { createSilentStepLogger } from "../src/agent/workflow/logging.ts";
import type { LLMClient } from "../src/agent/workflow/llm.ts";
import {
  validateProposal,
  validateThesis,
  type Thesis,
  type Universe,
  type Window,
} from "../src/agent/workflow/state.ts";

function pairThesis(overrides: Partial<Thesis> = {}): Thesis {
  return {
    objective: "balanced_growth",
    horizon_days: 180,
    weight_mode: "percentage",
    universe_hints: { top_n: 25, exclude_stablecoins: true, exclude_wrapped: true },
    constraints: {
      max_weight_per_asset: 1,
      max_cash_weight: 0,
      max_drawdown: 0.5,
      asset_count_min: 2,
      asset_count_max: 2,
    },
    rebalance_frequency: "monthly",
    interpretation_notes: "pair",
    strategy_mode: "pair_trade",
    allowed_sides: "long_short",
    long_coin_ids: ["ethereum"],
    short_coin_ids: ["bitcoin"],
    ...overrides,
  };
}

const PAIR_UNIVERSE: Universe = {
  coin_ids: ["ethereum", "bitcoin"],
  source: "hand_picked",
  effective_filters: { top_n: 2, exclude_stablecoins: true, exclude_wrapped: true },
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
    throw new Error("LLM must not be called for pair_trade");
  },
};

test("validateThesis accepts a 2-asset pair and rejects a non-2 count", () => {
  assert.doesNotThrow(() => validateThesis(pairThesis()));
  assert.throws(
    () =>
      validateThesis(
        pairThesis({
          constraints: { ...pairThesis().constraints, asset_count_max: 3 },
        }),
      ),
    /pair_trade strategy_mode requires/,
  );
});

test("select_templates forces explicit_pair_trade without the LLM", async () => {
  const result = await selectTemplates(
    { run_id: "pt", thesis: pairThesis() },
    { llm: throwingLLM, logger: createSilentStepLogger() },
  );
  assert.deepEqual(
    result.delta.template_selection.selected.map((s) => s.family),
    ["explicit_pair_trade"],
  );
});

test("select_universe resolves pair legs to a two-coin universe", async () => {
  const result = await selectUniverse(
    { run_id: "pt", thesis: pairThesis() },
    {
      rankUniverse: async () => {
        throw new Error("rank_universe must not be called for explicit pair legs");
      },
      logger: createSilentStepLogger(),
    },
  );
  assert.deepEqual(result.delta.universe.coin_ids, ["ethereum", "bitcoin"]);
});

test("buildPairTradeProposal sweeps the hedge ratio on the named legs", () => {
  const thesis = pairThesis();
  const proposal = buildPairTradeProposal({
    run_id: "pt",
    thesis,
    universe: PAIR_UNIVERSE,
    window: WINDOW,
    attempts: [],
  });
  assert.equal(proposal.candidates.length, 3);
  for (const c of proposal.candidates) {
    assert.equal(c.template_id, "explicit_pair_trade");
    assert.equal(c.select_top, 2);
    assert.equal(c.long_coin_id, "ethereum");
    assert.equal(c.short_coin_id, "bitcoin");
    assert.ok(typeof c.hedge_ratio === "number");
  }
  assert.doesNotThrow(() =>
    validateProposal(proposal, { thesis, universe: PAIR_UNIVERSE }),
  );
});

test("run_and_validate batch config for a pair carries the legs, not weighting", () => {
  const thesis = pairThesis();
  const proposal = buildPairTradeProposal({
    run_id: "pt",
    thesis,
    universe: PAIR_UNIVERSE,
    window: WINDOW,
    attempts: [],
  });
  const batch = buildBatchInput(
    { run_id: "pt", thesis, universe: PAIR_UNIVERSE, window: WINDOW, proposal, attempts: [] },
    1,
  );
  const entry = batch.candidates[0] as {
    select_top: number;
    config: Record<string, unknown>;
  };
  assert.equal(entry.select_top, 2);
  assert.equal(entry.config.weighting, undefined);
  assert.equal(entry.config.long_coin_id, "ethereum");
  assert.equal(entry.config.short_coin_id, "bitcoin");
  assert.equal(entry.config.hedge_ratio, 0.5);
});

test("thesisForValidate sends exposure constraints for a short-bearing book", () => {
  const thesis = pairThesis({
    constraints: {
      ...pairThesis().constraints,
      max_gross_exposure: 2.5,
      max_net_exposure: 1,
      max_leg_weight: 1.5,
    },
  });
  const sent = thesisForValidate(thesis) as {
    constraints: Record<string, unknown>;
  };
  // Long-book rules are dropped; exposure + drawdown are sent.
  assert.equal(sent.constraints.max_weight_per_asset, undefined);
  assert.equal(sent.constraints.asset_count_min, undefined);
  assert.equal(sent.constraints.max_gross_exposure, 2.5);
  assert.equal(sent.constraints.max_net_exposure, 1);
  assert.equal(sent.constraints.max_leg_weight, 1.5);
  assert.ok((sent.constraints.max_drawdown as number) < 0);
});

test("thesisForValidate keeps long-book rules for a basket thesis", () => {
  const basket = pairThesis({
    strategy_mode: undefined,
    allowed_sides: undefined,
    long_coin_ids: undefined,
    short_coin_ids: undefined,
    constraints: {
      max_weight_per_asset: 0.2,
      max_cash_weight: 0.1,
      max_drawdown: 0.35,
      asset_count_min: 5,
      asset_count_max: 10,
    },
  });
  const sent = thesisForValidate(basket) as {
    constraints: Record<string, unknown>;
  };
  assert.equal(sent.constraints.max_weight_per_asset, 0.2);
  assert.equal(sent.constraints.asset_count_min, 5);
});
