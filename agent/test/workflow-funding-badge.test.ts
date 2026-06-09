import assert from "node:assert/strict";
import test from "node:test";

import { workflowStateToStructuredResult } from "../src/agent/workflow/persist.ts";
import type {
  FinalWinner,
  Thesis,
  WorkflowState,
} from "../src/agent/workflow/state.ts";

function thesis(overrides: Partial<Thesis> = {}): Thesis {
  return {
    objective: "balanced_growth",
    horizon_days: 180,
    weight_mode: "percentage",
    universe_hints: { top_n: 25, exclude_stablecoins: true, exclude_wrapped: true },
    constraints: {
      max_weight_per_asset: 0.2,
      max_cash_weight: 0.1,
      max_drawdown: 0.35,
      asset_count_min: 5,
      asset_count_max: 10,
    },
    rebalance_frequency: "monthly",
    interpretation_notes: "x",
    ...overrides,
  };
}

function winnerState(t: Thesis): WorkflowState {
  const final: FinalWinner = {
    kind: "winner",
    run_id: "r",
    winner_candidate_id: "c1",
    winner_attempt_n: 1,
    candidate_batch_id: "batch_1",
    is_best_effort: false,
    unmet_constraints: [],
    thesis: t,
    universe: {
      coin_ids: ["bitcoin", "ethereum"],
      source: "hand_picked",
      effective_filters: {
        top_n: 2,
        exclude_stablecoins: true,
        exclude_wrapped: true,
      },
    },
    window: { start: "2022-01-01", end: "2024-01-01", horizon_days: 180 } as never,
    attempts_summary: [],
    narrative: {
      title: "t",
      summary: "s",
      reasoning: "r",
      assumptions: [],
      risks: [],
      next_steps: [],
    },
  };
  return {
    run_id: "r",
    brief: "b",
    workflow_version: "test",
    attempts: [],
    counters: { reinterpret_brief: 0, broaden_universe: 0 },
    final,
  };
}

test("structured result badges a short-bearing winner as funding-not-modelled", () => {
  const pair = thesis({
    strategy_mode: "pair_trade",
    allowed_sides: "long_short",
    constraints: {
      max_weight_per_asset: 1,
      max_cash_weight: 0,
      max_drawdown: 0.5,
      asset_count_min: 2,
      asset_count_max: 2,
    },
  });
  const result = workflowStateToStructuredResult(winnerState(pair)) as Record<
    string,
    unknown
  >;
  assert.deepEqual(result.costs, { funding_modeled: false });
});

test("structured result omits the funding badge for a long-only winner", () => {
  const result = workflowStateToStructuredResult(
    winnerState(thesis()),
  ) as Record<string, unknown>;
  assert.equal(result.costs, undefined);
});
