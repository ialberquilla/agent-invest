import assert from "node:assert/strict";
import test from "node:test";

import type {
  LLMClient,
  LLMResponse,
} from "../src/agent/workflow/llm.ts";
import { createSilentStepLogger } from "../src/agent/workflow/logging.ts";
import {
  finalize,
  type FinalizeDeps,
} from "../src/agent/workflow/steps/finalize.ts";
import type {
  Attempt,
  FinalizeInput,
  FinalizeNarrative,
  Proposal,
  Thesis,
  Universe,
  Window,
} from "../src/agent/workflow/state.ts";

const THESIS: Thesis = {
  objective: "balanced_growth",
  horizon_days: 365,
  weight_mode: "percentage",
  universe_hints: {
    top_n: 10,
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
  interpretation_notes: "fixture",
};

const UNIVERSE: Universe = {
  coin_ids: ["bitcoin", "ethereum", "binancecoin", "ripple", "solana"],
  source: "rank_universe",
  effective_filters: {
    top_n: 5,
    exclude_stablecoins: true,
    exclude_wrapped: true,
  },
};

const WINDOW: Window = {
  start: "2022-05-25",
  end: "2026-05-24",
  horizon_days: 365,
  effective: {
    window_length_days: 1460,
    target_window_length_days: 1460,
    rationale: "ok",
    limiting_coin: "bitcoin",
    covered_drawdowns_count: 2,
  },
};

const PROPOSAL: Proposal = {
  iteration_hypothesis: "Equal vs cap-weighted basket comparison.",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "synthetic_long_allocation",
      select_top: 5,
      weighting: "equal",
      rationale: "baseline",
    },
    {
      candidate_id: "c2",
      template_id: "periodic_rebalanced_allocation",
      select_top: 7,
      weighting: "cap",
      rebalance_trigger: "periodic_30d",
      rationale: "monthly cap-weighted",
    },
  ],
};

function passingAttempt(passingIds: string[]): Attempt {
  return {
    attempt_n: 1,
    proposal: PROPOSAL,
    batch_id: "candidate_batch_test",
    validation_summary: {
      passing_candidate_ids: passingIds,
      failing: [],
      candidates: passingIds.map((id) => ({
        candidate_id: id,
        passed: true,
        constraint_distance: 0,
      })),
    },
  };
}

const NARRATIVE: FinalizeNarrative = {
  title: "Balanced growth large-cap basket",
  summary:
    "A diversified large-cap crypto basket rebalanced monthly to target weights aligned with market cap.",
  reasoning:
    "Candidate c2 was the only configuration that satisfied the thesis constraints while still capturing balanced upside.",
  assumptions: [
    "Historical backtests over 2022-2026 are representative of forward conditions in scenario terms.",
  ],
  risks: [
    "Past performance does not guarantee future returns; crypto markets remain highly volatile.",
  ],
  next_steps: [
    "Review the constraints and rerun with stricter risk caps before committing capital.",
  ],
};

type RecordingLLM = LLMClient & {
  calls: Array<{ system: string; user: string }>;
};

function fakeLLM(responses: string[]): RecordingLLM {
  const calls: Array<{ system: string; user: string }> = [];
  let i = 0;
  return {
    calls,
    async complete(request) {
      calls.push({ system: request.system, user: request.user });
      const text = responses[i] ?? responses[responses.length - 1] ?? "";
      i += 1;
      return { text, tokens_in: 100, tokens_out: 50 } satisfies LLMResponse;
    },
  };
}

function deps(llm: LLMClient): FinalizeDeps {
  return { llm, logger: createSilentStepLogger() };
}

function baseInput(overrides: Partial<FinalizeInput> = {}): FinalizeInput {
  return {
    run_id: "test-finalize",
    thesis: THESIS,
    universe: UNIVERSE,
    window: WINDOW,
    attempts: [passingAttempt(["c2"])],
    winner_candidate_id: "c2",
    winner_attempt_n: 1,
    is_best_effort: false,
    decide_justification: "c2 was the only passing candidate.",
    ...overrides,
  };
}

test("finalize returns a FinalWinner and routes to complete", async () => {
  const llm = fakeLLM([JSON.stringify(NARRATIVE)]);

  const result = await finalize(baseInput(), deps(llm));

  assert.equal(result.next, "complete");
  assert.equal(result.delta.final.kind, "winner");
  assert.equal(result.delta.final.winner_candidate_id, "c2");
  assert.equal(result.delta.final.candidate_batch_id, "candidate_batch_test");
  assert.deepEqual(result.delta.final.narrative, NARRATIVE);
  assert.equal(result.delta.final.thesis, THESIS);
  assert.equal(result.delta.final.universe, UNIVERSE);
  assert.equal(result.delta.final.window, WINDOW);
  assert.equal(result.delta.final.attempts_summary.length, 1);
});

test("finalize includes the winner candidate config in the user message", async () => {
  const llm = fakeLLM([JSON.stringify(NARRATIVE)]);

  await finalize(baseInput(), deps(llm));

  const parsed = JSON.parse(llm.calls[0]!.user) as { winner: { candidate_id?: string } };
  assert.equal(parsed.winner.candidate_id, "c2");
});

test("finalize retries on parse failure", async () => {
  const llm = fakeLLM(["not json", JSON.stringify(NARRATIVE)]);

  await finalize(baseInput(), deps(llm));

  assert.equal(llm.calls.length, 2);
  assert.match(llm.calls[1]!.system, /could not be parsed/);
});

test("finalize retries with schema-violation note", async () => {
  const bad = { ...NARRATIVE, risks: [] };
  const llm = fakeLLM([JSON.stringify(bad), JSON.stringify(NARRATIVE)]);

  await finalize(baseInput(), deps(llm));

  assert.equal(llm.calls.length, 2);
  assert.match(llm.calls[1]!.system, /failed validation/);
  assert.match(llm.calls[1]!.system, /risks must be a non-empty array/);
});

test("finalize refuses when winner_candidate_id is not in passing set", async () => {
  const llm = fakeLLM([JSON.stringify(NARRATIVE)]);

  await assert.rejects(
    () =>
      finalize(
        baseInput({ winner_candidate_id: "c1" }),
        deps(llm),
      ),
    /is not in attempt 1's passing_candidate_ids/,
  );
  assert.equal(llm.calls.length, 0);
});

test("finalize accepts a best-effort winner that did not pass and surfaces unmet constraints", async () => {
  // No candidate passed; c1 is the closest fit. is_best_effort relaxes
  // the passing-set check, and the winner's violations flow into the
  // FinalWinner.unmet_constraints and the finalize user message.
  const bestEffortAttempt: Attempt = {
    attempt_n: 1,
    proposal: PROPOSAL,
    batch_id: "candidate_batch_best_effort",
    validation_summary: {
      passing_candidate_ids: [],
      failing: [
        {
          candidate_id: "c1",
          violations: [
            { constraint: "max_drawdown", observed: 0.41, target: 0.35 },
          ],
        },
      ],
      candidates: [
        { candidate_id: "c1", passed: false, constraint_distance: 0.17 },
      ],
    },
  };
  const llm = fakeLLM([JSON.stringify(NARRATIVE)]);

  const result = await finalize(
    baseInput({
      attempts: [bestEffortAttempt],
      winner_candidate_id: "c1",
      winner_attempt_n: 1,
      is_best_effort: true,
    }),
    deps(llm),
  );

  assert.equal(result.delta.final.is_best_effort, true);
  assert.deepEqual(result.delta.final.unmet_constraints, [
    { constraint: "max_drawdown", observed: 0.41, target: 0.35 },
  ]);
  const userMsg = JSON.parse(llm.calls[0]!.user) as {
    is_best_effort: boolean;
    unmet_constraints: Array<{ constraint: string }>;
  };
  assert.equal(userMsg.is_best_effort, true);
  assert.equal(userMsg.unmet_constraints[0]?.constraint, "max_drawdown");
});

test("finalize refuses when winner_candidate_id is not in proposal", async () => {
  // Validation summary lists c-unknown as passing, but the proposal doesn't contain it.
  const orphan = passingAttempt(["c-unknown"]);
  const llm = fakeLLM([JSON.stringify(NARRATIVE)]);

  await assert.rejects(
    () =>
      finalize(
        baseInput({
          attempts: [orphan],
          winner_candidate_id: "c-unknown",
        }),
        deps(llm),
      ),
    /is not in attempt 1's proposal/,
  );
});

test("finalize refuses when the winner's attempt is absent", async () => {
  const llm = fakeLLM([JSON.stringify(NARRATIVE)]);

  await assert.rejects(
    () => finalize(baseInput({ attempts: [] }), deps(llm)),
    /no attempt with attempt_n 1/,
  );
  assert.equal(llm.calls.length, 0);
});

test("finalize rejects narrative missing a required string array", async () => {
  const bad = { ...NARRATIVE, next_steps: [] };
  const llm = fakeLLM([JSON.stringify(bad), JSON.stringify(bad)]);

  await assert.rejects(
    () => finalize(baseInput(), deps(llm)),
    /next_steps must be a non-empty array/,
  );
});

test("finalize rejects narrative with empty title", async () => {
  const bad = { ...NARRATIVE, title: "" };
  const llm = fakeLLM([JSON.stringify(bad), JSON.stringify(bad)]);

  await assert.rejects(
    () => finalize(baseInput(), deps(llm)),
    /title must be a non-empty string/,
  );
});
