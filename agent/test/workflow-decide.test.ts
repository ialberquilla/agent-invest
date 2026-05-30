import assert from "node:assert/strict";
import test from "node:test";

import type {
  LLMClient,
  LLMResponse,
} from "../src/agent/workflow/llm.ts";
import { createSilentStepLogger } from "../src/agent/workflow/logging.ts";
import {
  decide,
  type DecideDeps,
} from "../src/agent/workflow/steps/decide.ts";
import type {
  Attempt,
  Counters,
  DecideInput,
  Decision,
  Proposal,
  Thesis,
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

const SAMPLE_PROPOSAL: Proposal = {
  iteration_hypothesis: "x",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "synthetic_long_allocation",
      select_top: 5,
      weighting: "equal",
      rationale: "baseline",
    },
  ],
};

const ZERO_COUNTERS: Counters = {
  reinterpret_brief: 0,
  broaden_universe: 0,
};

function attemptWithPass(passing: string[]): Attempt {
  return {
    attempt_n: 1,
    proposal: SAMPLE_PROPOSAL,
    batch_id: "batch_x",
    validation_summary: {
      passing_candidate_ids: passing,
      failing: [],
    },
  };
}

function attemptWithFailures(
  attempt_n: number,
  violations: Array<{ constraint: string; observed: number; target: number }>,
): Attempt {
  return {
    attempt_n,
    proposal: SAMPLE_PROPOSAL,
    batch_id: `batch_${attempt_n}`,
    validation_summary: {
      passing_candidate_ids: [],
      failing: [{ candidate_id: "c1", violations }],
    },
  };
}

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

function deps(llm: LLMClient): DecideDeps {
  return { llm, logger: createSilentStepLogger() };
}

function baseInput(overrides: Partial<DecideInput> = {}): DecideInput {
  return {
    run_id: "test-decide",
    thesis: THESIS,
    attempts: [attemptWithPass(["c1"])],
    counters: ZERO_COUNTERS,
    ...overrides,
  };
}

test("decide routes stop_winner to finalize", async () => {
  const decision: Decision = {
    action: "stop_winner",
    winner_candidate_id: "c1",
    justification: "Best Sharpe and within all constraints.",
  };
  const llm = fakeLLM([JSON.stringify(decision)]);

  const result = await decide(baseInput(), deps(llm));

  assert.equal(result.next, "finalize");
  assert.deepEqual(result.delta.decision, decision);
});

test("decide falls back to stop_no_viable when stop_winner picks a non-passing candidate twice", async () => {
  const bad: Decision = {
    action: "stop_winner",
    winner_candidate_id: "c2",
    justification: "I think this looks fine.",
  };
  const llm = fakeLLM([JSON.stringify(bad), JSON.stringify(bad)]);

  const result = await decide(baseInput(), deps(llm));

  assert.equal(result.next, "complete");
  const decision = result.delta.decision as Extract<
    Decision,
    { action: "stop_no_viable" }
  >;
  assert.equal(decision.action, "stop_no_viable");
  assert.match(
    decision.reasons.join(" "),
    /could not parse a valid Decision after retries/,
  );
});

test("decide routes stop_no_viable to complete", async () => {
  const decision: Decision = {
    action: "stop_no_viable",
    reasons: [
      "Two consecutive attempts failed on max_drawdown with no improvement.",
    ],
  };
  const llm = fakeLLM([JSON.stringify(decision)]);

  const input = baseInput({
    attempts: [
      attemptWithFailures(1, [
        { constraint: "max_drawdown", observed: -0.5, target: -0.35 },
      ]),
      attemptWithFailures(2, [
        { constraint: "max_drawdown", observed: -0.48, target: -0.35 },
      ]),
    ],
  });

  const result = await decide(input, deps(llm));

  assert.equal(result.next, "complete");
  assert.equal(
    (result.delta.decision as Extract<Decision, { action: "stop_no_viable" }>)
      .reasons.length,
    1,
  );
});

test("decide routes refine_candidates to propose_candidates with hint", async () => {
  const decision: Decision = {
    action: "refine_candidates",
    hint: {
      failed_constraints: [
        {
          constraint: "max_drawdown",
          observed: -0.5,
          target: -0.35,
          candidate_id: "c1",
        },
      ],
      suggested_changes: { tighten_weight_cap_to: 0.15 },
      rationale: "Equal-weight 5-asset basket hit 50% drawdown.",
    },
  };
  const llm = fakeLLM([JSON.stringify(decision)]);

  const result = await decide(
    baseInput({
      attempts: [
        attemptWithFailures(1, [
          { constraint: "max_drawdown", observed: -0.5, target: -0.35 },
        ]),
      ],
    }),
    deps(llm),
  );

  assert.equal(result.next, "propose_candidates");
  assert.equal(result.delta.decision.action, "refine_candidates");
});

test("decide rejects refine_candidates once attempts cap is hit", async () => {
  const decision: Decision = {
    action: "refine_candidates",
    hint: {
      failed_constraints: [
        {
          constraint: "max_drawdown",
          observed: -0.5,
          target: -0.35,
          candidate_id: "c1",
        },
      ],
      suggested_changes: {},
      rationale: "Try again.",
    },
  };
  const llm = fakeLLM([JSON.stringify(decision), JSON.stringify(decision)]);

  const input = baseInput({
    attempts: [
      attemptWithFailures(1, [
        { constraint: "max_drawdown", observed: -0.5, target: -0.35 },
      ]),
      attemptWithFailures(2, [
        { constraint: "max_drawdown", observed: -0.5, target: -0.35 },
      ]),
      attemptWithFailures(3, [
        { constraint: "max_drawdown", observed: -0.5, target: -0.35 },
      ]),
    ],
  });

  const result = await decide(input, deps(llm));

  // Refine is forbidden by the validator; the LLM kept trying it.
  // After both attempts fail validation, decide falls back to
  // stop_no_viable so the workflow finishes cleanly.
  assert.equal(result.next, "complete");
  assert.equal(result.delta.decision.action, "stop_no_viable");
});

test("decide routes broaden_universe to select_universe with hint", async () => {
  const decision: Decision = {
    action: "broaden_universe",
    hint: {
      reason: "too_narrow_after_filters",
      loosen: { raise_top_n_to: 30, lower_market_cap_floor_to: 500_000_000 },
      rationale: "Universe collapsed to 4 coins after filters.",
    },
  };
  const llm = fakeLLM([JSON.stringify(decision)]);

  const result = await decide(
    baseInput({
      attempts: [
        attemptWithFailures(1, [
          { constraint: "asset_count_min", observed: 4, target: 5 },
        ]),
      ],
    }),
    deps(llm),
  );

  assert.equal(result.next, "select_universe");
  assert.equal(result.delta.decision.action, "broaden_universe");
});

test("decide falls back to stop_no_viable when broaden_universe is repeated at cap", async () => {
  const decision: Decision = {
    action: "broaden_universe",
    hint: {
      reason: "too_narrow_after_filters",
      loosen: { raise_top_n_to: 50 },
      rationale: "Try broader.",
    },
  };
  const llm = fakeLLM([JSON.stringify(decision), JSON.stringify(decision)]);

  const result = await decide(
    baseInput({
      attempts: [
        attemptWithFailures(1, [
          { constraint: "asset_count_min", observed: 4, target: 5 },
        ]),
      ],
      counters: { reinterpret_brief: 0, broaden_universe: 1 },
    }),
    deps(llm),
  );

  assert.equal(result.delta.decision.action, "stop_no_viable");
});

test("decide routes reinterpret_brief to interpret_brief", async () => {
  const decision: Decision = {
    action: "reinterpret_brief",
    hint: {
      reason: "constraints_infeasible",
      fields_to_revisit: ["constraints"],
      rationale:
        "5 assets at 20% cap leaves no room for 30% cash; constraints are infeasible.",
    },
  };
  const llm = fakeLLM([JSON.stringify(decision)]);

  const result = await decide(
    baseInput({
      attempts: [
        attemptWithFailures(1, [
          { constraint: "asset_count_min", observed: 4, target: 5 },
        ]),
      ],
    }),
    deps(llm),
  );

  assert.equal(result.next, "interpret_brief");
  assert.equal(result.delta.decision.action, "reinterpret_brief");
});

test("decide falls back to stop_no_viable when reinterpret_brief is repeated at cap", async () => {
  const decision: Decision = {
    action: "reinterpret_brief",
    hint: {
      reason: "constraints_infeasible",
      fields_to_revisit: ["constraints"],
      rationale: "thesis wrong",
    },
  };
  const llm = fakeLLM([JSON.stringify(decision), JSON.stringify(decision)]);

  const result = await decide(
    baseInput({
      attempts: [
        attemptWithFailures(1, [
          { constraint: "max_drawdown", observed: -0.5, target: -0.35 },
        ]),
      ],
      counters: { reinterpret_brief: 1, broaden_universe: 0 },
    }),
    deps(llm),
  );

  assert.equal(result.delta.decision.action, "stop_no_viable");
});

test("decide retries once on parse failure", async () => {
  const decision: Decision = {
    action: "stop_winner",
    winner_candidate_id: "c1",
    justification: "Best of the set.",
  };
  const llm = fakeLLM(["not json", JSON.stringify(decision)]);

  await decide(baseInput(), deps(llm));

  assert.equal(llm.calls.length, 2);
  assert.match(llm.calls[1]!.system, /could not be parsed/);
});

test("decide retries with validation error in retry prompt", async () => {
  const bad: Decision = {
    action: "stop_winner",
    winner_candidate_id: "c-nonexistent",
    justification: "wrong",
  };
  const good: Decision = {
    action: "stop_winner",
    winner_candidate_id: "c1",
    justification: "right pick from the passing set.",
  };
  const llm = fakeLLM([JSON.stringify(bad), JSON.stringify(good)]);

  await decide(baseInput(), deps(llm));

  assert.equal(llm.calls.length, 2);
  assert.match(llm.calls[1]!.system, /failed validation/);
  assert.match(llm.calls[1]!.system, /not in the latest attempt/);
});

test("decide falls back to stop_no_viable when the LLM cannot emit JSON", async () => {
  // Mirrors the production failure: both attempts return prose
  // instead of a Decision object. The step degrades to stop_no_viable
  // with the parse error in reasons -- the workflow then ends with
  // FinalNoViable cleanly instead of short-circuiting via step_error.
  const llm = fakeLLM([
    "Given the constraints I'm not sure what to do here...",
    "I'd recommend rerunning with looser drawdown.",
  ]);

  const input = baseInput({
    attempts: [
      attemptWithFailures(1, [
        { constraint: "max_drawdown", observed: -0.5, target: -0.35 },
        { constraint: "max_weight_per_asset", observed: 0.27, target: 0.2 },
      ]),
      attemptWithFailures(2, [
        { constraint: "max_drawdown", observed: -0.5, target: -0.35 },
      ]),
      attemptWithFailures(3, [
        { constraint: "max_drawdown", observed: -0.5, target: -0.35 },
      ]),
    ],
  });

  const result = await decide(input, deps(llm));

  assert.equal(result.next, "complete");
  const decision = result.delta.decision as Extract<
    Decision,
    { action: "stop_no_viable" }
  >;
  assert.equal(decision.action, "stop_no_viable");
  // The fallback includes the parse error, the failed constraint
  // names from the latest attempt, and how many refinement attempts
  // were used -- enough context for the human reader to diagnose.
  assert.match(
    decision.reasons.join(" "),
    /response did not contain a JSON object/,
  );
  assert.match(decision.reasons.join(" "), /max_drawdown/);
  assert.match(decision.reasons.join(" "), /3\/3 candidate-refinement attempts/);
});

test("decide refuses to run with empty attempts", async () => {
  const llm = fakeLLM([JSON.stringify({ action: "stop_no_viable", reasons: ["x"] })]);

  await assert.rejects(
    () => decide(baseInput({ attempts: [] }), deps(llm)),
    /requires at least one attempt/,
  );
  assert.equal(llm.calls.length, 0);
});
