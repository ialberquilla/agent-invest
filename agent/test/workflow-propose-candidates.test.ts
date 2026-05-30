import assert from "node:assert/strict";
import test from "node:test";

import type {
  LLMClient,
  LLMResponse,
} from "../src/agent/workflow/llm.ts";
import { createSilentStepLogger } from "../src/agent/workflow/logging.ts";
import {
  proposeCandidates,
  type ProposeCandidatesDeps,
} from "../src/agent/workflow/steps/propose_candidates.ts";
import type {
  Attempt,
  Proposal,
  ProposeCandidatesInput,
  RefinementHint,
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
  coin_ids: [
    "bitcoin",
    "ethereum",
    "binancecoin",
    "ripple",
    "solana",
    "tron",
    "dogecoin",
    "hyperliquid",
    "cardano",
    "monero",
  ],
  source: "rank_universe",
  effective_filters: {
    top_n: 10,
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

const SAMPLE_PROPOSAL: Proposal = {
  iteration_hypothesis:
    "Compare equal-weight buy-and-hold vs monthly-rebalanced cap-weighted basket.",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "synthetic_long_allocation",
      select_top: 5,
      weighting: "equal",
      rationale: "baseline equal-weight large-cap basket",
    },
    {
      candidate_id: "c2",
      template_id: "periodic_rebalanced_allocation",
      select_top: 7,
      weighting: "cap",
      rebalance_trigger: "periodic_30d",
      rationale: "monthly-rebalanced cap-weighted broader basket",
    },
    {
      candidate_id: "c3",
      template_id: "periodic_rebalanced_allocation",
      select_top: 6,
      weighting: "vol_inverse",
      rebalance_trigger: "periodic_90d",
      rationale: "quarterly vol-inverse defensive variant",
    },
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
      return { text, tokens_in: 100, tokens_out: 200 } satisfies LLMResponse;
    },
  };
}

function deps(llm: LLMClient): ProposeCandidatesDeps {
  return { llm, logger: createSilentStepLogger() };
}

function baseInput(overrides: Partial<ProposeCandidatesInput> = {}): ProposeCandidatesInput {
  return {
    run_id: "test",
    thesis: THESIS,
    universe: UNIVERSE,
    window: WINDOW,
    ...overrides,
  };
}

test("proposeCandidates returns a valid Proposal and routes to run_and_validate", async () => {
  const llm = fakeLLM([JSON.stringify(SAMPLE_PROPOSAL)]);

  const result = await proposeCandidates(baseInput(), deps(llm));

  assert.equal(result.next, "run_and_validate");
  assert.equal(result.delta.proposal.candidates.length, 3);
  assert.equal(result.delta.proposal.candidates[0]?.template_id, "synthetic_long_allocation");
});

test("proposeCandidates retries once on parse failure", async () => {
  const llm = fakeLLM([
    "Sure, here's the plan -- no JSON though.",
    JSON.stringify(SAMPLE_PROPOSAL),
  ]);

  await proposeCandidates(baseInput(), deps(llm));

  assert.equal(llm.calls.length, 2);
  assert.match(llm.calls[1]!.system, /could not be parsed/);
});

test("proposeCandidates retries on schema failure with the validator error", async () => {
  const bad: Proposal = {
    ...SAMPLE_PROPOSAL,
    candidates: [
      { ...SAMPLE_PROPOSAL.candidates[0]!, select_top: 999 },
      SAMPLE_PROPOSAL.candidates[1]!,
      SAMPLE_PROPOSAL.candidates[2]!,
    ],
  };
  const llm = fakeLLM([JSON.stringify(bad), JSON.stringify(SAMPLE_PROPOSAL)]);

  await proposeCandidates(baseInput(), deps(llm));

  assert.equal(llm.calls.length, 2);
  assert.match(llm.calls[1]!.system, /failed validation/);
  assert.match(llm.calls[1]!.system, /select_top.*must be within/);
});

test("proposeCandidates throws after two failed attempts", async () => {
  const bad = { ...SAMPLE_PROPOSAL, candidates: [] };
  const llm = fakeLLM([JSON.stringify(bad), JSON.stringify(bad)]);

  await assert.rejects(
    () => proposeCandidates(baseInput(), deps(llm)),
    /between 3 and 5/,
  );
});

test("proposeCandidates rejects duplicate candidate_ids", async () => {
  const bad: Proposal = {
    ...SAMPLE_PROPOSAL,
    candidates: [
      { ...SAMPLE_PROPOSAL.candidates[0]!, candidate_id: "x" },
      { ...SAMPLE_PROPOSAL.candidates[1]!, candidate_id: "x" },
      { ...SAMPLE_PROPOSAL.candidates[2]!, candidate_id: "y" },
    ],
  };
  const llm = fakeLLM([JSON.stringify(bad), JSON.stringify(bad)]);

  await assert.rejects(
    () => proposeCandidates(baseInput(), deps(llm)),
    /not unique/,
  );
});

test("proposeCandidates rejects rebalance_trigger on buy_and_hold", async () => {
  const bad: Proposal = {
    ...SAMPLE_PROPOSAL,
    candidates: [
      {
        candidate_id: "c1",
        template_id: "synthetic_long_allocation",
        select_top: 5,
        weighting: "equal",
        rebalance_trigger: "periodic_30d",
        rationale: "should not have a trigger",
      },
      SAMPLE_PROPOSAL.candidates[1]!,
      SAMPLE_PROPOSAL.candidates[2]!,
    ],
  };
  const llm = fakeLLM([JSON.stringify(bad), JSON.stringify(bad)]);

  await assert.rejects(
    () => proposeCandidates(baseInput(), deps(llm)),
    /rebalance_trigger is not allowed/,
  );
});

test("proposeCandidates requires rebalance_trigger on periodic_rebalance", async () => {
  const bad: Proposal = {
    ...SAMPLE_PROPOSAL,
    candidates: [
      {
        candidate_id: "c1",
        template_id: "periodic_rebalanced_allocation",
        select_top: 5,
        weighting: "cap",
        rationale: "missing trigger",
      },
      SAMPLE_PROPOSAL.candidates[1]!,
      SAMPLE_PROPOSAL.candidates[2]!,
    ],
  };
  const llm = fakeLLM([JSON.stringify(bad), JSON.stringify(bad)]);

  await assert.rejects(
    () => proposeCandidates(baseInput(), deps(llm)),
    /rebalance_trigger must be one of/,
  );
});

test("proposeCandidates includes prior_attempts with RefinementHint in the user message", async () => {
  const hint: RefinementHint = {
    failed_constraints: [
      {
        constraint: "max_drawdown",
        observed: 0.42,
        target: 0.35,
        candidate_id: "c1",
      },
    ],
    suggested_changes: { tighten_weight_cap_to: 0.15 },
    rationale: "Equal-weight 5-asset basket hit 42% drawdown; tighten the per-asset cap.",
  };
  const prior: Attempt = {
    attempt_n: 1,
    proposal: SAMPLE_PROPOSAL,
    batch_id: "batch_abc",
    validation_summary: {
      passing_candidate_ids: [],
      failing: [
        {
          candidate_id: "c1",
          violations: [
            { constraint: "max_drawdown", observed: 0.42, target: 0.35 },
          ],
        },
      ],
    },
    refinement_hint: hint,
  };
  const llm = fakeLLM([JSON.stringify(SAMPLE_PROPOSAL)]);

  await proposeCandidates(baseInput({ attempts: [prior] }), deps(llm));

  const parsed = JSON.parse(llm.calls[0]!.user) as {
    prior_attempts: Array<{ refinement_hint?: RefinementHint }>;
  };
  assert.equal(parsed.prior_attempts.length, 1);
  assert.equal(
    parsed.prior_attempts[0]?.refinement_hint?.failed_constraints[0]?.constraint,
    "max_drawdown",
  );
});

test("proposeCandidates surfaces infeasible universe before the LLM responds", async () => {
  const narrow: Universe = {
    ...UNIVERSE,
    coin_ids: ["bitcoin", "ethereum"], // 2 coins; thesis requires asset_count_min=5
  };
  const llm = fakeLLM([JSON.stringify(SAMPLE_PROPOSAL), JSON.stringify(SAMPLE_PROPOSAL)]);

  await assert.rejects(
    () => proposeCandidates(baseInput({ universe: narrow }), deps(llm)),
    /cannot satisfy asset_count_min/,
  );
});

test("proposeCandidates accepts five candidates", async () => {
  const five: Proposal = {
    iteration_hypothesis: "scan five configurations in one round",
    candidates: Array.from({ length: 5 }, (_, i) => ({
      candidate_id: `c${i + 1}`,
      template_id: "synthetic_long_allocation" as const,
      select_top: 5 + i,
      weighting: "equal" as const,
      rationale: `variant ${i + 1}`,
    })),
  };
  const llm = fakeLLM([JSON.stringify(five)]);

  const result = await proposeCandidates(baseInput(), deps(llm));

  assert.equal(result.delta.proposal.candidates.length, 5);
});
