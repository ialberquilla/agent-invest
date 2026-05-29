import assert from "node:assert/strict";
import test from "node:test";

import type {
  RunCandidateBatchRequest,
  RunCandidateBatchResponse,
  ValidateAgainstThesisRequest,
  ValidateAgainstThesisResponse,
} from "../src/agent/workflow/cli.ts";
import { createSilentStepLogger } from "../src/agent/workflow/logging.ts";
import {
  buildBatchInput,
  normalizeValidationSummary,
  runAndValidate,
  thesisForValidate,
  type RunAndValidateDeps,
} from "../src/agent/workflow/steps/run_and_validate.ts";
import type {
  Attempt,
  Proposal,
  RunAndValidateInput,
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
  iteration_hypothesis: "Equal vs cap weighted 5-asset basket.",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "buy_and_hold",
      select_top: 5,
      weighting: "equal",
      rationale: "baseline",
    },
    {
      candidate_id: "c2",
      template_id: "periodic_rebalance",
      select_top: 5,
      weighting: "cap",
      rebalance_trigger: "periodic_30d",
      rationale: "monthly cap-weighted",
    },
  ],
};

const BASE_INPUT: RunAndValidateInput = {
  run_id: "test-run",
  thesis: THESIS,
  universe: UNIVERSE,
  window: WINDOW,
  proposal: PROPOSAL,
};

function fakeBatch(response: RunCandidateBatchResponse) {
  const calls: RunCandidateBatchRequest[] = [];
  const fn = async (req: RunCandidateBatchRequest) => {
    calls.push(req);
    return response;
  };
  return Object.assign(fn, { calls });
}

function fakeValidate(response: ValidateAgainstThesisResponse) {
  const calls: ValidateAgainstThesisRequest[] = [];
  const fn = async (req: ValidateAgainstThesisRequest) => {
    calls.push(req);
    return response;
  };
  return Object.assign(fn, { calls });
}

function deps(
  batch: ReturnType<typeof fakeBatch>,
  validate: ReturnType<typeof fakeValidate>,
): RunAndValidateDeps {
  return {
    runCandidateBatch: batch,
    runValidateAgainstThesis: validate,
    logger: createSilentStepLogger(),
  };
}

test("buildBatchInput wraps universe with hand_picked and uses start/end window", () => {
  const built = buildBatchInput(BASE_INPUT, 1);
  assert.equal(built.universe_override.id, "hand_picked");
  assert.deepEqual(built.universe_override.params.coin_ids, UNIVERSE.coin_ids);
  assert.deepEqual(built.window_override, {
    start: WINDOW.start,
    end: WINDOW.end,
  });
  assert.equal(built.iteration_hypothesis, PROPOSAL.iteration_hypothesis);
  assert.equal(built.round, 1);
});

test("buildBatchInput flattens candidates and omits trigger on buy_and_hold", () => {
  const built = buildBatchInput(BASE_INPUT, 1);
  assert.equal(built.candidates.length, 2);
  assert.equal(built.candidates[0]?.template_id, "buy_and_hold");
  assert.deepEqual(built.candidates[0]?.config, { weighting: "equal" });
  assert.equal(built.candidates[1]?.template_id, "periodic_rebalance");
  assert.deepEqual(built.candidates[1]?.config, {
    weighting: "cap",
    rebalance_trigger: "periodic_30d",
  });
});

test("buildBatchInput embeds candidate.thesis.objective for benchmark selection", () => {
  const built = buildBatchInput(BASE_INPUT, 1);
  for (const candidate of built.candidates) {
    // The workflow Thesis uses balanced_growth, which maps to the
    // script's "balanced" benchmark enum.
    assert.deepEqual((candidate as { thesis?: { objective?: string } }).thesis, {
      objective: "balanced",
    });
  }
});

test("buildBatchInput maps growth -> high_growth in candidate.thesis", () => {
  const growthInput = {
    ...BASE_INPUT,
    thesis: { ...THESIS, objective: "growth" as const },
  };
  const built = buildBatchInput(growthInput, 1);
  assert.deepEqual(
    (built.candidates[0] as { thesis?: { objective?: string } }).thesis,
    { objective: "high_growth" },
  );
});

test("thesisForValidate sends only objective, horizon_days, constraints", () => {
  const trimmed = thesisForValidate(THESIS);
  assert.deepEqual(Object.keys(trimmed).sort(), [
    "constraints",
    "horizon_days",
    "objective",
  ]);
});

test("normalizeValidationSummary maps expected/actual to target/observed", () => {
  const summary = normalizeValidationSummary({
    batch_id: "batch_x",
    results: [
      { candidate_id: "c1", passed: true },
      {
        candidate_id: "c2",
        passed: false,
        violations: [
          { constraint: "max_drawdown", expected: 0.35, actual: 0.42 },
          {
            constraint: "max_weight_per_asset",
            expected: "0.20",
            actual: "0.27",
          },
        ],
      },
    ],
    passing_candidate_ids: ["c1"],
  });
  assert.deepEqual(summary.passing_candidate_ids, ["c1"]);
  assert.equal(summary.failing.length, 1);
  assert.equal(summary.failing[0]?.candidate_id, "c2");
  assert.equal(summary.failing[0]?.violations[0]?.target, 0.35);
  assert.equal(summary.failing[0]?.violations[0]?.observed, 0.42);
  assert.equal(summary.failing[0]?.violations[1]?.target, 0.2);
  assert.equal(summary.failing[0]?.violations[1]?.observed, 0.27);
});

test("normalizeValidationSummary tolerates missing arrays", () => {
  const summary = normalizeValidationSummary({
    batch_id: "batch_x",
    results: [],
    passing_candidate_ids: [],
  });
  assert.deepEqual(summary.passing_candidate_ids, []);
  assert.deepEqual(summary.failing, []);
});

test("runAndValidate calls batch then validate with the returned batch_id", async () => {
  const batch = fakeBatch({
    batch_id: "candidate_batch_xyz",
    run_id: "test-run",
    round: 1,
  });
  const validate = fakeValidate({
    batch_id: "candidate_batch_xyz",
    results: [{ candidate_id: "c1", passed: true }],
    passing_candidate_ids: ["c1"],
  });

  const result = await runAndValidate(BASE_INPUT, deps(batch, validate));

  assert.equal(batch.calls.length, 1);
  assert.equal(validate.calls.length, 1);
  assert.equal(validate.calls[0]?.batch.batch_id, "candidate_batch_xyz");
  assert.equal(result.next, "decide");
  assert.equal(result.delta.batch_id, "candidate_batch_xyz");
  assert.deepEqual(result.delta.validation_summary.passing_candidate_ids, [
    "c1",
  ]);
});

test("runAndValidate uses attempts.length + 1 as the round number", async () => {
  const batch = fakeBatch({
    batch_id: "candidate_batch_abc",
    run_id: "test-run",
    round: 3,
  });
  const validate = fakeValidate({
    batch_id: "candidate_batch_abc",
    results: [],
    passing_candidate_ids: [],
  });
  const prior: Attempt[] = [
    { attempt_n: 1, proposal: PROPOSAL },
    { attempt_n: 2, proposal: PROPOSAL },
  ];

  await runAndValidate(
    { ...BASE_INPUT, attempts: prior },
    deps(batch, validate),
  );

  assert.equal(batch.calls[0]?.round, 3);
});

test("runAndValidate propagates batch CLI errors", async () => {
  const batch = async () => {
    throw new Error("run_candidate_batch failed: boom");
  };
  const validate = fakeValidate({
    batch_id: "",
    results: [],
    passing_candidate_ids: [],
  });

  await assert.rejects(
    () =>
      runAndValidate(BASE_INPUT, {
        runCandidateBatch: batch,
        runValidateAgainstThesis: validate,
        logger: createSilentStepLogger(),
      }),
    /run_candidate_batch failed/,
  );
  assert.equal(validate.calls.length, 0);
});

test("runAndValidate propagates validate CLI errors", async () => {
  const batch = fakeBatch({
    batch_id: "candidate_batch_xyz",
    run_id: "test-run",
    round: 1,
  });
  const validate = async () => {
    throw new Error("validate_against_thesis failed: missing batch");
  };

  await assert.rejects(
    () =>
      runAndValidate(BASE_INPUT, {
        runCandidateBatch: batch,
        runValidateAgainstThesis: validate,
        logger: createSilentStepLogger(),
      }),
    /validate_against_thesis failed/,
  );
});
