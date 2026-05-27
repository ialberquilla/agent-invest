import assert from "node:assert/strict";
import test from "node:test";

import {
  adjudicatorStage,
  createAdjudicatorStageRunner,
  type AdjudicatorStageOutput,
} from "../src/agent/stages/adjudicator";
import type { ThesisJson } from "../src/agent/stages/thesis";
import type { ManagedOpencode } from "../src/agent/session";
import type { NewStageRun, StageRun } from "../src/db/schema";

const thesis: ThesisJson = {
  run_id: "run-adjudicator-stage-1",
  objective: "balanced",
  primary_factors: [{ factor: "sharpe", direction: "high" }],
  constraints: { max_drawdown: -0.35, rebalance: "monthly" },
  horizon_days: 365,
  interpretation_notes: "Balanced risk-adjusted growth.",
};

const input = {
  run_id: thesis.run_id,
  round: 1 as const,
  thesis,
  batch_id: "33333333-3333-4333-8333-333333333333",
  candidates: [
    { candidate_id: "r1_c1", template_id: "periodic_rebalance", config: {} },
    { candidate_id: "r1_c2", template_id: "momentum", config: {} },
    { candidate_id: "r1_c3", template_id: "periodic_rebalance", config: {} },
  ],
};

function createRepositories() {
  const creates: NewStageRun[] = [];
  const updates: Array<{
    stageRunId: string;
    updates: Partial<Omit<NewStageRun, "stageRunId" | "runId">>;
  }> = [];

  return {
    creates,
    updates,
    async createStageRun(stageRun: NewStageRun) {
      creates.push(stageRun);
      return stageRun as StageRun;
    },
    async updateStageRun(
      stageRunId: string,
      stageUpdates: Partial<Omit<NewStageRun, "stageRunId" | "runId">>,
    ) {
      updates.push({ stageRunId, updates: stageUpdates });
      return { ...creates[0], ...stageUpdates } as StageRun;
    },
  };
}

function createManagedOpencode(
  response: AdjudicatorStageOutput,
  toolNames: string[] = [],
) {
  const prompts: unknown[] = [];

  async function* emptyStream() {}

  const managed = {
    client: {
      session: {
        async create() {
          return { data: { id: "session-adjudicator" } };
        },
        async prompt(promptInput: unknown) {
          prompts.push(promptInput);
          return {
            data: {
              info: { usage: { inputTokens: 51, outputTokens: 31 } },
              parts: [
                ...toolNames.map((name) => ({ type: "tool", name })),
                { type: "text", text: JSON.stringify(response) },
              ],
            },
          };
        },
      },
      event: {
        async subscribe() {
          return { stream: emptyStream() };
        },
      },
    },
    close() {},
  } as unknown as ManagedOpencode;

  return { managed, prompts };
}

test("adjudicator stage produces a winner and persists stage output", async () => {
  const output: AdjudicatorStageOutput = {
    kind: "winner",
    candidate_id: "r1_c1",
    justification:
      "r1_c1 validates against the thesis and leads risk-adjusted comparison; fired low_sample_warning is acceptable because all candidates share the same window.",
  };
  const repositories = createRepositories();
  const opencode = createManagedOpencode(output, [
    "validate_against_thesis",
    "compare_backtests",
  ]);
  const runner = createAdjudicatorStageRunner(adjudicatorStage, {
    getManagedOpencode: async () => opencode.managed,
    createStageRun: repositories.createStageRun,
    updateStageRun: repositories.updateStageRun,
    availableTools: [
      "compare_backtests",
      "validate_against_thesis",
      "submit_refinement",
      "bash",
    ],
    env: {},
  });

  const result = await runner.run(input, thesis.run_id, 1);

  assert.deepEqual(result, output);
  assert.equal(repositories.creates[0]?.stage, "adjudicator");
  assert.deepEqual(repositories.updates.at(-1)?.updates.output, output);
  assert.equal(repositories.updates.at(-1)?.updates.status, "succeeded");
  assert.match(JSON.stringify(opencode.prompts[0]), /validate_against_thesis/);
  assert.match(JSON.stringify(opencode.prompts[0]), /compare_backtests/);
});

test("adjudicator refinement path produces structured v3 reasons", async () => {
  const output: AdjudicatorStageOutput = {
    kind: "refine",
    reasons: [
      {
        candidate_id: "r1_c2",
        reason: "benchmark_underperformance",
        detail:
          "Candidate underperformed the benchmark on Sharpe and drawdown.",
        suggested_fix:
          "Reduce momentum lookback sensitivity and add volatility weighting.",
      },
    ],
  };
  const repositories = createRepositories();
  const opencode = createManagedOpencode(output, ["submit_refinement"]);
  const runner = createAdjudicatorStageRunner(adjudicatorStage, {
    getManagedOpencode: async () => opencode.managed,
    createStageRun: repositories.createStageRun,
    updateStageRun: repositories.updateStageRun,
    availableTools: [
      "compare_backtests",
      "validate_against_thesis",
      "submit_refinement",
    ],
    env: {},
  });

  const result = await runner.run(input, thesis.run_id, 1);

  assert.equal(result.kind, "refine");
  assert.deepEqual(result.reasons, output.reasons);
});

test("adjudicator rejects ambiguous winner plus submit_refinement", async () => {
  const output: AdjudicatorStageOutput = {
    kind: "winner",
    candidate_id: "r1_c1",
    justification: "Winner declared despite refinement.",
  };
  const repositories = createRepositories();
  const opencode = createManagedOpencode(output, ["submit_refinement"]);
  const runner = createAdjudicatorStageRunner(adjudicatorStage, {
    getManagedOpencode: async () => opencode.managed,
    createStageRun: repositories.createStageRun,
    updateStageRun: repositories.updateStageRun,
    availableTools: ["submit_refinement"],
    env: {},
  });

  await assert.rejects(
    () => runner.run(input, thesis.run_id, 1),
    /ambiguous: winner declared after submit_refinement/,
  );
  assert.equal(repositories.updates.at(-1)?.updates.status, "failed");
});
