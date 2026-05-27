import assert from "node:assert/strict";
import test from "node:test";

import {
  createReporterStageRunner,
  reporterStage,
  type ReporterStageInput,
  type ReporterStageOutput,
} from "../src/agent/stages/reporter";
import type { ManagedOpencode } from "../src/agent/session";
import type { NewStageRun, StageRun } from "../src/db/schema";

const input: ReporterStageInput = {
  run_id: "run-reporter-stage-1",
  thesis: {
    run_id: "run-reporter-stage-1",
    objective: "balanced",
    primary_factors: [{ factor: "sharpe", direction: "high" }],
    constraints: { max_drawdown: -0.35 },
    horizon_days: 365,
    interpretation_notes: "Balanced risk-adjusted growth.",
  },
  candidate_batch_id: "candidate_batch_test",
  winner_candidate_id: "winner",
  round_history: [
    {
      round: 1,
      candidate_batch_id: "candidate_batch_test",
      adjudication: {
        kind: "winner",
        candidate_id: "winner",
        justification: "Winner validated against the thesis.",
      },
    },
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
  response: ReporterStageOutput,
  toolNames: string[],
) {
  const prompts: unknown[] = [];

  async function* emptyStream() {}

  const managed = {
    client: {
      session: {
        async create() {
          return { data: { id: "session-reporter" } };
        },
        async prompt(promptInput: unknown) {
          prompts.push(promptInput);
          return {
            data: {
              info: { usage: { inputTokens: 37, outputTokens: 11 } },
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

test("reporter stage finalizes a winner and persists result id", async () => {
  const output = { result_id: "candidate_batch_test" };
  const repositories = createRepositories();
  const opencode = createManagedOpencode(output, ["finalize_strategy_result"]);
  const runner = createReporterStageRunner(reporterStage, {
    getManagedOpencode: async () => opencode.managed,
    createStageRun: repositories.createStageRun,
    updateStageRun: repositories.updateStageRun,
    availableTools: ["finalize_strategy_result", "bash"],
    env: {},
  });

  const result = await runner.run(input, input.run_id, 1);

  assert.deepEqual(result, output);
  assert.equal(repositories.creates[0]?.stage, "reporter");
  assert.deepEqual(repositories.updates.at(-1)?.updates.output, output);
  assert.equal(repositories.updates.at(-1)?.updates.status, "succeeded");
  assert.match(JSON.stringify(opencode.prompts[0]), /finalize_strategy_result/);
});

test("reporter rejects output without finalize tool call", async () => {
  const repositories = createRepositories();
  const opencode = createManagedOpencode({ result_id: "missing-tool" }, []);
  const runner = createReporterStageRunner(reporterStage, {
    getManagedOpencode: async () => opencode.managed,
    createStageRun: repositories.createStageRun,
    updateStageRun: repositories.updateStageRun,
    availableTools: ["finalize_strategy_result"],
    env: {},
  });

  await assert.rejects(
    () => runner.run(input, input.run_id, 1),
    /requires finalize_strategy_result to be called/,
  );
});
