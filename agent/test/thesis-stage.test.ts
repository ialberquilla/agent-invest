import assert from "node:assert/strict";
import test from "node:test";

import {
  createThesisStageRunner,
  formatThesisStageInput,
  thesisStage,
  type ThesisJson,
  type ThesisStageOutput,
} from "../src/agent/stages/thesis";
import type { ManagedOpencode } from "../src/agent/session";
import type { AllocationWizardParams } from "../src/agent/prompt";
import type { NewStageRun, StageRun } from "../src/db/schema";

const wizard: AllocationWizardParams = {
  universe: "top25",
  exclusions: ["stablecoins"],
  minimumMarketCap: "1b",
  concentrationLimit: "20",
  maxDrawdown: "35",
  riskPreference: "balanced",
  horizon: "1y",
  rebalance: "monthly",
  initialCapitalUsd: "10000",
  cashAllocation: "10",
  targetAssets: "5-10",
};

const thesis: ThesisJson = {
  run_id: "run-thesis-stage-1",
  objective: "balanced",
  primary_factors: [
    { factor: "sharpe", direction: "high" },
    { factor: "drawdown", direction: "low" },
  ],
  constraints: {
    excluded_assets: ["stablecoins"],
    max_allocation_pct: 20,
    max_drawdown: -0.35,
    min_market_cap_usd: 1_000_000_000,
    rebalance: "monthly",
  },
  horizon_days: 365,
  interpretation_notes: "Balanced wizard brief mapped to risk-adjusted growth.",
};

const output: ThesisStageOutput = {
  thesis_id: "11111111-1111-4111-8111-111111111111",
  thesis,
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
    async createStageRun(input: NewStageRun) {
      creates.push(input);
      return input as StageRun;
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

function createManagedOpencode(response: string) {
  const prompts: unknown[] = [];

  async function* emptyStream() {}

  const managed = {
    client: {
      session: {
        async create() {
          return { data: { id: "session-1" } };
        },
        async prompt(input: unknown) {
          prompts.push(input);
          return {
            data: {
              info: { usage: { inputTokens: 23, outputTokens: 17 } },
              parts: [{ type: "text", text: response }],
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

test("formats wizard briefs only for the thesis stage", () => {
  const formatted = formatThesisStageInput({ run_id: "run-1", wizard });

  assert.equal(formatted.run_id, "run-1");
  assert.match(
    formatted.request,
    /Create an educational investment research brief/,
  );
  assert.match(formatted.request, /Risk preference: balanced growth/);
});

test("passes free-text thesis inputs through unchanged", () => {
  const formatted = formatThesisStageInput({
    run_id: "run-1",
    request: "Build me a conservative BTC and ETH allocation.",
  });

  assert.deepEqual(formatted, {
    run_id: "run-1",
    request: "Build me a conservative BTC and ETH allocation.",
  });
});

test("thesis stage persists a well-formed output for a wizard brief", async () => {
  const repositories = createRepositories();
  const opencode = createManagedOpencode(JSON.stringify(output));
  const runner = createThesisStageRunner(thesisStage, {
    getManagedOpencode: async () => opencode.managed,
    createStageRun: repositories.createStageRun,
    updateStageRun: repositories.updateStageRun,
    availableTools: ["record_investment_thesis", "bash", "read"],
    env: {},
  });

  const result = await runner.run(
    { run_id: thesis.run_id, wizard },
    thesis.run_id,
    1,
  );

  assert.deepEqual(result, output);
  assert.equal(repositories.creates[0]?.stage, "thesis");
  assert.equal(repositories.updates.at(-1)?.updates.status, "succeeded");
  assert.deepEqual(repositories.updates.at(-1)?.updates.output, output);
  assert.equal(output.thesis.objective, "balanced");
  assert.ok(output.thesis.primary_factors.length > 0);
  assert.equal(Number.isInteger(output.thesis.horizon_days), true);
  assert.match(JSON.stringify(opencode.prompts[0]), /record_investment_thesis/);
});

test("thesis stage only allowlists record_investment_thesis", async () => {
  const repositories = createRepositories();
  const opencode = createManagedOpencode(JSON.stringify(output));
  const runner = createThesisStageRunner(thesisStage, {
    getManagedOpencode: async () => opencode.managed,
    createStageRun: repositories.createStageRun,
    updateStageRun: repositories.updateStageRun,
    availableTools: ["record_investment_thesis", "bash", "read"],
    env: {},
  });

  await runner.run(
    { run_id: thesis.run_id, request: "Balanced crypto allocation" },
    thesis.run_id,
    1,
  );

  const prompt = opencode.prompts[0] as {
    body: { tools: Record<string, boolean> };
  };
  assert.deepEqual(prompt.body.tools, {
    record_investment_thesis: true,
    bash: false,
    read: false,
  });
});
