import assert from "node:assert/strict";
import test from "node:test";

import {
  createDesignerStageRunner,
  designerStage,
  type DesignerStageOutput,
} from "../src/agent/stages/designer";
import type { ThesisJson } from "../src/agent/stages/thesis";
import type { ManagedOpencode } from "../src/agent/session";
import type { NewStageRun, StageRun } from "../src/db/schema";

const thesis: ThesisJson = {
  run_id: "run-designer-stage-1",
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

const candidates: DesignerStageOutput["candidates"] = [
  {
    candidate_id: "r1_c1",
    label: "Equal-weight quality baseline",
    template_id: "periodic_rebalance",
    select_top: 5,
    ranking: [{ factor: "sharpe", direction: "high", weight: 1 }],
    config: { weighting: "equal", rebalance_trigger: "periodic_30d" },
  },
  {
    candidate_id: "r1_c2",
    label: "Momentum tilt",
    template_id: "momentum",
    select_top: 5,
    ranking: [{ factor: "momentum", direction: "high", weight: 1 }],
    config: {
      signal_indicator: "roc",
      signal_indicator_params: { lookback_period: 90 },
      signal_threshold: 0,
      exit_rule: "signal_reverse",
    },
  },
  {
    candidate_id: "r1_c3",
    label: "Drawdown-aware recovery",
    template_id: "periodic_rebalance",
    select_top: 5,
    ranking: [{ factor: "drawdown", direction: "low", weight: 1 }],
    config: {
      weighting: "inverse_volatility",
      rebalance_trigger: "periodic_30d",
    },
  },
];

const output: DesignerStageOutput = {
  batch_id: "22222222-2222-4222-8222-222222222222",
  candidates,
  kpis: {
    design_summary:
      "Designed three distinct candidates across rebalance, momentum, and drawdown-aware hypotheses.",
    candidate_count: 3,
    template_ids: ["periodic_rebalance", "momentum"],
  },
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
          return { data: { id: "session-designer" } };
        },
        async prompt(input: unknown) {
          prompts.push(input);
          return {
            data: {
              info: { usage: { inputTokens: 41, outputTokens: 29 } },
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

test("designer stage produces a batch id and persists stage output", async () => {
  const repositories = createRepositories();
  const opencode = createManagedOpencode(JSON.stringify(output));
  const runner = createDesignerStageRunner(designerStage, {
    getManagedOpencode: async () => opencode.managed,
    createStageRun: repositories.createStageRun,
    updateStageRun: repositories.updateStageRun,
    availableTools: [
      "list_templates",
      "list_registry",
      "rank_universe",
      "analyze_recovery",
      "recommend_backtest_window",
      "run_candidate_batch",
      "bash",
      "read",
    ],
    env: {},
  });

  const result = await runner.run(
    { run_id: thesis.run_id, round: 1, thesis },
    thesis.run_id,
    1,
  );

  assert.equal(result.batch_id, output.batch_id);
  assert.equal(result.candidates.length, 3);
  assert.equal(repositories.creates[0]?.stage, "designer");
  assert.deepEqual(repositories.updates.at(-1)?.updates.output, output);
  assert.equal(repositories.updates.at(-1)?.updates.status, "succeeded");
  assert.match(JSON.stringify(opencode.prompts[0]), /run_candidate_batch/);
});

test("designer round 2 receives refinement reasons and prompt requires addressing them", async () => {
  const repositories = createRepositories();
  const opencode = createManagedOpencode(JSON.stringify(output));
  const runner = createDesignerStageRunner(designerStage, {
    getManagedOpencode: async () => opencode.managed,
    createStageRun: repositories.createStageRun,
    updateStageRun: repositories.updateStageRun,
    availableTools: [
      "list_templates",
      "list_registry",
      "rank_universe",
      "analyze_recovery",
      "recommend_backtest_window",
      "run_candidate_batch",
    ],
    env: {},
  });
  const refinement_reasons = [
    {
      candidate_id: "r1_c3",
      reason: "constraint_violation",
      detail: "Max drawdown exceeded thesis limit.",
    },
  ];

  await runner.run(
    { run_id: thesis.run_id, round: 2, thesis, refinement_reasons },
    thesis.run_id,
    2,
  );

  const stageInput = repositories.creates[0]?.input as Record<string, unknown>;
  assert.deepEqual(stageInput.refinement_reasons, refinement_reasons);
  assert.match(JSON.stringify(opencode.prompts[0]), /refinement_reasons/);
  assert.match(JSON.stringify(opencode.prompts[0]), /must drive the design/);
});

test("designer stage only allowlists designer tools", async () => {
  const repositories = createRepositories();
  const opencode = createManagedOpencode(JSON.stringify(output));
  const runner = createDesignerStageRunner(designerStage, {
    getManagedOpencode: async () => opencode.managed,
    createStageRun: repositories.createStageRun,
    updateStageRun: repositories.updateStageRun,
    availableTools: ["run_candidate_batch", "record_investment_thesis", "bash"],
    env: {},
  });

  await runner.run(
    { run_id: thesis.run_id, round: 1, thesis },
    thesis.run_id,
    1,
  );

  const prompt = opencode.prompts[0] as {
    body: { tools: Record<string, boolean> };
  };
  assert.equal(prompt.body.tools.run_candidate_batch, true);
  assert.equal(prompt.body.tools.record_investment_thesis, false);
  assert.equal(prompt.body.tools.bash, false);
});
