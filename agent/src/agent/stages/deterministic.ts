import { randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import { promisify } from "node:util";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { appendEvent } from "../../db/repositories/agent-events";
import {
  createStageRun,
  updateStageRun,
} from "../../db/repositories/stage-runs";
import {
  runCandidateBatch,
  type Candidate,
} from "../../tools/run-candidate-batch";
import { STAGE_EVENT_TYPES } from "../stage-events";
import type { StageRunner } from "./base";
import type {
  AdjudicatorStageInput,
  AdjudicatorStageOutput,
} from "./adjudicator";
import type { DesignerStageInput, DesignerStageOutput } from "./designer";
import type { ReporterStageInput, ReporterStageOutput } from "./reporter";

const execFileAsync = promisify(execFile);
const scriptsDir = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../../../scripts",
);

export const deterministicDesignerRunner: StageRunner<
  DesignerStageInput,
  DesignerStageOutput
> = {
  async run(input, runId, round) {
    return runPersistedStage(
      "designer",
      runId,
      round,
      input,
      async (stageRunId) => {
        const candidates = buildCandidates(input);
        await appendToolPart(
          runId,
          stageRunId,
          "designer",
          round,
          "run_candidate_batch",
          { run_id: runId, round, candidates },
          "running",
        );
        const batch = await runCandidateBatch({
          run_id: runId,
          round: input.round,
          candidates,
          timeoutSeconds: 120,
        });
        await appendToolPart(
          runId,
          stageRunId,
          "designer",
          round,
          "run_candidate_batch",
          { run_id: runId, round, candidates },
          "completed",
          batch,
        );
        return {
          batch_id: batch.batch_id,
          candidates,
          kpis: {
            design_summary:
              "Generated deterministic candidate batch from thesis constraints and executed local run_candidate_batch.",
            candidate_count: candidates.length,
            template_ids: [
              ...new Set(candidates.map((candidate) => candidate.template_id)),
            ],
            results: batch.results,
            refinement_response: input.refinement_reasons?.length
              ? "Candidates adjusted after refinement reasons."
              : "No refinement reasons were provided.",
          },
        };
      },
    );
  },
};

export const deterministicAdjudicatorRunner: StageRunner<
  AdjudicatorStageInput,
  AdjudicatorStageOutput
> = {
  async run(input, runId, round) {
    return runPersistedStage(
      "adjudicator",
      runId,
      round,
      input,
      async (stageRunId) => {
        const validation = await runPythonTool(
          runId,
          stageRunId,
          "adjudicator",
          round,
          "validate_against_thesis",
          [
            "-m",
            "agent_invest_scripts.validate_against_thesis",
            "--batch-id",
            input.batch_id,
            "--thesis",
            JSON.stringify(input.thesis),
          ],
        );
        const comparison = await runPythonTool(
          runId,
          stageRunId,
          "adjudicator",
          round,
          "compare_backtests",
          [
            "-m",
            "agent_invest_scripts.compare_backtests",
            "--batch-id",
            input.batch_id,
          ],
        );
        const winner =
          typeof comparison.winner_candidate_id === "string"
            ? comparison.winner_candidate_id
            : null;
        const passing = Array.isArray(validation.passing_candidate_ids)
          ? validation.passing_candidate_ids
          : [];
        if (winner && (passing.length === 0 || passing.includes(winner))) {
          return {
            kind: "winner",
            candidate_id: winner,
            justification:
              "Selected highest-ranked candidate from deterministic comparison that passed thesis validation.",
          };
        }
        return {
          kind: "refine",
          reasons: [
            {
              reason: "insufficient_evidence",
              detail:
                "No passing candidate was available after validation/comparison.",
              suggested_fix:
                "Generate a new candidate batch with thesis-compliant configs.",
            },
          ],
        };
      },
    );
  },
};

export const deterministicReporterRunner: StageRunner<
  ReporterStageInput,
  ReporterStageOutput
> = {
  async run(input, runId, round) {
    return runPersistedStage(
      "reporter",
      runId,
      round,
      input,
      async (stageRunId) => {
        const payload = {
          title: input.winner_candidate_id
            ? "Selected crypto allocation candidate"
            : "No viable strategy found",
          summary: input.winner_candidate_id
            ? "The pipeline selected the best validated candidate from the executed batch."
            : "No candidate satisfied the pipeline constraints.",
          reasoning:
            "Result was built from persisted candidate-batch metrics; no KPIs were invented.",
          candidate_batch_id: input.candidate_batch_id,
          winner_candidate_id: input.winner_candidate_id,
          assumptions: [
            "Historical backtests are scenario analysis, not financial advice.",
          ],
          risks: [
            "Crypto assets can experience severe drawdowns and regime changes.",
          ],
          next_steps: [
            "Review constraints and rerun with updated assumptions before making decisions.",
          ],
        };
        if (!input.winner_candidate_id) {
          await ensureNoViableBatch(
            input.candidate_batch_id,
            runId,
            input.round_history,
          );
          Object.assign(payload, {
            result_type: "no_viable_strategy",
            round_history: input.round_history.length
              ? input.round_history
              : [
                  {
                    round: 1,
                    candidate_batch_id: input.candidate_batch_id,
                    adjudication: {
                      kind: "refine",
                      reasons: [
                        {
                          reason: "insufficient_evidence",
                          detail:
                            "Pipeline failed before producing a viable candidate batch.",
                        },
                      ],
                    },
                  },
                ],
          });
        }
        const result = await runPythonTool(
          runId,
          stageRunId,
          "reporter",
          round,
          "finalize_strategy_result",
          [
            "-m",
            "agent_invest_scripts.finalize_strategy_result",
            "--payload",
            JSON.stringify(payload),
          ],
        );
        return { result_id: String(result.result_id) };
      },
    );
  },
};

function buildCandidates(input: DesignerStageInput): Candidate[] {
  const constraints = input.thesis.constraints ?? {};
  const selectTop = clampNumber(
    Number(constraints.target_asset_count_min ?? constraints.min_assets ?? 5),
    3,
    10,
  );
  const ranking = input.thesis.primary_factors?.length
    ? input.thesis.primary_factors
        .map((factor, index) =>
          normalizeRankingFactor(factor.factor, factor.direction, index),
        )
        .filter((factor): factor is NonNullable<typeof factor> =>
          Boolean(factor),
        )
    : [{ factor: "market_cap_rank", direction: "low" as const, weight: 1 }];
  const thesis = {
    objective: input.thesis.objective,
    primary_factors: ranking,
  };
  return [
    {
      candidate_id: `r${input.round}_c1`,
      label: "Large-cap buy and hold",
      template_id: "buy_and_hold",
      ranking,
      select_top: selectTop,
      config: { select_top: selectTop, weighting: "equal" },
      thesis,
    },
    {
      candidate_id: `r${input.round}_c2`,
      label: "Monthly rebalance",
      template_id: "periodic_rebalance",
      ranking,
      select_top: selectTop,
      config: {
        select_top: selectTop,
        weighting: "equal",
        rebalance_trigger: "periodic_30d",
      },
      thesis,
    },
    {
      candidate_id: `r${input.round}_c3`,
      label: "Concentrated quality",
      template_id: "buy_and_hold",
      ranking,
      select_top: Math.max(3, Math.min(selectTop, 5)),
      config: {
        select_top: Math.max(3, Math.min(selectTop, 5)),
        weighting: "equal",
      },
      thesis,
    },
  ];
}

function normalizeRankingFactor(
  factor: string,
  direction: "high" | "low",
  index: number,
) {
  const normalized = factor.toLowerCase();
  const mapped =
    normalized === "momentum"
      ? { factor: "return_180d", direction: "high" as const }
      : normalized === "sharpe"
        ? { factor: "sharpe_180d", direction: "high" as const }
        : normalized === "drawdown"
          ? null
          : normalized === "market_cap"
            ? { factor: "market_cap_rank", direction: "low" as const }
            : { factor, direction };

  return mapped ? { ...mapped, weight: index === 0 ? 1 : 0.5 } : null;
}

async function runPersistedStage<Input extends Record<string, unknown>, Output>(
  stage: "designer" | "adjudicator" | "reporter",
  runId: string,
  round: number,
  input: Input,
  execute: (stageRunId: string) => Promise<Output>,
): Promise<Output> {
  const stageRunId = randomUUID();
  const startedAt = new Date();
  const payload = { stage, round, stage_run_id: stageRunId };
  await createStageRun({
    stageRunId,
    runId,
    stage,
    round,
    status: "running",
    model: "deterministic-local",
    input,
    startedAt,
  });
  await appendEvent({ runId, eventType: STAGE_EVENT_TYPES.started, payload });
  try {
    const output = await execute(stageRunId);
    await updateStageRun(stageRunId, {
      status: "succeeded",
      output: output as Record<string, unknown>,
      endedAt: new Date(),
    });
    await appendEvent({
      runId,
      eventType: STAGE_EVENT_TYPES.completed,
      payload,
    });
    return output;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await updateStageRun(stageRunId, {
      status: "failed",
      error: message,
      endedAt: new Date(),
    });
    await appendEvent({
      runId,
      eventType: STAGE_EVENT_TYPES.failed,
      payload: { ...payload, error: message },
    });
    throw error;
  }
}

async function runPythonTool(
  runId: string,
  stageRunId: string,
  stage: "adjudicator" | "reporter",
  round: number,
  name: string,
  args: string[],
) {
  await appendToolPart(
    runId,
    stageRunId,
    stage,
    round,
    name,
    { args },
    "running",
  );
  const { stdout } = await execFileAsync(
    "uv",
    ["run", "--project", ".", "python", ...args],
    {
      cwd: scriptsDir,
      env: { ...process.env, PYTHONPATH: scriptsDir },
    },
  );
  const parsed = JSON.parse(stdout) as Record<string, unknown>;
  await appendToolPart(
    runId,
    stageRunId,
    stage,
    round,
    name,
    { args },
    "completed",
    parsed,
  );
  return parsed;
}

async function ensureNoViableBatch(
  batchId: string,
  runId: string,
  roundHistory: unknown[],
) {
  if (batchId !== "no_viable_strategy") return;
  const storageRoot =
    process.env.STORAGE_ROOT?.trim() ||
    resolve(
      dirname(fileURLToPath(import.meta.url)),
      "../../../../.data/storage",
    );
  const directory = resolve(storageRoot, "candidate_batches");
  await mkdir(directory, { recursive: true });
  await writeFile(
    resolve(directory, `${batchId}.json`),
    `${JSON.stringify(
      {
        batch_id: batchId,
        run_id: runId,
        round: roundHistory.length || 1,
        results: [],
      },
      null,
      2,
    )}\n`,
    "utf-8",
  );
}

async function appendToolPart(
  runId: string,
  stageRunId: string,
  stage: string,
  round: number,
  tool: string,
  input: unknown,
  status: "running" | "completed",
  output?: unknown,
) {
  await appendEvent({
    runId,
    eventType: "message.part.updated",
    payload: {
      stage,
      round,
      stage_run_id: stageRunId,
      event: {
        type: "message.part.updated",
        properties: {
          part: {
            id: `${stageRunId}:${tool}:${status}`,
            type: "tool",
            tool,
            state: {
              status,
              input,
              metadata:
                output === undefined
                  ? {}
                  : { output: JSON.stringify(output, null, 2) },
            },
          },
        },
      },
    },
  });
}

function clampNumber(value: number, min: number, max: number) {
  if (!Number.isFinite(value)) return min;
  return Math.max(min, Math.min(max, Math.trunc(value)));
}
