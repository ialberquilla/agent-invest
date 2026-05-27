import {
  createStageRunner,
  type StageDefinition,
  type StageRunner,
} from "./base";
import type { AdjudicatorStageOutput, RefinementReason } from "./adjudicator";
import type { ThesisJson } from "./thesis";

export type ReporterRoundHistory = {
  round: 1 | 2 | 3;
  candidate_batch_id: string;
  adjudication: AdjudicatorStageOutput;
  refinement_reasons?: RefinementReason[];
};

export type ReporterStageInput = {
  run_id: string;
  thesis: ThesisJson;
  candidate_batch_id: string;
  winner_candidate_id: string | null;
  round_history: ReporterRoundHistory[];
};

export type ReporterStageOutput = {
  result_id: string;
};

export const reporterStage: StageDefinition = {
  name: "reporter",
  allowedTools: ["finalize_strategy_result"],
  terminal: "required",
  promptPath: "reporter.md",
  validateOutput(output, result) {
    validateReporterOutput(output, finalizeWasCalled(result.parts));
  },
};

export function createReporterStageRunner(
  ...args: Parameters<
    typeof createStageRunner<ReporterStageInput, ReporterStageOutput>
  >
): StageRunner<ReporterStageInput, ReporterStageOutput> {
  return createStageRunner<ReporterStageInput, ReporterStageOutput>(...args);
}

export const reporterRunner = createReporterStageRunner(reporterStage);

function validateReporterOutput(output: unknown, finalized: boolean) {
  if (!finalized) {
    throw new Error(
      "reporter output requires finalize_strategy_result to be called",
    );
  }
  if (!isRecord(output)) throw new Error("reporter output must be an object");
  assertString(output.result_id, "result_id");
}

function finalizeWasCalled(parts: unknown[] = []) {
  return /"finalize_strategy_result"|\bfinalize_strategy_result\b/.test(
    JSON.stringify(parts),
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function assertString(value: unknown, name: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
}
