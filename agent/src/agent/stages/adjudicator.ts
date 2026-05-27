import {
  createStageRunner,
  type StageDefinition,
  type StageRunner,
} from "./base";
import type { Candidate } from "../../tools/run-candidate-batch";
import type { ThesisJson } from "./thesis";

export type RefinementReason = {
  candidate_id?: string;
  reason:
    | "constraint_violation"
    | "thesis_mismatch"
    | "weak_performance"
    | "benchmark_underperformance"
    | "robustness_warning"
    | "insufficient_evidence";
  detail: string;
  suggested_fix?: string;
};

export type AdjudicatorStageInput = {
  run_id: string;
  round: 1 | 2 | 3;
  thesis: ThesisJson;
  batch_id: string;
  candidates: Candidate[];
  kpis?: Record<string, unknown>;
};

export type AdjudicatorStageOutput =
  | {
      kind: "winner";
      candidate_id: string;
      justification: string;
    }
  | {
      kind: "refine";
      reasons: RefinementReason[];
    };

export const adjudicatorStage: StageDefinition = {
  name: "adjudicator",
  allowedTools: [
    "compare_backtests",
    "validate_against_thesis",
    "submit_refinement",
  ],
  terminal: "required",
  promptPath: "adjudicator.md",
  validateOutput(output, result) {
    validateAdjudicatorOutput(output, submitRefinementWasCalled(result.parts));
  },
};

export function createAdjudicatorStageRunner(
  ...args: Parameters<
    typeof createStageRunner<AdjudicatorStageInput, AdjudicatorStageOutput>
  >
): StageRunner<AdjudicatorStageInput, AdjudicatorStageOutput> {
  return createStageRunner<AdjudicatorStageInput, AdjudicatorStageOutput>(
    ...args,
  );
}

export const adjudicatorRunner = createAdjudicatorStageRunner(adjudicatorStage);

function validateAdjudicatorOutput(
  output: unknown,
  refinementSubmitted: boolean,
) {
  if (!isRecord(output))
    throw new Error("adjudicator output must be an object");

  if (output.kind === "winner") {
    if (refinementSubmitted) {
      throw new Error(
        "adjudicator output is ambiguous: winner declared after submit_refinement was called",
      );
    }
    assertString(output.candidate_id, "candidate_id");
    assertString(output.justification, "justification");
    return;
  }

  if (output.kind === "refine") {
    if (!refinementSubmitted) {
      throw new Error(
        "adjudicator refinement output requires submit_refinement to be called",
      );
    }
    if (!Array.isArray(output.reasons) || output.reasons.length === 0) {
      throw new Error("reasons must be a non-empty array");
    }
    for (const [index, reason] of output.reasons.entries()) {
      validateRefinementReason(reason, `reasons[${index}]`);
    }
    return;
  }

  throw new Error("adjudicator output kind must be winner or refine");
}

function validateRefinementReason(reason: unknown, path: string) {
  if (!isRecord(reason)) throw new Error(`${path} must be an object`);
  if (reason.candidate_id !== undefined) {
    assertString(reason.candidate_id, `${path}.candidate_id`);
  }
  assertString(reason.reason, `${path}.reason`);
  assertString(reason.detail, `${path}.detail`);
  if (reason.suggested_fix !== undefined) {
    assertString(reason.suggested_fix, `${path}.suggested_fix`);
  }
}

function submitRefinementWasCalled(parts: unknown[] = []) {
  return /"submit_refinement"|\bsubmit_refinement\b/.test(
    JSON.stringify(parts),
  );
}

function assertString(value: unknown, path: string) {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${path} must be a non-empty string`);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
