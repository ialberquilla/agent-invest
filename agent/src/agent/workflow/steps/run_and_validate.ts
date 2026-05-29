// run_and_validate -- fifth workflow step. Deterministic. Assembles a
// run_candidate_batch payload from the proposal + already-resolved
// universe + window, executes the batch backtest, runs the
// deterministic thesis gate, and normalizes the result into the
// compact AttemptValidationSummary that the next propose_candidates
// round will consume.

import {
  runCandidateBatch,
  runValidateAgainstThesis,
  type RunCandidateBatchRequest,
  type ValidateAgainstThesisResponse,
} from "../cli.ts";
import { createStepLogger, type StepLogger } from "../logging.ts";
import type {
  Attempt,
  AttemptValidationSummary,
  Proposal,
  ProposedCandidate,
  RunAndValidateInput,
  StepName,
  Thesis,
  Universe,
  Window,
} from "../state.ts";

export type RunAndValidateDeps = {
  runCandidateBatch?: typeof runCandidateBatch;
  runValidateAgainstThesis?: typeof runValidateAgainstThesis;
  logger?: StepLogger;
  timeoutSeconds?: number;
};

export type RunAndValidateResult = {
  delta: {
    batch_id: string;
    validation_summary: AttemptValidationSummary;
  };
  next: StepName;
};

export const NEXT_STEP: StepName = "decide";

export async function runAndValidate(
  input: RunAndValidateInput,
  deps: RunAndValidateDeps = {},
): Promise<RunAndValidateResult> {
  const attemptNumber = (input.attempts?.length ?? 0) + 1;
  const logger =
    deps.logger ??
    createStepLogger({
      run_id: input.run_id,
      step: "run_and_validate",
      attempt: attemptNumber,
    });
  const batchFn = deps.runCandidateBatch ?? runCandidateBatch;
  const validateFn = deps.runValidateAgainstThesis ?? runValidateAgainstThesis;
  const timeoutSeconds = deps.timeoutSeconds;

  logger.enter({
    round: attemptNumber,
    candidate_count: input.proposal.candidates.length,
    universe_size: input.universe.coin_ids.length,
    window: `${input.window.start}..${input.window.end}`,
  });

  try {
    const batchInput = buildBatchInput(input, attemptNumber);
    const batchResponse = await batchFn(batchInput, { timeoutSeconds });

    logger.llmResponse({
      response_chars: batchResponse.batch_id.length,
    });

    const validation = await validateFn(
      {
        batch: batchResponse,
        thesis: thesisForValidate(input.thesis),
      },
      { timeoutSeconds },
    );

    const summary = normalizeValidationSummary(validation);

    logger.exit(NEXT_STEP, {
      batch_id: batchResponse.batch_id,
      passing_count: summary.passing_candidate_ids.length,
      failing_count: summary.failing.length,
    });

    return {
      delta: {
        batch_id: batchResponse.batch_id,
        validation_summary: summary,
      },
      next: NEXT_STEP,
    };
  } catch (error) {
    logger.error(error);
    throw error;
  }
}

export function buildBatchInput(
  input: RunAndValidateInput,
  round: number,
): RunCandidateBatchRequest {
  return {
    run_id: input.run_id,
    round,
    iteration_hypothesis: input.proposal.iteration_hypothesis,
    universe_override: {
      id: "hand_picked",
      params: { coin_ids: input.universe.coin_ids },
    },
    window_override: {
      start: input.window.start,
      end: input.window.end,
    },
    candidates: input.proposal.candidates.map((c) =>
      candidateToBatchEntry(c, input.thesis),
    ),
  };
}

function candidateToBatchEntry(
  candidate: ProposedCandidate,
  thesis: Thesis,
) {
  const config: Record<string, unknown> = { weighting: candidate.weighting };
  if (candidate.template_id === "periodic_rebalance") {
    config.rebalance_trigger = candidate.rebalance_trigger;
  }
  return {
    candidate_id: candidate.candidate_id,
    template_id: candidate.template_id,
    select_top: candidate.select_top,
    config,
    // run_candidate_batch picks the benchmark from candidate.thesis.objective.
    // Forward the workflow objective (mapped to the script's enum) so each
    // candidate is scored against the appropriate benchmark.
    thesis: {
      objective: scriptObjectiveFromWorkflow(thesis.objective),
    },
  };
}

// Map workflow Objective values to the script's benchmark Objective
// enum ("high_growth" | "balanced" | "preserve_capital" | "income").
export function scriptObjectiveFromWorkflow(
  objective: Thesis["objective"],
): "high_growth" | "balanced" | "preserve_capital" | "income" {
  switch (objective) {
    case "balanced_growth":
      return "balanced";
    case "growth":
      return "high_growth";
    case "income":
      return "income";
    case "preserve_capital":
      return "preserve_capital";
  }
}

// validate_against_thesis only consumes objective, horizon_days and
// constraints. Send the trimmed shape (the workflow's Thesis has more
// fields that the Python script would just ignore).
//
// Sign convention adapter: the script checks max_drawdown as
// `actual < expected` where actual is the realised drawdown (always
// negative, e.g. -0.53). Its THESIS_EXAMPLE shows expected as a
// positive number (0.35), which makes every drawdown a violation.
// The workflow stores max_drawdown as positive (user-facing
// convention); we negate it here so the floor check matches semantics.
export function thesisForValidate(thesis: Thesis): Record<string, unknown> {
  return {
    objective: thesis.objective,
    horizon_days: thesis.horizon_days,
    constraints: {
      ...thesis.constraints,
      max_drawdown: -Math.abs(thesis.constraints.max_drawdown),
    },
  };
}

export function normalizeValidationSummary(
  response: ValidateAgainstThesisResponse,
): AttemptValidationSummary {
  const failing = (response.results ?? [])
    .filter((row) => row.passed === false)
    .map((row) => ({
      candidate_id: row.candidate_id,
      violations: (row.violations ?? []).map((v) => ({
        constraint: String(v.constraint ?? "unknown"),
        observed: coerceNumber(v.actual),
        target: coerceNumber(v.expected),
      })),
    }));
  return {
    passing_candidate_ids: Array.isArray(response.passing_candidate_ids)
      ? response.passing_candidate_ids
      : [],
    failing,
  };
}

function coerceNumber(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  // Use NaN as a sentinel so downstream consumers see a non-finite
  // value rather than silently 0. AttemptValidationSummary fields are
  // shown to the next LLM round; a NaN reads as "unknown" there.
  return Number.NaN;
}

// Re-export for callers wanting types from the step entrypoint.
export type { Universe, Window, Proposal, Attempt };
