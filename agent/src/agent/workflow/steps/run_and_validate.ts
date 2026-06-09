// run_and_validate -- fifth workflow step. Deterministic. Assembles a
// run_candidate_batch payload from the proposal + already-resolved
// universe + window, executes the batch backtest, runs the
// deterministic thesis gate, and normalizes the result into the
// compact AttemptValidationSummary that the next propose_candidates
// round will consume.

import {
  runCandidateBatch,
  runValidateAgainstThesis,
  type RunCandidateBatchResponse,
  type RunCandidateBatchRequest,
  type ValidateAgainstThesisResponse,
} from "../cli.ts";
import { createStepLogger, type StepLogger } from "../logging.ts";
import { resolveAllowedSides, resolveStrategyMode } from "../state.ts";
import type {
  AllocationWeight,
  Attempt,
  AttemptValidationSummary,
  CandidateBacktest,
  CandidateMetrics,
  CandidateOutcome,
  EquityPoint,
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
    // Full backtest output (metrics + equity/benchmark curves) per
    // candidate_id. The controller stashes these on
    // WorkflowState.backtests keyed by attempt; only the winner's is
    // ever surfaced. Empty when the batch produced no usable rows.
    backtests: Record<string, CandidateBacktest>;
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

    const summary = normalizeValidationSummary(validation, batchResponse);
    const backtests = indexBacktests(batchResponse);

    logger.exit(NEXT_STEP, {
      batch_id: batchResponse.batch_id,
      passing_count: summary.passing_candidate_ids.length,
      failing_count: summary.failing.length,
    });

    return {
      delta: {
        batch_id: batchResponse.batch_id,
        validation_summary: summary,
        backtests,
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
  const config: Record<string, unknown> = {};
  if (candidate.template_id === "single_asset_trend_setup") {
    // The single-asset recipe takes no weighting/select_top slot; its
    // config is the trend window and (optionally) the pinned coin.
    if (candidate.sma_lookback !== undefined) {
      config.sma_lookback = candidate.sma_lookback;
    }
    const target = candidate.target_coin_id ?? thesis.target_coin_id;
    if (target !== undefined) {
      config.target_coin_id = target;
    }
  } else if (candidate.template_id === "explicit_pair_trade") {
    // The pair recipe takes named legs and an optional hedge ratio; no
    // weighting/select_top slot.
    if (candidate.long_coin_id !== undefined) {
      config.long_coin_id = candidate.long_coin_id;
    }
    if (candidate.short_coin_id !== undefined) {
      config.short_coin_id = candidate.short_coin_id;
    }
    if (candidate.hedge_ratio !== undefined) {
      config.hedge_ratio = candidate.hedge_ratio;
    }
  } else {
    config.weighting = candidate.weighting;
    if (candidate.rebalance_trigger !== undefined) {
      config.rebalance_trigger = candidate.rebalance_trigger;
    }
    if (candidate.core_weight !== undefined) {
      config.core_weight = candidate.core_weight;
    }
    if (candidate.sleeve_cap !== undefined) {
      config.sleeve_cap = candidate.sleeve_cap;
    }
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
  const dd = -Math.abs(thesis.constraints.max_drawdown);
  // Short-bearing books (pair/hedge/long-short) are not long baskets, so the
  // long-only rules (max_weight_per_asset, asset_count) don't apply. Validate
  // drawdown plus any explicit exposure ceilings the thesis carries.
  if (resolveAllowedSides(thesis) === "long_short" || usesShorts(thesis)) {
    const constraints: Record<string, unknown> = { max_drawdown: dd };
    for (const key of [
      "max_gross_exposure",
      "max_net_exposure",
      "max_leg_weight",
    ] as const) {
      const value = thesis.constraints[key];
      if (typeof value === "number") constraints[key] = value;
    }
    return {
      objective: thesis.objective,
      horizon_days: thesis.horizon_days,
      constraints,
    };
  }
  return {
    objective: thesis.objective,
    horizon_days: thesis.horizon_days,
    constraints: {
      ...thesis.constraints,
      max_drawdown: dd,
    },
  };
}

function usesShorts(thesis: Thesis): boolean {
  const mode = resolveStrategyMode(thesis);
  return (
    mode === "pair_trade" ||
    mode === "hedge_overlay" ||
    mode === "long_short_portfolio"
  );
}

export function normalizeValidationSummary(
  response: ValidateAgainstThesisResponse,
  batch?: RunCandidateBatchResponse,
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

  // Index backtest metrics from the batch result rows by candidate_id so
  // each outcome carries its numbers. Tolerant of a missing batch (older
  // callers / tests) -- candidates then have no metrics and rank last.
  const metricsById = indexMetrics(batch);

  const candidates: CandidateOutcome[] = (response.results ?? []).map((row) => {
    const violations = (row.violations ?? []).map((v) => ({
      observed: coerceNumber(v.actual),
      target: coerceNumber(v.expected),
    }));
    return {
      candidate_id: row.candidate_id,
      passed: row.passed === true,
      constraint_distance:
        row.passed === true ? 0 : constraintDistance(violations),
      metrics: metricsById.get(row.candidate_id),
    };
  });

  return {
    passing_candidate_ids: Array.isArray(response.passing_candidate_ids)
      ? response.passing_candidate_ids
      : [],
    failing,
    candidates,
  };
}

// Sum of normalized constraint overshoot. Each violation contributes
// |observed - target| / max(|target|, 1) so constraints on different
// scales (a 0.4 drawdown vs a 5-asset count) are comparable. A
// non-finite observed/target contributes a large fixed penalty rather
// than NaN-poisoning the whole distance.
function constraintDistance(
  violations: Array<{ observed: number; target: number }>,
): number {
  let total = 0;
  for (const v of violations) {
    if (!Number.isFinite(v.observed) || !Number.isFinite(v.target)) {
      total += 1;
      continue;
    }
    total += Math.abs(v.observed - v.target) / Math.max(Math.abs(v.target), 1);
  }
  return total;
}

function indexMetrics(
  batch: RunCandidateBatchResponse | undefined,
): Map<string, CandidateMetrics> {
  const out = new Map<string, CandidateMetrics>();
  for (const row of batch?.results ?? []) {
    const r = asResultRow(row);
    if (!r) continue;
    const metrics = metricsFromRow(r.row);
    if (metrics) out.set(r.id, metrics);
  }
  return out;
}

// Extract the full backtest (metrics + equity/benchmark curves) per
// candidate from the batch response. The curves are already in the
// Python output (result.to_dict's equity_curve/benchmark_curve); we
// just keep them instead of discarding them after the gate. Candidates
// without metrics are skipped (nothing useful to show).
export function indexBacktests(
  batch: RunCandidateBatchResponse | undefined,
): Record<string, CandidateBacktest> {
  const out: Record<string, CandidateBacktest> = {};
  for (const row of batch?.results ?? []) {
    const r = asResultRow(row);
    if (!r) continue;
    const metrics = metricsFromRow(r.row);
    if (!metrics) continue;
    out[r.id] = {
      metrics,
      equity_curve: extractCurve(r.row.equity_curve),
      benchmark_curve: extractCurve(r.row.benchmark_curve),
      allocation: extractAllocation(r.row),
    };
  }
  return out;
}

// Narrow a raw result entry to an object with a string candidate_id.
function asResultRow(
  row: unknown,
): { id: string; row: Record<string, unknown> } | undefined {
  if (typeof row !== "object" || row === null) return undefined;
  const r = row as Record<string, unknown>;
  if (typeof r.candidate_id !== "string") return undefined;
  return { id: r.candidate_id, row: r };
}

function metricsFromRow(
  row: Record<string, unknown>,
): CandidateMetrics | undefined {
  const m = row.metrics;
  if (typeof m !== "object" || m === null) return undefined;
  const mm = m as Record<string, unknown>;
  return {
    total_return: coerceNumber(mm.total_return),
    cagr: coerceNumber(mm.cagr),
    volatility: coerceNumber(mm.volatility),
    max_drawdown: coerceNumber(mm.max_drawdown),
    sharpe: coerceNumber(mm.sharpe),
    sortino: coerceNumber(mm.sortino),
    calmar: coerceNumber(mm.calmar),
    composite_score:
      typeof row.composite_score === "number" &&
      Number.isFinite(row.composite_score)
        ? row.composite_score
        : null,
  };
}

// Normalize a Python `_series_to_records` list ([{date, value,
// drawdown_pct}, ...]) into EquityPoint[]. Drops malformed entries
// rather than throwing -- a missing or non-array curve yields [].
function extractCurve(value: unknown): EquityPoint[] {
  if (!Array.isArray(value)) return [];
  const points: EquityPoint[] = [];
  for (const entry of value) {
    if (typeof entry !== "object" || entry === null) continue;
    const rec = entry as Record<string, unknown>;
    if (typeof rec.date !== "string") continue;
    points.push({
      date: rec.date,
      value: coerceNumber(rec.value),
      drawdown_pct: coerceNumber(rec.drawdown_pct),
    });
  }
  return points;
}

// Pull the target allocation from the backtest's allocation_metrics: the
// weights of the first rebalance in holdings_history. Drops zero/near-zero
// weights and sorts heaviest first. Returns [] when nothing is available.
function extractAllocation(row: Record<string, unknown>): AllocationWeight[] {
  const am = row.allocation_metrics;
  if (typeof am !== "object" || am === null) return [];
  const history = (am as Record<string, unknown>).holdings_history;
  if (!Array.isArray(history) || history.length === 0) return [];
  const first = history[0];
  if (typeof first !== "object" || first === null) return [];
  const weights = (first as Record<string, unknown>).weights;
  if (typeof weights !== "object" || weights === null) return [];

  const out: AllocationWeight[] = [];
  for (const [coin_id, raw] of Object.entries(
    weights as Record<string, unknown>,
  )) {
    const weight = coerceNumber(raw);
    if (Number.isFinite(weight) && Math.abs(weight) > 1e-6) {
      out.push({ coin_id, weight });
    }
  }
  out.sort((a, b) => b.weight - a.weight);
  return out;
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
