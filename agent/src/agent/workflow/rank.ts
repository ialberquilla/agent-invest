// Deterministic cross-attempt candidate ranker.
//
// Used in two places:
//   - decide picks the best PASSING candidate for stop_winner (informed,
//     not blind).
//   - the controller picks the closest-fit candidate for stop_best_effort
//     when nothing passed, so the workflow always finalizes a strategy to
//     show instead of giving up with no_viable.
//
// Pure: no LLM, no IO. Ordering is total and reproducible so the winner
// choice is defensible.

import type {
  Attempt,
  CandidateMetrics,
  Objective,
  Thesis,
} from "./state.ts";

export type RankedCandidate = {
  candidate_id: string;
  // Which attempt this candidate belongs to (1-based, matches Attempt.attempt_n).
  attempt_n: number;
  batch_id?: string;
  passed: boolean;
  constraint_distance: number;
  metrics?: CandidateMetrics;
  // The objective-aligned metric the ranker tie-breaks on (higher is
  // better after the sign normalization in objectiveMetric). Exposed for
  // logging/inspection.
  objective_metric: number;
};

// The single metric we maximize for a given objective. preserve_capital
// flips drawdown's sign so "less drawdown" reads as "higher is better",
// keeping the comparator uniformly descending.
export function objectiveMetric(
  objective: Objective,
  metrics: CandidateMetrics | undefined,
): number {
  if (!metrics) return Number.NEGATIVE_INFINITY;
  switch (objective) {
    case "growth":
      return metrics.total_return;
    case "preserve_capital":
      return -Math.abs(metrics.max_drawdown);
    case "income":
    case "balanced_growth":
      return metrics.sharpe;
  }
}

// Flatten every backtested candidate across all attempts into a single
// ranked list. Order: passing candidates first; then by smallest
// constraint_distance (closest to satisfying the thesis); ties broken by
// the objective-aligned metric (descending). Candidates that never
// produced metrics sort last.
export function rankCandidates(
  attempts: Attempt[],
  thesis: Thesis,
): RankedCandidate[] {
  const rows: RankedCandidate[] = [];
  for (const attempt of attempts) {
    const outcomes = attempt.validation_summary?.candidates ?? [];
    for (const outcome of outcomes) {
      rows.push({
        candidate_id: outcome.candidate_id,
        attempt_n: attempt.attempt_n,
        batch_id: attempt.batch_id,
        passed: outcome.passed,
        constraint_distance: outcome.constraint_distance,
        metrics: outcome.metrics,
        objective_metric: objectiveMetric(thesis.objective, outcome.metrics),
      });
    }
  }

  rows.sort((a, b) => {
    // 1. passing before failing
    if (a.passed !== b.passed) return a.passed ? -1 : 1;
    // 2. closest to the constraints first (only meaningful among failing;
    //    passing candidates all have distance 0)
    if (a.constraint_distance !== b.constraint_distance) {
      return a.constraint_distance - b.constraint_distance;
    }
    // 3. better objective metric first; NaN/missing sort last
    return compareObjectiveDesc(a.objective_metric, b.objective_metric);
  });

  return rows;
}

// The best candidate across all attempts, or undefined when no candidate
// ever produced a backtest result (the genuine no_viable case).
export function bestCandidate(
  attempts: Attempt[],
  thesis: Thesis,
): RankedCandidate | undefined {
  return rankCandidates(attempts, thesis)[0];
}

// The best PASSING candidate across all attempts (for stop_winner), or
// undefined when nothing passed.
export function bestPassingCandidate(
  attempts: Attempt[],
  thesis: Thesis,
): RankedCandidate | undefined {
  return rankCandidates(attempts, thesis).find((r) => r.passed);
}

function compareObjectiveDesc(a: number, b: number): number {
  const aBad = !Number.isFinite(a);
  const bBad = !Number.isFinite(b);
  if (aBad && bBad) return 0;
  if (aBad) return 1;
  if (bBad) return -1;
  return b - a;
}
