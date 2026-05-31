import assert from "node:assert/strict";
import test from "node:test";

import {
  bestCandidate,
  bestPassingCandidate,
  rankCandidates,
} from "../src/agent/workflow/rank.ts";
import type {
  Attempt,
  CandidateMetrics,
  Proposal,
  Thesis,
} from "../src/agent/workflow/state.ts";

const PROPOSAL: Proposal = {
  iteration_hypothesis: "x",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "synthetic_long_allocation",
      select_top: 5,
      weighting: "equal",
      rationale: "r",
    },
  ],
};

function thesis(objective: Thesis["objective"]): Thesis {
  return {
    objective,
    horizon_days: 365,
    weight_mode: "percentage",
    universe_hints: { top_n: 10, exclude_stablecoins: true, exclude_wrapped: true },
    constraints: {
      max_weight_per_asset: 0.2,
      max_cash_weight: 0.1,
      max_drawdown: 0.35,
      asset_count_min: 5,
      asset_count_max: 10,
    },
    rebalance_frequency: "monthly",
    interpretation_notes: "fixture",
  };
}

function metrics(over: Partial<CandidateMetrics> = {}): CandidateMetrics {
  return {
    total_return: 0,
    cagr: 0,
    volatility: 0,
    max_drawdown: 0,
    sharpe: 0,
    sortino: 0,
    calmar: 0,
    composite_score: null,
    ...over,
  };
}

function attempt(
  attempt_n: number,
  candidates: Attempt["validation_summary"] extends infer S
    ? S extends { candidates: infer C }
      ? C
      : never
    : never,
): Attempt {
  return {
    attempt_n,
    proposal: PROPOSAL,
    batch_id: `batch_${attempt_n}`,
    validation_summary: {
      passing_candidate_ids: candidates
        .filter((c) => c.passed)
        .map((c) => c.candidate_id),
      failing: [],
      candidates,
    },
  };
}

test("rankCandidates puts passing candidates before failing ones", () => {
  const attempts = [
    attempt(1, [
      { candidate_id: "fail", passed: false, constraint_distance: 0.1, metrics: metrics({ sharpe: 9 }) },
      { candidate_id: "pass", passed: true, constraint_distance: 0, metrics: metrics({ sharpe: 1 }) },
    ]),
  ];

  const ranked = rankCandidates(attempts, thesis("balanced_growth"));

  assert.equal(ranked[0]?.candidate_id, "pass");
  assert.equal(ranked[1]?.candidate_id, "fail");
  assert.equal(bestPassingCandidate(attempts, thesis("balanced_growth"))?.candidate_id, "pass");
});

test("among failing candidates the closest to constraints wins", () => {
  const attempts = [
    attempt(1, [
      { candidate_id: "far", passed: false, constraint_distance: 0.5, metrics: metrics({ sharpe: 9 }) },
    ]),
    attempt(2, [
      { candidate_id: "near", passed: false, constraint_distance: 0.1, metrics: metrics({ sharpe: 1 }) },
    ]),
  ];

  const best = bestCandidate(attempts, thesis("balanced_growth"));

  // "near" missed by less even though "far" has a far better sharpe.
  assert.equal(best?.candidate_id, "near");
  assert.equal(best?.attempt_n, 2);
});

test("ties on constraint_distance break by the objective metric", () => {
  const attempts = [
    attempt(1, [
      { candidate_id: "lowret", passed: false, constraint_distance: 0.2, metrics: metrics({ total_return: 0.1, sharpe: 2 }) },
      { candidate_id: "highret", passed: false, constraint_distance: 0.2, metrics: metrics({ total_return: 0.9, sharpe: 0.1 }) },
    ]),
  ];

  // growth ranks on total_return -> highret; balanced ranks on sharpe -> lowret.
  assert.equal(bestCandidate(attempts, thesis("growth"))?.candidate_id, "highret");
  assert.equal(bestCandidate(attempts, thesis("balanced_growth"))?.candidate_id, "lowret");
});

test("preserve_capital prefers the smaller drawdown", () => {
  const attempts = [
    attempt(1, [
      { candidate_id: "deepdd", passed: false, constraint_distance: 0.2, metrics: metrics({ max_drawdown: -0.6 }) },
      { candidate_id: "shallowdd", passed: false, constraint_distance: 0.2, metrics: metrics({ max_drawdown: -0.2 }) },
    ]),
  ];

  assert.equal(
    bestCandidate(attempts, thesis("preserve_capital"))?.candidate_id,
    "shallowdd",
  );
});

test("bestCandidate is undefined when no candidate produced a result", () => {
  assert.equal(bestCandidate([], thesis("growth")), undefined);
});
