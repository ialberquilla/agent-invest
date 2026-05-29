// Eval runner for propose_candidates. Calls the real opencode LLM
// with realistic fixtures, including refinement scenarios that the
// "memory between iterations" design depends on.
//
// Run with:  pnpm eval:propose-candidates

import "../../../env.ts";

import { createOpencodeLLMClient } from "../llm.ts";
import { proposeCandidates } from "./propose_candidates.ts";
import {
  ProposalValidationError,
  type Attempt,
  type Proposal,
  type ProposeCandidatesInput,
  type RefinementHint,
  type Thesis,
  type Universe,
  type Window,
} from "../state.ts";

type Expect = {
  min_candidates?: number;
  max_candidates?: number;
  must_contain_template?: ProposedTemplate;
  must_not_contain_template?: ProposedTemplate;
  must_mention_in_hypothesis?: string[];
  must_implement_change?: keyof RefinementHint["suggested_changes"];
};

type ProposedTemplate = "buy_and_hold" | "periodic_rebalance";

type EvalCase = {
  name: string;
  input: ProposeCandidatesInput;
  expect: Expect;
};

function thesis(overrides: Partial<Thesis> = {}): Thesis {
  return {
    objective: "balanced_growth",
    horizon_days: 365,
    weight_mode: "percentage",
    universe_hints: {
      top_n: 10,
      exclude_stablecoins: true,
      exclude_wrapped: true,
    },
    constraints: {
      max_weight_per_asset: 0.2,
      max_cash_weight: 0.1,
      max_drawdown: 0.35,
      asset_count_min: 5,
      asset_count_max: 10,
    },
    rebalance_frequency: "monthly",
    interpretation_notes: "eval fixture",
    ...overrides,
  };
}

const UNIVERSE: Universe = {
  coin_ids: [
    "bitcoin",
    "ethereum",
    "binancecoin",
    "ripple",
    "solana",
    "tron",
    "dogecoin",
    "hyperliquid",
    "cardano",
    "monero",
  ],
  source: "rank_universe",
  effective_filters: {
    top_n: 10,
    exclude_stablecoins: true,
    exclude_wrapped: true,
  },
};

const WINDOW: Window = {
  start: "2022-05-25",
  end: "2026-05-24",
  horizon_days: 365,
  effective: {
    window_length_days: 1460,
    target_window_length_days: 1460,
    rationale: "ok",
    limiting_coin: "bitcoin",
    covered_drawdowns_count: 2,
  },
};

const PRIOR_PROPOSAL: Proposal = {
  iteration_hypothesis: "Try simple equal-weight large-cap basket.",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "buy_and_hold",
      select_top: 5,
      weighting: "equal",
      rationale: "baseline",
    },
  ],
};

const CASES: EvalCase[] = [
  {
    name: "cold_balanced",
    input: {
      run_id: "eval-propose-cold_balanced",
      thesis: thesis(),
      universe: UNIVERSE,
      window: WINDOW,
    },
    expect: {
      min_candidates: 2,
      max_candidates: 5,
    },
  },
  {
    name: "cold_growth",
    input: {
      run_id: "eval-propose-cold_growth",
      thesis: thesis({ objective: "growth", horizon_days: 180 }),
      universe: UNIVERSE,
      window: { ...WINDOW, horizon_days: 180 },
    },
    expect: {
      min_candidates: 2,
      max_candidates: 5,
    },
  },
  {
    name: "refine_after_drawdown",
    input: {
      run_id: "eval-propose-refine_after_drawdown",
      thesis: thesis(),
      universe: UNIVERSE,
      window: WINDOW,
      attempts: [
        {
          attempt_n: 1,
          proposal: PRIOR_PROPOSAL,
          batch_id: "batch_drawdown",
          validation_summary: {
            passing_candidate_ids: [],
            failing: [
              {
                candidate_id: "c1",
                violations: [
                  {
                    constraint: "max_drawdown",
                    observed: 0.46,
                    target: 0.35,
                  },
                ],
              },
            ],
          },
          refinement_hint: {
            failed_constraints: [
              {
                constraint: "max_drawdown",
                observed: 0.46,
                target: 0.35,
                candidate_id: "c1",
              },
            ],
            suggested_changes: {
              tighten_weight_cap_to: 0.15,
              increase_cash_to: 0.15,
            },
            rationale:
              "Equal-weight 5-asset basket hit 46% drawdown; tighten concentration and add cash buffer.",
          },
        } satisfies Attempt,
      ],
    },
    expect: {
      min_candidates: 1,
      max_candidates: 5,
      must_mention_in_hypothesis: ["drawdown"],
      must_implement_change: "tighten_weight_cap_to",
    },
  },
  {
    name: "refine_with_template_swap",
    input: {
      run_id: "eval-propose-refine_with_template_swap",
      thesis: thesis(),
      universe: UNIVERSE,
      window: WINDOW,
      attempts: [
        {
          attempt_n: 1,
          proposal: PRIOR_PROPOSAL,
          batch_id: "batch_swap",
          validation_summary: {
            passing_candidate_ids: [],
            failing: [
              {
                candidate_id: "c1",
                violations: [
                  {
                    constraint: "benchmark_underperformance",
                    observed: -0.18,
                    target: 0,
                  },
                ],
              },
            ],
          },
          refinement_hint: {
            failed_constraints: [
              {
                constraint: "benchmark_underperformance",
                observed: -0.18,
                target: 0,
                candidate_id: "c1",
              },
            ],
            suggested_changes: {
              change_template_to: "periodic_rebalance",
              change_rebalance_to: "periodic_30d",
            },
            rationale:
              "Buy-and-hold trailed benchmark by 18%; switch to periodic rebalancing.",
          },
        } satisfies Attempt,
      ],
    },
    expect: {
      min_candidates: 1,
      max_candidates: 5,
      must_contain_template: "periodic_rebalance",
      must_not_contain_template: undefined,
    },
  },
];

type CaseOutcome = {
  name: string;
  status: "pass" | "fail";
  duration_ms: number;
  violations: string[];
  proposal?: Proposal;
};

async function main(): Promise<number> {
  const llm = createOpencodeLLMClient({
    sessionTitle: "eval-propose_candidates",
  });
  const outcomes: CaseOutcome[] = [];

  for (const c of CASES) {
    process.stdout.write(`\n[${c.name}] running...\n`);
    const start = Date.now();
    try {
      const result = await proposeCandidates(c.input, { llm });
      const violations = checkExpect(c.expect, result.delta.proposal, c.input);
      const duration_ms = Date.now() - start;
      if (violations.length === 0) {
        process.stdout.write(`[${c.name}] PASS (${duration_ms} ms)\n`);
      } else {
        process.stdout.write(`[${c.name}] FAIL (${duration_ms} ms)\n`);
        for (const v of violations) process.stdout.write(`  - ${v}\n`);
      }
      process.stdout.write(
        `  proposal: ${JSON.stringify(result.delta.proposal)}\n`,
      );
      outcomes.push({
        name: c.name,
        status: violations.length === 0 ? "pass" : "fail",
        duration_ms,
        violations,
        proposal: result.delta.proposal,
      });
    } catch (error) {
      const duration_ms = Date.now() - start;
      const kind =
        error instanceof ProposalValidationError
          ? "ProposalValidationError"
          : error instanceof Error
            ? error.constructor.name
            : "Error";
      const message = error instanceof Error ? error.message : String(error);
      process.stdout.write(
        `[${c.name}] FAIL (${duration_ms} ms, ${kind})\n  ${message}\n`,
      );
      outcomes.push({
        name: c.name,
        status: "fail",
        duration_ms,
        violations: [`${kind}: ${message}`],
      });
    }
  }

  process.stdout.write("\n=== summary ===\n");
  for (const row of outcomes) {
    const reason =
      row.violations.length > 0 ? ` -- ${row.violations.join("; ")}` : "";
    process.stdout.write(
      `  ${row.status.toUpperCase().padEnd(4)} ${row.name.padEnd(32)} ${row.duration_ms} ms${reason}\n`,
    );
  }
  const failed = outcomes.filter((row) => row.status === "fail").length;
  process.stdout.write(
    `\n${outcomes.length - failed}/${outcomes.length} passed\n`,
  );
  return failed > 0 ? 1 : 0;
}

function checkExpect(
  expect: Expect,
  proposal: Proposal,
  input: ProposeCandidatesInput,
): string[] {
  const out: string[] = [];
  const n = proposal.candidates.length;
  if (expect.min_candidates !== undefined && n < expect.min_candidates) {
    out.push(`expected >= ${expect.min_candidates} candidates, got ${n}`);
  }
  if (expect.max_candidates !== undefined && n > expect.max_candidates) {
    out.push(`expected <= ${expect.max_candidates} candidates, got ${n}`);
  }
  if (
    expect.must_contain_template &&
    !proposal.candidates.some((c) => c.template_id === expect.must_contain_template)
  ) {
    out.push(
      `expected at least one candidate with template_id=${expect.must_contain_template}`,
    );
  }
  if (
    expect.must_not_contain_template &&
    proposal.candidates.some(
      (c) => c.template_id === expect.must_not_contain_template,
    )
  ) {
    out.push(
      `expected no candidate with template_id=${expect.must_not_contain_template}`,
    );
  }
  if (expect.must_mention_in_hypothesis) {
    const hypothesis = proposal.iteration_hypothesis.toLowerCase();
    for (const term of expect.must_mention_in_hypothesis) {
      if (!hypothesis.includes(term.toLowerCase())) {
        out.push(`expected iteration_hypothesis to mention "${term}"`);
      }
    }
  }
  if (expect.must_implement_change) {
    const change = expect.must_implement_change;
    const hint = input.attempts?.at(-1)?.refinement_hint?.suggested_changes;
    const observed = implementsChange(proposal, change, hint);
    if (!observed) {
      out.push(`expected proposal to implement refinement change "${change}"`);
    }
  }
  return out;
}

function implementsChange(
  proposal: Proposal,
  change: keyof RefinementHint["suggested_changes"],
  hint: RefinementHint["suggested_changes"] | undefined,
): boolean {
  if (!hint) return false;
  switch (change) {
    case "change_template_to":
      return proposal.candidates.some(
        (c) => c.template_id === hint.change_template_to,
      );
    case "change_rebalance_to":
      return proposal.candidates.some(
        (c) =>
          c.template_id === "periodic_rebalance" &&
          c.rebalance_trigger === hint.change_rebalance_to,
      );
    case "tighten_weight_cap_to":
      // The thesis itself controls the cap; the LLM can implement this by
      // raising select_top so per-asset weight drops. Heuristic: at least
      // one candidate uses select_top strictly larger than the prior
      // proposal's largest select_top OR uses a non-equal weighting.
      return proposal.candidates.some(
        (c) => c.select_top >= 7 || c.weighting !== "equal",
      );
    case "increase_cash_to":
      // Cash isn't directly a candidate field; the LLM responds via
      // higher select_top + concentration changes. Same heuristic.
      return proposal.candidates.some((c) => c.select_top >= 7);
    case "swap_assets":
      // We don't expose per-coin selection in this template scope.
      return proposal.candidates.length > 0;
  }
}

const entry = process.argv[1] ? `file://${process.argv[1]}` : undefined;
if (entry === import.meta.url) {
  main()
    .then((code) => {
      process.exit(code);
    })
    .catch((error: unknown) => {
      process.stderr.write(
        `fatal: ${error instanceof Error ? error.message : String(error)}\n`,
      );
      process.exit(2);
    });
}

export { CASES, checkExpect, main };
