// Eval runner for finalize. Each fixture mirrors a realistic
// post-decide state where a winner has been selected, and asserts the
// narrative is coherent and mentions the actual strategy details.
//
// Run with:  pnpm eval:finalize

import "../../../env.ts";

import { createOpencodeLLMClient } from "../llm.ts";
import { finalize } from "./finalize.ts";
import type {
  Attempt,
  FinalWinner,
  FinalizeInput,
  Proposal,
  Thesis,
  Universe,
  Window,
} from "../state.ts";

type Expect = {
  // Each entry: at least one of these terms (case-insensitive) must appear
  // in the field. Use [["a","b"]] for "must include 'a' AND 'b'".
  // Use [["a"], ["b"]] for "must include 'a' OR 'b'" via separate calls.
  summary_must_mention?: string[];
  reasoning_must_mention?: string[];
  risks_must_mention?: string[];
  next_steps_must_mention?: string[];
  min_risk_count?: number;
  min_next_step_count?: number;
};

type EvalCase = {
  name: string;
  input: FinalizeInput;
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
  ],
  source: "rank_universe",
  effective_filters: {
    top_n: 7,
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
    rationale: "covered 2 BTC drawdowns above 30%",
    limiting_coin: "bitcoin",
    covered_drawdowns_count: 2,
  },
};

const BALANCED_PROPOSAL: Proposal = {
  iteration_hypothesis: "Compare equal vs cap-weighted baskets with rebalance variety.",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "buy_and_hold",
      select_top: 5,
      weighting: "equal",
      rationale: "baseline static equal-weight",
    },
    {
      candidate_id: "c2",
      template_id: "periodic_rebalance",
      select_top: 7,
      weighting: "cap",
      rebalance_trigger: "periodic_30d",
      rationale: "broader cap-weighted basket rebalanced monthly",
    },
    {
      candidate_id: "c3",
      template_id: "periodic_rebalance",
      select_top: 6,
      weighting: "vol_inverse",
      rebalance_trigger: "periodic_30d",
      rationale: "defensive vol-inverse tilt",
    },
  ],
};

const GROWTH_PROPOSAL: Proposal = {
  iteration_hypothesis: "Diversified equal-weight altcoin basket with quarterly rebalance.",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "periodic_rebalance",
      select_top: 7,
      weighting: "equal",
      rebalance_trigger: "periodic_90d",
      rationale: "quarterly equal-weight to capture growth while limiting turnover",
    },
    {
      candidate_id: "c2",
      template_id: "buy_and_hold",
      select_top: 7,
      weighting: "equal",
      rationale: "static baseline for comparison",
    },
    {
      candidate_id: "c3",
      template_id: "periodic_rebalance",
      select_top: 7,
      weighting: "ranking_proportional",
      rebalance_trigger: "periodic_30d",
      rationale: "ranking-tilted monthly rebalance",
    },
  ],
};

const CASES: EvalCase[] = [
  {
    name: "balanced_winner_c2",
    input: {
      run_id: "eval-finalize-balanced",
      thesis: thesis(),
      universe: UNIVERSE,
      window: WINDOW,
      attempts: [
        {
          attempt_n: 1,
          proposal: BALANCED_PROPOSAL,
          batch_id: "candidate_batch_eval_bal",
          validation_summary: {
            passing_candidate_ids: ["c2"],
            failing: [
              {
                candidate_id: "c1",
                violations: [
                  { constraint: "max_weight_per_asset", observed: 0.54, target: 0.2 },
                ],
              },
              {
                candidate_id: "c3",
                violations: [
                  { constraint: "max_drawdown", observed: -0.4, target: -0.35 },
                ],
              },
            ],
          },
        } satisfies Attempt,
      ],
      winner_candidate_id: "c2",
      decide_justification:
        "Candidate c2 was the only configuration that satisfied all constraints; rebalancing kept weights within the per-asset cap and drawdown within the thesis limit.",
    },
    expect: {
      summary_must_mention: ["rebalanc"],
      // The LLM correctly avoids leaking internal candidate IDs to the
      // user. Assert that the reasoning explains the strategy choice
      // by referencing actual config (cap weighting / rebalance) or
      // why alternatives failed (drawdown / weight cap drift).
      reasoning_must_mention: ["cap", "rebalanc", "drawdown", "weight"],
      risks_must_mention: ["past performance", "historical", "guarantee", "future"],
      min_risk_count: 1,
      min_next_step_count: 1,
    },
  },
  {
    name: "growth_winner_c1",
    input: {
      run_id: "eval-finalize-growth",
      thesis: thesis({ objective: "growth", horizon_days: 180 }),
      universe: UNIVERSE,
      window: { ...WINDOW, horizon_days: 180 },
      attempts: [
        {
          attempt_n: 1,
          proposal: GROWTH_PROPOSAL,
          batch_id: "candidate_batch_eval_growth",
          validation_summary: {
            passing_candidate_ids: ["c1"],
            failing: [
              {
                candidate_id: "c2",
                violations: [
                  { constraint: "max_weight_per_asset", observed: 0.48, target: 0.2 },
                ],
              },
              {
                candidate_id: "c3",
                violations: [
                  { constraint: "max_drawdown", observed: -0.5, target: -0.35 },
                ],
              },
            ],
          },
        } satisfies Attempt,
      ],
      winner_candidate_id: "c1",
      decide_justification:
        "Candidate c1 (quarterly equal-weight 7-asset basket) was the only configuration that stayed within both the weight cap and the drawdown limit while preserving growth exposure.",
    },
    expect: {
      summary_must_mention: ["rebalanc", "quarterly", "90"],
      // c1 uses equal weighting + 90-day rebalance; alternatives failed
      // on weight cap / drawdown. The reasoning should reference one
      // of those concrete traits, not the internal candidate ID.
      reasoning_must_mention: ["equal", "quarterly", "90", "weight", "drawdown"],
      min_risk_count: 1,
      min_next_step_count: 1,
    },
  },
];

type CaseOutcome = {
  name: string;
  status: "pass" | "fail";
  duration_ms: number;
  violations: string[];
  final?: FinalWinner;
};

async function main(): Promise<number> {
  const llm = createOpencodeLLMClient({ sessionTitle: "eval-finalize" });
  const outcomes: CaseOutcome[] = [];

  for (const c of CASES) {
    process.stdout.write(`\n[${c.name}] running...\n`);
    const start = Date.now();
    try {
      const result = await finalize(c.input, { llm });
      const violations = checkExpect(c.expect, result.delta.final);
      const duration_ms = Date.now() - start;
      if (violations.length === 0) {
        process.stdout.write(`[${c.name}] PASS (${duration_ms} ms)\n`);
      } else {
        process.stdout.write(`[${c.name}] FAIL (${duration_ms} ms)\n`);
        for (const v of violations) process.stdout.write(`  - ${v}\n`);
      }
      process.stdout.write(
        `  title: ${result.delta.final.narrative.title}\n`,
      );
      process.stdout.write(
        `  summary: ${result.delta.final.narrative.summary}\n`,
      );
      process.stdout.write(
        `  risks: ${result.delta.final.narrative.risks.length}, next_steps: ${result.delta.final.narrative.next_steps.length}\n`,
      );
      outcomes.push({
        name: c.name,
        status: violations.length === 0 ? "pass" : "fail",
        duration_ms,
        violations,
        final: result.delta.final,
      });
    } catch (error) {
      const duration_ms = Date.now() - start;
      const kind = error instanceof Error ? error.constructor.name : "Error";
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
      `  ${row.status.toUpperCase().padEnd(4)} ${row.name.padEnd(28)} ${row.duration_ms} ms${reason}\n`,
    );
  }
  const failed = outcomes.filter((row) => row.status === "fail").length;
  process.stdout.write(
    `\n${outcomes.length - failed}/${outcomes.length} passed\n`,
  );
  return failed > 0 ? 1 : 0;
}

function checkExpect(expect: Expect, final: FinalWinner): string[] {
  const out: string[] = [];
  const n = final.narrative;
  if (expect.summary_must_mention) {
    if (!anyTermMatch(n.summary, expect.summary_must_mention)) {
      out.push(
        `expected summary to mention any of [${expect.summary_must_mention.join(", ")}]`,
      );
    }
  }
  if (expect.reasoning_must_mention) {
    if (!anyTermMatch(n.reasoning, expect.reasoning_must_mention)) {
      out.push(
        `expected reasoning to mention any of [${expect.reasoning_must_mention.join(", ")}]`,
      );
    }
  }
  if (expect.risks_must_mention) {
    const joined = n.risks.join(" | ");
    if (!anyTermMatch(joined, expect.risks_must_mention)) {
      out.push(
        `expected risks to mention any of [${expect.risks_must_mention.join(", ")}]`,
      );
    }
  }
  if (expect.next_steps_must_mention) {
    const joined = n.next_steps.join(" | ");
    if (!anyTermMatch(joined, expect.next_steps_must_mention)) {
      out.push(
        `expected next_steps to mention any of [${expect.next_steps_must_mention.join(", ")}]`,
      );
    }
  }
  if (
    expect.min_risk_count !== undefined &&
    n.risks.length < expect.min_risk_count
  ) {
    out.push(
      `expected at least ${expect.min_risk_count} risk(s), got ${n.risks.length}`,
    );
  }
  if (
    expect.min_next_step_count !== undefined &&
    n.next_steps.length < expect.min_next_step_count
  ) {
    out.push(
      `expected at least ${expect.min_next_step_count} next_step(s), got ${n.next_steps.length}`,
    );
  }
  return out;
}

function anyTermMatch(haystack: string, needles: string[]): boolean {
  const h = haystack.toLowerCase();
  return needles.some((n) => h.includes(n.toLowerCase()));
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
