// Eval runner for the finalize step.
//
// finalize is an LLM step: the deterministic gate has already chosen a winning
// candidate, and finalize turns that winner + the workflow state into a
// user-facing FinalizeNarrative (title / summary / reasoning / assumptions /
// risks / next_steps). The deterministic assembly -- building the FinalWinner
// record, routing to `complete`, the parse/schema retries, and the guard that
// refuses a winner_candidate_id outside the passing set -- is already covered
// hermetically by test/workflow-finalize.test.ts with a fake LLM. This eval's
// distinct job is the LIVE JUDGEMENT the schema validator can't make: is the
// narrative the real model writes FAITHFUL to the chosen winner and thesis?
// (names the right config, invents no metrics, tone tracks the objective.)
//
// Groups (named by the signal under test):
//   winner_fidelity  the narrative describes the ACTUAL winner config (real
//                    coins, real rebalance cadence/weighting) and the reasoning
//                    explains the pick -- and it fabricates NO backtest metrics
//                    (the input carries none, so any return/Sharpe figure is a
//                    hallucination).
//   objective        hold the winner config; sweep thesis.objective across all
//                    four options -- the tone/framing must track each objective
//                    (and always carry the not-financial-advice disclaimer).
//   persona          a couple of realistic full-state finals (multi-attempt
//                    history that refined to a winner): everything holds
//                    together -- fidelity + objective + iteration-aware reasoning.
//
// Run all:        pnpm eval:finalize
// Run one group:  pnpm eval:finalize winner_fidelity objective

import "../../../env.ts";

import { createOpencodeLLMClient } from "../llm.ts";
import { finalize } from "./finalize.ts";
import type {
  Attempt,
  AttemptValidationSummary,
  FinalWinner,
  FinalizeInput,
  Objective,
  Proposal,
  Thesis,
  Universe,
  Window,
} from "../state.ts";

type Expect = {
  // Each `*_any_of` list passes when the relevant text contains AT LEAST ONE
  // of the terms (case-insensitive). Loose `any_of` is used wherever several
  // phrasings are legitimately correct.
  summary_any_of?: string[];
  reasoning_any_of?: string[];
  // Searched across the whole narrative (summary + reasoning + assumptions +
  // risks + next_steps joined). Used for objective tone and disclaimers.
  narrative_any_of?: string[];
  risks_any_of?: string[];
  // The summary must name at least one actual coin from the universe.
  names_a_coin?: string[];
  // No fabricated performance metrics anywhere (the input has none).
  no_fabricated_metrics?: boolean;
  min_risk_count?: number;
  min_next_step_count?: number;
  min_assumption_count?: number;
};

type EvalCase = {
  group: string;
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

// Any of these (or their tickers) naming a real universe coin in the summary.
const COIN_TERMS = [
  "bitcoin",
  "btc",
  "ethereum",
  "eth",
  "binance",
  "bnb",
  "ripple",
  "xrp",
  "solana",
  "sol",
  "tron",
  "dogecoin",
  "doge",
];

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

// Winner = c2: a cap-weighted basket of 7 rebalanced MONTHLY (periodic_30d).
const BALANCED_PROPOSAL: Proposal = {
  iteration_hypothesis:
    "Compare equal vs cap-weighted baskets with rebalance variety.",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "synthetic_long_allocation",
      select_top: 5,
      weighting: "equal",
      rationale: "baseline static equal-weight",
    },
    {
      candidate_id: "c2",
      template_id: "periodic_rebalanced_allocation",
      select_top: 7,
      weighting: "cap",
      rebalance_trigger: "periodic_30d",
      rationale: "broader cap-weighted basket rebalanced monthly",
    },
    {
      candidate_id: "c3",
      template_id: "periodic_rebalanced_allocation",
      select_top: 6,
      weighting: "vol_inverse",
      rebalance_trigger: "periodic_30d",
      rationale: "defensive vol-inverse tilt",
    },
  ],
};

// Winner = c1: an equal-weight basket of 7 rebalanced QUARTERLY (periodic_90d).
const GROWTH_PROPOSAL: Proposal = {
  iteration_hypothesis:
    "Diversified equal-weight altcoin basket with quarterly rebalance.",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "periodic_rebalanced_allocation",
      select_top: 7,
      weighting: "equal",
      rebalance_trigger: "periodic_90d",
      rationale:
        "quarterly equal-weight to capture growth while limiting turnover",
    },
    {
      candidate_id: "c2",
      template_id: "synthetic_long_allocation",
      select_top: 7,
      weighting: "equal",
      rationale: "static baseline for comparison",
    },
    {
      candidate_id: "c3",
      template_id: "periodic_rebalanced_allocation",
      select_top: 7,
      weighting: "ranking_proportional",
      rebalance_trigger: "periodic_30d",
      rationale: "ranking-tilted monthly rebalance",
    },
  ],
};

// A defensive vol-inverse winner, used for the conservative objective sweeps so
// the framing the model is asked to produce is coherent with the strategy.
const DEFENSIVE_PROPOSAL: Proposal = {
  iteration_hypothesis: "Defensive inverse-volatility basket, monthly rebalance.",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "synthetic_long_allocation",
      select_top: 7,
      weighting: "cap",
      rationale: "cap-weighted baseline",
    },
    {
      candidate_id: "c2",
      template_id: "periodic_rebalanced_allocation",
      select_top: 7,
      weighting: "vol_inverse",
      rebalance_trigger: "periodic_30d",
      rationale: "inverse-volatility weights rebalanced monthly to damp risk",
    },
  ],
};

function attempt(
  attempt_n: number,
  proposal: Proposal,
  batch_id: string,
  passing: string[],
  failing: AttemptValidationSummary["failing"] = [],
  refinement_hint?: Attempt["refinement_hint"],
): Attempt {
  return {
    attempt_n,
    proposal,
    batch_id,
    validation_summary: { passing_candidate_ids: passing, failing },
    refinement_hint,
  };
}

const BALANCED_FAILING: AttemptValidationSummary["failing"] = [
  {
    candidate_id: "c1",
    violations: [{ constraint: "max_weight_per_asset", observed: 0.54, target: 0.2 }],
  },
  {
    candidate_id: "c3",
    violations: [{ constraint: "max_drawdown", observed: -0.4, target: -0.35 }],
  },
];

// Shared input for the objective sweep: the SAME winner (defensive c2, monthly
// inverse-vol basket) under each thesis objective. Only `objective` varies, so
// any framing difference in the narrative is attributable to it.
function objectiveSweep(objective: Objective): FinalizeInput {
  return {
    run_id: `eval-finalize-objective-${objective}`,
    thesis: thesis({ objective }),
    universe: UNIVERSE,
    window: WINDOW,
    attempts: [
      attempt(1, DEFENSIVE_PROPOSAL, "candidate_batch_eval_obj", ["c2"], [
        {
          candidate_id: "c1",
          violations: [
            { constraint: "max_drawdown", observed: -0.44, target: -0.35 },
          ],
        },
      ]),
    ],
    winner_candidate_id: "c2",
    decide_justification:
      "Candidate c2 (inverse-volatility weights rebalanced monthly) was the only configuration that held drawdown within the thesis limit.",
  };
}

const CASES: EvalCase[] = [
  // --- winner_fidelity: the narrative describes the ACTUAL winner -------------
  {
    group: "winner_fidelity",
    name: "winner_fidelity=balanced_c2_monthly_cap",
    input: {
      run_id: "eval-finalize-balanced",
      thesis: thesis(),
      universe: UNIVERSE,
      window: WINDOW,
      attempts: [
        attempt(1, BALANCED_PROPOSAL, "candidate_batch_eval_bal", ["c2"], BALANCED_FAILING),
      ],
      winner_candidate_id: "c2",
      decide_justification:
        "Candidate c2 was the only configuration that satisfied all constraints; rebalancing kept weights within the per-asset cap and drawdown within the thesis limit.",
    },
    expect: {
      // c2 rebalances monthly (periodic_30d) -- the summary must surface the
      // cadence in some form.
      summary_any_of: ["rebalanc", "month", "30"],
      // The model correctly does NOT leak internal candidate IDs to the user,
      // so the reasoning must justify the pick via the actual config or via why
      // the alternatives failed (cap drift / drawdown), not via "c2".
      reasoning_any_of: ["cap", "rebalanc", "drawdown", "weight"],
      names_a_coin: COIN_TERMS,
      no_fabricated_metrics: true,
      risks_any_of: ["past performance", "historical", "guarantee", "future", "no guarantee"],
      min_risk_count: 1,
      min_next_step_count: 1,
      min_assumption_count: 1,
    },
  },
  {
    group: "winner_fidelity",
    name: "winner_fidelity=growth_c1_quarterly_equal",
    input: {
      run_id: "eval-finalize-growth",
      thesis: thesis({ objective: "growth", horizon_days: 180 }),
      universe: UNIVERSE,
      window: { ...WINDOW, horizon_days: 180 },
      attempts: [
        attempt(1, GROWTH_PROPOSAL, "candidate_batch_eval_growth", ["c1"], [
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
        ]),
      ],
      winner_candidate_id: "c1",
      decide_justification:
        "Candidate c1 (quarterly equal-weight 7-asset basket) was the only configuration that stayed within both the weight cap and the drawdown limit while preserving growth exposure.",
    },
    expect: {
      // c1 rebalances QUARTERLY (periodic_90d) with EQUAL weights.
      summary_any_of: ["quarterly", "90", "rebalanc", "equal"],
      reasoning_any_of: ["equal", "quarterly", "90", "weight", "drawdown"],
      names_a_coin: COIN_TERMS,
      no_fabricated_metrics: true,
      min_risk_count: 1,
      min_next_step_count: 1,
      min_assumption_count: 1,
    },
  },

  // --- objective: hold the winner, sweep the objective across all 4 options ---
  {
    group: "objective",
    name: "objective=balanced_growth",
    input: objectiveSweep("balanced_growth"),
    expect: {
      narrative_any_of: ["balanc", "diversif", "growth", "steady"],
      no_fabricated_metrics: true,
      risks_any_of: ["past performance", "historical", "guarantee", "future", "no guarantee"],
      min_risk_count: 1,
      min_next_step_count: 1,
    },
  },
  {
    group: "objective",
    name: "objective=growth",
    input: objectiveSweep("growth"),
    expect: {
      narrative_any_of: ["growth", "upside", "appreciat", "aggressive", "return"],
      no_fabricated_metrics: true,
      risks_any_of: ["past performance", "historical", "guarantee", "future", "no guarantee"],
      min_risk_count: 1,
      min_next_step_count: 1,
    },
  },
  {
    group: "objective",
    name: "objective=income",
    input: objectiveSweep("income"),
    expect: {
      narrative_any_of: ["income", "yield", "stable", "cash", "conservativ", "steady"],
      no_fabricated_metrics: true,
      risks_any_of: ["past performance", "historical", "guarantee", "future", "no guarantee"],
      min_risk_count: 1,
      min_next_step_count: 1,
    },
  },
  {
    group: "objective",
    name: "objective=preserve_capital",
    input: objectiveSweep("preserve_capital"),
    expect: {
      narrative_any_of: ["preserv", "capital", "conservativ", "downside", "risk", "drawdown", "defensive"],
      no_fabricated_metrics: true,
      risks_any_of: ["past performance", "historical", "guarantee", "future", "no guarantee"],
      min_risk_count: 1,
      min_next_step_count: 1,
    },
  },

  // --- persona: full realistic finals (multi-attempt history -> a winner) -----
  {
    // A conservative client whose first round blew the drawdown cap; the second
    // round (tighter, inverse-vol, monthly) passed. The reasoning should be
    // iteration-aware ("the earlier/refined attempt...") and the framing
    // conservative, while still naming real coins and inventing no metrics.
    group: "persona",
    name: "persona=preserve_capital_refined_to_winner",
    input: {
      run_id: "eval-finalize-persona-preserve",
      thesis: thesis({
        objective: "preserve_capital",
        rebalance_frequency: "monthly",
        constraints: {
          max_weight_per_asset: 0.2,
          max_cash_weight: 0.2,
          max_drawdown: 0.25,
          asset_count_min: 5,
          asset_count_max: 10,
        },
        interpretation_notes:
          "Capital preservation first: cap drawdown hard, accept lower upside.",
      }),
      universe: UNIVERSE,
      window: WINDOW,
      attempts: [
        attempt(
          1,
          BALANCED_PROPOSAL,
          "candidate_batch_persona_1",
          [],
          [
            { candidate_id: "c1", violations: [{ constraint: "max_drawdown", observed: -0.41, target: -0.25 }] },
            { candidate_id: "c2", violations: [{ constraint: "max_drawdown", observed: -0.33, target: -0.25 }] },
            { candidate_id: "c3", violations: [{ constraint: "max_drawdown", observed: -0.28, target: -0.25 }] },
          ],
          {
            failed_constraints: [
              { constraint: "max_drawdown", observed: -0.28, target: -0.25, candidate_id: "c3" },
            ],
            suggested_changes: { change_template_to: "periodic_rebalanced_allocation", increase_cash_to: 0.2 },
            rationale: "Move to an inverse-vol monthly basket and hold cash to cut drawdown under the 25% cap.",
          },
        ),
        attempt(2, DEFENSIVE_PROPOSAL, "candidate_batch_persona_2", ["c2"], [
          { candidate_id: "c1", violations: [{ constraint: "max_drawdown", observed: -0.3, target: -0.25 }] },
        ]),
      ],
      winner_candidate_id: "c2",
      decide_justification:
        "After the first round breached the 25% drawdown cap, the refined inverse-volatility monthly basket (c2) brought drawdown within limit while keeping diversified exposure.",
    },
    expect: {
      summary_any_of: ["rebalanc", "month", "volatil", "inverse", "defensive"],
      reasoning_any_of: ["drawdown", "refin", "earlier", "previous", "first", "adjust", "tighten", "round"],
      narrative_any_of: ["preserv", "capital", "conservativ", "downside", "drawdown", "risk"],
      names_a_coin: COIN_TERMS,
      no_fabricated_metrics: true,
      risks_any_of: ["past performance", "historical", "guarantee", "future", "no guarantee"],
      min_risk_count: 1,
      min_next_step_count: 1,
      min_assumption_count: 1,
    },
  },
];

// --- assertions --------------------------------------------------------------

type Assertion = { label: string; expected: string; actual: string; ok: boolean };

// The input carries NO performance metrics, so any Sharpe figure or
// return/CAGR/annualized percentage in the narrative is a hallucination. Plain
// constraint percentages ("20% per asset", "monthly") are fine -- the patterns
// below only match performance-claim phrasings.
const FABRICATED_METRIC_PATTERNS: RegExp[] = [
  /sharpe/i,
  /\bcagr\b/i,
  /\d+(?:\.\d+)?\s*%\s*(?:annual(?:ized)?|return|gain|yield)\b/i,
  /\breturn(?:ed|s)?\s+of\s+\d+(?:\.\d+)?\s*%/i,
  /\b\d+(?:\.\d+)?\s*x\s+(?:return|gain)/i,
];

function fabricatedMetrics(text: string): string[] {
  const hits: string[] = [];
  for (const re of FABRICATED_METRIC_PATTERNS) {
    const m = text.match(re);
    if (m) hits.push(m[0].trim());
  }
  return hits;
}

function anyTerm(haystack: string, needles: string[]): boolean {
  const h = haystack.toLowerCase();
  return needles.some((n) => h.includes(n.toLowerCase()));
}

function evaluate(expect: Expect, final: FinalWinner): Assertion[] {
  const a: Assertion[] = [];
  const push = (label: string, expected: string, actual: string, ok: boolean) =>
    a.push({ label, expected, actual, ok });

  const n = final.narrative;
  const risksJoined = n.risks.join(" | ");
  const wholeNarrative = [
    n.title,
    n.summary,
    n.reasoning,
    ...n.assumptions,
    ...n.risks,
    ...n.next_steps,
  ].join(" | ");

  if (expect.summary_any_of) {
    push(
      "summary_any_of",
      `any of [${expect.summary_any_of.join(",")}]`,
      n.summary,
      anyTerm(n.summary, expect.summary_any_of),
    );
  }
  if (expect.reasoning_any_of) {
    push(
      "reasoning_any_of",
      `any of [${expect.reasoning_any_of.join(",")}]`,
      n.reasoning,
      anyTerm(n.reasoning, expect.reasoning_any_of),
    );
  }
  if (expect.narrative_any_of) {
    push(
      "narrative_any_of",
      `any of [${expect.narrative_any_of.join(",")}]`,
      "(whole narrative)",
      anyTerm(wholeNarrative, expect.narrative_any_of),
    );
  }
  if (expect.risks_any_of) {
    push(
      "risks_any_of",
      `any of [${expect.risks_any_of.join(",")}]`,
      risksJoined,
      anyTerm(risksJoined, expect.risks_any_of),
    );
  }
  if (expect.names_a_coin) {
    push(
      "names_a_coin",
      "a real universe coin in summary",
      n.summary,
      anyTerm(n.summary, expect.names_a_coin),
    );
  }
  if (expect.no_fabricated_metrics) {
    const hits = fabricatedMetrics(wholeNarrative);
    push(
      "no_fabricated_metrics",
      "no Sharpe/return/CAGR figures",
      hits.length ? `found [${hits.join(" ; ")}]` : "none",
      hits.length === 0,
    );
  }
  if (expect.min_assumption_count !== undefined) {
    push(
      "min_assumption_count",
      `>= ${expect.min_assumption_count}`,
      String(n.assumptions.length),
      n.assumptions.length >= expect.min_assumption_count,
    );
  }
  if (expect.min_risk_count !== undefined) {
    push(
      "min_risk_count",
      `>= ${expect.min_risk_count}`,
      String(n.risks.length),
      n.risks.length >= expect.min_risk_count,
    );
  }
  if (expect.min_next_step_count !== undefined) {
    push(
      "min_next_step_count",
      `>= ${expect.min_next_step_count}`,
      String(n.next_steps.length),
      n.next_steps.length >= expect.min_next_step_count,
    );
  }

  return a;
}

// --- a compact, uniform view of the input state per case ---------------------

function describeInput(input: FinalizeInput): string {
  const latest = input.attempts.at(-1)!;
  const winner = latest.proposal.candidates.find(
    (c) => c.candidate_id === input.winner_candidate_id,
  );
  const cfg = winner
    ? `${winner.template_id} select_top=${winner.select_top} weighting=${winner.weighting}` +
      `${winner.rebalance_trigger ? ` rebalance=${winner.rebalance_trigger}` : ""}`
    : "(winner not found)";
  return (
    `objective=${input.thesis.objective} horizon=${input.thesis.horizon_days}d ` +
    `attempts=${input.attempts.length} winner=${input.winner_candidate_id}\n` +
    `winner_cfg: ${cfg}\n` +
    `universe(${input.universe.coin_ids.length}): [${input.universe.coin_ids.join(",")}]`
  );
}

type CaseOutcome = {
  name: string;
  group: string;
  status: "pass" | "fail";
  duration_ms: number;
  failed: string[];
};

async function main(): Promise<number> {
  const groupFilter = new Set(process.argv.slice(2));
  const cases = CASES.filter(
    (c) => groupFilter.size === 0 || groupFilter.has(c.group),
  );
  if (groupFilter.size > 0) {
    process.stdout.write(`group filter: ${[...groupFilter].join(", ")}\n`);
  }
  process.stdout.write(`running ${cases.length} cases\n`);

  const llm = createOpencodeLLMClient({ sessionTitle: "eval-finalize" });
  const outcomes: CaseOutcome[] = [];

  for (const c of cases) {
    process.stdout.write(`\n${"=".repeat(72)}\n[${c.name}]\n`);
    process.stdout.write(`--- input ---\n${describeInput(c.input)}\n`);

    const start = Date.now();
    try {
      // No logger passed: the step's default pino logger streams its own
      // enter/llm/exit lines alongside the eval's assertions.
      const result = await finalize(c.input, { llm });
      const duration_ms = Date.now() - start;
      const final = result.delta.final;
      const n = final.narrative;

      process.stdout.write(`--- output ---\nnext=${result.next}\n`);
      process.stdout.write(`  title:    ${n.title}\n`);
      process.stdout.write(`  summary:  ${n.summary}\n`);
      process.stdout.write(`  reasoning: ${n.reasoning}\n`);
      process.stdout.write(`  assumptions (${n.assumptions.length}):\n`);
      for (const x of n.assumptions) process.stdout.write(`    - ${x}\n`);
      process.stdout.write(`  risks (${n.risks.length}):\n`);
      for (const x of n.risks) process.stdout.write(`    - ${x}\n`);
      process.stdout.write(`  next_steps (${n.next_steps.length}):\n`);
      for (const x of n.next_steps) process.stdout.write(`    - ${x}\n`);

      const assertions = evaluate(c.expect, final);
      process.stdout.write(`--- assertions ---\n`);
      for (const x of assertions) {
        process.stdout.write(
          `  ${x.ok ? "PASS" : "FAIL"} ${x.label}: expected ${x.expected}, got ${x.actual}\n`,
        );
      }
      const failed = assertions.filter((x) => !x.ok).map((x) => x.label);
      const status: "pass" | "fail" = failed.length === 0 ? "pass" : "fail";
      process.stdout.write(`[${c.name}] ${status.toUpperCase()} (${duration_ms} ms)\n`);
      outcomes.push({ name: c.name, group: c.group, status, duration_ms, failed });
    } catch (error) {
      const duration_ms = Date.now() - start;
      const kind = error instanceof Error ? error.constructor.name : "Error";
      const message = error instanceof Error ? error.message : String(error);
      process.stdout.write(`[${c.name}] FAIL (${duration_ms} ms, ${kind})\n  ${message}\n`);
      outcomes.push({
        name: c.name,
        group: c.group,
        status: "fail",
        duration_ms,
        failed: [`${kind}: ${message}`],
      });
    }
  }

  process.stdout.write(`\n${"=".repeat(72)}\n=== summary ===\n`);
  for (const row of outcomes) {
    const reason = row.failed.length > 0 ? ` -- ${row.failed.join("; ")}` : "";
    process.stdout.write(
      `  ${row.status.toUpperCase().padEnd(4)} ${row.name.padEnd(44)} ${row.duration_ms} ms${reason}\n`,
    );
  }
  const failed = outcomes.filter((row) => row.status === "fail").length;
  process.stdout.write(`\n${outcomes.length - failed}/${outcomes.length} passed\n`);
  return failed > 0 ? 1 : 0;
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

export { CASES, evaluate, main };
