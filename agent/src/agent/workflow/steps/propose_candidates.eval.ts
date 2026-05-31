// Eval runner for the propose_candidates step.
//
// LLM-driven. Emits 3-5 allocation-template candidates from the
// thesis/universe/window, biased by the select_templates family
// shortlist, and -- on refine rounds -- by the prior attempt's
// validation failures + refinement_hint. The signals worth sweeping:
//  - cold: objective/constraints with no prior attempt (structural sanity)
//  - templates: the family shortlist must map to the right executable
//    template (periodic_rebalanced -> periodic_rebalance; threshold ->
//    periodic_rebalance + threshold_drift_10pct)
//  - refine: the iteration memory -- hypothesis references the failure
//    and candidates implement the hint, without repeating the failed config
//  - bounds: select_top must respect asset_count bounds AND universe size
//
// Structural invariants the validator already enforces (count 3-5,
// select_top range, trigger rules) are re-checked for visibility; the
// one it does NOT enforce -- candidate diversity -- is asserted on every
// case.
//
// Run all:        pnpm eval:propose-candidates
// Run one group:  pnpm eval:propose-candidates refine templates
//   (group names: cold templates refine bounds)

import "../../../env.ts";

import { createOpencodeLLMClient } from "../llm.ts";
import {
  ProposalValidationError,
  type AllocationTemplate,
  type Attempt,
  type Proposal,
  type ProposeCandidatesInput,
  type RebalanceTrigger,
  type TemplateSelection,
  type Thesis,
  type Universe,
  type Window,
} from "../state.ts";
import { proposeCandidates } from "./propose_candidates.ts";

type Expect = {
  contains_template?: AllocationTemplate;
  excludes_template?: AllocationTemplate;
  // At least one periodic_rebalance candidate uses one of these triggers.
  periodic_trigger_in?: RebalanceTrigger[];
  hypothesis_mentions?: string[];
  // hint.change_template_to must appear among candidate templates.
  implements_template_swap?: AllocationTemplate;
  // A periodic_rebalance candidate must use this trigger.
  implements_rebalance?: RebalanceTrigger;
  // No candidate may reproduce this exact (already-failed) config.
  no_repeat_config?: {
    template_id: AllocationTemplate;
    select_top: number;
    weighting: string;
    rebalance_trigger?: RebalanceTrigger;
  };
};

type EvalCase = {
  group: string;
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

function universe(coinIds: string[]): Universe {
  return {
    coin_ids: coinIds,
    source: "rank_universe",
    effective_filters: {
      top_n: coinIds.length,
      exclude_stablecoins: true,
      exclude_wrapped: true,
    },
  };
}

const TEN = [
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
];

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

function families(...specs: Array<[TemplateSelection["selected"][number]["family"], number]>): TemplateSelection {
  return {
    rationale: "eval fixture shortlist",
    selected: specs.map(([family, rank]) => ({
      family,
      rank,
      rationale: "eval fixture",
    })),
  };
}

// The prior proposal that "failed" in the refine fixtures.
const PRIOR_PROPOSAL: Proposal = {
  iteration_hypothesis: "Try simple equal-weight large-cap basket.",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "synthetic_long_allocation",
      select_top: 5,
      weighting: "equal",
      rationale: "baseline",
    },
  ],
};

function failedAttempt(
  constraint: string,
  observed: number,
  target: number,
  suggested: Attempt["refinement_hint"] extends infer R
    ? R extends { suggested_changes: infer S }
      ? S
      : never
    : never,
  rationale: string,
): Attempt {
  return {
    attempt_n: 1,
    proposal: PRIOR_PROPOSAL,
    batch_id: `batch_${constraint}`,
    validation_summary: {
      passing_candidate_ids: [],
      failing: [
        { candidate_id: "c1", violations: [{ constraint, observed, target }] },
      ],
      candidates: [
        {
          candidate_id: "c1",
          passed: false,
          constraint_distance:
            Math.abs(observed - target) / Math.max(Math.abs(target), 1),
        },
      ],
    },
    refinement_hint: {
      failed_constraints: [
        { constraint: constraint as never, observed, target, candidate_id: "c1" },
      ],
      suggested_changes: suggested,
      rationale,
    },
  };
}

const CASES: EvalCase[] = [
  // --- cold: no prior attempt, just structural sanity per objective ---
  {
    group: "cold",
    name: "cold=balanced",
    input: { run_id: "eval-pc-cold-balanced", thesis: thesis(), universe: universe(TEN), window: WINDOW },
    expect: {},
  },
  {
    group: "cold",
    name: "cold=growth",
    input: {
      run_id: "eval-pc-cold-growth",
      thesis: thesis({ objective: "growth", horizon_days: 180 }),
      universe: universe(TEN),
      window: { ...WINDOW, horizon_days: 180 },
    },
    expect: {},
  },
  {
    group: "cold",
    name: "cold=preserve",
    input: {
      run_id: "eval-pc-cold-preserve",
      thesis: thesis({
        objective: "preserve_capital",
        constraints: {
          max_weight_per_asset: 0.4,
          max_cash_weight: 0.3,
          max_drawdown: 0.15,
          asset_count_min: 2,
          asset_count_max: 5,
        },
      }),
      universe: universe(TEN),
      window: WINDOW,
    },
    expect: {},
  },

  // --- templates: family shortlist maps to the right executable template ---
  {
    group: "templates",
    name: "templates=periodic",
    input: {
      run_id: "eval-pc-tpl-periodic",
      thesis: thesis(),
      universe: universe(TEN),
      window: WINDOW,
      template_selection: families(["periodic_rebalanced_allocation", 1], ["core_satellite_allocation", 2]),
    },
    expect: { contains_template: "periodic_rebalanced_allocation" },
  },
  {
    group: "templates",
    name: "templates=threshold",
    input: {
      run_id: "eval-pc-tpl-threshold",
      thesis: thesis(),
      universe: universe(TEN),
      window: WINDOW,
      template_selection: families(["threshold_rebalanced_allocation", 1]),
    },
    // threshold_rebalanced -> periodic_rebalance with the drift trigger.
    expect: {
      contains_template: "periodic_rebalanced_allocation",
      periodic_trigger_in: ["threshold_drift_10pct"],
    },
  },

  // --- refine: iteration memory (hypothesis + implemented hint + no repeat) ---
  {
    group: "refine",
    name: "refine=drawdown",
    input: {
      run_id: "eval-pc-refine-drawdown",
      thesis: thesis(),
      universe: universe(TEN),
      window: WINDOW,
      attempts: [
        failedAttempt("max_drawdown", 0.46, 0.35, {
          tighten_weight_cap_to: 0.15,
          increase_cash_to: 0.15,
        }, "Equal-weight 5-asset basket hit 46% drawdown; tighten concentration and add a cash buffer."),
      ],
    },
    expect: {
      hypothesis_mentions: ["drawdown"],
      // Must not re-run the exact config that already blew the drawdown.
      no_repeat_config: { template_id: "synthetic_long_allocation", select_top: 5, weighting: "equal" },
    },
  },
  {
    group: "refine",
    name: "refine=template-swap",
    input: {
      run_id: "eval-pc-refine-swap",
      thesis: thesis(),
      universe: universe(TEN),
      window: WINDOW,
      attempts: [
        failedAttempt("benchmark_underperformance", -0.18, 0, {
          change_template_to: "periodic_rebalanced_allocation",
          change_rebalance_to: "periodic_30d",
        }, "Buy-and-hold trailed the benchmark by 18%; switch to periodic rebalancing."),
      ],
    },
    expect: {
      implements_template_swap: "periodic_rebalanced_allocation",
      implements_rebalance: "periodic_30d",
    },
  },

  // --- bounds: select_top respects asset_count bounds AND universe size ---
  {
    group: "bounds",
    name: "bounds=fixed-3",
    input: {
      run_id: "eval-pc-bounds-fixed3",
      thesis: thesis({
        constraints: {
          max_weight_per_asset: 0.4,
          max_cash_weight: 0,
          max_drawdown: 0.35,
          asset_count_min: 3,
          asset_count_max: 3,
        },
      }),
      universe: universe(TEN),
      window: WINDOW,
    },
    // asset_count_min == max == 3 -> every candidate must pick exactly 3.
    expect: {},
  },
  {
    group: "bounds",
    name: "bounds=small-universe",
    input: {
      run_id: "eval-pc-bounds-small",
      thesis: thesis({
        constraints: {
          max_weight_per_asset: 0.4,
          max_cash_weight: 0,
          max_drawdown: 0.35,
          asset_count_min: 3,
          asset_count_max: 10,
        },
      }),
      // Only 4 coins available -> select_top can never exceed 4.
      universe: universe(["bitcoin", "ethereum", "solana", "binancecoin"]),
      window: WINDOW,
    },
    expect: {},
  },
];

type Assertion = { label: string; expected: string; actual: string; ok: boolean };

function configSig(c: Proposal["candidates"][number]): string {
  return `${c.template_id}|${c.select_top}|${c.weighting}|${c.rebalance_trigger ?? "-"}`;
}

function evaluate(expect: Expect, p: Proposal, input: ProposeCandidatesInput): Assertion[] {
  const a: Assertion[] = [];
  const push = (label: string, expected: unknown, actual: unknown, ok: boolean) =>
    a.push({ label, expected: String(expected), actual: String(actual), ok });
  const cs = p.candidates;
  const templates = cs.map((c) => c.template_id);

  // --- universal structural invariants ---
  push("candidate_count", "3..5", cs.length, cs.length >= 3 && cs.length <= 5);

  const lo = input.thesis.constraints.asset_count_min;
  const hi = Math.min(input.thesis.constraints.asset_count_max, input.universe.coin_ids.length);
  const outOfRange = cs.filter((c) => c.select_top < lo || c.select_top > hi);
  push(
    "select_top_bounds",
    `all in [${lo}, ${hi}]`,
    outOfRange.length ? outOfRange.map((c) => `${c.candidate_id}:${c.select_top}`).join(",") : `[${cs.map((c) => c.select_top).join(",")}]`,
    outOfRange.length === 0,
  );

  // trigger discipline (validator enforces; re-checked for visibility)
  const badTrigger = cs.filter(
    (c) =>
      (c.template_id === "periodic_rebalanced_allocation" && !c.rebalance_trigger) ||
      (c.template_id === "synthetic_long_allocation" && c.rebalance_trigger),
  );
  push("trigger_discipline", "periodic has trigger, buy_and_hold none", badTrigger.length ? "violation" : "ok", badTrigger.length === 0);

  // diversity (NOT enforced by the validator): when >1 candidate, configs differ
  if (cs.length > 1) {
    const distinct = new Set(cs.map(configSig)).size;
    push("diversity", "> 1 distinct config", `${distinct} distinct`, distinct > 1);
  }

  // --- per-case expectations ---
  if (expect.contains_template) {
    push("contains_template", expect.contains_template, `[${templates.join(",")}]`, templates.includes(expect.contains_template));
  }
  if (expect.excludes_template) {
    push("excludes_template", `no ${expect.excludes_template}`, `[${templates.join(",")}]`, !templates.includes(expect.excludes_template));
  }
  if (expect.periodic_trigger_in) {
    const triggers = cs.filter((c) => c.template_id === "periodic_rebalanced_allocation").map((c) => c.rebalance_trigger);
    const ok = triggers.some((t) => t !== undefined && expect.periodic_trigger_in!.includes(t));
    push("periodic_trigger_in", `>=1 of [${expect.periodic_trigger_in.join(",")}]`, `[${triggers.join(",")}]`, ok);
  }
  if (expect.hypothesis_mentions) {
    const h = p.iteration_hypothesis.toLowerCase();
    for (const term of expect.hypothesis_mentions) {
      push(`hypothesis_mentions:${term}`, "present", h.includes(term.toLowerCase()) ? "present" : "absent", h.includes(term.toLowerCase()));
    }
  }
  if (expect.implements_template_swap) {
    push("implements_template_swap", expect.implements_template_swap, `[${templates.join(",")}]`, templates.includes(expect.implements_template_swap));
  }
  if (expect.implements_rebalance) {
    const ok = cs.some((c) => c.template_id === "periodic_rebalanced_allocation" && c.rebalance_trigger === expect.implements_rebalance);
    push("implements_rebalance", expect.implements_rebalance, cs.map((c) => c.rebalance_trigger ?? "-").join(","), ok);
  }
  if (expect.no_repeat_config) {
    const sig = configSig({
      candidate_id: "x",
      rationale: "",
      template_id: expect.no_repeat_config.template_id,
      select_top: expect.no_repeat_config.select_top,
      weighting: expect.no_repeat_config.weighting as never,
      rebalance_trigger: expect.no_repeat_config.rebalance_trigger,
    });
    const repeated = cs.some((c) => configSig(c) === sig);
    push("no_repeat_config", `absent: ${sig}`, repeated ? "REPEATED" : "absent", !repeated);
  }
  return a;
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
  const llm = createOpencodeLLMClient({ sessionTitle: "eval-propose_candidates" });
  const cases = CASES.filter((c) => groupFilter.size === 0 || groupFilter.has(c.group));
  if (groupFilter.size > 0) {
    process.stdout.write(`group filter: ${[...groupFilter].join(", ")}\n`);
  }
  process.stdout.write(`running ${cases.length} cases\n`);

  const outcomes: CaseOutcome[] = [];

  for (const c of cases) {
    process.stdout.write(`\n${"=".repeat(72)}\n[${c.name}]\n`);
    const t = c.input.thesis;
    process.stdout.write(
      `--- input ---\nobjective=${t.objective} asset_count=[${t.constraints.asset_count_min},${t.constraints.asset_count_max}] universe_size=${c.input.universe.coin_ids.length}` +
        `${c.input.template_selection ? ` families=${JSON.stringify(c.input.template_selection.selected.map((s) => s.family))}` : ""}` +
        `${c.input.attempts?.length ? ` prior_hint=${JSON.stringify(c.input.attempts.at(-1)?.refinement_hint?.suggested_changes)}` : ""}\n`,
    );

    const start = Date.now();
    try {
      // No logger passed: the step's default pino logger streams its own
      // enter/llm_request/llm_response/exit lines alongside the assertions.
      const result = await proposeCandidates(c.input, { llm });
      const duration_ms = Date.now() - start;
      const p = result.delta.proposal;

      process.stdout.write(
        `--- proposal (output) ---\nhypothesis=${p.iteration_hypothesis}\ncandidates=${JSON.stringify(p.candidates)}\n`,
      );

      const assertions = evaluate(c.expect, p, c.input);
      process.stdout.write(`--- assertions ---\n`);
      for (const x of assertions) {
        process.stdout.write(`  ${x.ok ? "PASS" : "FAIL"} ${x.label}: expected ${x.expected}, got ${x.actual}\n`);
      }
      const failed = assertions.filter((x) => !x.ok).map((x) => x.label);
      const status: "pass" | "fail" = failed.length === 0 ? "pass" : "fail";
      process.stdout.write(`[${c.name}] ${status.toUpperCase()} (${duration_ms} ms)\n`);
      outcomes.push({ name: c.name, group: c.group, status, duration_ms, failed });
    } catch (error) {
      const duration_ms = Date.now() - start;
      const kind =
        error instanceof ProposalValidationError
          ? "ProposalValidationError"
          : error instanceof Error
            ? error.constructor.name
            : "Error";
      const message = error instanceof Error ? error.message : String(error);
      process.stdout.write(`[${c.name}] FAIL (${duration_ms} ms, ${kind})\n  ${message}\n`);
      outcomes.push({ name: c.name, group: c.group, status: "fail", duration_ms, failed: [`${kind}: ${message}`] });
    }
  }

  process.stdout.write(`\n${"=".repeat(72)}\n=== summary ===\n`);
  for (const row of outcomes) {
    const reason = row.failed.length > 0 ? ` -- ${row.failed.join("; ")}` : "";
    process.stdout.write(
      `  ${row.status.toUpperCase().padEnd(4)} ${row.name.padEnd(28)} ${row.duration_ms} ms${reason}\n`,
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
