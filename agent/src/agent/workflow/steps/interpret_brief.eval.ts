// Eval runner for the interpret_brief step.
//
// What this covers: the REAL wizard production path. The frontend
// AllocationWizard collects AllocationWizardParams, the API turns them
// into a text brief via buildAllocationWizardPrompt, and interpret_brief
// turns that brief into a Thesis. This eval drives that exact chain so a
// regression in the prompt builder OR the interpret_brief prompt shows up
// here.
//
// Strategy: the wizard's full cartesian product is ~millions of combos,
// which is infeasible (and pointless) to run through an LLM. Instead we
// do a PER-FIELD SWEEP: hold a feasible baseline, then vary one wizard
// field across all of its options and assert the thesis mapping for that
// field. That exercises every individual wizard option in ~40 cases.
// A handful of "persona" combos then check field interactions
// (feasibility tension, multi-field mapping). Freeform text briefs (the
// advanced chat path) are kept as their own group.
//
// Run all:        pnpm eval:interpret-brief
// Run one group:  pnpm eval:interpret-brief universe maxDrawdown
//   (group names: universe exclusions minimumMarketCap concentrationLimit
//    maxDrawdown riskPreference horizon rebalance cashAllocation
//    targetAssets persona freeform)

// Loads .env (Azure / opencode credentials) on import; matches the
// rest of the codebase rather than relying on the caller's shell.
import "../../../env.ts";

import {
  buildAllocationWizardPrompt,
  type AllocationWizardParams,
} from "../../prompt.ts";
import { createOpencodeLLMClient } from "../llm.ts";
import {
  ThesisValidationError,
  type Objective,
  type RebalanceFrequency,
  type Thesis,
} from "../state.ts";
import { interpretBrief } from "./interpret_brief.ts";

// Feasible baseline. concentrationLimit="agent" and cashAllocation="none"
// keep `max_weight_per_asset * asset_count_min >= 1 - max_cash_weight`
// satisfiable while we sweep every other field, so a single varied field
// is what drives any failure -- not an accidental feasibility conflict.
const BASELINE: AllocationWizardParams = {
  universe: "top25",
  exclusions: ["stablecoins", "wrapped"],
  minimumMarketCap: "1b",
  concentrationLimit: "agent",
  maxDrawdown: "35",
  riskPreference: "balanced",
  horizon: "1y",
  rebalance: "monthly",
  initialCapitalUsd: "",
  cashAllocation: "none",
  targetAssets: "5-10",
};

// Expectations are all optional; a case only asserts the fields relevant
// to what it varies. Numeric "_approx" use a relative tolerance; counts
// and enums are exact.
type Expect = {
  objective?: Objective;
  horizon_days_min?: number;
  horizon_days_max?: number;
  top_n_min?: number;
  top_n_max?: number;
  exclude_stablecoins?: boolean;
  exclude_wrapped?: boolean;
  market_cap_min_usd_approx?: number;
  market_cap_min_usd_absent?: boolean;
  max_weight_per_asset_approx?: number;
  max_drawdown_approx?: number;
  max_drawdown_min?: number;
  max_cash_weight_approx?: number;
  asset_count_min?: number;
  asset_count_max?: number;
  rebalance_frequency?: RebalanceFrequency;
  hand_picked?: boolean;
  // Asserts the thesis parsed and satisfied validateThesis without any
  // field-level claim. Used for "let agent decide" options and
  // feasibility-tension personas where the exact mapping is the LLM's
  // call but it must still produce a valid thesis.
  valid_only?: boolean;
};

type WizardCase = {
  group: string;
  name: string;
  params: AllocationWizardParams;
  expect: Expect;
};

// Build a sweep case: BASELINE with one field overridden.
function sweep(
  group: keyof AllocationWizardParams,
  value: string | string[],
  expect: Expect,
): WizardCase {
  const params = { ...BASELINE, [group]: value } as AllocationWizardParams;
  const label = Array.isArray(value) ? value.join("+") || "none" : value;
  return { group, name: `${group}=${label}`, params, expect };
}

const WIZARD_CASES: WizardCase[] = [
  // --- universe -> universe_hints.top_n ---
  sweep("universe", "top10", { top_n_min: 8, top_n_max: 12 }),
  sweep("universe", "top25", { top_n_min: 20, top_n_max: 30 }),
  sweep("universe", "top50", { top_n_min: 40, top_n_max: 60 }),
  sweep("universe", "all", { top_n_min: 50 }),

  // --- exclusions -> exclude_stablecoins / exclude_wrapped ---
  sweep("exclusions", [], {
    exclude_stablecoins: false,
    exclude_wrapped: false,
  }),
  sweep("exclusions", ["stablecoins"], {
    exclude_stablecoins: true,
    exclude_wrapped: false,
  }),
  sweep("exclusions", ["wrapped"], {
    exclude_stablecoins: false,
    exclude_wrapped: true,
  }),
  sweep("exclusions", ["stablecoins", "wrapped"], {
    exclude_stablecoins: true,
    exclude_wrapped: true,
  }),

  // --- minimumMarketCap -> universe_hints.market_cap_min_usd ---
  sweep("minimumMarketCap", "none", { market_cap_min_usd_absent: true }),
  sweep("minimumMarketCap", "100m", { market_cap_min_usd_approx: 1e8 }),
  sweep("minimumMarketCap", "500m", { market_cap_min_usd_approx: 5e8 }),
  sweep("minimumMarketCap", "1b", { market_cap_min_usd_approx: 1e9 }),
  sweep("minimumMarketCap", "10b", { market_cap_min_usd_approx: 1e10 }),

  // --- concentrationLimit -> constraints.max_weight_per_asset ---
  sweep("concentrationLimit", "20", { max_weight_per_asset_approx: 0.2 }),
  sweep("concentrationLimit", "30", { max_weight_per_asset_approx: 0.3 }),
  sweep("concentrationLimit", "agent", { valid_only: true }),

  // --- maxDrawdown -> constraints.max_drawdown ---
  sweep("maxDrawdown", "10", { max_drawdown_approx: 0.1 }),
  sweep("maxDrawdown", "20", { max_drawdown_approx: 0.2 }),
  sweep("maxDrawdown", "35", { max_drawdown_approx: 0.35 }),
  sweep("maxDrawdown", "50", { max_drawdown_approx: 0.5 }),
  sweep("maxDrawdown", "moreThan50", { max_drawdown_min: 0.5 }),

  // --- riskPreference -> objective ---
  sweep("riskPreference", "preserve", { objective: "preserve_capital" }),
  sweep("riskPreference", "balanced", { objective: "balanced_growth" }),
  sweep("riskPreference", "aggressive", { objective: "growth" }),
  sweep("riskPreference", "maxUpside", { objective: "growth" }),

  // --- horizon -> horizon_days ---
  sweep("horizon", "3m", { horizon_days_min: 80, horizon_days_max: 100 }),
  sweep("horizon", "6m", { horizon_days_min: 165, horizon_days_max: 195 }),
  sweep("horizon", "1y", { horizon_days_min: 330, horizon_days_max: 400 }),
  sweep("horizon", "3yPlus", { horizon_days_min: 1000 }),

  // --- rebalance -> rebalance_frequency ---
  // "none" (buy-and-hold) and "agent" have no single correct enum value
  // (the Thesis has no "none"); we only require a valid thesis there.
  sweep("rebalance", "none", { valid_only: true }),
  sweep("rebalance", "monthly", { rebalance_frequency: "monthly" }),
  sweep("rebalance", "weekly", { rebalance_frequency: "weekly" }),
  sweep("rebalance", "agent", { valid_only: true }),

  // --- cashAllocation -> constraints.max_cash_weight ---
  sweep("cashAllocation", "none", { max_cash_weight_approx: 0 }),
  sweep("cashAllocation", "10", { max_cash_weight_approx: 0.1 }),
  sweep("cashAllocation", "25", { max_cash_weight_approx: 0.25 }),
  sweep("cashAllocation", "agent", { valid_only: true }),

  // --- targetAssets -> constraints.asset_count_min / asset_count_max ---
  sweep("targetAssets", "3-5", { asset_count_min: 3, asset_count_max: 5 }),
  sweep("targetAssets", "5-10", { asset_count_min: 5, asset_count_max: 10 }),
  sweep("targetAssets", "10-20", {
    asset_count_min: 10,
    asset_count_max: 20,
  }),
  sweep("targetAssets", "agent", { valid_only: true }),

  // --- persona: realistic full combos that stress field interactions ---
  {
    group: "persona",
    name: "persona/wizard-defaults",
    // Mirrors AllocationWizard defaultState exactly.
    params: {
      universe: "top25",
      exclusions: ["stablecoins", "wrapped"],
      minimumMarketCap: "1b",
      concentrationLimit: "20",
      maxDrawdown: "35",
      riskPreference: "balanced",
      horizon: "1y",
      rebalance: "monthly",
      initialCapitalUsd: "",
      cashAllocation: "10",
      targetAssets: "5-10",
    },
    expect: {
      objective: "balanced_growth",
      horizon_days_min: 330,
      horizon_days_max: 400,
      top_n_min: 20,
      top_n_max: 30,
      exclude_stablecoins: true,
      exclude_wrapped: true,
      market_cap_min_usd_approx: 1e9,
      max_weight_per_asset_approx: 0.2,
      max_drawdown_approx: 0.35,
      max_cash_weight_approx: 0.1,
      asset_count_min: 5,
      asset_count_max: 10,
      rebalance_frequency: "monthly",
    },
  },
  {
    group: "persona",
    name: "persona/aggressive-alts",
    params: {
      universe: "top50",
      exclusions: ["stablecoins", "wrapped"],
      minimumMarketCap: "100m",
      concentrationLimit: "30",
      maxDrawdown: "50",
      riskPreference: "aggressive",
      horizon: "6m",
      rebalance: "weekly",
      initialCapitalUsd: "25000",
      cashAllocation: "none",
      targetAssets: "10-20",
    },
    expect: {
      objective: "growth",
      horizon_days_min: 165,
      horizon_days_max: 195,
      max_drawdown_approx: 0.5,
      max_weight_per_asset_approx: 0.3,
      max_cash_weight_approx: 0,
      asset_count_min: 10,
      asset_count_max: 20,
      rebalance_frequency: "weekly",
    },
  },
  {
    group: "persona",
    name: "persona/preserve-tight-tension",
    // Feasibility tension: cap 20% * min 3 = 0.6 < 1 - 0.1 cash = 0.9.
    // The LLM must reconcile (raise cap, lower cash, or raise count) and
    // still emit a VALID thesis. We only assert objective + that it's
    // valid -- which knob it turns is its call.
    params: {
      universe: "top10",
      exclusions: ["stablecoins", "wrapped"],
      minimumMarketCap: "10b",
      concentrationLimit: "20",
      maxDrawdown: "10",
      riskPreference: "preserve",
      horizon: "3yPlus",
      rebalance: "monthly",
      initialCapitalUsd: "",
      cashAllocation: "10",
      targetAssets: "3-5",
    },
    expect: {
      objective: "preserve_capital",
      max_drawdown_approx: 0.1,
      horizon_days_min: 1000,
      valid_only: true,
    },
  },
];

// Advanced/chat path: freeform text briefs (not wizard-generated).
type FreeformCase = { group: "freeform"; name: string; brief: string; expect: Expect };
const FREEFORM_CASES: FreeformCase[] = [
  {
    group: "freeform",
    name: "freeform/preserve-btc-eth-handpicked",
    brief:
      "Capital preservation: only BTC and ETH, equal weight, 1 year, monthly rebalance, max drawdown 20%.",
    expect: {
      objective: "preserve_capital",
      hand_picked: true,
      horizon_days_min: 300,
      horizon_days_max: 400,
      rebalance_frequency: "monthly",
    },
  },
  {
    group: "freeform",
    name: "freeform/growth-alts-outside-top5",
    brief:
      "I want a high-growth altcoin basket over the next 6 months. Pick 6-8 names outside the top 5 by market cap, allow up to 25% in any one. Acceptable drawdown is 50%.",
    expect: {
      objective: "growth",
      horizon_days_min: 150,
      horizon_days_max: 220,
      max_drawdown_approx: 0.5,
    },
  },
];

type Assertion = { label: string; expected: string; actual: string; ok: boolean };

const REL_TOL = 0.1; // 10% relative tolerance for numeric "_approx"

function approxEqual(actual: number, expected: number): boolean {
  if (expected === 0) return Math.abs(actual) < 1e-9;
  return Math.abs(actual - expected) / Math.abs(expected) <= REL_TOL;
}

function evaluate(expect: Expect, thesis: Thesis): Assertion[] {
  const a: Assertion[] = [];
  const push = (label: string, expected: unknown, actual: unknown, ok: boolean) =>
    a.push({ label, expected: String(expected), actual: String(actual), ok });

  if (expect.valid_only) {
    push("valid_only", "thesis passes validateThesis", "valid", true);
  }
  if (expect.objective !== undefined) {
    push("objective", expect.objective, thesis.objective, thesis.objective === expect.objective);
  }
  if (expect.horizon_days_min !== undefined) {
    push(
      "horizon_days_min",
      `>= ${expect.horizon_days_min}`,
      thesis.horizon_days,
      thesis.horizon_days >= expect.horizon_days_min,
    );
  }
  if (expect.horizon_days_max !== undefined) {
    push(
      "horizon_days_max",
      `<= ${expect.horizon_days_max}`,
      thesis.horizon_days,
      thesis.horizon_days <= expect.horizon_days_max,
    );
  }
  const topN = thesis.universe_hints.top_n;
  if (expect.top_n_min !== undefined) {
    push("top_n_min", `>= ${expect.top_n_min}`, topN, topN >= expect.top_n_min);
  }
  if (expect.top_n_max !== undefined) {
    push("top_n_max", `<= ${expect.top_n_max}`, topN, topN <= expect.top_n_max);
  }
  if (expect.exclude_stablecoins !== undefined) {
    const v = thesis.universe_hints.exclude_stablecoins;
    push("exclude_stablecoins", expect.exclude_stablecoins, v, v === expect.exclude_stablecoins);
  }
  if (expect.exclude_wrapped !== undefined) {
    const v = thesis.universe_hints.exclude_wrapped;
    push("exclude_wrapped", expect.exclude_wrapped, v, v === expect.exclude_wrapped);
  }
  if (expect.market_cap_min_usd_absent) {
    const v = thesis.universe_hints.market_cap_min_usd;
    const ok = v === undefined || v === 0;
    push("market_cap_min_usd_absent", "unset or 0", v ?? "unset", ok);
  }
  if (expect.market_cap_min_usd_approx !== undefined) {
    const v = thesis.universe_hints.market_cap_min_usd;
    const ok = typeof v === "number" && approxEqual(v, expect.market_cap_min_usd_approx);
    push("market_cap_min_usd", `~${expect.market_cap_min_usd_approx}`, v ?? "unset", ok);
  }
  if (expect.max_weight_per_asset_approx !== undefined) {
    const v = thesis.constraints.max_weight_per_asset;
    push(
      "max_weight_per_asset",
      `~${expect.max_weight_per_asset_approx}`,
      v,
      approxEqual(v, expect.max_weight_per_asset_approx),
    );
  }
  if (expect.max_drawdown_approx !== undefined) {
    const v = thesis.constraints.max_drawdown;
    push("max_drawdown", `~${expect.max_drawdown_approx}`, v, approxEqual(v, expect.max_drawdown_approx));
  }
  if (expect.max_drawdown_min !== undefined) {
    const v = thesis.constraints.max_drawdown;
    push("max_drawdown_min", `> ${expect.max_drawdown_min}`, v, v > expect.max_drawdown_min);
  }
  if (expect.max_cash_weight_approx !== undefined) {
    const v = thesis.constraints.max_cash_weight;
    push("max_cash_weight", `~${expect.max_cash_weight_approx}`, v, approxEqual(v, expect.max_cash_weight_approx));
  }
  if (expect.asset_count_min !== undefined) {
    const v = thesis.constraints.asset_count_min;
    push("asset_count_min", expect.asset_count_min, v, v === expect.asset_count_min);
  }
  if (expect.asset_count_max !== undefined) {
    const v = thesis.constraints.asset_count_max;
    push("asset_count_max", expect.asset_count_max, v, v === expect.asset_count_max);
  }
  if (expect.rebalance_frequency !== undefined) {
    const v = thesis.rebalance_frequency;
    push("rebalance_frequency", expect.rebalance_frequency, v, v === expect.rebalance_frequency);
  }
  if (expect.hand_picked) {
    const ids = thesis.universe_hints.hand_picked_coin_ids ?? [];
    push("hand_picked_coin_ids", "non-empty", `${ids.length} ids`, ids.length > 0);
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
  const llm = createOpencodeLLMClient({ sessionTitle: "eval-interpret_brief" });

  const wizardCases = WIZARD_CASES.filter(
    (c) => groupFilter.size === 0 || groupFilter.has(c.group),
  );
  const freeformCases = FREEFORM_CASES.filter(
    (c) => groupFilter.size === 0 || groupFilter.has(c.group),
  );

  if (groupFilter.size > 0) {
    process.stdout.write(`group filter: ${[...groupFilter].join(", ")}\n`);
  }
  process.stdout.write(
    `running ${wizardCases.length} wizard + ${freeformCases.length} freeform cases\n`,
  );

  const outcomes: CaseOutcome[] = [];

  const runOne = async (
    name: string,
    group: string,
    brief: string,
    paramsForLog: AllocationWizardParams | null,
    expect: Expect,
  ) => {
    process.stdout.write(`\n${"=".repeat(72)}\n[${name}]\n`);
    if (paramsForLog) {
      process.stdout.write(`wizard params: ${JSON.stringify(paramsForLog)}\n`);
    }
    process.stdout.write(`--- brief (interpret_brief input) ---\n${brief}\n`);

    const start = Date.now();
    try {
      // No logger passed: the step's default pino logger streams its own
      // enter/llm_request/llm_response/exit lines to stdout, so you see
      // what the STEP is doing interleaved with what the EVAL asserts.
      const result = await interpretBrief({ run_id: `eval-${name}`, brief }, { llm });
      const duration_ms = Date.now() - start;
      const thesis = result.delta.thesis;

      process.stdout.write(
        `--- thesis (interpret_brief output) ---\n${JSON.stringify(thesis, null, 2)}\n`,
      );

      const assertions = evaluate(expect, thesis);
      process.stdout.write(`--- assertions ---\n`);
      for (const x of assertions) {
        process.stdout.write(
          `  ${x.ok ? "PASS" : "FAIL"} ${x.label}: expected ${x.expected}, got ${x.actual}\n`,
        );
      }
      const failed = assertions.filter((x) => !x.ok).map((x) => x.label);
      const status: "pass" | "fail" = failed.length === 0 ? "pass" : "fail";
      process.stdout.write(`[${name}] ${status.toUpperCase()} (${duration_ms} ms)\n`);
      outcomes.push({ name, group, status, duration_ms, failed });
    } catch (error) {
      const duration_ms = Date.now() - start;
      const kind =
        error instanceof ThesisValidationError
          ? "ThesisValidationError"
          : error instanceof Error
            ? error.constructor.name
            : "Error";
      const message = error instanceof Error ? error.message : String(error);
      process.stdout.write(`[${name}] FAIL (${duration_ms} ms, ${kind})\n  ${message}\n`);
      outcomes.push({
        name,
        group,
        status: "fail",
        duration_ms,
        failed: [`${kind}: ${message}`],
      });
    }
  };

  for (const c of wizardCases) {
    await runOne(c.name, c.group, buildAllocationWizardPrompt(c.params), c.params, c.expect);
  }
  for (const c of freeformCases) {
    await runOne(c.name, c.group, c.brief, null, c.expect);
  }

  process.stdout.write(`\n${"=".repeat(72)}\n=== summary ===\n`);
  for (const row of outcomes) {
    const reason = row.failed.length > 0 ? ` -- ${row.failed.join("; ")}` : "";
    process.stdout.write(
      `  ${row.status.toUpperCase().padEnd(4)} ${row.name.padEnd(40)} ${row.duration_ms} ms${reason}\n`,
    );
  }
  const failed = outcomes.filter((row) => row.status === "fail").length;
  process.stdout.write(`\n${outcomes.length - failed}/${outcomes.length} passed\n`);
  return failed > 0 ? 1 : 0;
}

// Run when invoked directly, not when imported.
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

export { WIZARD_CASES, FREEFORM_CASES, evaluate, main };
