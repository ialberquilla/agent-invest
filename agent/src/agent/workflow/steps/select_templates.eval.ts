// Eval runner for the select_templates step.
//
// select_templates reads a Thesis and classifies it onto a ranked
// shortlist of strategy families (spec.md section 9). Unlike
// interpret_brief there is no wizard input here -- the step consumes a
// Thesis, and only a digest of it (objective, horizon_days,
// rebalance_frequency, max_drawdown, asset_count_range,
// interpretation_notes). So the "combinations" worth testing are the
// THESIS SIGNALS that drive family routing.
//
// Strategy: per-signal sweep. Hold a feasible long-only baseline thesis,
// then vary one routing signal at a time and assert the family bias the
// prompt promises. The shorts gate (only shortlist a REQUIRES-SHORTS
// family when the thesis opts into shorts/hedging) is the highest-risk
// behavior, so it gets its own group with both negative (gate holds) and
// positive (correct short family surfaces) cases. The original
// hand-crafted full-thesis cases live in the `persona` group.
//
// Run all:        pnpm eval:select-templates
// Run one group:  pnpm eval:select-templates shorts objective
//   (group names: objective rebalance trend shorts persona)

// Loads .env (Azure / opencode credentials) on import; matches the
// rest of the codebase rather than relying on the caller's shell.
import "../../../env.ts";

import { createOpencodeLLMClient } from "../llm.ts";
import {
  SHORT_REQUIRING_FAMILIES,
  TemplateSelectionValidationError,
  type StrategyFamily,
  type TemplateSelection,
  type Thesis,
} from "../state.ts";
import { selectTemplates } from "./select_templates.ts";

type Expect = {
  // At least one of these families must appear in the shortlist.
  any_of?: StrategyFamily[];
  // The rank-1 family must be one of these.
  top_in?: StrategyFamily[];
  // None of these families may appear.
  none_of?: StrategyFamily[];
  // When true, NO short-requiring family may appear (gate must hold).
  no_shorts?: boolean;
};

type EvalCase = {
  group: string;
  name: string;
  thesis: Thesis;
  expect: Expect;
};

// Feasible long-only baseline. Each case overrides only the fields that
// carry the signal under test (usually objective + interpretation_notes,
// sometimes max_drawdown), so a single signal drives any failure.
function thesis(overrides: Partial<Thesis>): Thesis {
  return {
    objective: "balanced_growth",
    horizon_days: 365,
    weight_mode: "percentage",
    universe_hints: {
      top_n: 25,
      exclude_stablecoins: true,
      exclude_wrapped: true,
    },
    constraints: {
      max_weight_per_asset: 0.25,
      max_cash_weight: 0.1,
      max_drawdown: 0.35,
      asset_count_min: 4,
      asset_count_max: 10,
    },
    rebalance_frequency: "monthly",
    interpretation_notes: "Long-only basket.",
    ...overrides,
  };
}

const CONSERVATIVE: StrategyFamily[] = [
  "core_satellite_allocation",
  "threshold_rebalanced_allocation",
  "periodic_rebalanced_allocation",
  "volatility_targeted_exposure",
];

const CASES: EvalCase[] = [
  // --- objective -> family bias (long-only notes throughout) ---
  {
    group: "objective",
    name: "objective=preserve_capital",
    thesis: thesis({
      objective: "preserve_capital",
      constraints: {
        max_weight_per_asset: 0.4,
        max_cash_weight: 0.3,
        max_drawdown: 0.15,
        asset_count_min: 2,
        asset_count_max: 5,
      },
      interpretation_notes:
        "Capital preservation, tight 15% drawdown limit, conservative large caps. Long-only, no shorts.",
    }),
    // Tight drawdown: a drawdown-aware/conservative family must LEAD, not
    // a plain calendar rebalance (periodic). See the prompt's tight-
    // drawdown rule.
    expect: {
      any_of: CONSERVATIVE,
      top_in: [
        "volatility_targeted_exposure",
        "threshold_rebalanced_allocation",
        "core_satellite_allocation",
      ],
      no_shorts: true,
    },
  },
  {
    group: "objective",
    name: "objective=income",
    thesis: thesis({
      objective: "income",
      interpretation_notes:
        "Income-oriented defensive long-only allocation in large caps. No shorts.",
    }),
    expect: { any_of: CONSERVATIVE, no_shorts: true },
  },
  {
    group: "objective",
    name: "objective=balanced_growth",
    thesis: thesis({
      objective: "balanced_growth",
      interpretation_notes:
        "Balanced growth long-only basket with some alt upside. No shorts.",
    }),
    expect: {
      any_of: [
        "core_satellite_allocation",
        "periodic_rebalanced_allocation",
        "synthetic_long_allocation",
      ],
      no_shorts: true,
    },
  },
  {
    group: "objective",
    name: "objective=growth",
    thesis: thesis({
      objective: "growth",
      constraints: {
        max_weight_per_asset: 0.3,
        max_cash_weight: 0.0,
        max_drawdown: 0.6,
        asset_count_min: 3,
        asset_count_max: 8,
      },
      interpretation_notes:
        "Aggressive growth, long-only, high risk tolerance with a small speculative sleeve. No shorts.",
    }),
    // "Large core + small speculative sleeve" is the barbell definition;
    // a core/sleeve structural family must lead (not get buried at #3).
    expect: {
      any_of: [
        "barbell_allocation",
        "relative_momentum_rotation",
        "synthetic_long_allocation",
      ],
      top_in: ["barbell_allocation", "core_satellite_allocation"],
      no_shorts: true,
    },
  },

  // --- rebalance: cadence emphasis vs drift control ---
  {
    group: "rebalance",
    name: "rebalance=periodic-cadence",
    thesis: thesis({
      rebalance_frequency: "monthly",
      interpretation_notes:
        "Long-only basket rebalanced monthly to keep weights on target. No shorts.",
    }),
    expect: {
      any_of: ["periodic_rebalanced_allocation"],
      top_in: ["periodic_rebalanced_allocation"],
      no_shorts: true,
    },
  },
  {
    group: "rebalance",
    name: "rebalance=drift-control",
    thesis: thesis({
      interpretation_notes:
        "Long-only; rebalance only when weights drift past a threshold, to avoid overtrading and trim winners. No shorts.",
    }),
    expect: {
      any_of: ["threshold_rebalanced_allocation"],
      top_in: ["threshold_rebalanced_allocation"],
      no_shorts: true,
    },
  },

  // --- trend / momentum signals (long-only) ---
  {
    group: "trend",
    name: "trend=momentum-rotation",
    thesis: thesis({
      objective: "growth",
      interpretation_notes:
        "Long-only; rotate into the strongest-momentum names each month. No shorts.",
    }),
    expect: {
      any_of: ["relative_momentum_rotation", "trend_following_long_neutral"],
      top_in: ["relative_momentum_rotation"],
      no_shorts: true,
    },
  },
  {
    group: "trend",
    name: "trend=long-neutral",
    thesis: thesis({
      interpretation_notes:
        "Long-only; stay long only while trend is positive, de-risk to cash when trend turns negative. No shorts.",
    }),
    expect: {
      any_of: ["trend_following_long_neutral", "relative_momentum_rotation"],
      top_in: ["trend_following_long_neutral"],
      no_shorts: true,
    },
  },

  // --- shorts gate: NEGATIVE (gate must hold, no short family) ---
  {
    group: "shorts",
    name: "shorts=gate-longonly-balanced",
    thesis: thesis({
      interpretation_notes: "Long-only basket. Shorts are not allowed.",
    }),
    expect: { no_shorts: true },
  },
  {
    group: "shorts",
    name: "shorts=gate-longonly-growth-drawdown",
    thesis: thesis({
      objective: "growth",
      constraints: {
        max_weight_per_asset: 0.3,
        max_cash_weight: 0.0,
        max_drawdown: 0.6,
        asset_count_min: 3,
        asset_count_max: 8,
      },
      // Mentions "downside" but explicitly long-only: the gate must NOT
      // be tripped by the word "downside" alone.
      interpretation_notes:
        "Aggressive growth. Worried about downside but wants to stay long-only; no shorts under any circumstances.",
    }),
    expect: { no_shorts: true },
  },

  // --- shorts gate: POSITIVE (shorts opted in -> right short family) ---
  {
    group: "shorts",
    name: "shorts=partial-hedge",
    thesis: thesis({
      objective: "growth",
      interpretation_notes:
        "Advanced user. Shorts explicitly allowed. Wants a partial short hedge on the long book for downside protection without fully exiting.",
    }),
    expect: {
      any_of: [
        "partial_hedge_overlay",
        "drawdown_based_hedge",
        "beta_hedged_alt_exposure",
      ],
      top_in: ["partial_hedge_overlay"],
    },
  },
  {
    group: "shorts",
    name: "shorts=long-short-trend",
    thesis: thesis({
      objective: "growth",
      interpretation_notes:
        "Advanced user, shorts allowed. Go long positive-trend markets and short negative-trend markets.",
    }),
    expect: {
      any_of: ["trend_following_long_short"],
      top_in: ["trend_following_long_short"],
    },
  },
  {
    group: "shorts",
    name: "shorts=pair-trade",
    thesis: thesis({
      objective: "growth",
      interpretation_notes:
        "Advanced user, shorts allowed. Express long SOL versus short ETH as a market-neutral relative-value pair.",
    }),
    expect: {
      any_of: ["relative_value_pair_trade"],
      top_in: ["relative_value_pair_trade"],
    },
  },
  {
    group: "shorts",
    name: "shorts=beta-hedged-alt",
    thesis: thesis({
      objective: "growth",
      interpretation_notes:
        "Advanced user, shorts allowed. Hold a long alt position while shorting BTC/ETH to strip out market beta.",
    }),
    expect: {
      any_of: ["beta_hedged_alt_exposure", "partial_hedge_overlay"],
      top_in: ["beta_hedged_alt_exposure"],
    },
  },
  {
    group: "shorts",
    name: "shorts=drawdown-hedge",
    thesis: thesis({
      interpretation_notes:
        "Shorts allowed. Add a short hedge that scales up as realized drawdown crosses precommitted thresholds, as a behavioral guardrail.",
    }),
    expect: {
      any_of: ["drawdown_based_hedge", "partial_hedge_overlay"],
      top_in: ["drawdown_based_hedge"],
    },
  },

  // --- persona: realistic full-thesis combos ---
  {
    group: "persona",
    name: "persona/balanced-monthly-rebalance",
    thesis: thesis({
      objective: "balanced_growth",
      rebalance_frequency: "monthly",
      interpretation_notes:
        "Balanced large-cap long basket, rebalanced monthly to keep weights on target. Long-only.",
    }),
    expect: {
      any_of: [
        "periodic_rebalanced_allocation",
        "core_satellite_allocation",
        "synthetic_long_allocation",
      ],
      no_shorts: true,
    },
  },
  {
    group: "persona",
    name: "persona/preserve-low-drawdown",
    thesis: thesis({
      objective: "preserve_capital",
      constraints: {
        max_weight_per_asset: 0.4,
        max_cash_weight: 0.3,
        max_drawdown: 0.15,
        asset_count_min: 2,
        asset_count_max: 5,
      },
      interpretation_notes:
        "Capital preservation, tight 15% drawdown limit, conservative large caps. Long-only.",
    }),
    expect: { any_of: CONSERVATIVE, no_shorts: true },
  },
  {
    group: "persona",
    name: "persona/growth-momentum",
    thesis: thesis({
      objective: "growth",
      constraints: {
        max_weight_per_asset: 0.3,
        max_cash_weight: 0.0,
        max_drawdown: 0.6,
        asset_count_min: 3,
        asset_count_max: 8,
      },
      interpretation_notes:
        "Aggressive growth; rotate into the strongest-momentum names each month. Long-only.",
    }),
    expect: {
      any_of: [
        "relative_momentum_rotation",
        "trend_following_long_neutral",
        "barbell_allocation",
      ],
      no_shorts: true,
    },
  },
  {
    group: "persona",
    name: "persona/hedged-long-short-allowed",
    thesis: thesis({
      objective: "growth",
      constraints: {
        max_weight_per_asset: 0.3,
        max_cash_weight: 0.0,
        max_drawdown: 0.4,
        asset_count_min: 3,
        asset_count_max: 8,
      },
      interpretation_notes:
        "Advanced user. Shorts are explicitly allowed. Wants downside protection via partial hedges and is open to long/short trend exposure.",
    }),
    expect: {
      any_of: [
        "partial_hedge_overlay",
        "trend_following_long_short",
        "drawdown_based_hedge",
        "beta_hedged_alt_exposure",
      ],
    },
  },
];

type Assertion = { label: string; expected: string; actual: string; ok: boolean };

function evaluate(expect: Expect, selection: TemplateSelection): Assertion[] {
  const a: Assertion[] = [];
  const families = selection.selected.map((s) => s.family);
  const top = selection.selected.find((s) => s.rank === 1)?.family;
  const shown = `[${families.join(", ")}]`;

  if (expect.any_of) {
    const ok = expect.any_of.some((f) => families.includes(f));
    a.push({
      label: "any_of",
      expected: `>=1 of [${expect.any_of.join(", ")}]`,
      actual: shown,
      ok,
    });
  }
  if (expect.top_in) {
    const ok = Boolean(top && expect.top_in.includes(top));
    a.push({
      label: "top_in",
      expected: `rank-1 in [${expect.top_in.join(", ")}]`,
      actual: top ?? "none",
      ok,
    });
  }
  if (expect.none_of) {
    const hit = families.filter((f) => expect.none_of!.includes(f));
    a.push({
      label: "none_of",
      expected: `none of [${expect.none_of.join(", ")}]`,
      actual: hit.length ? `[${hit.join(", ")}]` : "none present",
      ok: hit.length === 0,
    });
  }
  if (expect.no_shorts) {
    const shorts = families.filter((f) => SHORT_REQUIRING_FAMILIES.has(f));
    a.push({
      label: "no_shorts",
      expected: "no short-requiring families",
      actual: shorts.length ? `[${shorts.join(", ")}]` : "none present",
      ok: shorts.length === 0,
    });
  }
  return a;
}

// Mirror the digest select_templates actually feeds the LLM, so the log
// shows exactly which signals the routing decision saw.
function signals(t: Thesis) {
  return {
    objective: t.objective,
    horizon_days: t.horizon_days,
    rebalance_frequency: t.rebalance_frequency,
    max_drawdown: t.constraints.max_drawdown,
    asset_count_range: [
      t.constraints.asset_count_min,
      t.constraints.asset_count_max,
    ],
    interpretation_notes: t.interpretation_notes,
  };
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
  const llm = createOpencodeLLMClient({ sessionTitle: "eval-select_templates" });

  const cases = CASES.filter(
    (c) => groupFilter.size === 0 || groupFilter.has(c.group),
  );
  if (groupFilter.size > 0) {
    process.stdout.write(`group filter: ${[...groupFilter].join(", ")}\n`);
  }
  process.stdout.write(`running ${cases.length} cases\n`);

  const outcomes: CaseOutcome[] = [];

  for (const c of cases) {
    process.stdout.write(`\n${"=".repeat(72)}\n[${c.name}]\n`);
    process.stdout.write(
      `--- thesis signals (select_templates input) ---\n${JSON.stringify(signals(c.thesis), null, 2)}\n`,
    );
    const start = Date.now();
    try {
      // No logger passed: the step's default pino logger streams its own
      // enter/llm_request/llm_response/exit lines, so the STEP's behavior
      // and the EVAL's assertions show up together.
      const result = await selectTemplates(
        { run_id: `eval-${c.name}`, thesis: c.thesis },
        { llm },
      );
      const duration_ms = Date.now() - start;
      const selection = result.delta.template_selection;

      process.stdout.write(
        `--- selection (select_templates output) ---\n${JSON.stringify(selection, null, 2)}\n`,
      );

      const assertions = evaluate(c.expect, selection);
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
      const kind =
        error instanceof TemplateSelectionValidationError
          ? "TemplateSelectionValidationError"
          : error instanceof Error
            ? error.constructor.name
            : "Error";
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
      `  ${row.status.toUpperCase().padEnd(4)} ${row.name.padEnd(42)} ${row.duration_ms} ms${reason}\n`,
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

export { CASES, evaluate, main };
