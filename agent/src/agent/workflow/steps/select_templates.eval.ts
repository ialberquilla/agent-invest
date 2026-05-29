// Eval runner for the select_templates step. Runs fixture theses
// through the real opencode-backed LLM client and checks that the
// shortlisted strategy families are sane for each thesis.
//
// Run with:  pnpm tsx src/agent/workflow/steps/select_templates.eval.ts
// Or:        pnpm eval:select-templates

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
  // None of these families may appear (e.g. shorts when not allowed).
  none_of?: StrategyFamily[];
  // When true, no SHORT_REQUIRING_FAMILIES may appear.
  no_shorts?: boolean;
};

type EvalCase = {
  name: string;
  thesis: Thesis;
  expect: Expect;
};

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

const CASES: EvalCase[] = [
  {
    name: "balanced_monthly_rebalance",
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
    name: "preserve_capital_low_drawdown",
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
    expect: {
      any_of: [
        "core_satellite_allocation",
        "threshold_rebalanced_allocation",
        "periodic_rebalanced_allocation",
        "volatility_targeted_exposure",
      ],
      no_shorts: true,
    },
  },
  {
    name: "growth_momentum",
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
    name: "hedged_long_short_allowed",
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

type CaseOutcome = {
  name: string;
  status: "pass" | "fail";
  duration_ms: number;
  violations: string[];
  selection?: TemplateSelection;
};

async function main(): Promise<number> {
  const llm = createOpencodeLLMClient({ sessionTitle: "eval-select_templates" });
  const outcomes: CaseOutcome[] = [];

  for (const c of CASES) {
    process.stdout.write(`\n[${c.name}] running...\n`);
    const start = Date.now();
    try {
      const result = await selectTemplates(
        { run_id: `eval-select-templates-${c.name}`, thesis: c.thesis },
        { llm },
      );
      const violations = checkExpect(c.expect, result.delta.template_selection);
      const duration_ms = Date.now() - start;
      if (violations.length === 0) {
        process.stdout.write(`[${c.name}] PASS (${duration_ms} ms)\n`);
      } else {
        process.stdout.write(`[${c.name}] FAIL (${duration_ms} ms)\n`);
        for (const v of violations) process.stdout.write(`  - ${v}\n`);
      }
      process.stdout.write(
        `  selection: ${JSON.stringify(result.delta.template_selection)}\n`,
      );
      outcomes.push({
        name: c.name,
        status: violations.length === 0 ? "pass" : "fail",
        duration_ms,
        violations,
        selection: result.delta.template_selection,
      });
    } catch (error) {
      const duration_ms = Date.now() - start;
      const kind =
        error instanceof TemplateSelectionValidationError
          ? "TemplateSelectionValidationError"
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

function checkExpect(expect: Expect, selection: TemplateSelection): string[] {
  const out: string[] = [];
  const families = selection.selected.map((s) => s.family);
  const top = selection.selected.find((s) => s.rank === 1)?.family;

  if (expect.any_of && !expect.any_of.some((f) => families.includes(f))) {
    out.push(
      `expected at least one of [${expect.any_of.join(", ")}], got [${families.join(", ")}]`,
    );
  }
  if (expect.top_in && (!top || !expect.top_in.includes(top))) {
    out.push(
      `expected rank-1 family in [${expect.top_in.join(", ")}], got ${top ?? "none"}`,
    );
  }
  if (expect.none_of) {
    const hit = families.filter((f) => expect.none_of!.includes(f));
    if (hit.length > 0) {
      out.push(`expected none of [${expect.none_of.join(", ")}], got [${hit.join(", ")}]`);
    }
  }
  if (expect.no_shorts) {
    const shorts = families.filter((f) => SHORT_REQUIRING_FAMILIES.has(f));
    if (shorts.length > 0) {
      out.push(
        `expected no short-requiring families, got [${shorts.join(", ")}]`,
      );
    }
  }
  return out;
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

export { CASES, checkExpect, main };
