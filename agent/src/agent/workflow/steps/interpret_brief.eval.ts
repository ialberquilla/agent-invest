// Eval runner for the interpret_brief step. Runs each fixture brief
// through the real opencode-backed LLM client and prints per-fixture
// pass/fail with brief assertions on the resulting Thesis.
//
// Run with:  pnpm tsx src/agent/workflow/steps/interpret_brief.eval.ts
// Or:        pnpm eval:interpret-brief

// Loads .env (Azure / opencode credentials) on import; matches the
// rest of the codebase rather than relying on the caller's shell.
import "../../../env.ts";

import { createOpencodeLLMClient } from "../llm.ts";
import { ThesisValidationError, type Thesis } from "../state.ts";
import { interpretBrief } from "./interpret_brief.ts";

type Expect = {
  objective?: Thesis["objective"];
  horizon_days_min?: number;
  horizon_days_max?: number;
  hand_picked?: boolean;
  asset_count_max_lte?: number;
  asset_count_min_gte?: number;
  max_weight_per_asset_lte?: number;
  top_skip_gte?: number;
};

type EvalCase = {
  name: string;
  brief: string;
  expect: Expect;
};

const CASES: EvalCase[] = [
  {
    name: "balanced_top25_1y",
    brief:
      "Construct a 5-10 asset, monthly rebalanced basket from the top-25 non-stablecoin non-wrapped cryptoassets above $1B market cap. Use percentage weights only, capped at 20% per token and up to 10% cash, balanced-growth objective over 1 year, treat 35% max drawdown as a hard risk limit.",
    expect: {
      objective: "balanced_growth",
      horizon_days_min: 300,
      horizon_days_max: 400,
      asset_count_max_lte: 10,
      asset_count_min_gte: 5,
      max_weight_per_asset_lte: 0.2,
    },
  },
  {
    name: "growth_alts_6mo",
    brief:
      "I want a high-growth altcoin basket over the next 6 months. Pick 6-8 names outside the top 5 by market cap, allow up to 25% in any one. Acceptable drawdown is 50%.",
    expect: {
      objective: "growth",
      horizon_days_min: 150,
      horizon_days_max: 220,
      top_skip_gte: 5,
    },
  },
  {
    name: "preserve_capital_btc_eth",
    brief:
      "Capital preservation: only BTC and ETH, equal weight, 1 year, monthly rebalance, max drawdown 20%.",
    expect: {
      objective: "preserve_capital",
      hand_picked: true,
      horizon_days_min: 300,
      horizon_days_max: 400,
    },
  },
  {
    name: "income_defensive_long",
    brief:
      "Income-oriented allocation over 2 years using up to 10 large-cap assets, max 15% per asset, ok with 5% cash buffer, drawdown limit 25%.",
    expect: {
      objective: "income",
      horizon_days_min: 600,
      horizon_days_max: 800,
      max_weight_per_asset_lte: 0.15,
    },
  },
];

type CaseOutcome = {
  name: string;
  status: "pass" | "fail";
  duration_ms: number;
  violations: string[];
  thesis?: Thesis;
};

async function main(): Promise<number> {
  const llm = createOpencodeLLMClient({ sessionTitle: "eval-interpret_brief" });
  const outcomes: CaseOutcome[] = [];

  for (const c of CASES) {
    process.stdout.write(`\n[${c.name}] running...\n`);
    const start = Date.now();
    try {
      const result = await interpretBrief(
        { run_id: `eval-interpret-brief-${c.name}`, brief: c.brief },
        { llm },
      );
      const violations = checkExpect(c.expect, result.delta.thesis);
      const duration_ms = Date.now() - start;
      if (violations.length === 0) {
        process.stdout.write(`[${c.name}] PASS (${duration_ms} ms)\n`);
      } else {
        process.stdout.write(`[${c.name}] FAIL (${duration_ms} ms)\n`);
        for (const v of violations) process.stdout.write(`  - ${v}\n`);
      }
      process.stdout.write(
        `  thesis: ${JSON.stringify(result.delta.thesis)}\n`,
      );
      outcomes.push({
        name: c.name,
        status: violations.length === 0 ? "pass" : "fail",
        duration_ms,
        violations,
        thesis: result.delta.thesis,
      });
    } catch (error) {
      const duration_ms = Date.now() - start;
      const kind =
        error instanceof ThesisValidationError
          ? "ThesisValidationError"
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
      `  ${row.status.toUpperCase().padEnd(4)} ${row.name.padEnd(28)} ${row.duration_ms} ms${reason}\n`,
    );
  }
  const failed = outcomes.filter((row) => row.status === "fail").length;
  process.stdout.write(
    `\n${outcomes.length - failed}/${outcomes.length} passed\n`,
  );
  return failed > 0 ? 1 : 0;
}

function checkExpect(expect: Expect, thesis: Thesis): string[] {
  const out: string[] = [];
  if (expect.objective && thesis.objective !== expect.objective) {
    out.push(
      `expected objective=${expect.objective} got ${thesis.objective}`,
    );
  }
  if (
    expect.horizon_days_min !== undefined &&
    thesis.horizon_days < expect.horizon_days_min
  ) {
    out.push(
      `expected horizon_days >= ${expect.horizon_days_min} got ${thesis.horizon_days}`,
    );
  }
  if (
    expect.horizon_days_max !== undefined &&
    thesis.horizon_days > expect.horizon_days_max
  ) {
    out.push(
      `expected horizon_days <= ${expect.horizon_days_max} got ${thesis.horizon_days}`,
    );
  }
  if (expect.hand_picked === true) {
    const ids = thesis.universe_hints.hand_picked_coin_ids ?? [];
    if (ids.length === 0) {
      out.push(`expected hand_picked_coin_ids to be non-empty`);
    }
  }
  if (
    expect.asset_count_max_lte !== undefined &&
    thesis.constraints.asset_count_max > expect.asset_count_max_lte
  ) {
    out.push(
      `expected asset_count_max <= ${expect.asset_count_max_lte} got ${thesis.constraints.asset_count_max}`,
    );
  }
  if (
    expect.asset_count_min_gte !== undefined &&
    thesis.constraints.asset_count_min < expect.asset_count_min_gte
  ) {
    out.push(
      `expected asset_count_min >= ${expect.asset_count_min_gte} got ${thesis.constraints.asset_count_min}`,
    );
  }
  if (
    expect.max_weight_per_asset_lte !== undefined &&
    thesis.constraints.max_weight_per_asset >
      expect.max_weight_per_asset_lte + 1e-6
  ) {
    out.push(
      `expected max_weight_per_asset <= ${expect.max_weight_per_asset_lte} got ${thesis.constraints.max_weight_per_asset}`,
    );
  }
  if (expect.top_skip_gte !== undefined) {
    const observed = thesis.universe_hints.top_skip ?? 0;
    if (observed < expect.top_skip_gte) {
      out.push(
        `expected universe_hints.top_skip >= ${expect.top_skip_gte} got ${observed}`,
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
