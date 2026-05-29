// Eval runner for run_and_validate. Calls the real run_candidate_batch
// + validate_against_thesis CLIs end-to-end.
//
// Run with:  pnpm eval:run-and-validate

import "../../../env.ts";

import { runAndValidate } from "./run_and_validate.ts";
import type {
  AttemptValidationSummary,
  Proposal,
  RunAndValidateInput,
  Thesis,
  Universe,
  Window,
} from "../state.ts";

type Expect = {
  min_passing?: number;
  min_failing?: number;
  must_have_violation?: string;
};

type EvalCase = {
  name: string;
  input: RunAndValidateInput;
  expect: Expect;
};

// Eval uses objective="growth" (→ "high_growth" benchmark = BTC HODL)
// because the dataset's usd-coin price series only starts 2023-09-25,
// which collides with the balanced_5050 benchmark that needs USDC from
// 2022-05-25. Real workflow runs with balanced_growth would hit the
// same data gap until the dataset is backfilled.
function thesis(overrides: Partial<Thesis> = {}): Thesis {
  return {
    objective: "growth",
    horizon_days: 365,
    weight_mode: "percentage",
    universe_hints: {
      top_n: 3,
      exclude_stablecoins: true,
      exclude_wrapped: true,
    },
    constraints: {
      max_weight_per_asset: 0.4,
      max_cash_weight: 0.0,
      max_drawdown: 0.7,
      asset_count_min: 3,
      asset_count_max: 3,
    },
    rebalance_frequency: "monthly",
    interpretation_notes: "eval fixture",
    ...overrides,
  };
}

// BTC+ETH+BNB is the smallest set known clean of price gaps across
// the recommend_backtest_window default range.
const UNIVERSE: Universe = {
  coin_ids: ["bitcoin", "ethereum", "binancecoin"],
  source: "rank_universe",
  effective_filters: {
    top_n: 3,
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

// run_candidate_batch requires at least 3 candidates per batch.
// Universe is 3 coins so select_top is fixed at 3 across candidates;
// variety comes from template / weighting / trigger.
const BUY_AND_HOLD_SPREAD: Proposal = {
  iteration_hypothesis: "Three baselines spanning buy-and-hold weightings and one rebalanced control.",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "buy_and_hold",
      select_top: 3,
      weighting: "equal",
      rationale: "static equal-weight baseline",
    },
    {
      candidate_id: "c2",
      template_id: "buy_and_hold",
      select_top: 3,
      weighting: "cap",
      rationale: "cap-weighted baseline",
    },
    {
      candidate_id: "c3",
      template_id: "periodic_rebalance",
      select_top: 3,
      weighting: "equal",
      rebalance_trigger: "periodic_30d",
      rationale: "monthly rebalanced control",
    },
  ],
};

const PERIODIC_REBALANCE_SPREAD: Proposal = {
  iteration_hypothesis: "Monthly equal-weight baskets across rebalance cadences.",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "periodic_rebalance",
      select_top: 3,
      weighting: "equal",
      rebalance_trigger: "periodic_30d",
      rationale: "monthly rebalance",
    },
    {
      candidate_id: "c2",
      template_id: "periodic_rebalance",
      select_top: 3,
      weighting: "equal",
      rebalance_trigger: "periodic_90d",
      rationale: "quarterly rebalance",
    },
    {
      candidate_id: "c3",
      template_id: "periodic_rebalance",
      select_top: 3,
      weighting: "cap",
      rebalance_trigger: "periodic_30d",
      rationale: "cap-weighted monthly rebalance",
    },
  ],
};

const CASES: EvalCase[] = [
  {
    name: "lax_buy_and_hold_should_pass",
    input: {
      run_id: "eval-run-and-validate-lax",
      thesis: thesis(),
      universe: UNIVERSE,
      window: WINDOW,
      proposal: BUY_AND_HOLD_SPREAD,
    },
    expect: { min_passing: 1 },
  },
  {
    name: "tight_drawdown_should_fail",
    // Equal-weight large-cap basket through 2022-2026 historically hits
    // ~50% drawdown, so a 20% drawdown cap is expected to fail.
    input: {
      run_id: "eval-run-and-validate-tight",
      thesis: thesis({ constraints: { ...thesis().constraints, max_drawdown: 0.2 } }),
      universe: UNIVERSE,
      window: WINDOW,
      proposal: PERIODIC_REBALANCE_SPREAD,
    },
    expect: {
      min_failing: 1,
      must_have_violation: "max_drawdown",
    },
  },
];

type CaseOutcome = {
  name: string;
  status: "pass" | "fail";
  duration_ms: number;
  violations: string[];
  batch_id?: string;
  summary?: AttemptValidationSummary;
};

async function main(): Promise<number> {
  const outcomes: CaseOutcome[] = [];

  for (const c of CASES) {
    process.stdout.write(`\n[${c.name}] running...\n`);
    const start = Date.now();
    try {
      const result = await runAndValidate(c.input, { timeoutSeconds: 120 });
      const violations = checkExpect(c.expect, result.delta.validation_summary);
      const duration_ms = Date.now() - start;
      if (violations.length === 0) {
        process.stdout.write(`[${c.name}] PASS (${duration_ms} ms)\n`);
      } else {
        process.stdout.write(`[${c.name}] FAIL (${duration_ms} ms)\n`);
        for (const v of violations) process.stdout.write(`  - ${v}\n`);
      }
      process.stdout.write(
        `  batch_id=${result.delta.batch_id} passing=${result.delta.validation_summary.passing_candidate_ids.length} failing=${result.delta.validation_summary.failing.length}\n`,
      );
      for (const f of result.delta.validation_summary.failing) {
        process.stdout.write(
          `    ${f.candidate_id}: ${f.violations.map((v) => `${v.constraint}(target=${v.target}, observed=${v.observed})`).join(", ")}\n`,
        );
      }
      outcomes.push({
        name: c.name,
        status: violations.length === 0 ? "pass" : "fail",
        duration_ms,
        violations,
        batch_id: result.delta.batch_id,
        summary: result.delta.validation_summary,
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
      `  ${row.status.toUpperCase().padEnd(4)} ${row.name.padEnd(34)} ${row.duration_ms} ms${reason}\n`,
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
  summary: AttemptValidationSummary,
): string[] {
  const out: string[] = [];
  const passing = summary.passing_candidate_ids.length;
  const failing = summary.failing.length;
  if (expect.min_passing !== undefined && passing < expect.min_passing) {
    out.push(`expected passing >= ${expect.min_passing}, got ${passing}`);
  }
  if (expect.min_failing !== undefined && failing < expect.min_failing) {
    out.push(`expected failing >= ${expect.min_failing}, got ${failing}`);
  }
  if (expect.must_have_violation) {
    const seen = summary.failing.some((f) =>
      f.violations.some((v) => v.constraint === expect.must_have_violation),
    );
    if (!seen) {
      out.push(
        `expected at least one violation with constraint=${expect.must_have_violation}`,
      );
    }
  }
  return out;
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
