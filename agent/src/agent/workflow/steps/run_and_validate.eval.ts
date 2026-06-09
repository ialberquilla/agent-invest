// Eval runner for the run_and_validate step.
//
// run_and_validate is DETERMINISTIC -- it assembles a run_candidate_batch
// request from proposal + already-resolved universe + window, runs the real
// backtest CLI, then runs the real validate_against_thesis gate and
// normalizes the response into an AttemptValidationSummary
// (passing_candidate_ids + per-candidate violations). Its pure mapping logic
// (buildBatchInput / thesisForValidate / normalizeValidationSummary) is already
// covered hermetically by test/workflow-run-and-validate.test.ts. This eval's
// distinct job is the LIVE INTEGRATION: run the real backtest + gate over the
// families the bt migration unlocked and assert behavior only a live run shows.
//
// Groups (named by the candidate path under test):
//   baseline        synthetic_long / periodic buy-and-hold spread
//   momentum        relative_momentum_rotation + volatility_targeted_exposure
//   trend           trend_following_long_neutral spread
//   core_satellite  core_satellite_allocation + barbell_allocation
//   shorts          beta_hedged / partial_hedge / pair_trade -- gross-weight
//                   accounting (a short leg counts at its absolute size)
//   drawdown        the thesisForValidate sign-convention adapter: the SAME
//                   candidates pass under a loose max_drawdown and fail under a
//                   tight one with a max_drawdown violation -- only correct if
//                   the adapter negates the (positive) thesis drawdown so the
//                   floor check `realised_dd < -expected` has the right sign.
//
// Requires Postgres up with ingested price history (host port 5434).
//
// Run all:        pnpm eval:run-and-validate
// Run one group:  pnpm eval:run-and-validate momentum shorts

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
  // realised passing/failing counts across the batch.
  min_passing?: number;
  min_failing?: number;
  // specific candidate_ids that must land in passing / failing -- a stronger
  // claim than counts: "this family actually backtested and (didn't) clear".
  must_pass?: string[];
  must_fail?: string[];
  // at least one failing candidate carries a violation with this constraint.
  must_have_violation?: string;
};

type EvalCase = {
  group: string;
  name: string;
  input: RunAndValidateInput;
  expect: Expect;
};

// objective="growth" (-> "high_growth" benchmark = BTC HODL) because the
// dataset's usd-coin series only starts 2023-09-25, which collides with the
// balanced_5050 benchmark that needs USDC from 2022-05-25.
function thesis(overrides: { constraints?: Partial<Thesis["constraints"]> } = {}): Thesis {
  return {
    objective: "growth",
    horizon_days: 365,
    weight_mode: "percentage",
    universe_hints: {
      top_n: 3,
      exclude_stablecoins: true,
      exclude_wrapped: true,
    },
    // Loose defaults: every constraint wide open so a case passes unless the
    // backtest itself errors. Cases that probe a gate tighten ONE constraint.
    // The weight cap is 1.5 (not 1.0): max_single_weight is a GROSS figure off
    // bt's drifted security_weights, so a fully-invested single-name trend book
    // drifts a hair over 1.0 (~1.01) and a hedge book's long leg runs to ~1.2.
    constraints: {
      max_weight_per_asset: 1.5,
      max_cash_weight: 0.0,
      max_drawdown: 0.9,
      asset_count_min: 1,
      asset_count_max: 50,
      ...overrides.constraints,
    },
    rebalance_frequency: "monthly",
    interpretation_notes: "eval fixture",
  };
}

// BTC+ETH+BNB is the smallest set known clean of price gaps across the window.
// For the shorts families this also fixes the core/satellite split: BTC+ETH are
// the CORE_COINS hedged short, BNB is the long satellite.
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

// --- proposals (each >= 3 candidates: run_candidate_batch requires it) -------

const BASELINE: Proposal = {
  iteration_hypothesis: "Buy-and-hold weightings plus a rebalanced control.",
  candidates: [
    { candidate_id: "bh_equal", template_id: "synthetic_long_allocation", select_top: 3, weighting: "equal", rationale: "static equal-weight baseline" },
    { candidate_id: "bh_cap", template_id: "synthetic_long_allocation", select_top: 3, weighting: "cap", rationale: "cap-weighted baseline" },
    { candidate_id: "periodic_ctrl", template_id: "periodic_rebalanced_allocation", select_top: 3, weighting: "equal", rebalance_trigger: "periodic_30d", rationale: "monthly rebalanced control" },
  ],
};

const MOMENTUM: Proposal = {
  iteration_hypothesis: "Signal-driven rotation + vol-targeted exposure vs a periodic control.",
  candidates: [
    { candidate_id: "momentum", template_id: "relative_momentum_rotation", select_top: 3, weighting: "equal", rebalance_trigger: "periodic_30d", rationale: "rotate into the strongest half monthly" },
    { candidate_id: "vol_target", template_id: "volatility_targeted_exposure", select_top: 3, weighting: "vol_inverse", rebalance_trigger: "periodic_30d", rationale: "inverse-vol weighted, monthly" },
    { candidate_id: "periodic_ctrl", template_id: "periodic_rebalanced_allocation", select_top: 3, weighting: "equal", rebalance_trigger: "periodic_30d", rationale: "monthly rebalanced control" },
  ],
};

// trend_following_long_neutral carries only select_top + weighting (no
// rebalance_trigger slot -- it rebalances weekly by construction).
const TREND: Proposal = {
  iteration_hypothesis: "Long-when-above-SMA, cash otherwise, across weightings and breadth.",
  candidates: [
    { candidate_id: "trend_equal", template_id: "trend_following_long_neutral", select_top: 3, weighting: "equal", rationale: "equal-weight in-trend names" },
    { candidate_id: "trend_cap", template_id: "trend_following_long_neutral", select_top: 3, weighting: "cap", rationale: "cap-weight in-trend names" },
    { candidate_id: "trend_narrow", template_id: "trend_following_long_neutral", select_top: 2, weighting: "equal", rationale: "narrower in-trend basket" },
  ],
};

const CORE_SATELLITE: Proposal = {
  iteration_hypothesis: "Large stable core with a small speculative sleeve, two structures.",
  candidates: [
    { candidate_id: "core_sat", template_id: "core_satellite_allocation", select_top: 3, weighting: "equal", rebalance_trigger: "periodic_90d", core_weight: 0.7, rationale: "70% core, quarterly" },
    { candidate_id: "barbell", template_id: "barbell_allocation", select_top: 3, weighting: "equal", rebalance_trigger: "periodic_90d", core_weight: 0.85, sleeve_cap: 0.05, rationale: "85% core, capped 5% sleeves" },
    { candidate_id: "periodic_ctrl", template_id: "periodic_rebalanced_allocation", select_top: 3, weighting: "equal", rebalance_trigger: "periodic_90d", rationale: "quarterly rebalanced control" },
  ],
};

// Short/hedge families: negative legs. beta_hedged longs the BNB satellite and
// shorts the BTC/ETH core; pair_trade longs the top name and shorts the next.
const SHORTS: Proposal = {
  iteration_hypothesis: "Beta-hedged alt exposure, a partial hedge overlay, and a relative-value pair.",
  candidates: [
    { candidate_id: "beta_hedged", template_id: "beta_hedged_alt_exposure", select_top: 3, weighting: "equal", rebalance_trigger: "periodic_30d", rationale: "long alts, short BTC/ETH beta" },
    { candidate_id: "partial_hedge", template_id: "partial_hedge_overlay", select_top: 3, weighting: "equal", rebalance_trigger: "periodic_30d", rationale: "long book with a fixed short hedge" },
    { candidate_id: "pair_trade", template_id: "relative_value_pair_trade", select_top: 3, weighting: "equal", rationale: "long top name, short runner-up" },
  ],
};

const CASES: EvalCase[] = [
  // --- baseline: the long-only paths that existed pre-migration ------------
  {
    group: "baseline",
    name: "baseline=lax_passes",
    input: { run_id: "eval-rv-baseline", thesis: thesis(), universe: UNIVERSE, window: WINDOW, proposal: BASELINE },
    expect: { min_passing: 1, must_pass: ["bh_equal"] },
  },

  // --- momentum: signal-driven rotation, unlocked by the bt migration ------
  {
    group: "momentum",
    name: "momentum=lax_passes",
    input: { run_id: "eval-rv-momentum", thesis: thesis(), universe: UNIVERSE, window: WINDOW, proposal: MOMENTUM },
    expect: { min_passing: 1, must_pass: ["momentum"] },
  },

  // --- trend: SMA long/cash, unlocked by the bt migration ------------------
  {
    group: "trend",
    name: "trend=lax_passes",
    input: { run_id: "eval-rv-trend", thesis: thesis(), universe: UNIVERSE, window: WINDOW, proposal: TREND },
    expect: { min_passing: 1, must_pass: ["trend_equal"] },
  },

  // --- core_satellite: structural-slot family, unlocked by the migration ---
  {
    group: "core_satellite",
    name: "core_satellite=lax_passes",
    input: { run_id: "eval-rv-coresat", thesis: thesis(), universe: UNIVERSE, window: WINDOW, proposal: CORE_SATELLITE },
    expect: { min_passing: 1, must_pass: ["core_sat"] },
  },

  // --- shorts: a hedge book runs and (under a loose weight cap) clears ------
  {
    group: "shorts",
    name: "shorts=lax_passes",
    input: { run_id: "eval-rv-shorts-lax", thesis: thesis(), universe: UNIVERSE, window: WINDOW, proposal: SHORTS },
    expect: { min_passing: 1 },
  },
  {
    // Gross-weight accounting: max_single_weight counts a short leg at its
    // absolute size, so a tight per-asset cap flags the gross book. The long
    // BNB leg of beta_hedged sits at gross 1.0, well over 0.20.
    group: "shorts",
    name: "shorts=tight_weight_fails",
    input: {
      run_id: "eval-rv-shorts-tight",
      thesis: thesis({ constraints: { max_weight_per_asset: 0.2 } }),
      universe: UNIVERSE,
      window: WINDOW,
      proposal: SHORTS,
    },
    expect: { min_failing: 1, must_have_violation: "max_weight_per_asset" },
  },

  // --- drawdown: the thesisForValidate sign-convention adapter, live -------
  // The realised ~50% drawdown of a 2022-2026 large-cap basket sits between a
  // loose 0.7 cap and a tight 0.2 cap. The SAME candidates passing under 0.7
  // and failing under 0.2 (with a max_drawdown violation) is only possible if
  // the adapter negates the positive thesis value to match the floor check.
  {
    group: "drawdown",
    name: "drawdown=loose_passes",
    input: {
      run_id: "eval-rv-dd-loose",
      thesis: thesis({ constraints: { max_drawdown: 0.7 } }),
      universe: UNIVERSE,
      window: WINDOW,
      proposal: BASELINE,
    },
    expect: { min_passing: 1, must_pass: ["bh_equal"] },
  },
  {
    group: "drawdown",
    name: "drawdown=tight_fails",
    input: {
      run_id: "eval-rv-dd-tight",
      thesis: thesis({ constraints: { max_drawdown: 0.2 } }),
      universe: UNIVERSE,
      window: WINDOW,
      proposal: BASELINE,
    },
    expect: { min_failing: 1, must_fail: ["bh_equal"], must_have_violation: "max_drawdown" },
  },
];

type Assertion = { label: string; expected: string; actual: string; ok: boolean };

function evaluate(expect: Expect, summary: AttemptValidationSummary): Assertion[] {
  const a: Assertion[] = [];
  const push = (label: string, expected: unknown, actual: unknown, ok: boolean) =>
    a.push({ label, expected: String(expected), actual: String(actual), ok });

  const passing = summary.passing_candidate_ids;
  const failingIds = summary.failing.map((f) => f.candidate_id);

  if (expect.min_passing !== undefined) {
    push("min_passing", `>= ${expect.min_passing}`, passing.length, passing.length >= expect.min_passing);
  }
  if (expect.min_failing !== undefined) {
    push("min_failing", `>= ${expect.min_failing}`, failingIds.length, failingIds.length >= expect.min_failing);
  }
  for (const id of expect.must_pass ?? []) {
    push(`must_pass[${id}]`, "in passing", passing.includes(id) ? "passing" : `not (failing=${failingIds.includes(id)})`, passing.includes(id));
  }
  for (const id of expect.must_fail ?? []) {
    push(`must_fail[${id}]`, "in failing", failingIds.includes(id) ? "failing" : "not", failingIds.includes(id));
  }
  if (expect.must_have_violation) {
    const seen = summary.failing.some((f) =>
      f.violations.some((v) => v.constraint === expect.must_have_violation),
    );
    const names = [...new Set(summary.failing.flatMap((f) => f.violations.map((v) => v.constraint)))];
    push("must_have_violation", expect.must_have_violation, seen ? expect.must_have_violation : `[${names.join(",")}]`, seen);
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
  const cases = CASES.filter((c) => groupFilter.size === 0 || groupFilter.has(c.group));
  if (groupFilter.size > 0) {
    process.stdout.write(`group filter: ${[...groupFilter].join(", ")}\n`);
  }
  process.stdout.write(`running ${cases.length} cases\n`);

  const outcomes: CaseOutcome[] = [];

  for (const c of cases) {
    process.stdout.write(`\n${"=".repeat(72)}\n[${c.name}]\n`);
    process.stdout.write(
      `--- input ---\nthesis.constraints=${JSON.stringify(c.input.thesis.constraints)}\n` +
        `templates=${JSON.stringify(c.input.proposal.candidates.map((x) => `${x.candidate_id}:${x.template_id}`))}\n`,
    );

    const start = Date.now();
    try {
      // No logger passed: the step's default pino logger streams its own
      // enter/exit lines alongside the eval's assertions.
      const result = await runAndValidate(c.input, { timeoutSeconds: 180 });
      const duration_ms = Date.now() - start;
      const summary = result.delta.validation_summary;

      process.stdout.write(
        `--- summary (output) ---\nbatch_id=${result.delta.batch_id} passing=${JSON.stringify(summary.passing_candidate_ids)} failing=${summary.failing.length}\n`,
      );
      for (const f of summary.failing) {
        process.stdout.write(
          `    ${f.candidate_id}: ${f.violations.map((v) => `${v.constraint}(target=${v.target}, observed=${v.observed})`).join(", ")}\n`,
        );
      }

      const assertions = evaluate(c.expect, summary);
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
      const kind = error instanceof Error ? error.constructor.name : "Error";
      const message = error instanceof Error ? error.message : String(error);
      process.stdout.write(`[${c.name}] FAIL (${duration_ms} ms, ${kind})\n  ${message}\n`);
      outcomes.push({ name: c.name, group: c.group, status: "fail", duration_ms, failed: [`${kind}: ${message}`] });
    }
  }

  process.stdout.write(`\n${"=".repeat(72)}\n=== summary ===\n`);
  for (const row of outcomes) {
    const reason = row.failed.length > 0 ? ` -- ${row.failed.join("; ")}` : "";
    process.stdout.write(
      `  ${row.status.toUpperCase().padEnd(4)} ${row.name.padEnd(30)} ${row.duration_ms} ms${reason}\n`,
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
