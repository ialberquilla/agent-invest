// Eval runner for the select_window step.
//
// select_window is DETERMINISTIC -- it forwards the universe coin set +
// thesis horizon to the real Python recommend_backtest_window CLI (which
// reads price history from Postgres) and maps the response into a Window.
// Its pure mapping logic is already covered hermetically by
// test/workflow-select-window.test.ts. This eval's distinct job is the
// LIVE INTEGRATION: run the real recommender against real history and
// assert the resolved window + diagnostics across the branches that only
// a live run exercises.
//
// The recommender targets target_window_length_days = max(2*horizon, 1460)
// and:
//  - returns a window of that length when enough common history exists
//    (window_length == target), or
//  - CLAMPS to the common-history intersection when a short-history coin
//    (the limiting_coin) can't span the target -- which can drop the
//    realised window below the horizon (the signal decide acts on).
//
// Requires Postgres up with ingested price history.
//
// Run all:        pnpm eval:select-window
// Run one group:  pnpm eval:select-window horizon clamp
//   (group names: horizon clamp below_horizon single)

import "../../../env.ts";

import type {
  SelectWindowInput,
  Thesis,
  Universe,
  Window,
} from "../state.ts";
import { selectWindow } from "./select_window.ts";

type Expect = {
  // target_window_length_days is a pure function of horizon -- exact.
  target_window_length_days?: number;
  min_window_length_days?: number;
  max_window_length_days?: number;
  // true  => realised window must be BELOW the horizon (clamp signal)
  // false => realised window must be >= the horizon (healthy)
  window_below_horizon?: boolean;
  limiting_coin?: string;
  intersection_start_present?: boolean;
  min_drawdowns?: number;
  end_after?: string; // YYYY-MM-DD
};

type EvalCase = {
  group: string;
  name: string;
  input: SelectWindowInput;
  expect: Expect;
};

function baseThesis(overrides: Partial<Thesis> = {}): Thesis {
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

// max(2*horizon, 1460) -- the recommender's target window.
const target = (h: number) => Math.max(2 * h, 1460);

// Deep, long-history universe: BTC/ETH span enough history that the
// window reaches the target for the horizons swept here.
const LONG = ["bitcoin", "ethereum"];

function horizonCase(h: number): EvalCase {
  return {
    group: "horizon",
    name: `horizon=${h}`,
    input: {
      run_id: `eval-sw-h${h}`,
      thesis: baseThesis({ horizon_days: h }),
      universe: universe(LONG),
    },
    expect: {
      target_window_length_days: target(h),
      // Enough history => window reaches the target and never dips below
      // the horizon.
      min_window_length_days: h,
      max_window_length_days: target(h),
      window_below_horizon: false,
      end_after: "2026-01-01",
      min_drawdowns: 1,
    },
  };
}

const CASES: EvalCase[] = [
  // --- horizon: target = max(2*horizon, 1460); window reaches it ---
  horizonCase(180),
  horizonCase(365),
  horizonCase(730),
  horizonCase(1000),

  // --- clamp: a short-history coin limits the common-history window ---
  {
    group: "clamp",
    name: "clamp=hyperliquid-limits",
    input: {
      run_id: "eval-sw-clamp",
      thesis: baseThesis({ horizon_days: 180 }),
      universe: universe(["bitcoin", "ethereum", "hyperliquid"]),
    },
    expect: {
      // hyperliquid (recent listing) clamps the intersection below the
      // 1460 target, but still clears the 180 horizon.
      limiting_coin: "hyperliquid",
      max_window_length_days: target(180) - 1,
      min_window_length_days: 180,
      window_below_horizon: false,
      intersection_start_present: true,
      end_after: "2026-01-01",
    },
  },
  {
    group: "clamp",
    name: "clamp=long-universe-no-clamp",
    input: {
      run_id: "eval-sw-noclamp",
      thesis: baseThesis({ horizon_days: 365 }),
      universe: universe(LONG),
    },
    expect: {
      // Deep history -> window hits the full 1460 target and spans
      // multiple drawdowns.
      target_window_length_days: 1460,
      min_window_length_days: 1460,
      min_drawdowns: 2,
    },
  },

  // --- below_horizon: short history + long horizon -> window < horizon ---
  {
    group: "below_horizon",
    name: "below_horizon=hyperliquid-730d",
    input: {
      run_id: "eval-sw-below",
      thesis: baseThesis({ horizon_days: 730 }),
      universe: universe(["bitcoin", "ethereum", "hyperliquid"]),
    },
    expect: {
      // ~445 days of common history can't span a 730-day horizon: the
      // realised window comes back BELOW horizon (decide's broaden signal).
      window_below_horizon: true,
      limiting_coin: "hyperliquid",
      intersection_start_present: true,
    },
  },

  // --- single coin: still targets the 1460 floor on deep history ---
  {
    group: "single",
    name: "single=bitcoin-365d",
    input: {
      run_id: "eval-sw-single",
      thesis: baseThesis({ horizon_days: 365 }),
      universe: universe(["bitcoin"]),
    },
    expect: {
      target_window_length_days: 1460,
      min_window_length_days: 365,
      window_below_horizon: false,
      min_drawdowns: 1,
    },
  },
];

type Assertion = { label: string; expected: string; actual: string; ok: boolean };

function evaluate(expect: Expect, w: Window): Assertion[] {
  const a: Assertion[] = [];
  const push = (label: string, expected: unknown, actual: unknown, ok: boolean) =>
    a.push({ label, expected: String(expected), actual: String(actual), ok });
  const len = w.effective.window_length_days;
  const ef = w.effective;

  if (expect.target_window_length_days !== undefined) {
    push("target_window_length_days", expect.target_window_length_days, ef.target_window_length_days, ef.target_window_length_days === expect.target_window_length_days);
  }
  if (expect.min_window_length_days !== undefined) {
    push("min_window_length_days", `>= ${expect.min_window_length_days}`, len, len >= expect.min_window_length_days);
  }
  if (expect.max_window_length_days !== undefined) {
    push("max_window_length_days", `<= ${expect.max_window_length_days}`, len, len <= expect.max_window_length_days);
  }
  if (expect.window_below_horizon !== undefined) {
    const below = len < w.horizon_days;
    push(
      "window_below_horizon",
      expect.window_below_horizon,
      `${below} (len ${len} vs horizon ${w.horizon_days})`,
      below === expect.window_below_horizon,
    );
  }
  if (expect.limiting_coin !== undefined) {
    push("limiting_coin", expect.limiting_coin, ef.limiting_coin ?? "none", ef.limiting_coin === expect.limiting_coin);
  }
  if (expect.intersection_start_present) {
    push("intersection_start_present", "present", ef.intersection_start ?? "absent", Boolean(ef.intersection_start));
  }
  if (expect.min_drawdowns !== undefined) {
    push("min_drawdowns", `>= ${expect.min_drawdowns}`, ef.covered_drawdowns_count, ef.covered_drawdowns_count >= expect.min_drawdowns);
  }
  if (expect.end_after) {
    push("end_after", `>= ${expect.end_after}`, w.end, w.end >= expect.end_after);
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
      `--- input ---\nhorizon_days=${c.input.thesis.horizon_days} coin_ids=${JSON.stringify(c.input.universe.coin_ids)}\n`,
    );

    const start = Date.now();
    try {
      // No logger passed: the step's default pino logger streams its own
      // enter/exit (and capHit on below-horizon) lines, so the STEP's
      // behavior and the EVAL's assertions show up together.
      const result = await selectWindow(c.input);
      const duration_ms = Date.now() - start;
      const w = result.delta.window;

      process.stdout.write(
        `--- window (output) ---\nstart=${w.start} end=${w.end} horizon=${w.horizon_days}\neffective=${JSON.stringify(w.effective)}\n`,
      );

      const assertions = evaluate(c.expect, w);
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
      `  ${row.status.toUpperCase().padEnd(4)} ${row.name.padEnd(34)} ${row.duration_ms} ms${reason}\n`,
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
