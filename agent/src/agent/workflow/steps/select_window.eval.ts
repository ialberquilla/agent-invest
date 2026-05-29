// Eval runner for select_window. Calls the real Python
// recommend_backtest_window through the same path as the workflow.
//
// Run with:  pnpm eval:select-window

import "../../../env.ts";

import { selectWindow } from "./select_window.ts";
import type {
  SelectWindowInput,
  Thesis,
  Universe,
  Window,
} from "../state.ts";

type Expect = {
  min_window_length_days?: number;
  max_window_length_days?: number;
  start_after?: string;        // YYYY-MM-DD
  end_after?: string;          // YYYY-MM-DD
  min_drawdowns?: number;
};

type EvalCase = {
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

const CASES: EvalCase[] = [
  {
    name: "btc_eth_bnb_365d",
    input: {
      run_id: "eval-select-window-btc_eth_bnb_365d",
      thesis: baseThesis(),
      universe: universe(["bitcoin", "ethereum", "binancecoin"]),
    },
    expect: {
      min_window_length_days: 365,
      end_after: "2026-01-01",
      min_drawdowns: 1,
    },
  },
  {
    name: "btc_eth_only_730d",
    input: {
      run_id: "eval-select-window-btc_eth_only_730d",
      thesis: baseThesis({ horizon_days: 730 }),
      universe: universe(["bitcoin", "ethereum"]),
    },
    expect: {
      min_window_length_days: 1000,
      end_after: "2026-01-01",
    },
  },
  {
    name: "narrow_recent_universe",
    input: {
      run_id: "eval-select-window-narrow_recent_universe",
      thesis: baseThesis({ horizon_days: 180 }),
      // A coin with limited history will clamp the window.
      universe: universe(["bitcoin", "ethereum", "hyperliquid"]),
    },
    expect: {
      end_after: "2026-01-01",
      // Lower bound: hyperliquid limits, but should still produce >= 180 days.
      min_window_length_days: 180,
    },
  },
];

type CaseOutcome = {
  name: string;
  status: "pass" | "fail";
  duration_ms: number;
  violations: string[];
  window?: Window;
};

async function main(): Promise<number> {
  const outcomes: CaseOutcome[] = [];

  for (const c of CASES) {
    process.stdout.write(`\n[${c.name}] running...\n`);
    const start = Date.now();
    try {
      const result = await selectWindow(c.input);
      const violations = checkExpect(c.expect, result.delta.window);
      const duration_ms = Date.now() - start;
      if (violations.length === 0) {
        process.stdout.write(`[${c.name}] PASS (${duration_ms} ms)\n`);
      } else {
        process.stdout.write(`[${c.name}] FAIL (${duration_ms} ms)\n`);
        for (const v of violations) process.stdout.write(`  - ${v}\n`);
      }
      process.stdout.write(
        `  window: start=${result.delta.window.start} end=${result.delta.window.end} length_days=${result.delta.window.effective.window_length_days} limiting=${result.delta.window.effective.limiting_coin ?? "(none)"} drawdowns=${result.delta.window.effective.covered_drawdowns_count}\n`,
      );
      outcomes.push({
        name: c.name,
        status: violations.length === 0 ? "pass" : "fail",
        duration_ms,
        violations,
        window: result.delta.window,
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
      `  ${row.status.toUpperCase().padEnd(4)} ${row.name.padEnd(32)} ${row.duration_ms} ms${reason}\n`,
    );
  }
  const failed = outcomes.filter((row) => row.status === "fail").length;
  process.stdout.write(
    `\n${outcomes.length - failed}/${outcomes.length} passed\n`,
  );
  return failed > 0 ? 1 : 0;
}

function checkExpect(expect: Expect, window: Window): string[] {
  const out: string[] = [];
  const length = window.effective.window_length_days;
  if (
    expect.min_window_length_days !== undefined &&
    length < expect.min_window_length_days
  ) {
    out.push(
      `expected window_length_days >= ${expect.min_window_length_days} got ${length}`,
    );
  }
  if (
    expect.max_window_length_days !== undefined &&
    length > expect.max_window_length_days
  ) {
    out.push(
      `expected window_length_days <= ${expect.max_window_length_days} got ${length}`,
    );
  }
  if (expect.start_after && window.start < expect.start_after) {
    out.push(`expected start >= ${expect.start_after} got ${window.start}`);
  }
  if (expect.end_after && window.end < expect.end_after) {
    out.push(`expected end >= ${expect.end_after} got ${window.end}`);
  }
  if (
    expect.min_drawdowns !== undefined &&
    window.effective.covered_drawdowns_count < expect.min_drawdowns
  ) {
    out.push(
      `expected covered_drawdowns_count >= ${expect.min_drawdowns} got ${window.effective.covered_drawdowns_count}`,
    );
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
