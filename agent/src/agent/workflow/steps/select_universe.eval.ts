// Eval runner for the select_universe step. Calls the real Python
// rank_universe CLI through the same code path the workflow uses and
// prints PASS/FAIL per fixture with the resolved universe.
//
// Run with:  pnpm eval:select-universe

import "../../../env.ts";

import { selectUniverse } from "./select_universe.ts";
import type {
  SelectUniverseInput,
  Thesis,
  Universe,
  UniverseHint,
} from "../state.ts";

type Expect = {
  source?: Universe["source"];
  min_size?: number;
  max_size?: number;
  contains?: string[];
  excludes?: string[];
};

type EvalCase = {
  name: string;
  input: SelectUniverseInput;
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

const CASES: EvalCase[] = [
  {
    name: "balanced_top10",
    input: {
      run_id: "eval-select-universe-balanced_top10",
      thesis: thesis(),
    },
    expect: {
      source: "rank_universe",
      min_size: 10,
      max_size: 10,
      contains: ["bitcoin", "ethereum"],
    },
  },
  {
    name: "growth_top10_skip5",
    input: {
      run_id: "eval-select-universe-growth_top10_skip5",
      thesis: thesis({
        objective: "growth",
        universe_hints: {
          top_n: 10,
          top_skip: 5,
          exclude_stablecoins: true,
          exclude_wrapped: true,
        },
      }),
    },
    expect: {
      source: "rank_universe",
      min_size: 10,
      max_size: 10,
      excludes: ["bitcoin", "ethereum"],
    },
  },
  {
    name: "hand_picked_btc_eth",
    input: {
      run_id: "eval-select-universe-hand_picked_btc_eth",
      thesis: thesis({
        objective: "preserve_capital",
        universe_hints: {
          top_n: 2,
          exclude_stablecoins: true,
          exclude_wrapped: true,
          hand_picked_coin_ids: ["bitcoin", "ethereum"],
        },
        constraints: {
          max_weight_per_asset: 0.5,
          max_cash_weight: 0,
          max_drawdown: 0.2,
          asset_count_min: 2,
          asset_count_max: 2,
        },
      }),
    },
    expect: {
      source: "hand_picked",
      min_size: 2,
      max_size: 2,
      contains: ["bitcoin", "ethereum"],
    },
  },
  {
    name: "broaden_universe_hint",
    input: {
      run_id: "eval-select-universe-broaden",
      thesis: thesis({
        universe_hints: {
          top_n: 5,
          exclude_stablecoins: true,
          exclude_wrapped: true,
          market_cap_min_usd: 1_000_000_000_000,
        },
      }),
      hint: {
        reason: "too_narrow_after_filters",
        loosen: {
          raise_top_n_to: 20,
          lower_market_cap_floor_to: 0,
        },
        rationale: "thesis floor was too high",
      } satisfies UniverseHint,
    },
    expect: {
      source: "rank_universe",
      min_size: 10,
      max_size: 20,
    },
  },
];

type CaseOutcome = {
  name: string;
  status: "pass" | "fail";
  duration_ms: number;
  violations: string[];
  universe?: Universe;
};

async function main(): Promise<number> {
  const outcomes: CaseOutcome[] = [];

  for (const c of CASES) {
    process.stdout.write(`\n[${c.name}] running...\n`);
    const start = Date.now();
    try {
      const result = await selectUniverse(c.input);
      const violations = checkExpect(c.expect, result.delta.universe);
      const duration_ms = Date.now() - start;
      if (violations.length === 0) {
        process.stdout.write(`[${c.name}] PASS (${duration_ms} ms)\n`);
      } else {
        process.stdout.write(`[${c.name}] FAIL (${duration_ms} ms)\n`);
        for (const v of violations) process.stdout.write(`  - ${v}\n`);
      }
      process.stdout.write(
        `  universe: source=${result.delta.universe.source} size=${result.delta.universe.coin_ids.length} ids=${JSON.stringify(result.delta.universe.coin_ids)}\n`,
      );
      outcomes.push({
        name: c.name,
        status: violations.length === 0 ? "pass" : "fail",
        duration_ms,
        violations,
        universe: result.delta.universe,
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
      `  ${row.status.toUpperCase().padEnd(4)} ${row.name.padEnd(28)} ${row.duration_ms} ms${reason}\n`,
    );
  }
  const failed = outcomes.filter((row) => row.status === "fail").length;
  process.stdout.write(
    `\n${outcomes.length - failed}/${outcomes.length} passed\n`,
  );
  return failed > 0 ? 1 : 0;
}

function checkExpect(expect: Expect, universe: Universe): string[] {
  const out: string[] = [];
  if (expect.source && universe.source !== expect.source) {
    out.push(`expected source=${expect.source} got ${universe.source}`);
  }
  if (
    expect.min_size !== undefined &&
    universe.coin_ids.length < expect.min_size
  ) {
    out.push(
      `expected universe size >= ${expect.min_size} got ${universe.coin_ids.length}`,
    );
  }
  if (
    expect.max_size !== undefined &&
    universe.coin_ids.length > expect.max_size
  ) {
    out.push(
      `expected universe size <= ${expect.max_size} got ${universe.coin_ids.length}`,
    );
  }
  if (expect.contains) {
    const set = new Set(universe.coin_ids);
    for (const id of expect.contains) {
      if (!set.has(id)) out.push(`expected universe to contain ${id}`);
    }
  }
  if (expect.excludes) {
    const set = new Set(universe.coin_ids);
    for (const id of expect.excludes) {
      if (set.has(id)) out.push(`expected universe to exclude ${id}`);
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
