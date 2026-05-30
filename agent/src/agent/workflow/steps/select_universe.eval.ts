// Eval runner for the select_universe step.
//
// select_universe is DETERMINISTIC -- it builds a rank_universe request
// from the thesis (+ any broaden_universe UniverseHint) and calls the
// real Python rank_universe CLI, which reads Postgres. Its pure logic
// (request building, top_skip slicing, hint loosening, EmptyUniverseError,
// risk_profile mapping) is already covered hermetically by
// test/workflow-select-universe.test.ts. This eval's distinct job is the
// LIVE INTEGRATION: run the real CLI against real market data and assert
// the resolved universe + effective_filters across the branches that only
// a live run exercises.
//
// Requires Postgres to be up and ranked (the workflow DB). If the CLI
// can't reach data, cases error out (reported as FAIL with the message).
//
// Run all:        pnpm eval:select-universe
// Run one group:  pnpm eval:select-universe skip broaden
//   (group names: topn skip objective marketcap exclusions handpicked
//    broaden)

import "../../../env.ts";

import type {
  SelectUniverseInput,
  Thesis,
  Universe,
  UniverseHint,
} from "../state.ts";
import {
  buildRankUniverseRequest,
  selectUniverse,
} from "./select_universe.ts";

type Expect = {
  source?: Universe["source"];
  exact_size?: number;
  min_size?: number;
  max_size?: number;
  contains?: string[];
  excludes?: string[];
  // effective_filters assertions (only a live run produces these):
  top_skip?: number;
  risk_profile?: string | null; // null => must be undefined
  market_cap_min_usd?: number;
  market_cap_min_usd_absent?: boolean;
  exclude_stablecoins?: boolean;
  exclude_wrapped?: boolean;
  dropped_filters?: string[];
};

type EvalCase = {
  group: string;
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

// Convenience: a thesis whose only change is universe_hints, merged onto
// the baseline hints so each case states just what it varies.
function withHints(hints: Partial<Thesis["universe_hints"]>, rest: Partial<Thesis> = {}): Thesis {
  const base = thesis(rest);
  return {
    ...base,
    universe_hints: { ...base.universe_hints, ...hints },
  };
}

const CASES: EvalCase[] = [
  // --- topn: universe size tracks top_n; broad balanced sets hold BTC/ETH ---
  {
    group: "topn",
    name: "topn=5",
    input: { run_id: "eval-su-topn5", thesis: withHints({ top_n: 5 }) },
    expect: { source: "rank_universe", exact_size: 5, contains: ["bitcoin", "ethereum"] },
  },
  {
    group: "topn",
    name: "topn=10",
    input: { run_id: "eval-su-topn10", thesis: withHints({ top_n: 10 }) },
    expect: { source: "rank_universe", exact_size: 10, contains: ["bitcoin", "ethereum"] },
  },
  {
    group: "topn",
    name: "topn=25",
    input: { run_id: "eval-su-topn25", thesis: withHints({ top_n: 25 }) },
    expect: { source: "rank_universe", exact_size: 25, contains: ["bitcoin", "ethereum"] },
  },

  // --- skip: top_skip drops the leading market-cap ranks ---
  {
    group: "skip",
    name: "skip=5",
    input: {
      run_id: "eval-su-skip5",
      thesis: withHints({ top_n: 10, top_skip: 5 }),
    },
    expect: {
      source: "rank_universe",
      exact_size: 10,
      excludes: ["bitcoin", "ethereum"],
      top_skip: 5,
    },
  },

  // --- objective -> risk_profile mapping (and the CLI runs under each) ---
  {
    group: "objective",
    name: "objective=balanced_growth",
    input: { run_id: "eval-su-balanced", thesis: thesis({ objective: "balanced_growth" }) },
    // balanced_growth maps to the default sort (no risk_profile).
    expect: { exact_size: 10, risk_profile: null, contains: ["bitcoin", "ethereum"] },
  },
  {
    group: "objective",
    name: "objective=growth",
    input: { run_id: "eval-su-growth", thesis: thesis({ objective: "growth" }) },
    // growth -> aggressive (momentum-ranked): BTC/ETH need NOT be present.
    expect: { exact_size: 10, risk_profile: "aggressive" },
  },
  {
    group: "objective",
    name: "objective=preserve_capital",
    input: { run_id: "eval-su-preserve", thesis: thesis({ objective: "preserve_capital" }) },
    expect: { exact_size: 10, risk_profile: "preserve" },
  },
  {
    group: "objective",
    name: "objective=income",
    input: { run_id: "eval-su-income", thesis: thesis({ objective: "income" }) },
    expect: { exact_size: 10, risk_profile: "preserve" },
  },

  // --- marketcap: a high floor keeps only large caps but still holds BTC ---
  {
    group: "marketcap",
    name: "marketcap=10b-floor",
    input: {
      run_id: "eval-su-mcap10b",
      thesis: withHints({ top_n: 25, market_cap_min_usd: 10_000_000_000 }),
    },
    expect: {
      source: "rank_universe",
      min_size: 1,
      contains: ["bitcoin"],
      market_cap_min_usd: 10_000_000_000,
    },
  },

  // --- exclusions: stablecoin filter both directions (deterministic) ---
  {
    group: "exclusions",
    name: "exclusions=stables-excluded",
    input: {
      run_id: "eval-su-excl-on",
      thesis: withHints({ top_n: 25, exclude_stablecoins: true, exclude_wrapped: true }),
    },
    expect: {
      exact_size: 25,
      excludes: ["tether", "usd-coin"],
      exclude_stablecoins: true,
      exclude_wrapped: true,
    },
  },
  {
    group: "exclusions",
    name: "exclusions=stables-included",
    input: {
      run_id: "eval-su-excl-off",
      thesis: withHints({ top_n: 25, exclude_stablecoins: false, exclude_wrapped: false }),
    },
    expect: {
      exact_size: 25,
      contains: ["tether"],
      exclude_stablecoins: false,
      exclude_wrapped: false,
    },
  },

  // --- handpicked: short-circuit, verbatim ids, no CLI call ---
  {
    group: "handpicked",
    name: "handpicked=btc-eth-sol",
    input: {
      run_id: "eval-su-handpicked",
      thesis: withHints(
        { top_n: 3, hand_picked_coin_ids: ["bitcoin", "ethereum", "solana"] },
        {
          constraints: {
            max_weight_per_asset: 0.5,
            max_cash_weight: 0,
            max_drawdown: 0.3,
            asset_count_min: 3,
            asset_count_max: 3,
          },
        },
      ),
    },
    expect: {
      source: "hand_picked",
      exact_size: 3,
      contains: ["bitcoin", "ethereum", "solana"],
    },
  },

  // --- broaden: UniverseHint loosening (broaden_universe backward edge) ---
  {
    group: "broaden",
    name: "broaden=raise-top-n",
    input: {
      run_id: "eval-su-broaden-topn",
      thesis: withHints({ top_n: 5 }),
      hint: {
        reason: "too_narrow_after_filters",
        loosen: { raise_top_n_to: 20 },
        rationale: "too few names after filters",
      } satisfies UniverseHint,
    },
    expect: { source: "rank_universe", exact_size: 20 },
  },
  {
    group: "broaden",
    name: "broaden=drop-stablecoin-filter",
    input: {
      run_id: "eval-su-broaden-drop",
      thesis: withHints({ top_n: 25, exclude_stablecoins: true }),
      hint: {
        reason: "too_narrow_after_filters",
        loosen: { drop_filter: ["exclude_stablecoins"] },
        rationale: "drop the stablecoin exclusion to widen the set",
      } satisfies UniverseHint,
    },
    expect: {
      exact_size: 25,
      contains: ["tether"],
      exclude_stablecoins: false,
      dropped_filters: ["exclude_stablecoins"],
    },
  },
  {
    group: "broaden",
    name: "broaden=lower-floor-to-zero",
    input: {
      run_id: "eval-su-broaden-floor",
      thesis: withHints({ top_n: 20, market_cap_min_usd: 1_000_000_000_000 }),
      hint: {
        reason: "too_narrow_after_filters",
        loosen: { raise_top_n_to: 20, lower_market_cap_floor_to: 0 },
        rationale: "thesis floor was unsatisfiably high",
      } satisfies UniverseHint,
    },
    // floor of 0 means "no floor" -> effective market_cap_min_usd absent.
    expect: { source: "rank_universe", exact_size: 20, market_cap_min_usd_absent: true },
  },
];

type Assertion = { label: string; expected: string; actual: string; ok: boolean };

function evaluate(expect: Expect, u: Universe): Assertion[] {
  const a: Assertion[] = [];
  const push = (label: string, expected: unknown, actual: unknown, ok: boolean) =>
    a.push({ label, expected: String(expected), actual: String(actual), ok });
  const ids = new Set(u.coin_ids);
  const ef = u.effective_filters;

  if (expect.source !== undefined) {
    push("source", expect.source, u.source, u.source === expect.source);
  }
  if (expect.exact_size !== undefined) {
    push("exact_size", expect.exact_size, u.coin_ids.length, u.coin_ids.length === expect.exact_size);
  }
  if (expect.min_size !== undefined) {
    push("min_size", `>= ${expect.min_size}`, u.coin_ids.length, u.coin_ids.length >= expect.min_size);
  }
  if (expect.max_size !== undefined) {
    push("max_size", `<= ${expect.max_size}`, u.coin_ids.length, u.coin_ids.length <= expect.max_size);
  }
  if (expect.contains) {
    for (const id of expect.contains) push(`contains:${id}`, "present", ids.has(id) ? "present" : "absent", ids.has(id));
  }
  if (expect.excludes) {
    for (const id of expect.excludes) push(`excludes:${id}`, "absent", ids.has(id) ? "present" : "absent", !ids.has(id));
  }
  if (expect.top_skip !== undefined) {
    push("top_skip", expect.top_skip, ef.top_skip ?? "unset", ef.top_skip === expect.top_skip);
  }
  if (expect.risk_profile !== undefined) {
    if (expect.risk_profile === null) {
      push("risk_profile", "unset", ef.risk_profile ?? "unset", ef.risk_profile === undefined);
    } else {
      push("risk_profile", expect.risk_profile, ef.risk_profile ?? "unset", ef.risk_profile === expect.risk_profile);
    }
  }
  if (expect.market_cap_min_usd !== undefined) {
    push("market_cap_min_usd", expect.market_cap_min_usd, ef.market_cap_min_usd ?? "unset", ef.market_cap_min_usd === expect.market_cap_min_usd);
  }
  if (expect.market_cap_min_usd_absent) {
    push("market_cap_min_usd_absent", "unset", ef.market_cap_min_usd ?? "unset", ef.market_cap_min_usd === undefined);
  }
  if (expect.exclude_stablecoins !== undefined) {
    push("exclude_stablecoins", expect.exclude_stablecoins, ef.exclude_stablecoins, ef.exclude_stablecoins === expect.exclude_stablecoins);
  }
  if (expect.exclude_wrapped !== undefined) {
    push("exclude_wrapped", expect.exclude_wrapped, ef.exclude_wrapped, ef.exclude_wrapped === expect.exclude_wrapped);
  }
  if (expect.dropped_filters) {
    const got = ef.dropped_filters ?? [];
    const ok = expect.dropped_filters.every((f) => got.includes(f as never));
    push("dropped_filters", `includes [${expect.dropped_filters.join(", ")}]`, `[${got.join(", ")}]`, ok);
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
    const hints = c.input.thesis.universe_hints;
    process.stdout.write(
      `--- input ---\nobjective=${c.input.thesis.objective} hints=${JSON.stringify(hints)}${c.input.hint ? ` hint=${JSON.stringify(c.input.hint)}` : ""}\n`,
    );
    // Show the rank_universe request the step would build (skipped for
    // the hand_picked short-circuit, which never calls the CLI).
    if (!hints.hand_picked_coin_ids?.length) {
      process.stdout.write(
        `--- rank_universe request (what the step sends) ---\n${JSON.stringify(buildRankUniverseRequest(c.input))}\n`,
      );
    }

    const start = Date.now();
    try {
      // No logger passed: the step's default pino logger streams its own
      // enter/exit lines, so the STEP's behavior and the EVAL's assertions
      // show up together.
      const result = await selectUniverse(c.input);
      const duration_ms = Date.now() - start;
      const u = result.delta.universe;

      process.stdout.write(
        `--- universe (output) ---\nsource=${u.source} size=${u.coin_ids.length}\neffective_filters=${JSON.stringify(u.effective_filters)}\ncoin_ids=${JSON.stringify(u.coin_ids)}\n`,
      );

      const assertions = evaluate(c.expect, u);
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
      `  ${row.status.toUpperCase().padEnd(4)} ${row.name.padEnd(36)} ${row.duration_ms} ms${reason}\n`,
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
