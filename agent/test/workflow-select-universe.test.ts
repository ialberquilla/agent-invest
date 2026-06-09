import assert from "node:assert/strict";
import test from "node:test";

import type { RankUniverseRequest, RankUniverseRow } from "../src/agent/workflow/cli.ts";
import { createSilentStepLogger } from "../src/agent/workflow/logging.ts";
import {
  EmptyUniverseError,
  riskProfileFromObjective,
  selectUniverse,
  type SelectUniverseDeps,
} from "../src/agent/workflow/steps/select_universe.ts";
import type { Thesis } from "../src/agent/workflow/state.ts";

const BASE_THESIS: Thesis = {
  objective: "balanced_growth",
  horizon_days: 365,
  weight_mode: "percentage",
  universe_hints: {
    top_n: 25,
    market_cap_min_usd: 1_000_000_000,
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
  interpretation_notes: "fixture",
};

function row(coinId: string, rank: number): RankUniverseRow {
  return {
    coin_id: coinId,
    rank,
    market_cap_rank: rank,
    symbol: coinId.slice(0, 4).toUpperCase(),
    name: coinId,
  };
}

function fakeRank(rows: RankUniverseRow[]) {
  const calls: RankUniverseRequest[] = [];
  const fn = async (req: RankUniverseRequest) => {
    calls.push(req);
    return rows;
  };
  return Object.assign(fn, { calls });
}

function deps(
  rankUniverse: ReturnType<typeof fakeRank>,
): SelectUniverseDeps {
  return { rankUniverse, logger: createSilentStepLogger() };
}

test("selectUniverse short-circuits when hand_picked_coin_ids is set", async () => {
  const thesis: Thesis = {
    ...BASE_THESIS,
    universe_hints: {
      ...BASE_THESIS.universe_hints,
      hand_picked_coin_ids: ["bitcoin", "ethereum"],
    },
    constraints: {
      ...BASE_THESIS.constraints,
      asset_count_min: 2,
      asset_count_max: 2,
      max_weight_per_asset: 0.5,
    },
  };
  const rankUniverse = fakeRank([]);

  const result = await selectUniverse(
    { run_id: "test-hp", thesis },
    deps(rankUniverse),
  );

  assert.equal(result.next, "select_window");
  assert.equal(result.delta.universe.source, "hand_picked");
  assert.deepEqual(result.delta.universe.coin_ids, ["bitcoin", "ethereum"]);
  assert.equal(rankUniverse.calls.length, 0);
});

test("selectUniverse passes thesis filters and risk_profile to rank_universe", async () => {
  const rankUniverse = fakeRank([
    row("bitcoin", 1),
    row("ethereum", 2),
    row("binancecoin", 3),
  ]);

  const result = await selectUniverse(
    { run_id: "test-basic", thesis: BASE_THESIS },
    deps(rankUniverse),
  );

  assert.equal(rankUniverse.calls.length, 1);
  assert.deepEqual(rankUniverse.calls[0], {
    top_n: 25,
    exclude_stablecoins: true,
    exclude_wrapped: true,
    min_market_cap: 1_000_000_000,
    min_history_days: undefined,
    risk_profile: undefined,
  });
});

test("selectUniverse forwards thesis.min_history_days to rank_universe", async () => {
  const rankUniverse = fakeRank([row("bitcoin", 1), row("ethereum", 2)]);
  const thesisWithHistoryFloor: Thesis = {
    ...BASE_THESIS,
    universe_hints: {
      ...BASE_THESIS.universe_hints,
      min_history_days: 1460,
    },
  };

  await selectUniverse(
    { run_id: "test-history", thesis: thesisWithHistoryFloor },
    deps(rankUniverse),
  );

  assert.equal(rankUniverse.calls[0]?.min_history_days, 1460);
});

test("UniverseHint.lower_min_history_days_to overrides the thesis floor", async () => {
  const rankUniverse = fakeRank([row("a", 1)]);
  const thesisWithHistoryFloor: Thesis = {
    ...BASE_THESIS,
    universe_hints: {
      ...BASE_THESIS.universe_hints,
      min_history_days: 1460,
    },
  };

  await selectUniverse(
    {
      run_id: "test-history-loosen",
      thesis: thesisWithHistoryFloor,
      hint: {
        reason: "too_narrow_after_filters",
        loosen: { lower_min_history_days_to: 365 },
        rationale: "shorter history acceptable",
      },
    },
    deps(rankUniverse),
  );

  assert.equal(rankUniverse.calls[0]?.min_history_days, 365);
});

test("UniverseHint.lower_min_history_days_to set to 0 disables the filter", async () => {
  const rankUniverse = fakeRank([row("a", 1)]);
  const thesisWithHistoryFloor: Thesis = {
    ...BASE_THESIS,
    universe_hints: {
      ...BASE_THESIS.universe_hints,
      min_history_days: 1460,
    },
  };

  await selectUniverse(
    {
      run_id: "test-history-disable",
      thesis: thesisWithHistoryFloor,
      hint: {
        reason: "too_narrow_after_filters",
        loosen: { lower_min_history_days_to: 0 },
        rationale: "drop the filter",
      },
    },
    deps(rankUniverse),
  );

  assert.equal(rankUniverse.calls[0]?.min_history_days, undefined);
});

test("selectUniverse asks for top_n+top_skip and slices the leading rows", async () => {
  const thesis: Thesis = {
    ...BASE_THESIS,
    universe_hints: { ...BASE_THESIS.universe_hints, top_n: 3, top_skip: 2 },
  };
  // Returns 5 rows; we expect the first 2 sliced off, leaving ranks 3..5.
  const rankUniverse = fakeRank([
    row("a", 1),
    row("b", 2),
    row("c", 3),
    row("d", 4),
    row("e", 5),
  ]);

  const result = await selectUniverse(
    { run_id: "test-skip", thesis },
    deps(rankUniverse),
  );

  assert.equal(rankUniverse.calls[0]?.top_n, 5);
  assert.deepEqual(result.delta.universe.coin_ids, ["c", "d", "e"]);
  assert.equal(result.delta.universe.effective_filters.top_skip, 2);
});

test("UniverseHint.raise_top_n_to overrides thesis top_n", async () => {
  const rankUniverse = fakeRank([row("a", 1)]);

  await selectUniverse(
    {
      run_id: "test-raise",
      thesis: BASE_THESIS,
      hint: {
        reason: "too_narrow_after_filters",
        loosen: { raise_top_n_to: 50 },
        rationale: "broader pool needed",
      },
    },
    deps(rankUniverse),
  );

  assert.equal(rankUniverse.calls[0]?.top_n, 50);
});

test("UniverseHint.drop_filter removes the requested exclusion", async () => {
  const rankUniverse = fakeRank([row("usdt", 1)]);

  const result = await selectUniverse(
    {
      run_id: "test-drop",
      thesis: BASE_THESIS,
      hint: {
        reason: "too_narrow_after_filters",
        loosen: { drop_filter: ["exclude_stablecoins"] },
        rationale: "allow stables",
      },
    },
    deps(rankUniverse),
  );

  assert.equal(rankUniverse.calls[0]?.exclude_stablecoins, false);
  assert.equal(rankUniverse.calls[0]?.exclude_wrapped, true);
  assert.deepEqual(
    result.delta.universe.effective_filters.dropped_filters,
    ["exclude_stablecoins"],
  );
});

test("UniverseHint.lower_market_cap_floor_to overrides the thesis floor", async () => {
  const rankUniverse = fakeRank([row("a", 1)]);

  await selectUniverse(
    {
      run_id: "test-floor",
      thesis: BASE_THESIS,
      hint: {
        reason: "too_narrow_after_filters",
        loosen: { lower_market_cap_floor_to: 100_000_000 },
        rationale: "smaller floor",
      },
    },
    deps(rankUniverse),
  );

  assert.equal(rankUniverse.calls[0]?.min_market_cap, 100_000_000);
});

test("selectUniverse throws EmptyUniverseError when rank returns nothing", async () => {
  const rankUniverse = fakeRank([]);

  await assert.rejects(
    () =>
      selectUniverse(
        { run_id: "test-empty", thesis: BASE_THESIS },
        deps(rankUniverse),
      ),
    EmptyUniverseError,
  );
});

test("riskProfileFromObjective maps every objective to a working CLI value", () => {
  // rank_universe.py raises KeyError for the `balanced`, `income`,
  // and `preserve_capital` profiles; we map to working ones here.
  assert.equal(riskProfileFromObjective("balanced_growth"), undefined);
  assert.equal(riskProfileFromObjective("growth"), "aggressive");
  assert.equal(riskProfileFromObjective("income"), "preserve");
  assert.equal(riskProfileFromObjective("preserve_capital"), "preserve");
});

test("top_skip filters by market_cap_rank, not by return list order", async () => {
  // Returned in momentum order (returned `rank`) but with mixed mcap ranks.
  // top_skip=5 should drop the row with market_cap_rank=3 (BTC analogue),
  // not row 1 of the returned list.
  const rankUniverse = fakeRank([
    { coin_id: "alpha", rank: 1, market_cap_rank: 50 },
    { coin_id: "bravo", rank: 2, market_cap_rank: 3 }, // gets filtered (mcr <= 5)
    { coin_id: "charlie", rank: 3, market_cap_rank: 40 },
    { coin_id: "delta", rank: 4, market_cap_rank: 2 }, // gets filtered (mcr <= 5)
    { coin_id: "echo", rank: 5, market_cap_rank: 30 },
  ]);
  const thesis: Thesis = {
    ...BASE_THESIS,
    objective: "growth",
    universe_hints: {
      top_n: 3,
      top_skip: 5,
      exclude_stablecoins: true,
      exclude_wrapped: true,
    },
  };

  const result = await selectUniverse(
    { run_id: "test-skip-by-mcap", thesis },
    deps(rankUniverse),
  );

  assert.deepEqual(result.delta.universe.coin_ids, [
    "alpha",
    "charlie",
    "echo",
  ]);
});

test("selectUniverse records an exploration of considered vs rejected assets", async () => {
  // top_n 3, no skip: ranks 1-3 selected, 4-5 rejected as below the cutoff.
  const thesis: Thesis = {
    ...BASE_THESIS,
    universe_hints: { ...BASE_THESIS.universe_hints, top_n: 3 },
    constraints: { ...BASE_THESIS.constraints, asset_count_min: 3, asset_count_max: 3 },
  };
  const rankUniverse = fakeRank([
    row("a", 1),
    row("b", 2),
    row("c", 3),
    row("d", 4),
    row("e", 5),
  ]);

  const result = await selectUniverse(
    { run_id: "explore", thesis },
    deps(rankUniverse),
  );

  const exploration = result.delta.universe.exploration;
  assert.ok(exploration, "exploration should be populated on the rank path");
  assert.equal(exploration.considered_count, 5);
  assert.equal(exploration.selected_count, 3);
  assert.deepEqual(exploration.selected, ["a", "b", "c"]);
  assert.deepEqual(
    exploration.rejected.map((r) => r.coin_id),
    ["d", "e"],
  );
  assert.match(exploration.rejected[0]!.reason, /below the top 3/);
});

test("selectUniverse explains top_skip exclusions distinctly", async () => {
  const thesis: Thesis = {
    ...BASE_THESIS,
    universe_hints: { ...BASE_THESIS.universe_hints, top_n: 2, top_skip: 1 },
    constraints: { ...BASE_THESIS.constraints, asset_count_min: 2, asset_count_max: 2 },
  };
  const rankUniverse = fakeRank([row("a", 1), row("b", 2), row("c", 3)]);

  const result = await selectUniverse(
    { run_id: "skip", thesis },
    deps(rankUniverse),
  );

  // a (rank 1) is excluded by top_skip; b,c selected.
  assert.deepEqual(result.delta.universe.coin_ids, ["b", "c"]);
  const rejected = result.delta.universe.exploration?.rejected ?? [];
  const a = rejected.find((r) => r.coin_id === "a");
  assert.ok(a);
  assert.match(a.reason, /top_skip/);
});

test("selectUniverse omits exploration for a hand-picked universe", async () => {
  const thesis: Thesis = {
    ...BASE_THESIS,
    universe_hints: {
      ...BASE_THESIS.universe_hints,
      hand_picked_coin_ids: ["bitcoin", "ethereum"],
    },
    constraints: {
      ...BASE_THESIS.constraints,
      asset_count_min: 2,
      asset_count_max: 2,
      max_weight_per_asset: 0.5,
    },
  };
  const result = await selectUniverse(
    { run_id: "hp", thesis },
    deps(fakeRank([])),
  );
  assert.equal(result.delta.universe.exploration, undefined);
});
