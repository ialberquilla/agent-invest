import assert from "node:assert/strict";
import test from "node:test";

import type {
  RecommendWindowRequest,
  RecommendWindowResponse,
} from "../src/agent/workflow/cli.ts";
import { createSilentStepLogger } from "../src/agent/workflow/logging.ts";
import {
  selectWindow,
  type SelectWindowDeps,
} from "../src/agent/workflow/steps/select_window.ts";
import type {
  Thesis,
  Universe,
} from "../src/agent/workflow/state.ts";

const BASE_THESIS: Thesis = {
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
  interpretation_notes: "fixture",
};

const BASE_UNIVERSE: Universe = {
  coin_ids: ["bitcoin", "ethereum", "binancecoin"],
  source: "rank_universe",
  effective_filters: {
    top_n: 3,
    exclude_stablecoins: true,
    exclude_wrapped: true,
  },
};

function fakeRecommend(response: RecommendWindowResponse) {
  const calls: RecommendWindowRequest[] = [];
  const fn = async (req: RecommendWindowRequest) => {
    calls.push(req);
    return response;
  };
  return Object.assign(fn, { calls });
}

function fakeRecommendSequence(
  responses: RecommendWindowResponse[],
) {
  const calls: RecommendWindowRequest[] = [];
  let i = 0;
  const fn = async (req: RecommendWindowRequest) => {
    calls.push(req);
    const response = responses[i] ?? responses[responses.length - 1];
    i += 1;
    if (!response) throw new Error("missing fake response");
    return response;
  };
  return Object.assign(fn, { calls });
}

function deps(
  recommendWindow: ReturnType<typeof fakeRecommend>,
): SelectWindowDeps {
  return { recommendWindow, logger: createSilentStepLogger() };
}

test("selectWindow forwards coin_ids and horizon_days to the CLI", async () => {
  const recommendWindow = fakeRecommend({
    start: "2022-05-25",
    end: "2026-05-24",
    rationale: "ok",
    covered_drawdowns: [],
    history_constraints: {
      intersection_start: "2021-10-25",
      intersection_end: "2026-05-24",
      target_window_length_days: 1460,
      window_length_days: 1460,
      limiting_coin: "bitcoin",
    },
  });

  await selectWindow(
    {
      run_id: "test-basic",
      thesis: BASE_THESIS,
      universe: BASE_UNIVERSE,
    },
    deps(recommendWindow),
  );

  assert.deepEqual(recommendWindow.calls[0], {
    coin_ids: ["bitcoin", "ethereum", "binancecoin"],
    horizon_days: 365,
    benchmark_objective: "balanced",
  });
});

test("selectWindow returns a Window with thesis horizon and CLI dates", async () => {
  const recommendWindow = fakeRecommend({
    start: "2022-05-25",
    end: "2026-05-24",
    rationale: "covered 1 drawdown",
    covered_drawdowns: [
      { asset: "bitcoin", drawdown_pct: -0.5 },
      { asset: "bitcoin", drawdown_pct: -0.4 },
    ],
    history_constraints: {
      intersection_start: "2021-10-25",
      intersection_end: "2026-05-24",
      target_window_length_days: 1460,
      window_length_days: 1460,
      limiting_coin: "bitcoin",
    },
  });

  const result = await selectWindow(
    {
      run_id: "test-shape",
      thesis: BASE_THESIS,
      universe: BASE_UNIVERSE,
    },
    deps(recommendWindow),
  );

  assert.equal(result.next, "propose_candidates");
  assert.equal(result.delta.window.start, "2022-05-25");
  assert.equal(result.delta.window.end, "2026-05-24");
  assert.equal(result.delta.window.horizon_days, 365);
  assert.equal(result.delta.window.effective.window_length_days, 1460);
  assert.equal(
    result.delta.window.effective.target_window_length_days,
    1460,
  );
  assert.equal(result.delta.window.effective.limiting_coin, "bitcoin");
  assert.equal(result.delta.window.effective.covered_drawdowns_count, 2);
});

test("selectWindow tolerates missing history_constraints", async () => {
  const recommendWindow = fakeRecommend({
    start: "2025-01-01",
    end: "2026-01-01",
    rationale: "fallback",
    covered_drawdowns: [],
  });

  const result = await selectWindow(
    {
      run_id: "test-no-constraints",
      thesis: BASE_THESIS,
      universe: BASE_UNIVERSE,
    },
    deps(recommendWindow),
  );

  assert.equal(result.delta.window.effective.window_length_days, 365);
  assert.equal(
    result.delta.window.effective.target_window_length_days,
    730, // horizon_days * 2 fallback
  );
});

test("selectWindow propagates CLI errors", async () => {
  const recommendWindow = async () => {
    throw new Error("coin histories do not overlap");
  };

  await assert.rejects(
    () =>
      selectWindow(
        {
          run_id: "test-error",
          thesis: BASE_THESIS,
          universe: BASE_UNIVERSE,
        },
        { recommendWindow, logger: createSilentStepLogger() },
      ),
    /do not overlap/,
  );
});

test("selectWindow flags a window shorter than the thesis horizon", async () => {
  const recommendWindow = fakeRecommend({
    start: "2026-05-01",
    end: "2026-05-24",
    rationale: "narrow",
    covered_drawdowns: [],
    history_constraints: {
      intersection_start: "2026-05-01",
      intersection_end: "2026-05-24",
      target_window_length_days: 730,
      window_length_days: 23,
      limiting_coin: "tinytoken",
    },
  });
  const capHits: Array<{ cap: string; observed?: number; limit?: number }> = [];
  const logger = createSilentStepLogger();
  logger.capHit = (payload) => {
    capHits.push(payload);
  };

  const result = await selectWindow(
    {
      run_id: "test-narrow",
      thesis: BASE_THESIS,
      universe: BASE_UNIVERSE,
    },
    { recommendWindow, logger },
  );

  // Step still returns; the cap is informational so decide can pick
  // up the signal from logs/state.
  assert.equal(result.next, "propose_candidates");
  assert.equal(result.delta.window.effective.window_length_days, 23);
  assert.equal(capHits.length, 1);
  assert.equal(capHits[0]?.cap, "window_length_below_horizon");
});

test("selectWindow retries dynamic-universe windows without short-history limiting coins", async () => {
  const recommendWindow = fakeRecommendSequence([
    {
      start: "2025-01-01",
      end: "2026-05-24",
      rationale: "limited by hyperliquid",
      covered_drawdowns: [],
      history_constraints: {
        intersection_start: "2025-01-01",
        intersection_end: "2026-05-24",
        target_window_length_days: 1460,
        window_length_days: 500,
        limiting_coin: "hyperliquid",
      },
    },
    {
      start: "2022-05-25",
      end: "2026-05-24",
      rationale: "deep anchor universe",
      covered_drawdowns: [{ asset: "bitcoin", drawdown_pct: -0.5 }],
      history_constraints: {
        intersection_start: "2021-01-01",
        intersection_end: "2026-05-24",
        target_window_length_days: 1460,
        window_length_days: 1460,
        limiting_coin: "bitcoin",
      },
    },
  ]);

  const result = await selectWindow(
    {
      run_id: "test-dynamic-window",
      thesis: BASE_THESIS,
      universe: {
        ...BASE_UNIVERSE,
        coin_ids: [
          "bitcoin",
          "ethereum",
          "binancecoin",
          "solana",
          "ripple",
          "hyperliquid",
        ],
      },
      template_selection: {
        rationale: "momentum",
        selected: [
          {
            family: "relative_momentum_rotation",
            rank: 1,
            rationale: "dynamic selector",
          },
        ],
      },
    },
    deps(recommendWindow),
  );

  assert.equal(recommendWindow.calls.length, 2);
  assert.deepEqual(recommendWindow.calls[0]?.coin_ids, [
    "bitcoin",
    "ethereum",
    "binancecoin",
    "solana",
    "ripple",
    "hyperliquid",
  ]);
  assert.deepEqual(recommendWindow.calls[1]?.coin_ids, [
    "bitcoin",
    "ethereum",
    "binancecoin",
    "solana",
    "ripple",
  ]);
  assert.equal(
    result.delta.window.effective.strategy_window_mode,
    "dynamic_universe",
  );
  assert.deepEqual(result.delta.window.effective.excluded_window_coin_ids, [
    "hyperliquid",
  ]);
  assert.equal(result.delta.window.effective.window_length_days, 1460);
});

test("selectWindow keeps fixed-universe windows on the full coin intersection", async () => {
  const recommendWindow = fakeRecommend({
    start: "2025-01-01",
    end: "2026-05-24",
    rationale: "limited by hyperliquid",
    covered_drawdowns: [],
    history_constraints: {
      intersection_start: "2025-01-01",
      intersection_end: "2026-05-24",
      target_window_length_days: 1460,
      window_length_days: 500,
      limiting_coin: "hyperliquid",
    },
  });

  const result = await selectWindow(
    {
      run_id: "test-fixed-window",
      thesis: BASE_THESIS,
      universe: {
        ...BASE_UNIVERSE,
        coin_ids: ["bitcoin", "ethereum", "hyperliquid"],
      },
      template_selection: {
        rationale: "fixed",
        selected: [
          {
            family: "periodic_rebalanced_allocation",
            rank: 1,
            rationale: "fixed basket",
          },
        ],
      },
    },
    deps(recommendWindow),
  );

  assert.equal(recommendWindow.calls.length, 1);
  assert.deepEqual(recommendWindow.calls[0]?.coin_ids, [
    "bitcoin",
    "ethereum",
    "hyperliquid",
  ]);
  assert.equal(
    result.delta.window.effective.strategy_window_mode,
    "fixed_universe",
  );
  assert.equal(result.delta.window.effective.excluded_window_coin_ids, undefined);
});
