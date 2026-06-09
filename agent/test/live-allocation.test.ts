import assert from "node:assert/strict";
import test from "node:test";

import type { ExecFileLike } from "../src/agent/workflow/cli.ts";
import {
  computeMandateAllocation,
  mandateToLiveAllocationRequest,
} from "../src/agent/keeper/live-allocation.ts";
import type { StrategyMandate } from "../src/agent/workflow/mandate.ts";

function makeMandate(overrides: Partial<StrategyMandate> = {}): StrategyMandate {
  return {
    mandate_id: "m1",
    run_id: "run-1",
    version: 1,
    created_at: "2026-06-01T00:00:00.000Z",
    template_id: "relative_momentum_rotation",
    select_top: 5,
    weighting: "equal",
    rebalance_trigger: "periodic_30d",
    objective: "growth",
    rebalance_frequency: "monthly",
    horizon_days: 365,
    universe_hints: { top_n: 10, exclude_stablecoins: true, exclude_wrapped: true },
    coin_ids: ["bitcoin", "ethereum", "solana"],
    dynamic_universe: true,
    constraints: {
      max_weight_per_asset: 0.3,
      max_cash_weight: 0.2,
      max_drawdown: 0.4,
    },
    allowed_sides: "long_only",
    initial_target_allocation: [],
    status: "pending",
    ...overrides,
  };
}

test("mandateToLiveAllocationRequest maps params and the script objective enum", () => {
  const req = mandateToLiveAllocationRequest(makeMandate(), { asOf: "2026-06-01" });
  assert.equal(req.template_id, "relative_momentum_rotation");
  assert.equal(req.select_top, 5);
  assert.equal(req.objective, "high_growth"); // growth -> high_growth
  assert.equal(req.horizon_days, 365);
  assert.equal(req.as_of, "2026-06-01");
  assert.deepEqual(req.coin_ids, ["bitcoin", "ethereum", "solana"]);
  assert.deepEqual(req.config, { weighting: "equal", rebalance_trigger: "periodic_30d" });
});

test("mandateToLiveAllocationRequest forwards structural slots only when set", () => {
  const req = mandateToLiveAllocationRequest(
    makeMandate({
      template_id: "barbell_allocation",
      core_weight: 0.6,
      sleeve_cap: 0.1,
      rebalance_trigger: undefined,
    }),
  );
  assert.deepEqual(req.config, {
    weighting: "equal",
    core_weight: 0.6,
    sleeve_cap: 0.1,
  });
  assert.equal(req.as_of, undefined);
});

test("computeMandateAllocation invokes the script and parses the allocation", async () => {
  const calls: string[][] = [];
  const fakeExecFile: ExecFileLike = (_file, args, _options, callback) => {
    calls.push(args);
    const payload = {
      as_of: "2026-06-01",
      rebalance_date: "2026-05-28",
      weights: [
        { coin_id: "bitcoin", weight: 0.6, side: "long" },
        { coin_id: "ethereum", weight: 0.4, side: "long" },
      ],
      net_weight: 1.0,
      gross_weight: 1.0,
      cash_weight: 0.0,
      template_id: "relative_momentum_rotation",
      coin_ids: ["bitcoin", "ethereum", "solana"],
    };
    callback(null, JSON.stringify(payload), "");
  };

  // computeMandateAllocation does not expose execFile injection, so exercise
  // the wrapper directly through the request mapper + computeLiveAllocation.
  const { computeLiveAllocation } = await import("../src/agent/workflow/cli.ts");
  const req = mandateToLiveAllocationRequest(makeMandate());
  const result = await computeLiveAllocation(req, { execFile: fakeExecFile });

  assert.equal(result.rebalance_date, "2026-05-28");
  assert.equal(result.weights.length, 2);
  assert.equal(result.weights[0]?.coin_id, "bitcoin");
  // the script module + --input were passed through
  const flat = calls[0]?.join(" ") ?? "";
  assert.match(flat, /compute_live_allocation/);
  assert.match(flat, /relative_momentum_rotation/);
});

test("computeLiveAllocation surfaces a structured script error", async () => {
  const fakeExecFile: ExecFileLike = (_file, _args, _options, callback) => {
    callback(null, JSON.stringify({ error: { type: "ValueError", message: "boom" } }), "");
  };
  const { computeLiveAllocation } = await import("../src/agent/workflow/cli.ts");
  await assert.rejects(
    () =>
      computeLiveAllocation(mandateToLiveAllocationRequest(makeMandate()), {
        execFile: fakeExecFile,
      }),
    /compute_live_allocation failed: ValueError boom/,
  );
});
