import assert from "node:assert/strict";
import test from "node:test";

import {
  ALLOCATION_TEMPLATES,
  type AllocationTemplate,
  type FinalWinner,
  REBALANCE_TRIGGER_FAMILIES,
  SHORT_REQUIRING_FAMILIES,
  type StrategyFamily,
  type WorkflowState,
} from "../src/agent/workflow/state.ts";
import {
  buildMandate,
  DYNAMIC_UNIVERSE_FAMILIES,
  MANDATE_VERSION,
} from "../src/agent/workflow/mandate.ts";

// Minimal fixtures: a winner referencing a single candidate in attempt 1, plus
// the surrounding state the builder reads (attempts to locate the candidate).
function makeFixture(
  template: AllocationTemplate,
  overrides: {
    select_top?: number;
    rebalance_trigger?: "periodic_30d" | "periodic_90d" | "threshold_drift_10pct";
    core_weight?: number;
    sleeve_cap?: number;
    coin_ids?: string[];
  } = {},
): { final: FinalWinner; state: WorkflowState } {
  const candidate = {
    candidate_id: "cand-1",
    template_id: template,
    select_top: overrides.select_top ?? 5,
    weighting: "equal" as const,
    rebalance_trigger: overrides.rebalance_trigger,
    core_weight: overrides.core_weight,
    sleeve_cap: overrides.sleeve_cap,
    rationale: "test",
  };

  const thesis = {
    objective: "growth" as const,
    horizon_days: 365,
    weight_mode: "percentage" as const,
    universe_hints: {
      top_n: 10,
      exclude_stablecoins: true,
      exclude_wrapped: true,
    },
    constraints: {
      max_weight_per_asset: 0.3,
      max_cash_weight: 0.2,
      max_drawdown: 0.4,
      asset_count_min: 2,
      asset_count_max: 10,
    },
    rebalance_frequency: "monthly" as const,
    interpretation_notes: "",
  };

  const coin_ids = overrides.coin_ids ?? ["bitcoin", "ethereum", "solana"];

  const final = {
    kind: "winner",
    run_id: "run-1",
    winner_candidate_id: "cand-1",
    winner_attempt_n: 1,
    candidate_batch_id: "batch-1",
    is_best_effort: false,
    unmet_constraints: [],
    thesis,
    universe: {
      coin_ids,
      source: "rank_universe" as const,
      effective_filters: {
        top_n: 10,
        exclude_stablecoins: true,
        exclude_wrapped: true,
      },
    },
    window: {
      start: "2024-01-01",
      end: "2025-01-01",
      horizon_days: 365,
      effective: {
        window_length_days: 366,
        target_window_length_days: 365,
        rationale: "",
        covered_drawdowns_count: 1,
      },
    },
    attempts_summary: [],
    narrative: {
      title: "t",
      summary: "s",
      reasoning: "r",
      assumptions: [],
      risks: [],
      next_steps: [],
    },
    winner_backtest: {
      metrics: {
        total_return: 1,
        cagr: 0.1,
        volatility: 0.2,
        max_drawdown: -0.1,
        sharpe: 1,
        sortino: 1,
        calmar: 1,
        composite_score: null,
      },
      equity_curve: [],
      benchmark_curve: [],
      allocation: [
        { coin_id: "bitcoin", weight: 0.6 },
        { coin_id: "ethereum", weight: 0.4 },
      ],
    },
  } as unknown as FinalWinner;

  const state = {
    attempts: [
      {
        attempt_n: 1,
        proposal: {
          iteration_hypothesis: "h",
          candidates: [candidate],
        },
      },
    ],
  } as unknown as WorkflowState;

  return { final, state };
}

test("buildMandate produces a valid pending mandate for every executable family", () => {
  for (const template of ALLOCATION_TEMPLATES) {
    const needsTrigger = (
      REBALANCE_TRIGGER_FAMILIES as readonly string[]
    ).includes(template);
    const { final, state } = makeFixture(template, {
      rebalance_trigger: needsTrigger ? "periodic_30d" : undefined,
      core_weight: template.startsWith("core_satellite") ? 0.7 : undefined,
    });

    const mandate = buildMandate(final, state, {
      mandateId: `m-${template}`,
      createdAt: "2026-06-01T00:00:00.000Z",
    });

    assert.ok(mandate, `expected a mandate for ${template}`);
    assert.equal(mandate.template_id, template);
    assert.equal(mandate.status, "pending");
    assert.equal(mandate.version, MANDATE_VERSION);
    assert.equal(mandate.run_id, "run-1");
    assert.equal(mandate.mandate_id, `m-${template}`);
    assert.deepEqual(mandate.coin_ids, ["bitcoin", "ethereum", "solana"]);
    assert.equal(mandate.constraints.max_weight_per_asset, 0.3);
    assert.equal(mandate.constraints.max_cash_weight, 0.2);
    assert.equal(mandate.constraints.max_drawdown, 0.4);
    assert.equal(mandate.initial_target_allocation.length, 2);
  }
});

test("dynamic_universe flag is set only for rotation/trend families", () => {
  for (const template of ALLOCATION_TEMPLATES) {
    const { final, state } = makeFixture(template, {
      rebalance_trigger: (
        REBALANCE_TRIGGER_FAMILIES as readonly string[]
      ).includes(template)
        ? "periodic_30d"
        : undefined,
    });
    const mandate = buildMandate(final, state, { mandateId: "m" });
    assert.ok(mandate);
    assert.equal(
      mandate.dynamic_universe,
      DYNAMIC_UNIVERSE_FAMILIES.has(template),
      `dynamic_universe mismatch for ${template}`,
    );
  }
});

test("allowed_sides is long_short for short/hedge families, long_only otherwise", () => {
  for (const template of ALLOCATION_TEMPLATES) {
    const { final, state } = makeFixture(template, {
      rebalance_trigger: (
        REBALANCE_TRIGGER_FAMILIES as readonly string[]
      ).includes(template)
        ? "periodic_30d"
        : undefined,
    });
    const mandate = buildMandate(final, state, { mandateId: "m" });
    assert.ok(mandate);
    const expected = SHORT_REQUIRING_FAMILIES.has(template as StrategyFamily)
      ? "long_short"
      : "long_only";
    assert.equal(mandate.allowed_sides, expected, `allowed_sides for ${template}`);
  }
});

test("buildMandate carries candidate structural slots through", () => {
  const { final, state } = makeFixture("barbell_allocation", {
    rebalance_trigger: "periodic_90d",
    core_weight: 0.6,
    sleeve_cap: 0.1,
    select_top: 8,
  });
  const mandate = buildMandate(final, state, { mandateId: "m" });
  assert.ok(mandate);
  assert.equal(mandate.select_top, 8);
  assert.equal(mandate.weighting, "equal");
  assert.equal(mandate.rebalance_trigger, "periodic_90d");
  assert.equal(mandate.core_weight, 0.6);
  assert.equal(mandate.sleeve_cap, 0.1);
});

test("buildMandate returns null when the winning candidate cannot be located", () => {
  const { final, state } = makeFixture("synthetic_long_allocation");
  final.winner_candidate_id = "does-not-exist";
  const mandate = buildMandate(final, state, { mandateId: "m" });
  assert.equal(mandate, null);
});

test("buildMandate tolerates a winner with no backtest allocation", () => {
  const { final, state } = makeFixture("synthetic_long_allocation");
  delete (final as { winner_backtest?: unknown }).winner_backtest;
  const mandate = buildMandate(final, state, { mandateId: "m" });
  assert.ok(mandate);
  assert.deepEqual(mandate.initial_target_allocation, []);
});
