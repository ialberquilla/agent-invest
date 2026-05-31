import assert from "node:assert/strict";
import test from "node:test";

import type { AppendEventInput } from "../src/db/repositories/agent-events.ts";
import {
  runWorkflow,
  type StepRunners,
} from "../src/agent/workflow/controller.ts";
import type {
  Decision,
  FinalWinner,
  FinalNoViable,
  Proposal,
  Thesis,
  Universe,
  Window,
} from "../src/agent/workflow/state.ts";

const THESIS: Thesis = {
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

const UNIVERSE: Universe = {
  coin_ids: ["bitcoin", "ethereum", "binancecoin", "ripple", "solana"],
  source: "rank_universe",
  effective_filters: {
    top_n: 5,
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

const PROPOSAL: Proposal = {
  iteration_hypothesis: "x",
  candidates: [
    {
      candidate_id: "c1",
      template_id: "synthetic_long_allocation",
      select_top: 5,
      weighting: "equal",
      rationale: "baseline",
    },
  ],
};

type CallLog = string[];

// Convenience wrapper for tests that don't care about events but
// shouldn't fall through to the real database default. Injects a
// no-op appendEvent.
async function runWorkflowQuiet(
  ...args: Parameters<typeof runWorkflow>
): ReturnType<typeof runWorkflow> {
  const [run_id, brief, deps = {}] = args;
  return runWorkflow(run_id, brief, {
    appendEvent: async () => ({}) as never,
    ...deps,
  });
}

// Capture every agent_events row the controller would write, so tests
// can assert event ordering without touching a real database.
function fakeAppendEventCollector() {
  const events: Array<{ event_type: string; payload: unknown }> = [];
  const fn = async (
    input: AppendEventInput,
  ): Promise<never> => {
    events.push({ event_type: input.eventType, payload: input.payload });
    // Controller only uses the return value to extract eventId for tool
    // calls; the workflow path never reads it.
    return {} as never;
  };
  return Object.assign(fn, { events });
}

function trackingRunners(
  decisions: Decision[],
  options: {
    universes?: Universe[];
    failOn?: { step: string; message: string };
    finalNarrative?: FinalWinner["narrative"];
  } = {},
): { runners: Partial<StepRunners>; calls: CallLog } {
  const calls: CallLog = [];
  let decisionI = 0;
  let universeI = 0;
  const universes = options.universes ?? [UNIVERSE];

  const maybeFail = (step: string) => {
    if (options.failOn?.step === step) {
      throw new Error(options.failOn.message);
    }
  };

  return {
    calls,
    runners: {
      async interpretBrief(input) {
        calls.push(`interpret_brief(${input.hint ? "with-hint" : "cold"})`);
        maybeFail("interpret_brief");
        return { delta: { thesis: THESIS }, next: "select_templates" };
      },
      async selectTemplates() {
        calls.push("select_templates");
        maybeFail("select_templates");
        return {
          delta: {
            template_selection: {
              rationale: "fixture",
              selected: [
                {
                  family: "periodic_rebalanced_allocation",
                  rank: 1,
                  rationale: "fixture",
                },
              ],
            },
          },
          next: "select_universe",
        };
      },
      async selectUniverse(input) {
        calls.push(
          `select_universe(${input.hint ? "with-hint" : "cold"})`,
        );
        maybeFail("select_universe");
        const u = universes[universeI] ?? universes[universes.length - 1]!;
        universeI += 1;
        return { delta: { universe: u }, next: "select_window" };
      },
      async selectWindow() {
        calls.push("select_window");
        maybeFail("select_window");
        return { delta: { window: WINDOW }, next: "propose_candidates" };
      },
      async proposeCandidates(input) {
        calls.push(
          `propose_candidates(attempts=${input.attempts?.length ?? 0})`,
        );
        maybeFail("propose_candidates");
        return {
          delta: { proposal: PROPOSAL },
          next: "run_and_validate",
        };
      },
      async runAndValidate() {
        calls.push("run_and_validate");
        maybeFail("run_and_validate");
        return {
          delta: {
            batch_id: `batch_${calls.length}`,
            validation_summary: {
              passing_candidate_ids: [],
              failing: [
                {
                  candidate_id: "c1",
                  violations: [
                    {
                      constraint: "max_drawdown",
                      observed: -0.5,
                      target: -0.35,
                    },
                  ],
                },
              ],
              candidates: [
                {
                  candidate_id: "c1",
                  passed: false,
                  constraint_distance: 0.43,
                  metrics: {
                    total_return: 0.1,
                    cagr: 0.05,
                    volatility: 0.6,
                    max_drawdown: -0.5,
                    sharpe: 0.2,
                    sortino: 0.3,
                    calmar: 0.1,
                    composite_score: 0.4,
                  },
                },
              ],
            },
          },
          next: "decide",
        };
      },
      async decide(input) {
        calls.push(
          `decide(attempts=${input.attempts.length},counters=${input.counters.broaden_universe}/${input.counters.reinterpret_brief})`,
        );
        maybeFail("decide");
        const decision = decisions[decisionI] ?? decisions[decisions.length - 1]!;
        decisionI += 1;
        // Patch the runAndValidate result to look like a winner when needed.
        if (decision.action === "stop_winner") {
          const currentAttempt = input.attempts.at(-1);
          if (currentAttempt?.validation_summary) {
            currentAttempt.validation_summary.passing_candidate_ids = [
              decision.winner_candidate_id,
            ];
          }
        }
        return {
          delta: { decision },
          next: nextStepFor(decision),
        };
      },
      async finalize(input) {
        calls.push(`finalize(winner=${input.winner_candidate_id})`);
        maybeFail("finalize");
        const final: FinalWinner = {
          kind: "winner",
          run_id: input.run_id,
          winner_candidate_id: input.winner_candidate_id,
          winner_attempt_n: input.winner_attempt_n,
          candidate_batch_id: input.attempts.at(-1)!.batch_id!,
          is_best_effort: input.is_best_effort,
          unmet_constraints: [],
          thesis: input.thesis,
          universe: input.universe,
          window: input.window,
          attempts_summary: input.attempts,
          narrative: options.finalNarrative ?? {
            title: "t",
            summary: "s",
            reasoning: "r",
            assumptions: ["a"],
            risks: ["risk"],
            next_steps: ["next"],
          },
        };
        return { delta: { final }, next: "complete" };
      },
    },
  };
}

function nextStepFor(decision: Decision): "finalize" | "complete" | "propose_candidates" | "select_universe" | "interpret_brief" {
  switch (decision.action) {
    case "stop_winner":
      return "finalize";
    case "stop_best_effort":
      return "finalize";
    case "stop_no_viable":
      return "complete";
    case "refine_candidates":
      return "propose_candidates";
    case "broaden_universe":
      return "select_universe";
    case "reinterpret_brief":
      return "interpret_brief";
  }
}

test("workflow runs the happy path through to FinalWinner", async () => {
  const { runners, calls } = trackingRunners([
    {
      action: "stop_winner",
      winner_candidate_id: "c1",
      justification: "best fit",
    },
  ]);
  const appendEvent = fakeAppendEventCollector();

  const state = await runWorkflowQuiet("run-happy", "balanced brief", {
    runners,
    appendEvent,
  });

  assert.equal(state.final?.kind, "winner");
  assert.equal((state.final as FinalWinner).winner_candidate_id, "c1");
  assert.deepEqual(calls, [
    "interpret_brief(cold)",
    "select_templates",
    "select_universe(cold)",
    "select_window",
    "propose_candidates(attempts=0)",
    "run_and_validate",
    "decide(attempts=1,counters=0/0)",
    "finalize(winner=c1)",
  ]);

  // Event stream the frontend will see: the workflow-level stage.started,
  // one stage.started+stage.completed per step (8 of each), and the
  // final workflow-level stage.completed. The frontend's existing
  // filter for `stage.*` events picks them up.
  const types = appendEvent.events.map((e) => e.event_type);
  assert.equal(types[0], "stage.started");
  assert.equal(types.at(-1), "stage.completed");
  assert.equal(types.filter((t) => t === "stage.started").length, 1 + 8);
  assert.equal(types.filter((t) => t === "stage.completed").length, 8 + 1);

  // The first event identifies the workflow as a whole; per-step
  // events carry the step name in payload.stage.
  const firstPayload = appendEvent.events[0]?.payload as { stage?: string };
  assert.equal(firstPayload.stage, "workflow");
  const perStepStages = appendEvent.events
    .filter((e) => e.event_type === "stage.started")
    .slice(1)
    .map((e) => (e.payload as { stage?: string }).stage);
  assert.deepEqual(perStepStages, [
    "interpret_brief",
    "select_templates",
    "select_universe",
    "select_window",
    "propose_candidates",
    "run_and_validate",
    "decide",
    "finalize",
  ]);
});

test("controller emits stage.failed + final stage.completed on a budget short-circuit", async () => {
  const { runners } = trackingRunners([
    {
      action: "refine_candidates",
      hint: {
        failed_constraints: [
          {
            constraint: "max_drawdown",
            observed: -0.5,
            target: -0.35,
            candidate_id: "c1",
          },
        ],
        suggested_changes: {},
        rationale: "x",
      },
    },
  ]);
  const appendEvent = fakeAppendEventCollector();

  await runWorkflowQuiet("run-cap-events", "x", {
    runners,
    appendEvent,
    caps: { max_step_transitions: 4 },
  });

  const types = appendEvent.events.map((e) => e.event_type);
  // A workflow-level stage.failed marks the cap hit; the final
  // stage.completed marks the workflow itself ending.
  assert.ok(types.includes("stage.failed"));
  assert.equal(types.at(-1), "stage.completed");
  const failed = appendEvent.events.find((e) => e.event_type === "stage.failed");
  assert.equal((failed?.payload as { stage?: string })?.stage, "workflow");
});

test("controller emits stage.failed for the offending step on an exception", async () => {
  const { runners } = trackingRunners(
    [
      {
        action: "stop_winner",
        winner_candidate_id: "c1",
        justification: "ok",
      },
    ],
    { failOn: { step: "select_window", message: "histories do not overlap" } },
  );
  const appendEvent = fakeAppendEventCollector();

  await runWorkflowQuiet("run-step-error-events", "x", { runners, appendEvent });

  const errorEvent = appendEvent.events.find(
    (e) =>
      e.event_type === "stage.failed" &&
      (e.payload as { stage?: string }).stage === "select_window",
  );
  assert.ok(errorEvent);
  assert.match(
    JSON.stringify(errorEvent.payload),
    /histories do not overlap/,
  );
});

test("workflow stops cleanly on stop_no_viable", async () => {
  const { runners, calls } = trackingRunners([
    {
      action: "stop_no_viable",
      reasons: ["no candidate could meet max_drawdown."],
    },
  ]);

  const state = await runWorkflowQuiet("run-noviable", "x", { runners });

  assert.equal(state.final?.kind, "no_viable_strategy");
  assert.deepEqual((state.final as FinalNoViable).reasons, [
    "no candidate could meet max_drawdown.",
  ]);
  assert.equal(calls.at(-1), "decide(attempts=1,counters=0/0)");
});

test("refine_candidates re-enters propose_candidates with attempts growing", async () => {
  const { runners, calls } = trackingRunners([
    {
      action: "refine_candidates",
      hint: {
        failed_constraints: [
          {
            constraint: "max_drawdown",
            observed: -0.5,
            target: -0.35,
            candidate_id: "c1",
          },
        ],
        suggested_changes: { tighten_weight_cap_to: 0.15 },
        rationale: "tighten",
      },
    },
    {
      action: "stop_winner",
      winner_candidate_id: "c1",
      justification: "fixed",
    },
  ]);

  const state = await runWorkflowQuiet("run-refine", "x", { runners });

  assert.equal(state.final?.kind, "winner");
  assert.deepEqual(calls, [
    "interpret_brief(cold)",
    "select_templates",
    "select_universe(cold)",
    "select_window",
    "propose_candidates(attempts=0)",
    "run_and_validate",
    "decide(attempts=1,counters=0/0)",
    "propose_candidates(attempts=1)",
    "run_and_validate",
    "decide(attempts=2,counters=0/0)",
    "finalize(winner=c1)",
  ]);
  // The refinement hint should be attached to attempt 1, ready for the
  // next propose_candidates round to consume.
  assert.equal(
    state.attempts[0]?.refinement_hint?.failed_constraints[0]?.constraint,
    "max_drawdown",
  );
});

test("broaden_universe re-enters select_universe and bumps the counter", async () => {
  const broaderUniverse: Universe = {
    ...UNIVERSE,
    coin_ids: [
      ...UNIVERSE.coin_ids,
      "tron",
      "dogecoin",
      "hyperliquid",
      "cardano",
      "monero",
    ],
    effective_filters: { ...UNIVERSE.effective_filters, top_n: 10 },
  };
  const { runners, calls } = trackingRunners(
    [
      {
        action: "broaden_universe",
        hint: {
          reason: "too_narrow_after_filters",
          loosen: { raise_top_n_to: 30 },
          rationale: "too narrow",
        },
      },
      {
        action: "stop_winner",
        winner_candidate_id: "c1",
        justification: "ok",
      },
    ],
    { universes: [UNIVERSE, broaderUniverse] },
  );

  const state = await runWorkflowQuiet("run-broaden", "x", { runners });

  assert.equal(state.final?.kind, "winner");
  assert.equal(state.counters.broaden_universe, 1);
  assert.deepEqual(calls, [
    "interpret_brief(cold)",
    "select_templates",
    "select_universe(cold)",
    "select_window",
    "propose_candidates(attempts=0)",
    "run_and_validate",
    "decide(attempts=1,counters=0/0)",
    "select_universe(with-hint)",
    "select_window",
    "propose_candidates(attempts=1)",
    "run_and_validate",
    "decide(attempts=2,counters=1/0)",
    "finalize(winner=c1)",
  ]);
});

test("reinterpret_brief re-enters interpret_brief and bumps the counter", async () => {
  const { runners, calls } = trackingRunners([
    {
      action: "reinterpret_brief",
      hint: {
        reason: "constraints_infeasible",
        fields_to_revisit: ["constraints"],
        rationale: "constraints conflict",
      },
    },
    {
      action: "stop_winner",
      winner_candidate_id: "c1",
      justification: "ok",
    },
  ]);

  const state = await runWorkflowQuiet("run-reinterpret", "x", { runners });

  assert.equal(state.final?.kind, "winner");
  assert.equal(state.counters.reinterpret_brief, 1);
  assert.deepEqual(calls, [
    "interpret_brief(cold)",
    "select_templates",
    "select_universe(cold)",
    "select_window",
    "propose_candidates(attempts=0)",
    "run_and_validate",
    "decide(attempts=1,counters=0/0)",
    "interpret_brief(with-hint)",
    "select_templates",
    "select_universe(cold)",
    "select_window",
    "propose_candidates(attempts=1)",
    "run_and_validate",
    "decide(attempts=2,counters=0/1)",
    "finalize(winner=c1)",
  ]);
});

test("max_step_transitions cap short-circuits to FinalNoViable", async () => {
  const { runners } = trackingRunners([
    {
      action: "refine_candidates",
      hint: {
        failed_constraints: [
          {
            constraint: "max_drawdown",
            observed: -0.5,
            target: -0.35,
            candidate_id: "c1",
          },
        ],
        suggested_changes: {},
        rationale: "x",
      },
    },
  ]);

  // 4 transitions: interpret -> select_templates -> select_universe
  // -> select_window. The 5th transition would be propose_candidates,
  // which the cap blocks.
  const state = await runWorkflowQuiet("run-cap-transitions", "x", {
    runners,
    caps: { max_step_transitions: 4 },
  });

  assert.equal(state.final?.kind, "no_viable_strategy");
  assert.match(
    (state.final as FinalNoViable).reasons[0]!,
    /budget_exhausted: max_step_transitions=4/,
  );
});

test("max_llm_calls cap short-circuits when an LLM step is next", async () => {
  const { runners } = trackingRunners([
    {
      action: "stop_winner",
      winner_candidate_id: "c1",
      justification: "ok",
    },
  ]);

  // Cap at 2 LLM calls: interpret_brief (1) -> select_templates (2)
  // -> propose_candidates is blocked before invocation.
  const state = await runWorkflowQuiet("run-cap-llm", "x", {
    runners,
    caps: { max_llm_calls: 2 },
  });

  assert.equal(state.final?.kind, "no_viable_strategy");
  assert.match(
    (state.final as FinalNoViable).reasons[0]!,
    /budget_exhausted: max_llm_calls=2/,
  );
});

test("a step error short-circuits to FinalNoViable with the error message", async () => {
  const { runners } = trackingRunners(
    [
      {
        action: "stop_winner",
        winner_candidate_id: "c1",
        justification: "ok",
      },
    ],
    { failOn: { step: "select_window", message: "histories do not overlap" } },
  );

  const state = await runWorkflowQuiet("run-step-error", "x", { runners });

  assert.equal(state.final?.kind, "no_viable_strategy");
  assert.match(
    (state.final as FinalNoViable).reasons[0]!,
    /step_error: select_window: histories do not overlap/,
  );
});

test("short-circuit salvages a best-effort winner when an attempt has results", async () => {
  // run_and_validate produces a backtest result for c1, then decide
  // throws. The short-circuit must NOT give up with no_viable_strategy --
  // it finalizes the closest-fit candidate as a best-effort winner.
  const { runners, calls } = trackingRunners(
    [{ action: "stop_winner", winner_candidate_id: "c1", justification: "ok" }],
    { failOn: { step: "decide", message: "boom" } },
  );

  const state = await runWorkflowQuiet("run-salvage", "x", { runners });

  assert.equal(state.final?.kind, "winner");
  assert.equal((state.final as FinalWinner).is_best_effort, true);
  assert.equal((state.final as FinalWinner).winner_candidate_id, "c1");
  // The salvage path ran the finalize step on the ranked best candidate.
  assert.ok(calls.includes("finalize(winner=c1)"));
});

test("workflow runs to completion with all default caps", async () => {
  const { runners } = trackingRunners([
    {
      action: "stop_winner",
      winner_candidate_id: "c1",
      justification: "ok",
    },
  ]);

  const state = await runWorkflowQuiet("run-defaults", "x", { runners });

  assert.equal(state.final?.kind, "winner");
});
