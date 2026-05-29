// Workflow controller. while-loop dispatcher over the 7 steps. Owns
// WorkflowState, maintains per-edge counters, enforces global caps,
// short-circuits to FinalNoViable on cap hit or step error.
//
// No new LLM calls here -- pure orchestration. Step LLM/CLI deps are
// passed through via WorkflowDeps.

import pino from "pino";

import { appendEvent as defaultAppendEvent } from "../../db/repositories/agent-events.ts";
import type { LLMClient } from "./llm.ts";
import {
  DEFAULT_WORKFLOW_CAPS,
  type Attempt,
  type Decision,
  type Final,
  type FinalNoViable,
  type ReinterpretHint,
  type StepName,
  type UniverseHint,
  type WizardBrief,
  type WorkflowCaps,
  type WorkflowState,
} from "./state.ts";
import { decide as defaultDecide } from "./steps/decide.ts";
import { finalize as defaultFinalize } from "./steps/finalize.ts";
import { interpretBrief as defaultInterpretBrief } from "./steps/interpret_brief.ts";
import { proposeCandidates as defaultProposeCandidates } from "./steps/propose_candidates.ts";
import { runAndValidate as defaultRunAndValidate } from "./steps/run_and_validate.ts";
import { selectUniverse as defaultSelectUniverse } from "./steps/select_universe.ts";
import { selectWindow as defaultSelectWindow } from "./steps/select_window.ts";

const baseLogger = pino({
  level: process.env.LOG_LEVEL ?? "info",
  base: undefined,
  formatters: {
    level(label) {
      return { level: label };
    },
  },
});

const LLM_STEPS: ReadonlySet<StepName> = new Set([
  "interpret_brief",
  "propose_candidates",
  "decide",
  "finalize",
]);

export type StepRunners = {
  interpretBrief: typeof defaultInterpretBrief;
  selectUniverse: typeof defaultSelectUniverse;
  selectWindow: typeof defaultSelectWindow;
  proposeCandidates: typeof defaultProposeCandidates;
  runAndValidate: typeof defaultRunAndValidate;
  decide: typeof defaultDecide;
  finalize: typeof defaultFinalize;
};

export const DEFAULT_RUNNERS: StepRunners = {
  interpretBrief: defaultInterpretBrief,
  selectUniverse: defaultSelectUniverse,
  selectWindow: defaultSelectWindow,
  proposeCandidates: defaultProposeCandidates,
  runAndValidate: defaultRunAndValidate,
  decide: defaultDecide,
  finalize: defaultFinalize,
};

export type WorkflowDeps = {
  // Used by LLM-backed steps when they fall through to the defaults.
  // Ignored when the corresponding runner is overridden.
  llm?: LLMClient;
  runners?: Partial<StepRunners>;
  caps?: Partial<WorkflowCaps>;
  // Persists workflow.* rows into the agent_events table so the
  // frontend's /runs/:id/events polling and /stream SSE see progress
  // as the workflow moves. Defaults to the real repository.
  appendEvent?: typeof defaultAppendEvent;
};

export async function runWorkflow(
  run_id: string,
  brief: string | WizardBrief,
  deps: WorkflowDeps = {},
): Promise<WorkflowState> {
  const runners: StepRunners = { ...DEFAULT_RUNNERS, ...deps.runners };
  const caps: WorkflowCaps = { ...DEFAULT_WORKFLOW_CAPS, ...deps.caps };
  const appendEvent = deps.appendEvent ?? defaultAppendEvent;
  const log = baseLogger.child({ run_id });
  const startedAt = Date.now();
  const state: WorkflowState = {
    run_id,
    brief,
    attempts: [],
    counters: { reinterpret_brief: 0, broaden_universe: 0 },
  };

  log.info({ phase: "workflow_start", caps });
  await safeAppend(appendEvent, log, {
    runId: run_id,
    eventType: "stage.started",
    payload: {
      stage: "workflow",
      stage_run_id: `workflow.${run_id}`,
      round: 0,
      caps,
      brief_kind: typeof brief === "string" ? "text" : "wizard",
    },
  });

  let next: StepName = "interpret_brief";
  let transitions = 0;
  let llmStepsInvoked = 0;

  while (next !== "complete") {
    // Pre-dispatch budget checks. The order matters: prefer the
    // earliest-tripping cap so the user sees the most informative
    // reason.
    const elapsed = Date.now() - startedAt;
    if (elapsed >= caps.max_wall_clock_ms) {
      return finishShortCircuit(
        state,
        `budget_exhausted: max_wall_clock_ms=${caps.max_wall_clock_ms} (elapsed ${elapsed})`,
        log,
        startedAt,
        appendEvent,
      );
    }
    if (transitions >= caps.max_step_transitions) {
      return finishShortCircuit(
        state,
        `budget_exhausted: max_step_transitions=${caps.max_step_transitions}`,
        log,
        startedAt,
        appendEvent,
      );
    }
    if (LLM_STEPS.has(next) && llmStepsInvoked >= caps.max_llm_calls) {
      return finishShortCircuit(
        state,
        `budget_exhausted: max_llm_calls=${caps.max_llm_calls}`,
        log,
        startedAt,
        appendEvent,
      );
    }

    const current = next;
    const transitionN = transitions + 1;
    const stageRunId = `${current}.${transitionN}`;
    await safeAppend(appendEvent, log, {
      runId: run_id,
      eventType: "stage.started",
      payload: {
        stage: current,
        stage_run_id: stageRunId,
        round: transitionN,
        transition_n: transitionN,
      },
    });

    let dispatched: StepName;
    try {
      dispatched = await dispatch(current, state, runners, deps.llm);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log.error({ phase: "step_error", step: current, message });
      await safeAppend(appendEvent, log, {
        runId: run_id,
        eventType: "stage.failed",
        payload: {
          stage: current,
          stage_run_id: stageRunId,
          round: transitionN,
          error: message,
        },
      });
      return finishShortCircuit(
        state,
        `step_error: ${current}: ${message}`,
        log,
        startedAt,
        appendEvent,
      );
    }

    transitions += 1;
    if (LLM_STEPS.has(current)) llmStepsInvoked += 1;

    log.info({
      phase: "transition",
      from: current,
      to: dispatched,
      transition_n: transitions,
      llm_steps_invoked: llmStepsInvoked,
    });
    await safeAppend(appendEvent, log, {
      runId: run_id,
      eventType: "stage.completed",
      payload: {
        stage: current,
        stage_run_id: stageRunId,
        round: transitionN,
        next: dispatched,
        transition_n: transitions,
        llm_steps_invoked: llmStepsInvoked,
        ...transitionDigest(current, state),
      },
    });

    next = dispatched;
  }

  const duration_ms = Date.now() - startedAt;
  log.info({
    phase: "workflow_end",
    final_kind: state.final?.kind,
    transitions,
    llm_steps_invoked: llmStepsInvoked,
    duration_ms,
  });
  await safeAppend(appendEvent, log, {
    runId: run_id,
    eventType: "stage.completed",
    payload: {
      stage: "workflow",
      stage_run_id: `workflow.${run_id}`,
      round: 0,
      final_kind: state.final?.kind ?? null,
      transitions,
      llm_steps_invoked: llmStepsInvoked,
      duration_ms,
    },
  });
  return state;
}

// Small per-step digest published on each transition so the frontend
// can render meaningful progress without having to inspect the full
// state. Keep the payloads compact -- this is rendered as text in the
// UI, not consumed as authoritative data.
function transitionDigest(
  step: StepName,
  state: WorkflowState,
): Record<string, unknown> {
  switch (step) {
    case "interpret_brief":
      return state.thesis
        ? {
            objective: state.thesis.objective,
            horizon_days: state.thesis.horizon_days,
            asset_count_range: [
              state.thesis.constraints.asset_count_min,
              state.thesis.constraints.asset_count_max,
            ],
            max_weight_per_asset: state.thesis.constraints.max_weight_per_asset,
            max_drawdown: state.thesis.constraints.max_drawdown,
            rebalance_frequency: state.thesis.rebalance_frequency,
            interpretation_notes: excerpt(state.thesis.interpretation_notes, 220),
          }
        : {};
    case "select_universe":
      return state.universe
        ? {
            universe_size: state.universe.coin_ids.length,
            source: state.universe.source,
            // Show the full list; the UI wraps long values across
            // multiple lines now and seeing all the picks beats
            // hiding 15 of 25 behind an unexplained "...".
            coins: state.universe.coin_ids,
            risk_profile:
              state.universe.effective_filters.risk_profile ?? null,
          }
        : {};
    case "select_window":
      return state.window
        ? {
            start: state.window.start,
            end: state.window.end,
            window_length_days: state.window.effective.window_length_days,
            target_window_length_days:
              state.window.effective.target_window_length_days,
            limiting_coin: state.window.effective.limiting_coin ?? null,
            // When below_horizon, surface why the window collapses:
            // the limiting coin's first/last price dates explain it.
            limiting_coin_first_price_date:
              state.window.effective.limiting_coin_first_price_date ?? null,
            limiting_coin_last_price_date:
              state.window.effective.limiting_coin_last_price_date ?? null,
            intersection_start:
              state.window.effective.intersection_start ?? null,
            intersection_end: state.window.effective.intersection_end ?? null,
            drawdowns_covered:
              state.window.effective.covered_drawdowns_count,
            below_horizon:
              state.window.effective.window_length_days <
              state.window.horizon_days,
          }
        : {};
    case "propose_candidates": {
      const last = state.attempts.at(-1);
      if (!last) return {};
      const templates = last.proposal.candidates.reduce<Record<string, number>>(
        (acc, c) => {
          acc[c.template_id] = (acc[c.template_id] ?? 0) + 1;
          return acc;
        },
        {},
      );
      return {
        attempt_n: last.attempt_n,
        candidate_count: last.proposal.candidates.length,
        template_mix: templates,
        iteration_hypothesis: excerpt(last.proposal.iteration_hypothesis, 220),
      };
    }
    case "run_and_validate": {
      const last = state.attempts.at(-1);
      if (!last?.validation_summary) return {};
      const failedConstraints = Array.from(
        new Set(
          last.validation_summary.failing.flatMap((f) =>
            f.violations.map((v) => v.constraint),
          ),
        ),
      );
      return {
        attempt_n: last.attempt_n,
        batch_id: last.batch_id,
        passing: last.validation_summary.passing_candidate_ids.length,
        failing: last.validation_summary.failing.length,
        passing_ids: last.validation_summary.passing_candidate_ids,
        failed_constraints: failedConstraints,
      };
    }
    case "decide": {
      const last = state.attempts.at(-1);
      const decision = last?.decision;
      if (!decision) return {};
      switch (decision.action) {
        case "stop_winner":
          return {
            action: decision.action,
            winner_candidate_id: decision.winner_candidate_id,
            justification: excerpt(decision.justification, 220),
          };
        case "stop_no_viable":
          return {
            action: decision.action,
            reasons: decision.reasons,
          };
        case "refine_candidates":
          return {
            action: decision.action,
            failed_constraints: decision.hint.failed_constraints.map(
              (fc) => fc.constraint,
            ),
            suggested_changes: Object.keys(decision.hint.suggested_changes),
            rationale: excerpt(decision.hint.rationale, 220),
          };
        case "broaden_universe":
          return {
            action: decision.action,
            reason: decision.hint.reason,
            loosen: Object.keys(decision.hint.loosen),
            rationale: excerpt(decision.hint.rationale, 220),
          };
        case "reinterpret_brief":
          return {
            action: decision.action,
            reason: decision.hint.reason,
            fields_to_revisit: decision.hint.fields_to_revisit,
            rationale: excerpt(decision.hint.rationale, 220),
          };
      }
      return {};
    }
    case "finalize":
      return state.final?.kind === "winner"
        ? {
            winner_candidate_id: state.final.winner_candidate_id,
            candidate_batch_id: state.final.candidate_batch_id,
            title: state.final.narrative.title,
            summary: excerpt(state.final.narrative.summary, 280),
          }
        : {};
    case "complete":
      return {};
  }
}

function excerpt(value: string, max: number): string {
  if (value.length <= max) return value;
  return `${value.slice(0, max).trim()}...`;
}

async function safeAppend(
  appendEvent: typeof defaultAppendEvent,
  log: pino.Logger,
  payload: Parameters<typeof defaultAppendEvent>[0],
): Promise<void> {
  try {
    await appendEvent(payload);
  } catch (error) {
    // The workflow keeps running even if the event row can't be
    // written; we don't want the UI being offline to block real work.
    log.warn({
      phase: "agent_event_error",
      message: error instanceof Error ? error.message : String(error),
      event_type: payload.eventType,
    });
  }
}

async function dispatch(
  step: StepName,
  state: WorkflowState,
  runners: StepRunners,
  llm: LLMClient | undefined,
): Promise<StepName> {
  switch (step) {
    case "interpret_brief": {
      const hint = lastDecisionHint(state, "reinterpret_brief") as
        | ReinterpretHint
        | undefined;
      const result = await runners.interpretBrief(
        { run_id: state.run_id, brief: state.brief, hint },
        { llm: resolveLLM(llm) },
      );
      state.thesis = result.delta.thesis;
      return result.next;
    }
    case "select_universe": {
      if (!state.thesis) {
        throw new Error("select_universe entered without a thesis");
      }
      const hint = lastDecisionHint(state, "broaden_universe") as
        | UniverseHint
        | undefined;
      const result = await runners.selectUniverse(
        { run_id: state.run_id, thesis: state.thesis, hint },
        {},
      );
      state.universe = result.delta.universe;
      return result.next;
    }
    case "select_window": {
      if (!state.thesis || !state.universe) {
        throw new Error("select_window entered before thesis/universe");
      }
      const result = await runners.selectWindow(
        {
          run_id: state.run_id,
          thesis: state.thesis,
          universe: state.universe,
        },
        {},
      );
      state.window = result.delta.window;
      return result.next;
    }
    case "propose_candidates": {
      if (!state.thesis || !state.universe || !state.window) {
        throw new Error(
          "propose_candidates entered before thesis/universe/window",
        );
      }
      const result = await runners.proposeCandidates(
        {
          run_id: state.run_id,
          thesis: state.thesis,
          universe: state.universe,
          window: state.window,
          attempts: state.attempts,
        },
        { llm: resolveLLM(llm) },
      );
      state.attempts.push({
        attempt_n: state.attempts.length + 1,
        proposal: result.delta.proposal,
      });
      return result.next;
    }
    case "run_and_validate": {
      if (!state.thesis || !state.universe || !state.window) {
        throw new Error(
          "run_and_validate entered before thesis/universe/window",
        );
      }
      const current = state.attempts.at(-1);
      if (!current) {
        throw new Error("run_and_validate entered with no current attempt");
      }
      const result = await runners.runAndValidate(
        {
          run_id: state.run_id,
          thesis: state.thesis,
          universe: state.universe,
          window: state.window,
          proposal: current.proposal,
          attempts: state.attempts.slice(0, -1),
        },
        {},
      );
      current.batch_id = result.delta.batch_id;
      current.validation_summary = result.delta.validation_summary;
      return result.next;
    }
    case "decide": {
      if (!state.thesis) {
        throw new Error("decide entered without a thesis");
      }
      const current = state.attempts.at(-1);
      if (!current) {
        throw new Error("decide entered with no current attempt");
      }
      const result = await runners.decide(
        {
          run_id: state.run_id,
          thesis: state.thesis,
          attempts: state.attempts,
          counters: state.counters,
        },
        { llm: resolveLLM(llm) },
      );
      const decision = result.delta.decision;
      current.decision = decision;
      applyDecisionSideEffects(state, current, decision);
      return result.next;
    }
    case "finalize": {
      if (!state.thesis || !state.universe || !state.window) {
        throw new Error("finalize entered before thesis/universe/window");
      }
      const current = state.attempts.at(-1);
      if (!current || !current.decision) {
        throw new Error("finalize entered without a decided attempt");
      }
      if (current.decision.action !== "stop_winner") {
        throw new Error(
          `finalize entered after action=${current.decision.action} (expected stop_winner)`,
        );
      }
      const result = await runners.finalize(
        {
          run_id: state.run_id,
          thesis: state.thesis,
          universe: state.universe,
          window: state.window,
          attempts: state.attempts,
          winner_candidate_id: current.decision.winner_candidate_id,
          decide_justification: current.decision.justification,
        },
        { llm: resolveLLM(llm) },
      );
      state.final = result.delta.final;
      return result.next;
    }
    case "complete":
      throw new Error("dispatch called with step=complete");
  }
}

// Apply state changes that depend on the decision action: attach the
// refinement_hint for refine_candidates, bump backward-edge counters,
// or build the FinalNoViable record for stop_no_viable.
function applyDecisionSideEffects(
  state: WorkflowState,
  current: Attempt,
  decision: Decision,
): void {
  switch (decision.action) {
    case "refine_candidates":
      current.refinement_hint = decision.hint;
      return;
    case "broaden_universe":
      state.counters.broaden_universe += 1;
      return;
    case "reinterpret_brief":
      state.counters.reinterpret_brief += 1;
      return;
    case "stop_no_viable":
      state.final = {
        kind: "no_viable_strategy",
        run_id: state.run_id,
        thesis: state.thesis,
        reasons: decision.reasons,
        attempts_summary: state.attempts,
      };
      return;
    case "stop_winner":
      // finalize will build the FinalWinner record from this decision.
      return;
  }
}

// Reads the hint payload from the most recent attempt's decision when
// the action matches `expectedAction`. Returns undefined otherwise --
// callers pass through to the step with no hint, which is the
// cold-start signal.
function lastDecisionHint(
  state: WorkflowState,
  expectedAction: Decision["action"],
): Decision extends { hint: infer H } ? H | undefined : undefined {
  type Hinted = Extract<Decision, { hint: unknown }>;
  const last = state.attempts.at(-1);
  const decision = last?.decision;
  if (!decision) return undefined as never;
  if (decision.action !== expectedAction) return undefined as never;
  if (!("hint" in decision)) return undefined as never;
  return (decision as Hinted).hint as never;
}

// Fallback LLM that fails only when actually called. Lets the
// controller stay typed while keeping deps.llm optional for tests that
// override the LLM-backed runners with deterministic doubles.
const nullLLM: LLMClient = {
  async complete() {
    throw new Error(
      "no LLM client configured; pass deps.llm or override the step runner",
    );
  },
};

function resolveLLM(llm: LLMClient | undefined): LLMClient {
  return llm ?? nullLLM;
}

async function finishShortCircuit(
  state: WorkflowState,
  reason: string,
  log: pino.Logger,
  startedAt: number,
  appendEvent: typeof defaultAppendEvent,
): Promise<WorkflowState> {
  log.warn({ phase: "cap_hit", reason });
  await safeAppend(appendEvent, log, {
    runId: state.run_id,
    eventType: "stage.failed",
    payload: {
      stage: "workflow",
      stage_run_id: `workflow.${state.run_id}`,
      round: 0,
      error: reason,
    },
  });
  const noViable: FinalNoViable = {
    kind: "no_viable_strategy",
    run_id: state.run_id,
    thesis: state.thesis,
    reasons: [reason],
    attempts_summary: state.attempts,
  };
  state.final = noViable;
  const duration_ms = Date.now() - startedAt;
  log.info({
    phase: "workflow_end",
    final_kind: state.final.kind,
    short_circuit: true,
    duration_ms,
  });
  await safeAppend(appendEvent, log, {
    runId: state.run_id,
    eventType: "stage.completed",
    payload: {
      stage: "workflow",
      stage_run_id: `workflow.${state.run_id}`,
      round: 0,
      final_kind: state.final.kind,
      short_circuit: true,
      reason,
      duration_ms,
    },
  });
  return state;
}

// Re-exports for callers that want the type elsewhere.
export type { Final, WorkflowState, WorkflowCaps };
