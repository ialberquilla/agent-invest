// Thin wrapper around `runWorkflow` that updates the `runs` row when
// the workflow finishes. Chat agent calls this so the API/UI can read
// the final state from the standard `runs` table without needing to
// know about WorkflowState shape directly.

import { eq, sql } from "drizzle-orm";

import { db as defaultDb } from "../../db/client.ts";
import { runs } from "../../db/schema.ts";
import { runWorkflow, type WorkflowDeps } from "./controller.ts";
import type {
  FinalWinner,
  WizardBrief,
  WorkflowState,
} from "./state.ts";

type Db = typeof defaultDb;

export type RunWorkflowAndPersistDeps = WorkflowDeps & {
  db?: Db;
};

export type RunWorkflowAndPersistResult = {
  state: WorkflowState;
};

export async function runWorkflowAndPersist(
  run_id: string,
  brief: string | WizardBrief,
  deps: RunWorkflowAndPersistDeps = {},
): Promise<RunWorkflowAndPersistResult> {
  const db = deps.db ?? defaultDb;
  const startedAt = Date.now();
  let state: WorkflowState;

  try {
    state = await runWorkflow(run_id, brief, deps);
  } catch (error) {
    // Unhandled controller exception (the controller is supposed to
    // short-circuit instead, but if something throws above that we
    // still surface a useful failed run).
    const message = error instanceof Error ? error.message : String(error);
    await db
      .update(runs)
      .set({
        status: "failed",
        endedAt: sql`NOW()`,
        exitCode: 1,
        error: message,
      })
      .where(eq(runs.runId, run_id));
    throw error;
  }

  await persistFinal(db, run_id, state, Date.now() - startedAt);
  return { state };
}

async function persistFinal(
  db: Db,
  run_id: string,
  state: WorkflowState,
  duration_ms: number,
): Promise<void> {
  const final = state.final;
  if (!final) {
    // Defensive: the controller should always set state.final before
    // returning. If it didn't, mark the run failed so callers notice.
    await db
      .update(runs)
      .set({
        status: "failed",
        endedAt: sql`NOW()`,
        exitCode: 1,
        error: "workflow ended without a final state",
      })
      .where(eq(runs.runId, run_id));
    return;
  }

  const metadata = buildMetadata(state, duration_ms);
  const reply = buildReply(final);
  const winnerTemplate =
    final.kind === "winner" ? winnerTemplateId(final, state) : null;
  const errorString =
    final.kind === "no_viable_strategy" ? final.reasons.join(" | ") : null;
  const roundHistory = state.attempts;
  const refinementReasons = collectRefinementReasons(state);

  await db
    .update(runs)
    .set({
      status: "completed",
      endedAt: sql`NOW()`,
      exitCode: 0,
      error: errorString,
      reply,
      winnerTemplateId: winnerTemplate,
      roundHistory: roundHistory as never,
      refinementReasons: refinementReasons as never,
      metadata: metadata as never,
    })
    .where(eq(runs.runId, run_id));
}

function buildMetadata(state: WorkflowState, duration_ms: number) {
  return {
    final: state.final,
    counters: state.counters,
    attempt_count: state.attempts.length,
    duration_ms,
    thesis: state.thesis,
    universe_size: state.universe?.coin_ids.length,
    window: state.window
      ? {
          start: state.window.start,
          end: state.window.end,
          horizon_days: state.window.horizon_days,
          window_length_days: state.window.effective.window_length_days,
        }
      : undefined,
  };
}

function buildReply(final: WorkflowState["final"]): string {
  if (!final) return "";
  if (final.kind === "winner") {
    return [final.narrative.title, final.narrative.summary]
      .filter(Boolean)
      .join("\n\n");
  }
  return `No viable strategy found:\n- ${final.reasons.join("\n- ")}`;
}

function winnerTemplate(
  final: FinalWinner,
  state: WorkflowState,
): string | null {
  const latest = state.attempts.at(-1);
  const winnerCandidate = latest?.proposal.candidates.find(
    (c) => c.candidate_id === final.winner_candidate_id,
  );
  return winnerCandidate?.template_id ?? null;
}

// Map a finished WorkflowState into the structured-result shape the
// synchronous /messages endpoint (and the frontend) expects. Lets us
// swap the old artifact-reading code path without changing the wire
// format. Returns null when nothing meaningful is available yet.
export function workflowStateToStructuredResult(
  state: WorkflowState,
): Record<string, unknown> | null {
  const final = state.final;
  if (!final) return null;

  if (final.kind === "winner") {
    const winnerTemplate = winnerTemplateId(final, state);
    return {
      title: final.narrative.title,
      summary: final.narrative.summary,
      reasoning: final.narrative.reasoning,
      assumptions: final.narrative.assumptions,
      risks: final.narrative.risks,
      next_steps: final.narrative.next_steps,
      template_id: winnerTemplate,
      winner_candidate_id: final.winner_candidate_id,
      backtest: {
        candidate_batch_id: final.candidate_batch_id,
      },
      winners_by_dimension: null,
      round_history: state.attempts,
      refinement_reasons: collectRefinementReasons(state),
      allocation: [],
      kpis: {},
      artifacts: [],
    };
  }

  return {
    title: "No viable strategy",
    summary: final.reasons.join(" "),
    reasoning: final.reasons.join(" "),
    assumptions: [],
    risks: [],
    next_steps: ["Revise the brief or constraints and rerun."],
    template_id: null,
    winner_candidate_id: null,
    backtest: null,
    winners_by_dimension: null,
    round_history: state.attempts,
    refinement_reasons: final.reasons,
    allocation: [],
    kpis: {},
    artifacts: [],
  };
}

function winnerTemplateId(
  final: FinalWinner,
  state: WorkflowState,
): string | null {
  const latest = state.attempts.at(-1);
  const winnerCandidate = latest?.proposal.candidates.find(
    (c) => c.candidate_id === final.winner_candidate_id,
  );
  return winnerCandidate?.template_id ?? null;
}

function collectRefinementReasons(state: WorkflowState): string[] {
  const reasons: string[] = [];
  for (const attempt of state.attempts) {
    const hint = attempt.refinement_hint;
    if (hint?.rationale) {
      reasons.push(`attempt ${attempt.attempt_n}: ${hint.rationale}`);
    }
  }
  if (state.final?.kind === "no_viable_strategy") {
    for (const reason of state.final.reasons) {
      reasons.push(reason);
    }
  }
  return reasons;
}
