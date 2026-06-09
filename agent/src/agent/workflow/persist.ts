// Thin wrapper around `runWorkflow` that updates the `runs` row when
// the workflow finishes. Chat agent calls this so the API/UI can read
// the final state from the standard `runs` table without needing to
// know about WorkflowState shape directly.

import { eq, sql } from "drizzle-orm";

import { randomUUID } from "node:crypto";

import { db as defaultDb } from "../../db/client.ts";
import { insertMandate } from "../../db/repositories/strategy-mandates.ts";
import { runs } from "../../db/schema.ts";
import { runWorkflow, type WorkflowDeps } from "./controller.ts";
import { buildMandate } from "./mandate.ts";
import type {
  CandidateBacktest,
  EquityPoint,
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

  // Phase 1 of plans/integrate_contracts.md: on a finalized winner, emit a
  // pending executable mandate alongside the run. Additive and best-effort --
  // a mandate failure must never break the existing run persistence.
  if (final.kind === "winner") {
    await persistMandate(db, final, state);
  }
}

async function persistMandate(
  db: Db,
  final: FinalWinner,
  state: WorkflowState,
): Promise<void> {
  try {
    const mandate = buildMandate(final, state, { mandateId: randomUUID() });
    if (!mandate) {
      console.error(
        `[persist] winner ${final.run_id} produced no mandate (winning candidate not found)`,
      );
      return;
    }
    await insertMandate(mandate, db);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[persist] failed to write mandate for ${final.run_id}: ${message}`);
  }
}

function buildMetadata(state: WorkflowState, duration_ms: number) {
  return {
    brief: state.brief,
    structured_result: workflowStateToStructuredResult(state),
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
    const backtest = final.winner_backtest;
    const charts = backtest ? buildCharts(backtest) : {};
    return {
      title: final.narrative.title,
      summary: final.narrative.summary,
      reasoning: final.narrative.reasoning,
      assumptions: final.narrative.assumptions,
      risks: final.narrative.risks,
      next_steps: final.narrative.next_steps,
      constraint_violations: final.unmet_constraints.map(
        (c) =>
          `${c.constraint}: observed ${c.observed}, target ${c.target}`,
      ),
      template_id: winnerTemplate,
      winner_candidate_id: final.winner_candidate_id,
      is_best_effort: final.is_best_effort,
      unmet_constraints: final.unmet_constraints,
      backtest: {
        candidate_batch_id: final.candidate_batch_id,
        start_date: final.window.start,
        end_date: final.window.end,
        rebalance: rebalanceLabel(final),
        initial_capital_usd: backtest?.equity_curve[0]?.value ?? null,
        capital_mode: backtest ? "usd" : null,
        benchmark: final.thesis.objective === "growth" ? "bitcoin" : null,
      },
      winners_by_dimension: null,
      round_history: state.attempts,
      refinement_reasons: collectRefinementReasons(state),
      allocation: buildAllocation(backtest),
      kpis: backtest ? buildKpis(backtest) : {},
      charts,
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

function isFiniteNum(value: number | undefined): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function finiteOrNull(value: number | undefined): number | null {
  return isFiniteNum(value) ? value : null;
}

// Map the winner's scalar metrics onto the StrategyKpis shape the
// frontend result card reads. monthly_hit_rate / trading-cost / swaps
// are not produced by run_candidate_batch, so they stay null (the card
// renders "Not provided"). final_equity_* derive from the equity curve.
function buildKpis(backtest: CandidateBacktest) {
  const m = backtest.metrics;
  const curve = backtest.equity_curve;
  const first = curve[0]?.value;
  const last = curve[curve.length - 1]?.value;
  const multiple =
    isFiniteNum(first) && isFiniteNum(last) && first !== 0
      ? last / first
      : null;
  return {
    cagr: finiteOrNull(m.cagr),
    sharpe_ratio: finiteOrNull(m.sharpe),
    sortino_ratio: finiteOrNull(m.sortino),
    max_drawdown: finiteOrNull(m.max_drawdown),
    calmar_ratio: finiteOrNull(m.calmar),
    monthly_hit_rate: null,
    final_equity_usd: finiteOrNull(last),
    final_equity_multiple: multiple,
    total_trading_cost_usd: null,
    total_num_swaps: null,
  };
}

// Merge the winner's strategy and benchmark curves into the
// date-aligned series the frontend charts plot. The benchmark curve is
// normalized to 1.0 at its start, so we rescale it to the strategy's
// starting capital -- "the same money invested in the benchmark" --
// keeping both series on one dollar axis. Drawdowns are already pct, so
// they pass through unscaled.
function buildCharts(backtest: CandidateBacktest) {
  const first = backtest.equity_curve[0]?.value;
  const benchScale = isFiniteNum(first) ? first : 1;
  const benchByDate = new Map<string, EquityPoint>();
  for (const point of backtest.benchmark_curve) {
    benchByDate.set(point.date, point);
  }

  const equity_curve = backtest.equity_curve.map((point) => {
    const bench = benchByDate.get(point.date);
    return {
      date: point.date,
      strategy_equity: point.value,
      benchmark_equity:
        bench && isFiniteNum(bench.value) ? bench.value * benchScale : null,
    };
  });
  const drawdown = backtest.equity_curve.map((point) => {
    const bench = benchByDate.get(point.date);
    return {
      date: point.date,
      strategy_drawdown: point.drawdown_pct,
      benchmark_drawdown: bench ? bench.drawdown_pct : null,
    };
  });
  const target_allocation = backtest.allocation.map((a) => ({
    asset: assetLabel(a.coin_id),
    weight: a.weight,
  }));
  return { equity_curve, drawdown, target_allocation };
}

// The richer "Selected assets" list (asset/symbol/coin_id/weight/rationale).
// We only have coin ids + weights from the backtest, so symbol/rationale are
// left unset; the card renders those as "Not provided" per asset.
function buildAllocation(backtest: CandidateBacktest | undefined) {
  if (!backtest) return [];
  return backtest.allocation.map((a) => ({
    asset: assetLabel(a.coin_id),
    coin_id: a.coin_id,
    symbol: null,
    weight: a.weight,
    rationale: "",
  }));
}

// "render-token" -> "Render Token". Just a display nicety for the coin id.
function assetLabel(coinId: string): string {
  return coinId
    .split(/[-_]/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

// Narrow the thesis rebalance cadence to the labels the card accepts
// ("quarterly" has no slot, so it shows as unspecified).
function rebalanceLabel(
  final: FinalWinner,
): "daily" | "weekly" | "monthly" | null {
  switch (final.thesis.rebalance_frequency) {
    case "daily":
    case "weekly":
    case "monthly":
      return final.thesis.rebalance_frequency;
    default:
      return null;
  }
}

function winnerTemplateId(
  final: FinalWinner,
  state: WorkflowState,
): string | null {
  // Locate the winner by its attempt: a best-effort winner can be from
  // an earlier attempt, and candidate_id is only unique within a batch.
  const attempt = state.attempts.find(
    (a) => a.attempt_n === final.winner_attempt_n,
  );
  const winnerCandidate = attempt?.proposal.candidates.find(
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
