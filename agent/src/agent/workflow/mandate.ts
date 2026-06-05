// Phase 1 of plans/integrate_contracts.md: when a run finalizes a winner, we
// emit a self-contained, versioned StrategyMandate the rebalance bot can execute
// without re-deriving anything from prose. This module is the pure core: it maps
// a FinalWinner + WorkflowState into a mandate. Persistence lives in
// db/repositories/strategy-mandates.ts; the workflow hook lives in persist.ts.

import {
  type AllocationTemplate,
  type AllocationWeight,
  DYNAMIC_UNIVERSE_FAMILIES,
  type FinalWinner,
  type Objective,
  type ProposedCandidate,
  type RebalanceFrequency,
  type RebalanceTrigger,
  SHORT_REQUIRING_FAMILIES,
  type StrategyFamily,
  type ThesisConstraints,
  type UniverseHints,
  type WeightingScheme,
  type WorkflowState,
} from "./state.ts";

// Format version of the mandate payload. Bump when the StrategyMandate shape
// changes in a way the executor must branch on. Not the same as a re-issue
// version (a re-run produces a fresh mandate_id, not a new version here).
export const MANDATE_VERSION = 1;
export { DYNAMIC_UNIVERSE_FAMILIES };

export type MandateStatus = "pending" | "active" | "halted" | "retired";

// long_only maps to per-market long positions on GMX; long_short additionally
// allows isLong=false legs. Derived from the family, not free-form.
export type AllowedSides = "long_only" | "long_short";

// Everything the rebalance bot needs to execute the strategy, with no prose to
// re-interpret. Stored as the `spec` jsonb on strategy_mandates.
export type StrategyMandate = {
  mandate_id: string;
  run_id: string;
  version: number;
  created_at: string; // ISO 8601

  // Candidate parameters (the algorithmic knobs the bt recipe consumed).
  template_id: AllocationTemplate;
  select_top: number;
  weighting: WeightingScheme;
  rebalance_trigger?: RebalanceTrigger;
  core_weight?: number;
  sleeve_cap?: number;

  objective: Objective;
  rebalance_frequency: RebalanceFrequency;
  // Lookback length (days) the live allocator uses to recommend its window, so
  // the to-today run has enough history for the recipe's signals.
  horizon_days: number;

  // Universe selection rule. coin_ids is the resolved eligible pool; when
  // dynamic_universe is true the bot re-ranks/re-selects from it each period,
  // otherwise the held set is fixed (weights may still drift/re-normalize).
  universe_hints: UniverseHints;
  coin_ids: string[];
  dynamic_universe: boolean;

  // Execution constraints lifted from the thesis.
  constraints: Pick<
    ThesisConstraints,
    "max_weight_per_asset" | "max_cash_weight" | "max_drawdown"
  >;

  allowed_sides: AllowedSides;

  // First rebalance's target weights (winner_backtest.allocation). The bot
  // recomputes live weights each period; this is the bootstrap / sanity anchor.
  initial_target_allocation: AllocationWeight[];

  status: MandateStatus;
};

// Locate the winning candidate's full parameter set. A best-effort winner can
// come from an earlier attempt and candidate_id is only unique within a batch,
// so both attempt_n and candidate_id are needed (mirrors persist.ts).
export function findWinnerCandidate(
  final: FinalWinner,
  state: WorkflowState,
): ProposedCandidate | null {
  const attempt = state.attempts.find(
    (a) => a.attempt_n === final.winner_attempt_n,
  );
  return (
    attempt?.proposal.candidates.find(
      (c) => c.candidate_id === final.winner_candidate_id,
    ) ?? null
  );
}

function allowedSidesFor(template: AllocationTemplate): AllowedSides {
  // AllocationTemplate is a subset of StrategyFamily (same string values).
  return SHORT_REQUIRING_FAMILIES.has(template as StrategyFamily)
    ? "long_short"
    : "long_only";
}

export type BuildMandateOptions = {
  mandateId: string;
  createdAt?: string;
};

// Pure mapping FinalWinner + state -> StrategyMandate. Returns null when the
// winning candidate cannot be located (defensive; the caller logs and skips).
export function buildMandate(
  final: FinalWinner,
  state: WorkflowState,
  options: BuildMandateOptions,
): StrategyMandate | null {
  const candidate = findWinnerCandidate(final, state);
  if (!candidate) return null;

  const template = candidate.template_id;
  const thesis = final.thesis;

  return {
    mandate_id: options.mandateId,
    run_id: final.run_id,
    version: MANDATE_VERSION,
    created_at: options.createdAt ?? new Date().toISOString(),

    template_id: template,
    select_top: candidate.select_top,
    weighting: candidate.weighting,
    rebalance_trigger: candidate.rebalance_trigger,
    core_weight: candidate.core_weight,
    sleeve_cap: candidate.sleeve_cap,

    objective: thesis.objective,
    rebalance_frequency: thesis.rebalance_frequency,
    horizon_days: thesis.horizon_days,

    universe_hints: thesis.universe_hints,
    coin_ids: final.universe.coin_ids,
    dynamic_universe: DYNAMIC_UNIVERSE_FAMILIES.has(template),

    constraints: {
      max_weight_per_asset: thesis.constraints.max_weight_per_asset,
      max_cash_weight: thesis.constraints.max_cash_weight,
      max_drawdown: thesis.constraints.max_drawdown,
    },

    allowed_sides: allowedSidesFor(template),

    initial_target_allocation: final.winner_backtest?.allocation ?? [],

    status: "pending",
  };
}
