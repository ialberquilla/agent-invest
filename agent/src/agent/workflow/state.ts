// Typed state for the strategy workflow.
//
// Steps consume `WorkflowState` read-only and return a `StateDelta`
// plus the name of the next step. The controller (not yet implemented)
// merges deltas and dispatches.

export const STEP_NAMES = [
  "interpret_brief",
  "select_templates",
  "select_universe",
  "select_window",
  "propose_candidates",
  "run_and_validate",
  "decide",
  "finalize",
  "complete",
] as const;
export type StepName = (typeof STEP_NAMES)[number];

export type Objective =
  | "balanced_growth"
  | "growth"
  | "income"
  | "preserve_capital";

export type RebalanceFrequency =
  | "daily"
  | "weekly"
  | "monthly"
  | "quarterly";

export type WeightMode = "percentage" | "dollar";

// The strategy SHAPE the thesis takes. Drives feasibility rules, eligible
// templates, and universe sizing downstream. basket_allocation is the
// default and the only fully-runnable shape today; the others land across
// later phases (see plans/workflow_backtest_improvements.md). Optional on
// the Thesis so existing long-basket runs are unchanged when it is unset.
export type StrategyMode =
  | "single_asset"
  | "pair_trade"
  | "hedge_overlay"
  | "basket_allocation"
  | "momentum_rotation"
  | "long_short_portfolio";

export const STRATEGY_MODES: readonly StrategyMode[] = [
  "single_asset",
  "pair_trade",
  "hedge_overlay",
  "basket_allocation",
  "momentum_rotation",
  "long_short_portfolio",
];

// The direction permission, kept as a separate axis from the mode so a
// shape (e.g. momentum_rotation) can be long-only, long/flat, or
// long/short without a combinatorial enum. Shorts require explicit user
// intent: never inferred from "growth"/"momentum" language.
export type AllowedSides = "long_only" | "long_flat" | "long_short";

export const ALLOWED_SIDES: readonly AllowedSides[] = [
  "long_only",
  "long_flat",
  "long_short",
];

export type ExecutionMode = "wallet_direct" | "strategy_vault";

export const EXECUTION_MODES: readonly ExecutionMode[] = [
  "wallet_direct",
  "strategy_vault",
];

// Defaults applied when the thesis leaves a mode field unset. They
// reproduce the pre-mode behavior (a long-only basket deployed to a
// vault) so unset === unchanged.
export const DEFAULT_STRATEGY_MODE: StrategyMode = "basket_allocation";
export const DEFAULT_ALLOWED_SIDES: AllowedSides = "long_only";
export const DEFAULT_EXECUTION_MODE: ExecutionMode = "strategy_vault";

export type UniverseHints = {
  top_n: number;
  // Skip the first N market-cap ranks before applying top_n. Lets the
  // brief express "outside the top 5" without forcing the LLM to bury
  // the intent in interpretation_notes.
  top_skip?: number;
  market_cap_min_usd?: number;
  // Minimum price-history length to require from each candidate coin.
  // When set, short-history coins are filtered out at the universe
  // stage before they can collapse the backtest window. When unset
  // (the default), no history filter is applied -- appropriate for
  // momentum/rebalancing strategies that pick dynamically each round.
  // See interpret_brief prompt for when the LLM should set it.
  min_history_days?: number;
  exclude_stablecoins: boolean;
  exclude_wrapped: boolean;
  sectors_include?: string[];
  sectors_exclude?: string[];
  hand_picked_coin_ids?: string[];
};

export type ThesisConstraints = {
  max_weight_per_asset: number;
  max_cash_weight: number;
  max_drawdown: number;
  asset_count_min: number;
  asset_count_max: number;
  // Exposure limits for short-bearing books (pair/hedge/long-short). Long
  // modes ignore them; short-bearing modes validate against these instead
  // of the long-only max_weight_per_asset rule. See validate_against_thesis.
  max_gross_exposure?: number;
  max_net_exposure?: number;
  max_leg_weight?: number;
  // Intended net market beta (0 for market-neutral, ~1 for fully-long).
  // Validated against the realised beta of the backtest within a tolerance.
  target_net_beta?: number;
};

export type Thesis = {
  objective: Objective;
  horizon_days: number;
  weight_mode: WeightMode;
  universe_hints: UniverseHints;
  constraints: ThesisConstraints;
  rebalance_frequency: RebalanceFrequency;
  interpretation_notes: string;
  // Strategy shape + direction permission (optional; default via the
  // resolve* helpers). Gate feasibility and template selection.
  strategy_mode?: StrategyMode;
  allowed_sides?: AllowedSides;
  execution_mode?: ExecutionMode;
  // Explicit legs for single-asset / pair / hedge shapes. Unused by
  // basket modes. Consumed by select_universe and the dedicated recipes
  // in later phases; carried here so they survive an override + rerun.
  target_coin_id?: string;
  long_coin_ids?: string[];
  short_coin_ids?: string[];
};

// Resolve a thesis mode field to its effective value, applying the
// default when unset. Use these everywhere downstream instead of reading
// the optional field directly, so an unset thesis behaves exactly like a
// long-only basket.
export function resolveStrategyMode(
  thesis: Pick<Thesis, "strategy_mode">,
): StrategyMode {
  return thesis.strategy_mode ?? DEFAULT_STRATEGY_MODE;
}

export function resolveAllowedSides(
  thesis: Pick<Thesis, "allowed_sides">,
): AllowedSides {
  return thesis.allowed_sides ?? DEFAULT_ALLOWED_SIDES;
}

export function resolveExecutionMode(
  thesis: Pick<Thesis, "execution_mode">,
): ExecutionMode {
  return thesis.execution_mode ?? DEFAULT_EXECUTION_MODE;
}

// Deterministic overrides applied to the interpreted Thesis after
// interpret_brief and before universe/template selection. They let a
// follow-up run re-shape a prior result ("fewer assets", "lower
// drawdown") without re-briefing from scratch. PR1 covers the subset
// that maps cleanly onto existing Thesis fields and re-validates;
// strategy-mode / leg / template-family overrides arrive in later phases
// (see plans/workflow_backtest_improvements.md section 13). Applied by
// applyOverrides() in overrides.ts.
export type StrategyRunOverrides = {
  asset_count_min?: number;
  asset_count_max?: number;
  max_weight_per_asset?: number;
  max_cash_weight?: number;
  max_drawdown?: number;
  horizon_days?: number;
  rebalance_frequency?: RebalanceFrequency;
  top_n?: number;
  top_skip?: number;
  exclude_stablecoins?: boolean;
  exclude_wrapped?: boolean;
  hand_picked_coin_ids?: string[];
  // Shape overrides: pivot a finished run into a different strategy mode
  // (e.g. a basket result into a single-asset trend setup) on a rerun.
  strategy_mode?: StrategyMode;
  allowed_sides?: AllowedSides;
  target_coin_id?: string;
  long_coin_ids?: string[];
  short_coin_ids?: string[];
};

// The strategy-family catalog mirrors spec.md section 9. select_templates
// classifies a Thesis onto a ranked shortlist of these families before
// propose_candidates parameterizes concrete candidates. The eight long-only
// families compile to executable candidates today (see ALLOCATION_TEMPLATES);
// the four short/hedge families are catalogued here for routing but only
// become executable in Phase 2, behind the SHORTS gate.
export const STRATEGY_FAMILIES = [
  "synthetic_long_allocation", // 9.1
  "periodic_rebalanced_allocation", // 9.2
  "threshold_rebalanced_allocation", // 9.3
  "core_satellite_allocation", // 9.4
  "barbell_allocation", // 9.5
  "partial_hedge_overlay", // 9.6
  "trend_following_long_neutral", // 9.7
  "trend_following_long_short", // 9.8
  "relative_momentum_rotation", // 9.9
  "relative_value_pair_trade", // 9.10
  "beta_hedged_alt_exposure", // 9.11
  "drawdown_based_hedge", // 9.12
  "volatility_targeted_exposure", // 9.13
  // single_asset strategy_mode only. select_templates forces this family
  // for single-asset theses and drops it from every other shortlist.
  "single_asset_trend_setup",
  // pair_trade strategy_mode only (REQUIRES SHORTS). Explicit long/short legs.
  "explicit_pair_trade",
  // momentum_rotation strategy_mode: regime-gated long/flat rotation.
  "long_flat_momentum_rotation",
  // long_short_portfolio strategy_mode (REQUIRES SHORTS): long strongest,
  // short weakest by momentum.
  "long_short_momentum_rotation",
] as const;
export type StrategyFamily = (typeof STRATEGY_FAMILIES)[number];

// Families usable only under a specific strategy_mode. select_templates
// gates these the same way the SHORTS gate handles short families: a
// mode-only family is forced in for its mode and dropped everywhere else.
export const SINGLE_ASSET_FAMILY = "single_asset_trend_setup";
export const PAIR_TRADE_FAMILY = "explicit_pair_trade";
export const MOMENTUM_ROTATION_FAMILY = "long_flat_momentum_rotation";
export const LONG_SHORT_FAMILY = "long_short_momentum_rotation";

// Mode-only families, forced in by select_templates for their mode and
// dropped from every other shortlist.
export const MODE_ONLY_FAMILIES: ReadonlySet<string> = new Set([
  SINGLE_ASSET_FAMILY,
  PAIR_TRADE_FAMILY,
  MOMENTUM_ROTATION_FAMILY,
  LONG_SHORT_FAMILY,
]);

// Families that require short exposure. select_templates only shortlists
// these when the thesis permits shorts (allowed_sides === "long_short").
export const SHORT_REQUIRING_FAMILIES: ReadonlySet<StrategyFamily> = new Set([
  "partial_hedge_overlay",
  "trend_following_long_short",
  "relative_value_pair_trade",
  "beta_hedged_alt_exposure",
  "drawdown_based_hedge",
  "long_short_momentum_rotation",
  "explicit_pair_trade",
]);

export type SelectedFamily = {
  family: StrategyFamily;
  // 1-based rank; ranks across the shortlist must be unique and
  // contiguous starting at 1 (best fit first).
  rank: number;
  rationale: string;
};

// Output of select_templates: a ranked shortlist of strategy families
// that fit the thesis, with an overall rationale. propose_candidates
// reads this to bias which template configurations it parameterizes.
export type TemplateSelection = {
  rationale: string;
  selected: SelectedFamily[];
};

export type SelectTemplatesInput = {
  run_id: string;
  thesis: Thesis;
};

export type ReinterpretHint = {
  reason:
    | "constraints_infeasible"
    | "objective_mismatch"
    | "horizon_too_long";
  fields_to_revisit: Array<keyof Thesis>;
  rationale: string;
};

// Backward-edge hint when `decide` chooses `broaden_universe`. Each
// field tells `select_universe` how to loosen prior filters.
export type UniverseHint = {
  reason:
    | "too_narrow_after_filters"
    | "missing_sector"
    | "horizon_unsatisfiable";
  loosen: {
    raise_top_n_to?: number;
    drop_filter?: Array<"exclude_stablecoins" | "exclude_wrapped">;
    add_sectors?: string[];
    lower_market_cap_floor_to?: number;
    // Lower the per-coin history requirement (or set it to 0 to
    // disable). Useful when the thesis asked for long history but
    // there aren't enough candidates that satisfy it.
    lower_min_history_days_to?: number;
  };
  rationale: string;
};

// Resolved universe handed to subsequent steps. `effective_filters` is
// included so `decide` can see what was actually applied (vs what the
// thesis requested).
export type Universe = {
  coin_ids: string[];
  source: "rank_universe" | "hand_picked";
  effective_filters: {
    top_n: number;
    top_skip?: number;
    market_cap_min_usd?: number;
    // Minimum price-history length required of each candidate coin.
    // Derived from thesis.horizon_days so the universe is wide enough
    // to feed the recommender's target backtest window.
    min_history_days?: number;
    exclude_stablecoins: boolean;
    exclude_wrapped: boolean;
    risk_profile?: string;
    dropped_filters?: Array<"exclude_stablecoins" | "exclude_wrapped">;
  };
};

export type SelectUniverseInput = {
  run_id: string;
  thesis: Thesis;
  hint?: UniverseHint;
};

// Resolved backtest window. `horizon_days` preserves the *intended*
// trade horizon from the thesis (needed downstream by
// validate_against_thesis). `start`/`end` are the actual backtest
// window, which is typically longer in order to cover realistic
// drawdowns. `effective` exposes recommender diagnostics so `decide`
// can reason about why the window came out the way it did.
export type Window = {
  start: string; // YYYY-MM-DD
  end: string;   // YYYY-MM-DD
  horizon_days: number;
  effective: {
    window_length_days: number;
    target_window_length_days: number;
    rationale: string;
    limiting_coin?: string;
    // First/last price dates for the limiting coin. Surfaced so the
    // workflow (and the human reader of the run timeline) can see why
    // a particular coin is collapsing the window -- e.g. "the window
    // starts 2025-08-13 because crypto-com-chain first has prices on
    // that date". Lets `decide` reason concretely about which coin to
    // drop on a broaden_universe.
    limiting_coin_first_price_date?: string;
    limiting_coin_last_price_date?: string;
    // The common-history intersection across the requested coin set.
    // intersection_start equals max(first_price_date) over all coins;
    // intersection_end equals min(last_price_date).
    intersection_start?: string;
    intersection_end?: string;
    covered_drawdowns_count: number;
    strategy_window_mode?: "fixed_universe" | "dynamic_universe";
    window_coin_ids?: string[];
    excluded_window_coin_ids?: string[];
  };
};

export type SelectWindowInput = {
  run_id: string;
  thesis: Thesis;
  universe: Universe;
  template_selection?: TemplateSelection;
};

// Executable strategy families, each backed by a `bt` recipe in agent/scripts
// bt_templates. Phase 1 shipped the eight long-only families; Phase 2 adds the
// five short/hedge families (negative weights), which select_templates only
// shortlists when the thesis opts into shorts (the SHORTS gate).
export const ALLOCATION_TEMPLATES = [
  "synthetic_long_allocation",
  "periodic_rebalanced_allocation",
  "threshold_rebalanced_allocation",
  "core_satellite_allocation",
  "barbell_allocation",
  "volatility_targeted_exposure",
  "relative_momentum_rotation",
  "trend_following_long_neutral",
  "partial_hedge_overlay",
  "beta_hedged_alt_exposure",
  "relative_value_pair_trade",
  "trend_following_long_short",
  "drawdown_based_hedge",
  "single_asset_trend_setup",
  "explicit_pair_trade",
  "long_flat_momentum_rotation",
  "long_short_momentum_rotation",
] as const;
export type AllocationTemplate = (typeof ALLOCATION_TEMPLATES)[number];

// Families that re-select WHICH coins are held each period from the ranked
// universe. Their backtest window should not be forced to the common-history
// intersection of every eligible coin, because newly-listed coins can be
// ignored until they have enough signal history.
export const DYNAMIC_UNIVERSE_FAMILIES: ReadonlySet<AllocationTemplate> =
  new Set([
    "relative_momentum_rotation",
    "trend_following_long_neutral",
    "trend_following_long_short",
  ]);

// Families that accept a rebalance_trigger slot. synthetic_long (held once),
// trend_following (weekly by construction), pair-trade/drawdown-hedge (fixed
// schedule) do not.
export const REBALANCE_TRIGGER_FAMILIES = [
  "periodic_rebalanced_allocation",
  "threshold_rebalanced_allocation",
  "core_satellite_allocation",
  "barbell_allocation",
  "volatility_targeted_exposure",
  "relative_momentum_rotation",
  "partial_hedge_overlay",
  "beta_hedged_alt_exposure",
  "long_flat_momentum_rotation",
  "long_short_momentum_rotation",
] as const;
// Structural-slot families (core_weight; barbell additionally caps the sleeve).
export const CORE_WEIGHT_FAMILIES = [
  "core_satellite_allocation",
  "barbell_allocation",
] as const;

export const WEIGHTING_SCHEMES = [
  "equal",
  "cap",
  "vol_inverse",
  "ranking_proportional",
] as const;
export type WeightingScheme = (typeof WEIGHTING_SCHEMES)[number];

export const REBALANCE_TRIGGERS = [
  "periodic_30d",
  "periodic_90d",
  "threshold_drift_10pct",
] as const;
export type RebalanceTrigger = (typeof REBALANCE_TRIGGERS)[number];

export type ProposedCandidate = {
  candidate_id: string;
  template_id: AllocationTemplate;
  select_top: number;
  weighting: WeightingScheme;
  // Allowed only for REBALANCE_TRIGGER_FAMILIES; required for
  // periodic_rebalanced_allocation (its defining knob).
  rebalance_trigger?: RebalanceTrigger;
  // Structural slots for CORE_WEIGHT_FAMILIES. sleeve_cap is barbell-only.
  core_weight?: number;
  sleeve_cap?: number;
  // single_asset_trend_setup slots. sma_lookback is the trend-signal
  // window; target_coin_id pins the single market (else the recipe uses
  // the top-ranked coin). Forbidden on every other family.
  sma_lookback?: number;
  target_coin_id?: string;
  // explicit_pair_trade slots: the named long/short legs and the optional
  // hedge ratio sizing the short. Forbidden on every other family.
  long_coin_id?: string;
  short_coin_id?: string;
  hedge_ratio?: number;
  // Momentum-rotation slot (long_flat / long_short rotation): the trailing
  // return window used to rank the pool. Forbidden on every other family.
  momentum_lookback?: number;
  rationale: string;
};

export type Proposal = {
  iteration_hypothesis: string;
  candidates: ProposedCandidate[];
};

// Emitted by `decide` and consumed by the next propose_candidates call.
// Structured so the refinement is actionable, not narrative.
export type RefinementHint = {
  failed_constraints: Array<{
    // horizon_days intentionally absent: it is the forward-looking
    // holding period, not a per-candidate validation floor.
    constraint:
      | "max_weight_per_asset"
      | "max_drawdown"
      | "max_cash_weight"
      | "asset_count_min"
      | "asset_count_max"
      | "benchmark_underperformance";
    observed: number;
    target: number;
    candidate_id: string;
  }>;
  suggested_changes: {
    tighten_weight_cap_to?: number;
    increase_cash_to?: number;
    swap_assets?: { remove: string[]; consider: string[] };
    change_rebalance_to?: RebalanceTrigger;
    change_template_to?: AllocationTemplate;
  };
  rationale: string;
};

// Per-candidate backtest metrics, lifted from the run_candidate_batch
// result row. Retained (rather than discarded after the pass/fail gate)
// so `decide` can pick the genuinely-best passing candidate and the
// ranker can choose a closest-fit winner when nothing fully passes.
export type CandidateMetrics = {
  total_return: number;
  cagr: number;
  volatility: number;
  max_drawdown: number;
  sharpe: number;
  sortino: number;
  calmar: number;
  composite_score: number | null;
};

// One point on a backtest equity (or benchmark) curve, mirroring the
// Python run_candidate_batch output: portfolio value indexed by date,
// with the running drawdown at that point (<= 0, e.g. -0.25 for -25%).
export type EquityPoint = {
  date: string;
  value: number;
  drawdown_pct: number;
};

// One asset's target weight in the winner's allocation, lifted from the
// first rebalance in the backtest's holdings_history.
export type AllocationWeight = {
  coin_id: string;
  weight: number;
};

// A candidate's full backtest output: scalar metrics plus the per-day
// equity and benchmark curves. Captured in run_and_validate but kept on
// a side-channel (WorkflowState.backtests) rather than in the
// LLM-visible Attempt, because the curves are large and only the winner
// needs them. The winner's CandidateBacktest is attached to FinalWinner
// at finalize so the result card can show real numbers and plot the
// equity curve.
export type CandidateBacktest = {
  metrics: CandidateMetrics;
  equity_curve: EquityPoint[];
  benchmark_curve: EquityPoint[];
  // Target weights per asset (the first rebalance's allocation). Empty
  // when the backtest produced no holdings history.
  allocation: AllocationWeight[];
};

// One row per backtested candidate, carrying its gate result, how far
// it missed (0 when passing), and its metrics. This is the unit the
// cross-attempt ranker consumes.
export type CandidateOutcome = {
  candidate_id: string;
  passed: boolean;
  // 0 when passed. Otherwise the sum of normalized constraint overshoot
  // across this candidate's violations -- a single "distance from
  // satisfying the thesis" scalar the ranker minimizes.
  constraint_distance: number;
  metrics?: CandidateMetrics;
};

// Compact summary of what happened in a prior attempt. Avoid passing
// the full validation payload back through the LLM -- this is the
// pruned view propose_candidates needs.
export type AttemptValidationSummary = {
  passing_candidate_ids: string[];
  failing: Array<{
    candidate_id: string;
    violations: Array<{
      constraint: string;
      observed: number;
      target: number;
    }>;
  }>;
  // Every backtested candidate (passing and failing) with metrics and
  // constraint_distance. Populated by run_and_validate from the batch +
  // validation results; read by the ranker and surfaced to decide.
  candidates: CandidateOutcome[];
};

export type Attempt = {
  attempt_n: number;
  proposal: Proposal;
  batch_id?: string;
  validation_summary?: AttemptValidationSummary;
  refinement_hint?: RefinementHint;
  // Set by `decide` after validation. Lets re-entering steps
  // (propose_candidates, select_universe, interpret_brief) read what
  // was decided and route on the structured hint inside it.
  decision?: Decision;
};

export type ProposeCandidatesInput = {
  run_id: string;
  thesis: Thesis;
  universe: Universe;
  window: Window;
  // Ranked strategy-family shortlist from select_templates. Advisory:
  // biases which template configurations to parameterize.
  template_selection?: TemplateSelection;
  attempts?: Attempt[];
};

export type RunAndValidateInput = {
  run_id: string;
  thesis: Thesis;
  universe: Universe;
  window: Window;
  proposal: Proposal;
  attempts?: Attempt[];
};

// Per-edge backward-transition counters tracked across a single
// workflow run. The controller maintains these; `decide` reads them
// to know which backward edges are still permitted.
export type Counters = {
  reinterpret_brief: number;
  broaden_universe: number;
};

export type DecisionCaps = {
  max_attempts: number;
  max_broaden_universe: number;
  max_reinterpret_brief: number;
};

export const DEFAULT_DECISION_CAPS: DecisionCaps = {
  max_attempts: 3,
  max_broaden_universe: 1,
  max_reinterpret_brief: 1,
};

// Discriminated union over the five possible decide outcomes. Each
// variant carries the structured payload that the destination step
// will consume on the next visit.
export type Decision =
  | {
      action: "stop_winner";
      winner_candidate_id: string;
      justification: string;
    }
  // Normal give-up: nothing fully satisfied the thesis, but the run still
  // produced at least one backtested candidate. The deterministic ranker
  // selects the closest-fit winner downstream; the LLM only supplies the
  // reasons it could not fully satisfy the brief.
  | { action: "stop_best_effort"; reasons: string[] }
  // Hard failure only: no candidate ever produced a usable backtest, so
  // there is literally nothing to show. Not offered to the LLM as a
  // normal choice -- reserved for the decide parse-failure fallback and
  // the controller's cap/error short-circuit.
  | { action: "stop_no_viable"; reasons: string[] }
  | { action: "refine_candidates"; hint: RefinementHint }
  | { action: "broaden_universe"; hint: UniverseHint }
  | { action: "reinterpret_brief"; hint: ReinterpretHint };

export type DecideInput = {
  run_id: string;
  thesis: Thesis;
  attempts: Attempt[];
  counters: Counters;
  caps?: Partial<DecisionCaps>;
};

// User-facing narrative emitted by `finalize`. Structured fields are
// what the LLM controls; the surrounding FinalWinner record is
// assembled deterministically from workflow state.
export type FinalizeNarrative = {
  title: string;
  summary: string;
  reasoning: string;
  assumptions: string[];
  risks: string[];
  next_steps: string[];
};

export type FinalWinner = {
  kind: "winner";
  run_id: string;
  winner_candidate_id: string;
  // Which attempt the winner came from. Needed because a best-effort
  // winner can be from an earlier attempt and candidate_id is only
  // unique within a batch, so locating the candidate requires both.
  winner_attempt_n: number;
  candidate_batch_id: string;
  // True when no candidate fully satisfied the thesis and this is the
  // closest-fit fallback. False for a candidate that passed the gate.
  is_best_effort: boolean;
  // Constraints the winner did NOT satisfy. Empty for a full winner;
  // populated for a best-effort winner so the narrative can disclose
  // what it missed and by how much.
  unmet_constraints: Array<{
    constraint: string;
    observed: number;
    target: number;
  }>;
  thesis: Thesis;
  universe: Universe;
  window: Window;
  attempts_summary: Attempt[];
  narrative: FinalizeNarrative;
  // The winner's full backtest (metrics + equity/benchmark curves),
  // resolved from WorkflowState.backtests at finalize. Optional because
  // an older state or a winner whose curves were not captured still
  // produces a valid (curve-less) record.
  winner_backtest?: CandidateBacktest;
};

export type FinalNoViable = {
  kind: "no_viable_strategy";
  run_id: string;
  thesis?: Thesis;
  reasons: string[];
  attempts_summary: Attempt[];
};

export type Final = FinalWinner | FinalNoViable;

// Hard global caps for a single workflow run. The controller checks
// each before dispatching the next step and short-circuits to a
// FinalNoViable with a `budget_exhausted: <which>` reason if any
// trips. Separate from `DecisionCaps` (per-edge caps used by `decide`).
export type WorkflowCaps = {
  max_step_transitions: number;
  max_llm_calls: number;
  max_wall_clock_ms: number;
};

export const DEFAULT_WORKFLOW_CAPS: WorkflowCaps = {
  max_step_transitions: 25,
  max_llm_calls: 12,
  max_wall_clock_ms: 15 * 60 * 1000,
};

// Bumped whenever a change could alter the mandate produced from the same
// typed inputs + data snapshot (new steps, changed sweep/validation, new
// override semantics). Persisted on every run so a `based_on_run_id`
// rerun can be checked for reproducibility against its parent.
export const WORKFLOW_VERSION = "2026-06-09.1";

// Full workflow state. The controller maintains one of these per run
// and passes it (read-only) into each step; deltas are merged into
// state by the dispatcher rather than by the step itself.
export type WorkflowState = {
  run_id: string;
  brief: string | WizardBrief;
  // Provenance + reproducibility (PR1). workflow_version is stamped at
  // run start; overrides/based_on_run_id/data_as_of carry the rerun
  // inputs so the persisted run records exactly what reshaped it.
  workflow_version: string;
  overrides?: StrategyRunOverrides;
  based_on_run_id?: string;
  data_as_of?: string;
  thesis?: Thesis;
  template_selection?: TemplateSelection;
  universe?: Universe;
  window?: Window;
  attempts: Attempt[];
  counters: Counters;
  // Per-candidate full backtest output (metrics + equity/benchmark
  // curves), keyed by `${attempt_n}:${candidate_id}`. A side-channel
  // populated by run_and_validate and read only at finalize to attach
  // the winner's curve -- deliberately NOT part of Attempt so the large
  // curves never enter LLM context or the persisted round history.
  backtests?: Record<string, CandidateBacktest>;
  final?: Final;
};

// Input for the LLM finalize step. Invoked when `decide` chose
// stop_winner (a passing candidate in the latest attempt) or
// stop_best_effort (the ranker's closest-fit candidate, which may be in
// an earlier attempt). winner_attempt_n locates the winner; is_best_effort
// tells the step whether to narrate it as a full match or a closest fit.
export type FinalizeInput = {
  run_id: string;
  thesis: Thesis;
  universe: Universe;
  window: Window;
  attempts: Attempt[];
  winner_candidate_id: string;
  winner_attempt_n: number;
  is_best_effort: boolean;
  decide_justification: string;
  // The winner's full backtest, resolved by the controller from
  // WorkflowState.backtests and threaded through so finalize can attach
  // it to the FinalWinner record.
  winner_backtest?: CandidateBacktest;
};

export class FinalizeValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FinalizeValidationError";
  }
}

export function validateFinalizeNarrative(
  value: unknown,
): asserts value is FinalizeNarrative {
  if (!isRecord(value)) {
    throw new FinalizeValidationError("narrative must be an object");
  }
  requireFinalizeString(value.title, "title");
  requireFinalizeString(value.summary, "summary");
  requireFinalizeString(value.reasoning, "reasoning");
  requireFinalizeStringArray(value.assumptions, "assumptions");
  requireFinalizeStringArray(value.risks, "risks");
  requireFinalizeStringArray(value.next_steps, "next_steps");
}

function requireFinalizeString(value: unknown, field: string) {
  if (typeof value !== "string" || !value.trim()) {
    throw new FinalizeValidationError(
      `${field} must be a non-empty string`,
    );
  }
}

function requireFinalizeStringArray(value: unknown, field: string) {
  if (!Array.isArray(value) || value.length === 0) {
    throw new FinalizeValidationError(
      `${field} must be a non-empty array`,
    );
  }
  for (const entry of value) {
    if (typeof entry !== "string" || !entry.trim()) {
      throw new FinalizeValidationError(
        `${field} entries must be non-empty strings`,
      );
    }
  }
}

export class DecisionValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DecisionValidationError";
  }
}

export function validateDecision(
  value: unknown,
  context: {
    attempts: Attempt[];
    counters: Counters;
    caps: DecisionCaps;
  },
): asserts value is Decision {
  if (!isRecord(value)) {
    throw new DecisionValidationError("decision must be an object");
  }
  const action = value.action;
  if (typeof action !== "string") {
    throw new DecisionValidationError("decision.action must be a string");
  }

  switch (action) {
    case "stop_winner":
      validateStopWinner(value, context);
      return;
    case "stop_best_effort":
      validateStopReasons(value, "stop_best_effort");
      return;
    case "stop_no_viable":
      validateStopReasons(value, "stop_no_viable");
      return;
    case "refine_candidates":
      validateRefineCandidates(value, context);
      return;
    case "broaden_universe":
      validateBroadenUniverse(value, context);
      return;
    case "reinterpret_brief":
      validateReinterpretBrief(value, context);
      return;
    default:
      throw new DecisionValidationError(
        `decision.action must be one of: stop_winner, stop_best_effort, stop_no_viable, refine_candidates, broaden_universe, reinterpret_brief (got "${action}")`,
      );
  }
}

function validateStopWinner(
  value: Record<string, unknown>,
  context: { attempts: Attempt[] },
): void {
  requireString(value.winner_candidate_id, "winner_candidate_id");
  requireString(value.justification, "justification");

  const latest = context.attempts.at(-1);
  const passingIds = latest?.validation_summary?.passing_candidate_ids ?? [];
  if (!passingIds.includes(value.winner_candidate_id as string)) {
    throw new DecisionValidationError(
      `winner_candidate_id "${value.winner_candidate_id as string}" is not in the latest attempt's passing_candidate_ids [${passingIds.join(", ")}]`,
    );
  }
}

function validateStopReasons(
  value: Record<string, unknown>,
  action: "stop_best_effort" | "stop_no_viable",
): void {
  if (
    !Array.isArray(value.reasons) ||
    value.reasons.length === 0 ||
    value.reasons.some((reason) => typeof reason !== "string" || !reason.trim())
  ) {
    throw new DecisionValidationError(
      `${action}.reasons must be a non-empty array of non-empty strings`,
    );
  }
}

function validateRefineCandidates(
  value: Record<string, unknown>,
  context: { attempts: Attempt[]; caps: DecisionCaps },
): void {
  if (context.attempts.length >= context.caps.max_attempts) {
    throw new DecisionValidationError(
      `refine_candidates not allowed: attempts (${context.attempts.length}) already at max_attempts (${context.caps.max_attempts})`,
    );
  }
  validateRefinementHint(value.hint);
}

function validateBroadenUniverse(
  value: Record<string, unknown>,
  context: { counters: Counters; caps: DecisionCaps },
): void {
  if (
    context.counters.broaden_universe >= context.caps.max_broaden_universe
  ) {
    throw new DecisionValidationError(
      `broaden_universe not allowed: counter (${context.counters.broaden_universe}) already at max (${context.caps.max_broaden_universe})`,
    );
  }
  validateUniverseHint(value.hint);
}

function validateReinterpretBrief(
  value: Record<string, unknown>,
  context: { counters: Counters; caps: DecisionCaps },
): void {
  if (
    context.counters.reinterpret_brief >= context.caps.max_reinterpret_brief
  ) {
    throw new DecisionValidationError(
      `reinterpret_brief not allowed: counter (${context.counters.reinterpret_brief}) already at max (${context.caps.max_reinterpret_brief})`,
    );
  }
  validateReinterpretHint(value.hint);
}

function validateRefinementHint(hint: unknown): void {
  if (!isRecord(hint)) {
    throw new DecisionValidationError("hint must be an object");
  }
  if (
    !Array.isArray(hint.failed_constraints) ||
    hint.failed_constraints.length === 0
  ) {
    throw new DecisionValidationError(
      "hint.failed_constraints must be a non-empty array",
    );
  }
  for (const fc of hint.failed_constraints) {
    if (!isRecord(fc)) {
      throw new DecisionValidationError(
        "hint.failed_constraints entries must be objects",
      );
    }
    requireString(fc.constraint, "failed_constraints[].constraint");
    requireString(fc.candidate_id, "failed_constraints[].candidate_id");
    if (typeof fc.observed !== "number" || typeof fc.target !== "number") {
      throw new DecisionValidationError(
        "failed_constraints[].observed and .target must be numbers",
      );
    }
  }
  if (!isRecord(hint.suggested_changes)) {
    throw new DecisionValidationError(
      "hint.suggested_changes must be an object (may be empty)",
    );
  }
  requireString(hint.rationale, "hint.rationale");
}

function validateUniverseHint(hint: unknown): void {
  if (!isRecord(hint)) {
    throw new DecisionValidationError("hint must be an object");
  }
  const reasons = [
    "too_narrow_after_filters",
    "missing_sector",
    "horizon_unsatisfiable",
  ] as const;
  requireEnum(hint.reason, reasons, "hint.reason");
  if (!isRecord(hint.loosen)) {
    throw new DecisionValidationError("hint.loosen must be an object");
  }
  requireString(hint.rationale, "hint.rationale");
}

function validateReinterpretHint(hint: unknown): void {
  if (!isRecord(hint)) {
    throw new DecisionValidationError("hint must be an object");
  }
  const reasons = [
    "constraints_infeasible",
    "objective_mismatch",
    "horizon_too_long",
  ] as const;
  requireEnum(hint.reason, reasons, "hint.reason");
  if (
    !Array.isArray(hint.fields_to_revisit) ||
    hint.fields_to_revisit.length === 0
  ) {
    throw new DecisionValidationError(
      "hint.fields_to_revisit must be a non-empty array",
    );
  }
  requireString(hint.rationale, "hint.rationale");
}

export class TemplateSelectionValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TemplateSelectionValidationError";
  }
}

// Pure validation for select_templates output. Throws on any schema or
// structural violation (unknown family, duplicate/non-contiguous ranks,
// shortlist out of bounds). After this returns, value is a
// TemplateSelection.
export function validateTemplateSelection(
  value: unknown,
): asserts value is TemplateSelection {
  if (!isRecord(value)) {
    throw new TemplateSelectionValidationError(
      "template_selection must be an object",
    );
  }
  if (
    typeof value.rationale !== "string" ||
    !value.rationale.trim()
  ) {
    throw new TemplateSelectionValidationError(
      "rationale must be a non-empty string",
    );
  }
  if (!Array.isArray(value.selected)) {
    throw new TemplateSelectionValidationError("selected must be an array");
  }
  if (value.selected.length < 1 || value.selected.length > 3) {
    throw new TemplateSelectionValidationError(
      `selected must contain between 1 and 3 entries (got ${value.selected.length})`,
    );
  }

  const seenFamilies = new Set<string>();
  const seenRanks = new Set<number>();
  for (const [index, entry] of value.selected.entries()) {
    if (!isRecord(entry)) {
      throw new TemplateSelectionValidationError(
        `selected[${index}] must be an object`,
      );
    }
    if (
      typeof entry.family !== "string" ||
      !STRATEGY_FAMILIES.includes(entry.family as StrategyFamily)
    ) {
      throw new TemplateSelectionValidationError(
        `selected[${index}].family must be one of: ${STRATEGY_FAMILIES.join(", ")}`,
      );
    }
    if (seenFamilies.has(entry.family)) {
      throw new TemplateSelectionValidationError(
        `selected[${index}].family "${entry.family}" is duplicated`,
      );
    }
    seenFamilies.add(entry.family);

    if (
      typeof entry.rank !== "number" ||
      !Number.isInteger(entry.rank) ||
      entry.rank < 1
    ) {
      throw new TemplateSelectionValidationError(
        `selected[${index}].rank must be a positive integer`,
      );
    }
    if (seenRanks.has(entry.rank)) {
      throw new TemplateSelectionValidationError(
        `selected[${index}].rank ${entry.rank} is duplicated`,
      );
    }
    seenRanks.add(entry.rank);

    if (typeof entry.rationale !== "string" || !entry.rationale.trim()) {
      throw new TemplateSelectionValidationError(
        `selected[${index}].rationale must be a non-empty string`,
      );
    }
  }

  // Ranks must be the contiguous set {1..n} so the shortlist is a clean
  // ordering with no gaps.
  for (let rank = 1; rank <= value.selected.length; rank += 1) {
    if (!seenRanks.has(rank)) {
      throw new TemplateSelectionValidationError(
        `ranks must be contiguous from 1 to ${value.selected.length}; missing rank ${rank}`,
      );
    }
  }
}

export class ProposalValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProposalValidationError";
  }
}

export function validateProposal(
  value: unknown,
  context: { thesis: Thesis; universe: Universe },
): asserts value is Proposal {
  if (!isRecord(value)) {
    throw new ProposalValidationError("proposal must be an object");
  }
  requireString(
    value.iteration_hypothesis,
    "iteration_hypothesis",
  );
  if (!Array.isArray(value.candidates)) {
    throw new ProposalValidationError("candidates must be an array");
  }
  // run_candidate_batch (the downstream Python CLI) requires >= 3
  // candidates per batch and defaults to a max of 8. The workflow allows up to 8 because
  // propose_candidates expands the LLM's small seed shortlist into a
  // bounded deterministic parameter sweep before backtesting.
  if (value.candidates.length < 3 || value.candidates.length > 8) {
    throw new ProposalValidationError(
      `candidates must contain between 3 and 8 entries (got ${value.candidates.length})`,
    );
  }

  const seenIds = new Set<string>();
  const minTop = context.thesis.constraints.asset_count_min;
  const maxTop = Math.min(
    context.thesis.constraints.asset_count_max,
    context.universe.coin_ids.length,
  );
  if (maxTop < minTop) {
    throw new ProposalValidationError(
      `universe (${context.universe.coin_ids.length} coins) cannot satisfy asset_count_min=${minTop}; broaden universe first`,
    );
  }

  for (const [index, candidate] of value.candidates.entries()) {
    validateCandidate(candidate, index, {
      seenIds,
      minTop,
      maxTop,
    });
  }
}

function validateCandidate(
  candidate: unknown,
  index: number,
  ctx: { seenIds: Set<string>; minTop: number; maxTop: number },
): asserts candidate is ProposedCandidate {
  if (!isRecord(candidate)) {
    throw new ProposalValidationError(`candidates[${index}] must be an object`);
  }
  requireString(candidate.candidate_id, `candidates[${index}].candidate_id`);
  if (ctx.seenIds.has(candidate.candidate_id as string)) {
    throw new ProposalValidationError(
      `candidate_id "${candidate.candidate_id as string}" is not unique`,
    );
  }
  ctx.seenIds.add(candidate.candidate_id as string);

  requireEnum(
    candidate.template_id,
    ALLOCATION_TEMPLATES,
    `candidates[${index}].template_id`,
  );
  requireEnum(
    candidate.weighting,
    WEIGHTING_SCHEMES,
    `candidates[${index}].weighting`,
  );
  requireFiniteInteger(
    candidate.select_top,
    `candidates[${index}].select_top`,
  );
  const selectTop = candidate.select_top as number;
  if (selectTop < ctx.minTop || selectTop > ctx.maxTop) {
    throw new ProposalValidationError(
      `candidates[${index}].select_top (${selectTop}) must be within [${ctx.minTop}, ${ctx.maxTop}]`,
    );
  }

  requireString(candidate.rationale, `candidates[${index}].rationale`);

  const family = candidate.template_id as AllocationTemplate;
  const allowsTrigger = (
    REBALANCE_TRIGGER_FAMILIES as readonly string[]
  ).includes(family);
  if (family === "periodic_rebalanced_allocation") {
    requireEnum(
      candidate.rebalance_trigger,
      REBALANCE_TRIGGERS,
      `candidates[${index}].rebalance_trigger`,
    );
  } else if (candidate.rebalance_trigger !== undefined) {
    if (!allowsTrigger) {
      throw new ProposalValidationError(
        `candidates[${index}]: rebalance_trigger is not allowed on template "${family}"`,
      );
    }
    requireEnum(
      candidate.rebalance_trigger,
      REBALANCE_TRIGGERS,
      `candidates[${index}].rebalance_trigger`,
    );
  }

  const allowsCoreWeight = (
    CORE_WEIGHT_FAMILIES as readonly string[]
  ).includes(family);
  if (candidate.core_weight !== undefined) {
    if (!allowsCoreWeight) {
      throw new ProposalValidationError(
        `candidates[${index}]: core_weight is not allowed on template "${family}"`,
      );
    }
    requireFiniteNumber(candidate.core_weight, `candidates[${index}].core_weight`);
    if ((candidate.core_weight as number) <= 0 || (candidate.core_weight as number) >= 1) {
      throw new ProposalValidationError(
        `candidates[${index}].core_weight must be in (0, 1)`,
      );
    }
  }
  if (candidate.sleeve_cap !== undefined) {
    if (family !== "barbell_allocation") {
      throw new ProposalValidationError(
        `candidates[${index}]: sleeve_cap is only allowed on barbell_allocation`,
      );
    }
    requireFiniteNumber(candidate.sleeve_cap, `candidates[${index}].sleeve_cap`);
    if ((candidate.sleeve_cap as number) <= 0 || (candidate.sleeve_cap as number) >= 1) {
      throw new ProposalValidationError(
        `candidates[${index}].sleeve_cap must be in (0, 1)`,
      );
    }
  }

  // single_asset_trend_setup slots. sma_lookback / target_coin_id only
  // make sense for the single-asset family; for it, select_top must be 1
  // (one position is the whole book).
  if (candidate.sma_lookback !== undefined) {
    if (family !== SINGLE_ASSET_FAMILY) {
      throw new ProposalValidationError(
        `candidates[${index}]: sma_lookback is only allowed on ${SINGLE_ASSET_FAMILY}`,
      );
    }
    requireFiniteInteger(candidate.sma_lookback, `candidates[${index}].sma_lookback`);
    const lookback = candidate.sma_lookback as number;
    if (lookback < 2 || lookback > 400) {
      throw new ProposalValidationError(
        `candidates[${index}].sma_lookback must be in [2, 400]`,
      );
    }
  }
  if (candidate.target_coin_id !== undefined) {
    if (family !== SINGLE_ASSET_FAMILY) {
      throw new ProposalValidationError(
        `candidates[${index}]: target_coin_id is only allowed on ${SINGLE_ASSET_FAMILY}`,
      );
    }
    requireString(candidate.target_coin_id, `candidates[${index}].target_coin_id`);
  }
  if (family === SINGLE_ASSET_FAMILY && selectTop !== 1) {
    throw new ProposalValidationError(
      `candidates[${index}]: ${SINGLE_ASSET_FAMILY} requires select_top = 1 (got ${selectTop})`,
    );
  }

  // explicit_pair_trade slots: named long/short legs and an optional hedge
  // ratio. The two legs span exactly two markets, so select_top must be 2.
  for (const legField of ["long_coin_id", "short_coin_id"] as const) {
    if (candidate[legField] !== undefined) {
      if (family !== PAIR_TRADE_FAMILY) {
        throw new ProposalValidationError(
          `candidates[${index}]: ${legField} is only allowed on ${PAIR_TRADE_FAMILY}`,
        );
      }
      requireString(candidate[legField], `candidates[${index}].${legField}`);
    }
  }
  if (candidate.hedge_ratio !== undefined) {
    if (family !== PAIR_TRADE_FAMILY) {
      throw new ProposalValidationError(
        `candidates[${index}]: hedge_ratio is only allowed on ${PAIR_TRADE_FAMILY}`,
      );
    }
    requireFiniteNumber(candidate.hedge_ratio, `candidates[${index}].hedge_ratio`);
    const ratio = candidate.hedge_ratio as number;
    if (ratio < 0 || ratio > 2) {
      throw new ProposalValidationError(
        `candidates[${index}].hedge_ratio must be in [0, 2]`,
      );
    }
  }
  if (family === PAIR_TRADE_FAMILY) {
    if (selectTop !== 2) {
      throw new ProposalValidationError(
        `candidates[${index}]: ${PAIR_TRADE_FAMILY} requires select_top = 2 (got ${selectTop})`,
      );
    }
    if (
      candidate.long_coin_id === undefined ||
      candidate.short_coin_id === undefined
    ) {
      throw new ProposalValidationError(
        `candidates[${index}]: ${PAIR_TRADE_FAMILY} requires long_coin_id and short_coin_id`,
      );
    }
    if (candidate.long_coin_id === candidate.short_coin_id) {
      throw new ProposalValidationError(
        `candidates[${index}]: long_coin_id and short_coin_id must differ`,
      );
    }
  }

  // Momentum-rotation slot. Only the rotation families accept it.
  if (candidate.momentum_lookback !== undefined) {
    if (
      family !== MOMENTUM_ROTATION_FAMILY &&
      family !== LONG_SHORT_FAMILY
    ) {
      throw new ProposalValidationError(
        `candidates[${index}]: momentum_lookback is only allowed on the momentum rotation families`,
      );
    }
    requireFiniteInteger(
      candidate.momentum_lookback,
      `candidates[${index}].momentum_lookback`,
    );
    const lookback = candidate.momentum_lookback as number;
    if (lookback < 2 || lookback > 400) {
      throw new ProposalValidationError(
        `candidates[${index}].momentum_lookback must be in [2, 400]`,
      );
    }
  }
}

// Wizard brief stays loosely typed until we re-wire it in the controller.
export type WizardBrief = Record<string, unknown>;

export type InterpretBriefInput = {
  run_id: string;
  brief: string | WizardBrief;
  hint?: ReinterpretHint;
};

export class ThesisValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ThesisValidationError";
  }
}

// Pure validation. Throws ThesisValidationError on any schema or
// feasibility violation. After this returns, the value is structurally
// a Thesis and satisfies the cross-field feasibility rules.
export function validateThesis(value: unknown): asserts value is Thesis {
  if (!isRecord(value)) {
    throw new ThesisValidationError("thesis must be an object");
  }

  const objectives: Objective[] = [
    "balanced_growth",
    "growth",
    "income",
    "preserve_capital",
  ];
  requireEnum(value.objective, objectives, "objective");

  const frequencies: RebalanceFrequency[] = [
    "daily",
    "weekly",
    "monthly",
    "quarterly",
  ];
  requireEnum(value.rebalance_frequency, frequencies, "rebalance_frequency");

  const weightModes: WeightMode[] = ["percentage", "dollar"];
  requireEnum(value.weight_mode, weightModes, "weight_mode");

  requireFiniteInteger(value.horizon_days, "horizon_days");
  if ((value.horizon_days as number) < 30) {
    throw new ThesisValidationError("horizon_days must be >= 30");
  }

  requireString(value.interpretation_notes, "interpretation_notes");

  if (value.strategy_mode !== undefined) {
    requireEnum(value.strategy_mode, STRATEGY_MODES, "strategy_mode");
  }
  if (value.allowed_sides !== undefined) {
    requireEnum(value.allowed_sides, ALLOWED_SIDES, "allowed_sides");
  }
  if (value.execution_mode !== undefined) {
    requireEnum(value.execution_mode, EXECUTION_MODES, "execution_mode");
  }
  if (value.target_coin_id !== undefined) {
    requireString(value.target_coin_id, "target_coin_id");
  }
  optionalStringArray(value.long_coin_ids, "long_coin_ids");
  optionalStringArray(value.short_coin_ids, "short_coin_ids");

  validateUniverseHints(value.universe_hints);
  validateConstraints(value.constraints);
  validateFeasibility(
    value.constraints as ThesisConstraints,
    value.universe_hints as UniverseHints,
    (value.strategy_mode as StrategyMode | undefined) ?? DEFAULT_STRATEGY_MODE,
  );
}

function validateUniverseHints(
  value: unknown,
): asserts value is UniverseHints {
  if (!isRecord(value)) {
    throw new ThesisValidationError("universe_hints must be an object");
  }
  requireFiniteInteger(value.top_n, "universe_hints.top_n");
  if ((value.top_n as number) < 1) {
    throw new ThesisValidationError("universe_hints.top_n must be >= 1");
  }
  if (value.top_skip !== undefined) {
    requireFiniteInteger(value.top_skip, "universe_hints.top_skip");
    if ((value.top_skip as number) < 0) {
      throw new ThesisValidationError(
        "universe_hints.top_skip must be >= 0",
      );
    }
    if ((value.top_skip as number) >= (value.top_n as number)) {
      throw new ThesisValidationError(
        `universe_hints.top_skip (${value.top_skip as number}) must be < top_n (${value.top_n as number})`,
      );
    }
  }
  requireBoolean(
    value.exclude_stablecoins,
    "universe_hints.exclude_stablecoins",
  );
  requireBoolean(value.exclude_wrapped, "universe_hints.exclude_wrapped");

  if (value.market_cap_min_usd !== undefined) {
    requireFiniteNumber(
      value.market_cap_min_usd,
      "universe_hints.market_cap_min_usd",
    );
  }
  if (value.min_history_days !== undefined) {
    requireFiniteInteger(
      value.min_history_days,
      "universe_hints.min_history_days",
    );
    if ((value.min_history_days as number) < 0) {
      throw new ThesisValidationError(
        "universe_hints.min_history_days must be >= 0",
      );
    }
  }
  optionalStringArray(value.sectors_include, "universe_hints.sectors_include");
  optionalStringArray(value.sectors_exclude, "universe_hints.sectors_exclude");
  optionalStringArray(
    value.hand_picked_coin_ids,
    "universe_hints.hand_picked_coin_ids",
  );
}

function validateConstraints(
  value: unknown,
): asserts value is ThesisConstraints {
  if (!isRecord(value)) {
    throw new ThesisValidationError("constraints must be an object");
  }
  for (const field of [
    "max_weight_per_asset",
    "max_cash_weight",
    "max_drawdown",
  ] as const) {
    requireFiniteNumber(value[field], `constraints.${field}`);
    const v = value[field] as number;
    if (v < 0 || v > 1) {
      throw new ThesisValidationError(
        `constraints.${field} must be between 0 and 1`,
      );
    }
  }
  for (const field of ["asset_count_min", "asset_count_max"] as const) {
    requireFiniteInteger(value[field], `constraints.${field}`);
    if ((value[field] as number) < 1) {
      throw new ThesisValidationError(`constraints.${field} must be >= 1`);
    }
  }
  // Optional exposure ceilings for short-bearing books. Allow up to 5x
  // gross/net leverage and a 2x per-leg cap -- generous bounds; the real
  // limits come from the thesis values, this just rejects nonsense.
  for (const [field, max] of [
    ["max_gross_exposure", 5],
    ["max_net_exposure", 5],
    ["max_leg_weight", 2],
  ] as const) {
    if (value[field] !== undefined) {
      requireFiniteNumber(value[field], `constraints.${field}`);
      const v = value[field] as number;
      if (v < 0 || v > max) {
        throw new ThesisValidationError(
          `constraints.${field} must be between 0 and ${max}`,
        );
      }
    }
  }
  // Net beta can be negative (a net-short book), so it has a signed range.
  if (value.target_net_beta !== undefined) {
    requireFiniteNumber(value.target_net_beta, "constraints.target_net_beta");
    const v = value.target_net_beta as number;
    if (v < -2 || v > 2) {
      throw new ThesisValidationError(
        "constraints.target_net_beta must be between -2 and 2",
      );
    }
  }
}

function validateFeasibility(
  constraints: ThesisConstraints,
  universe: UniverseHints,
  mode: StrategyMode,
) {
  if (constraints.asset_count_min > constraints.asset_count_max) {
    throw new ThesisValidationError(
      "constraints.asset_count_min must be <= asset_count_max",
    );
  }
  // single_asset is a single position that fills the whole non-cash book,
  // so the basket coverage inequality below does not apply -- it would
  // otherwise reject a 1-asset thesis whose per-asset cap is < 100%.
  // Require the count to actually be 1 so the mode and the constraints agree.
  if (mode === "single_asset") {
    if (
      constraints.asset_count_min !== 1 ||
      constraints.asset_count_max !== 1
    ) {
      throw new ThesisValidationError(
        "single_asset strategy_mode requires asset_count_min == asset_count_max == 1",
      );
    }
  } else if (mode === "pair_trade") {
    // A pair is exactly two legs (+1 long / -hedge short). The long-book
    // coverage inequality does not apply; gross/net exposure is validated
    // against the backtest at run time, not here.
    if (
      constraints.asset_count_min !== 2 ||
      constraints.asset_count_max !== 2
    ) {
      throw new ThesisValidationError(
        "pair_trade strategy_mode requires asset_count_min == asset_count_max == 2",
      );
    }
  } else if (
    mode === "momentum_rotation" ||
    mode === "long_short_portfolio"
  ) {
    // Rotation books hold a subset of a ranked pool (long/flat) or both
    // sides of it (long/short), so the fully-invested long-basket coverage
    // inequality does not apply. They just need a pool big enough to rank:
    // long/short needs both sides, so it wants more names.
    const minPool = mode === "long_short_portfolio" ? 4 : 3;
    if (constraints.asset_count_min < minPool) {
      throw new ThesisValidationError(
        `${mode} strategy_mode requires asset_count_min >= ${minPool} (a pool to rotate within)`,
      );
    }
  } else {
    // Need enough cap room to fill the non-cash portion across at least asset_count_min assets.
    const maxAssetCoverage =
      constraints.max_weight_per_asset * constraints.asset_count_min;
    const requiredAssetCoverage = 1 - constraints.max_cash_weight;
    if (maxAssetCoverage < requiredAssetCoverage - 1e-6) {
      throw new ThesisValidationError(
        `infeasible: max_weight_per_asset * asset_count_min (${maxAssetCoverage.toFixed(
          4,
        )}) < 1 - max_cash_weight (${requiredAssetCoverage.toFixed(4)})`,
      );
    }
  }
  if (
    universe.hand_picked_coin_ids &&
    universe.hand_picked_coin_ids.length > 0
  ) {
    const n = universe.hand_picked_coin_ids.length;
    if (
      n < constraints.asset_count_min ||
      n > constraints.asset_count_max
    ) {
      throw new ThesisValidationError(
        `hand_picked_coin_ids length (${n}) must be within [asset_count_min=${constraints.asset_count_min}, asset_count_max=${constraints.asset_count_max}]`,
      );
    }
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requireString(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string" || !value.trim()) {
    throw new ThesisValidationError(`${field} must be a non-empty string`);
  }
}

function requireBoolean(
  value: unknown,
  field: string,
): asserts value is boolean {
  if (typeof value !== "boolean") {
    throw new ThesisValidationError(`${field} must be a boolean`);
  }
}

function requireFiniteNumber(
  value: unknown,
  field: string,
): asserts value is number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new ThesisValidationError(`${field} must be a finite number`);
  }
}

function requireFiniteInteger(
  value: unknown,
  field: string,
): asserts value is number {
  requireFiniteNumber(value, field);
  if (!Number.isInteger(value as number)) {
    throw new ThesisValidationError(`${field} must be an integer`);
  }
}

function requireEnum<T extends string>(
  value: unknown,
  values: readonly T[],
  field: string,
): asserts value is T {
  if (typeof value !== "string" || !values.includes(value as T)) {
    throw new ThesisValidationError(
      `${field} must be one of: ${values.join(", ")}`,
    );
  }
}

function optionalStringArray(value: unknown, field: string) {
  if (value === undefined) return;
  if (!Array.isArray(value) || value.some((v) => typeof v !== "string")) {
    throw new ThesisValidationError(`${field} must be an array of strings`);
  }
}
