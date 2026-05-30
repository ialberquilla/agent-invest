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
};

export type Thesis = {
  objective: Objective;
  horizon_days: number;
  weight_mode: WeightMode;
  universe_hints: UniverseHints;
  constraints: ThesisConstraints;
  rebalance_frequency: RebalanceFrequency;
  interpretation_notes: string;
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
] as const;
export type StrategyFamily = (typeof STRATEGY_FAMILIES)[number];

// Families that require short exposure. Until the Thesis carries an
// explicit allowed_sides field, select_templates only shortlists these
// when the brief/interpretation_notes opt into shorts or hedging.
export const SHORT_REQUIRING_FAMILIES: ReadonlySet<StrategyFamily> = new Set([
  "partial_hedge_overlay",
  "trend_following_long_short",
  "relative_value_pair_trade",
  "beta_hedged_alt_exposure",
  "drawdown_based_hedge",
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
  };
};

export type SelectWindowInput = {
  run_id: string;
  thesis: Thesis;
  universe: Universe;
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
] as const;
export type AllocationTemplate = (typeof ALLOCATION_TEMPLATES)[number];

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
  candidate_batch_id: string;
  thesis: Thesis;
  universe: Universe;
  window: Window;
  attempts_summary: Attempt[];
  narrative: FinalizeNarrative;
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

// Full workflow state. The controller maintains one of these per run
// and passes it (read-only) into each step; deltas are merged into
// state by the dispatcher rather than by the step itself.
export type WorkflowState = {
  run_id: string;
  brief: string | WizardBrief;
  thesis?: Thesis;
  template_selection?: TemplateSelection;
  universe?: Universe;
  window?: Window;
  attempts: Attempt[];
  counters: Counters;
  final?: Final;
};

// Input for the LLM finalize step. The step is only invoked when
// `decide` chose stop_winner, so the winner pointer is known and
// guaranteed to be in the latest attempt's passing set.
export type FinalizeInput = {
  run_id: string;
  thesis: Thesis;
  universe: Universe;
  window: Window;
  attempts: Attempt[];
  winner_candidate_id: string;
  decide_justification: string;
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
    case "stop_no_viable":
      validateStopNoViable(value);
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
        `decision.action must be one of: stop_winner, stop_no_viable, refine_candidates, broaden_universe, reinterpret_brief (got "${action}")`,
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

function validateStopNoViable(value: Record<string, unknown>): void {
  if (
    !Array.isArray(value.reasons) ||
    value.reasons.length === 0 ||
    value.reasons.some((reason) => typeof reason !== "string" || !reason.trim())
  ) {
    throw new DecisionValidationError(
      "reasons must be a non-empty array of non-empty strings",
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
  // candidates per batch. Match that here so we fail fast instead of
  // burning a backtest call on an invalid batch.
  if (value.candidates.length < 3 || value.candidates.length > 5) {
    throw new ProposalValidationError(
      `candidates must contain between 3 and 5 entries (got ${value.candidates.length})`,
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

  validateUniverseHints(value.universe_hints);
  validateConstraints(value.constraints);
  validateFeasibility(
    value.constraints as ThesisConstraints,
    value.universe_hints as UniverseHints,
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
}

function validateFeasibility(
  constraints: ThesisConstraints,
  universe: UniverseHints,
) {
  if (constraints.asset_count_min > constraints.asset_count_max) {
    throw new ThesisValidationError(
      "constraints.asset_count_min must be <= asset_count_max",
    );
  }
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
