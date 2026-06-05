// propose_candidates -- fourth workflow step. LLM-driven. Takes the
// resolved thesis/universe/window and any prior attempts (with their
// validation failures + refinement hints) and emits a Proposal: a
// small set of allocation-template configurations to backtest next.

import type { LLMClient } from "../llm.ts";
import { createStepLogger, type StepLogger } from "../logging.ts";
import {
  ALLOCATION_TEMPLATES,
  ProposalValidationError,
  REBALANCE_TRIGGER_FAMILIES,
  REBALANCE_TRIGGERS,
  WEIGHTING_SCHEMES,
  validateProposal,
  type AllocationTemplate,
  type Attempt,
  type Proposal,
  type ProposedCandidate,
  type ProposeCandidatesInput,
  type StepName,
  type Thesis,
  type Universe,
  type WeightingScheme,
  type Window,
} from "../state.ts";

export type ProposeCandidatesDeps = {
  llm: LLMClient;
  logger?: StepLogger;
};

export type ProposeCandidatesResult = {
  delta: { proposal: Proposal };
  next: StepName;
};

export const NEXT_STEP: StepName = "run_and_validate";

const MAX_EXPANDED_CANDIDATES = 12;

const PARSE_RETRY_NOTE =
  "Your previous response could not be parsed as JSON. Reply with only valid JSON matching the Proposal schema. No Markdown fences. No prose.";

const SCHEMA_RETRY_NOTE =
  "Your previous Proposal JSON failed validation: %ERROR%. Re-emit a corrected Proposal JSON object. Output JSON only.";

export const PROPOSE_CANDIDATES_PROMPT = `You are the candidate-proposal step for an investment-strategy workflow.

Inputs you receive in the user message:
- thesis: the constraints, objective, horizon, and weighting mode the brief was translated into.
- selected_families: a ranked strategy-family shortlist (best fit first) chosen by the select_templates step. Each family below is directly executable -- carry the family id through as the candidate's template_id. Favor the higher-ranked families. May be absent.
- universe: { coin_ids, source } -- the eligible coin set already chosen by the previous step. You may select any subset of these via select_top, but you cannot add coins outside this set.
- window: { start, end, horizon_days } -- the backtest window already chosen by the previous step.
- prior_attempts: an array of prior attempts (may be empty). When present, each entry contains the previous proposal, the validation_summary (which constraints failed and by how much), and a refinement_hint with structured suggested_changes. Treat the hint as a directive: use it to inform this round's candidates.

Your job: emit 3 to 5 high-conviction seed candidates spanning the most promising strategy-family configurations for this thesis. The system will deterministically expand your seed shortlist into a bounded parameter sweep before backtesting, so focus on picking the right families and representative knobs rather than exhaustively enumerating every variant. The downstream backtest engine requires at least 3 candidates per batch -- always emit at least 3. Emit JSON only -- no prose, no Markdown fences.

Allowed template_id values. The long-only families:
- "synthetic_long_allocation"        -- one-shot long basket, no rebalance_trigger.
- "periodic_rebalanced_allocation"   -- long basket on a fixed cadence; REQUIRES a rebalance_trigger.
- "threshold_rebalanced_allocation"  -- long basket rebalanced on weight drift; rebalance_trigger optional.
- "core_satellite_allocation"        -- BTC/ETH core + satellites; set core_weight (0..1); rebalance_trigger optional.
- "barbell_allocation"               -- safe core + small capped speculative sleeve; set core_weight and sleeve_cap (0..1).
- "volatility_targeted_exposure"     -- inverse-vol weighted long book; rebalance_trigger optional.
- "relative_momentum_rotation"       -- rotate into the strongest names each rebalance; rebalance_trigger optional.
- "trend_following_long_neutral"     -- hold only trending names, else de-risk; no rebalance_trigger.
The short/hedge families (ONLY use one when it appears in selected_families):
- "partial_hedge_overlay"            -- long alt book with a partial short BTC/ETH hedge; rebalance_trigger optional.
- "beta_hedged_alt_exposure"         -- long alts, short BTC/ETH to strip market beta; rebalance_trigger optional.
- "relative_value_pair_trade"        -- long the top name, short the runner-up; no rebalance_trigger. Needs select_top >= 2.
- "trend_following_long_short"       -- long uptrend names, short downtrend names; no rebalance_trigger.
- "drawdown_based_hedge"             -- long book that adds a short hedge as drawdown deepens; no rebalance_trigger.

Allowed weightings: ${WEIGHTING_SCHEMES.join(", ")}
Allowed rebalance_triggers: ${REBALANCE_TRIGGERS.join(", ")}
- rebalance_trigger is REQUIRED on periodic_rebalanced_allocation; FORBIDDEN on synthetic_long_allocation, trend_following_long_neutral, relative_value_pair_trade, trend_following_long_short, and drawdown_based_hedge; OPTIONAL on the rest.
- core_weight is only allowed on core_satellite_allocation and barbell_allocation. sleeve_cap is only allowed on barbell_allocation.
- SHORTS GATE: only propose a short/hedge family when it is present in selected_families (select_templates shortlists them only when the thesis opts into shorts/hedging). When selected_families has no short family, propose long-only families only.

Proposal schema:
{
  "iteration_hypothesis": "one short sentence on what this attempt tests differently",
  "candidates": [
    {
      "candidate_id": "unique short string (e.g. c1, c2, c3)",
      "template_id": ${ALLOCATION_TEMPLATES.map((t) => `"${t}"`).join(" | ")},
      "select_top": integer in [thesis.constraints.asset_count_min, min(asset_count_max, universe.size)],
      "weighting": one of the allowed weightings,
      "rebalance_trigger": one of the allowed triggers (see rules above),
      "core_weight": number in (0,1) -- core_satellite/barbell only,
      "sleeve_cap": number in (0,1) -- barbell only,
      "rationale": "one short sentence on why this configuration is worth trying"
    }
  ]
}

Rules:
- candidates array MUST contain at least 3 entries and at most 5.
- candidate_id values must be unique within the proposal.
- select_top must satisfy the thesis asset-count bounds AND be <= universe.coin_ids.length.
- Prefer diversity: when proposing >1 candidate, vary at least one of {template_id, select_top, weighting, rebalance_trigger} per candidate.
- When prior_attempts is non-empty, your iteration_hypothesis must reference what was learned from the prior failure(s) and the candidates must implement the refinement_hint.suggested_changes when those are present. Do not repeat a configuration that already failed validation.
- Pick conservative defaults when the brief is silent. The thesis already encodes feasibility constraints -- respect them.`;

export async function proposeCandidates(
  input: ProposeCandidatesInput,
  deps: ProposeCandidatesDeps,
): Promise<ProposeCandidatesResult> {
  const logger =
    deps.logger ??
    createStepLogger({
      run_id: input.run_id,
      step: "propose_candidates",
      attempt: nextAttemptNumber(input.attempts),
    });

  logger.enter({
    objective: input.thesis.objective,
    universe_size: input.universe.coin_ids.length,
    horizon_days: input.thesis.horizon_days,
    prior_attempts: input.attempts?.length ?? 0,
    has_refinement_hint: lastHint(input.attempts) !== undefined,
  });

  const userMessage = buildUserMessage(input);
  let lastError: Error | undefined;

  for (let attempt = 0; attempt < 2; attempt += 1) {
    const system = buildSystem(attempt, lastError);
    logger.llmRequest({ prompt_chars: system.length + userMessage.length });

    const response = await deps.llm.complete({ system, user: userMessage });
    logger.llmResponse({
      tokens_in: response.tokens_in,
      tokens_out: response.tokens_out,
      response_chars: response.text.length,
    });

    try {
      const parsed = parseJson(response.text);
      validateProposal(parsed, {
        thesis: input.thesis,
        universe: input.universe,
      });
      const expanded = expandProposalWithSweep(parsed, input);
      validateProposal(expanded, {
        thesis: input.thesis,
        universe: input.universe,
      });
      logger.exit(NEXT_STEP, {
        candidate_count: expanded.candidates.length,
        llm_candidate_count: parsed.candidates.length,
        template_mix: summariseTemplates(expanded.candidates),
      });
      return { delta: { proposal: expanded }, next: NEXT_STEP };
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      if (attempt === 1) {
        logger.error(lastError, {
          raw_response_chars: response.text.length,
        });
        throw lastError;
      }
    }
  }

  throw lastError ?? new Error("propose_candidates failed without an error");
}

export function expandProposalWithSweep(
  proposal: Proposal,
  input: ProposeCandidatesInput,
): Proposal {
  const minTop = input.thesis.constraints.asset_count_min;
  const maxTop = Math.min(
    input.thesis.constraints.asset_count_max,
    input.universe.coin_ids.length,
  );
  if (maxTop < minTop) return proposal;

  const out: ProposedCandidate[] = proposal.candidates.map((c) => ({ ...c }));
  const seen = new Set(out.map(candidateSignature));
  const prior = new Set(
    (input.attempts ?? []).flatMap((attempt) =>
      attempt.proposal.candidates.map(candidateSignature),
    ),
  );
  const usedIds = new Set(out.map((c) => c.candidate_id));
  const families = orderedSweepFamilies(proposal, input);

  let sweepN = 1;
  for (const family of families) {
    for (const variant of sweepVariants(family, input.thesis, minTop, maxTop)) {
      if (out.length >= MAX_EXPANDED_CANDIDATES) {
        return withExpandedHypothesis(proposal, out);
      }
      const signature = candidateSignature(variant);
      if (seen.has(signature) || prior.has(signature)) continue;

      seen.add(signature);
      const candidateId = nextSweepId(usedIds, sweepN);
      sweepN += 1;
      usedIds.add(candidateId);
      out.push({
        ...variant,
        candidate_id: candidateId,
        rationale: `Deterministic sweep variant for ${variant.template_id}.`,
      });
    }
  }

  return withExpandedHypothesis(proposal, out);
}

function withExpandedHypothesis(
  proposal: Proposal,
  candidates: ProposedCandidate[],
): Proposal {
  if (candidates.length === proposal.candidates.length) return proposal;
  return {
    iteration_hypothesis: `${proposal.iteration_hypothesis} Deterministic sweep expands the LLM shortlist to ${candidates.length} parameter variants.`,
    candidates,
  };
}

function orderedSweepFamilies(
  proposal: Proposal,
  input: ProposeCandidatesInput,
): AllocationTemplate[] {
  const out: AllocationTemplate[] = [];
  const add = (family: string | undefined) => {
    if (!family) return;
    if (!(ALLOCATION_TEMPLATES as readonly string[]).includes(family)) return;
    const typed = family as AllocationTemplate;
    if (!out.includes(typed)) out.push(typed);
  };

  for (const c of proposal.candidates) add(c.template_id);
  for (const selected of [...(input.template_selection?.selected ?? [])].sort(
    (a, b) => a.rank - b.rank,
  )) {
    add(selected.family);
  }
  return out;
}

function sweepVariants(
  family: AllocationTemplate,
  thesis: Thesis,
  minTop: number,
  maxTop: number,
): ProposedCandidate[] {
  const tops = selectTopSweep(minTop, maxTop);
  const weightings = weightingSweep(thesis.objective);
  const variants: ProposedCandidate[] = [];
  const add = (candidate: Omit<ProposedCandidate, "candidate_id" | "rationale">) => {
    variants.push({ ...candidate, candidate_id: "", rationale: "" });
  };

  switch (family) {
    case "synthetic_long_allocation":
      for (const select_top of tops) {
        for (const weighting of weightings.slice(0, 3)) {
          add({ template_id: family, select_top, weighting });
        }
      }
      break;
    case "periodic_rebalanced_allocation":
      for (const rebalance_trigger of REBALANCE_TRIGGERS) {
        for (const weighting of weightings.slice(0, 2)) {
          add({
            template_id: family,
            select_top: tops[1] ?? tops[0]!,
            weighting,
            rebalance_trigger,
          });
        }
      }
      break;
    case "threshold_rebalanced_allocation":
      for (const weighting of weightings.slice(0, 3)) {
        add({
          template_id: family,
          select_top: tops[1] ?? tops[0]!,
          weighting,
          rebalance_trigger: "threshold_drift_10pct",
        });
      }
      break;
    case "relative_momentum_rotation":
      for (const select_top of tops) {
        add({
          template_id: family,
          select_top,
          weighting: "ranking_proportional",
          rebalance_trigger: "periodic_30d",
        });
        add({
          template_id: family,
          select_top,
          weighting: "equal",
          rebalance_trigger: "periodic_90d",
        });
      }
      break;
    case "trend_following_long_neutral":
    case "trend_following_long_short":
      for (const select_top of tops) {
        for (const weighting of weightings.slice(0, 2)) {
          add({ template_id: family, select_top, weighting });
        }
      }
      break;
    case "volatility_targeted_exposure":
      for (const rebalance_trigger of [
        "periodic_30d",
        "periodic_90d",
        "threshold_drift_10pct",
      ] as const) {
        add({
          template_id: family,
          select_top: tops[1] ?? tops[0]!,
          weighting: "vol_inverse",
          rebalance_trigger,
        });
      }
      break;
    case "core_satellite_allocation":
      for (const core_weight of [0.6, 0.75]) {
        add({
          template_id: family,
          select_top: tops[1] ?? tops[0]!,
          weighting: weightings[0]!,
          rebalance_trigger: "periodic_90d",
          core_weight,
        });
      }
      break;
    case "barbell_allocation":
      for (const [core_weight, sleeve_cap] of [
        [0.8, 0.05],
        [0.9, 0.03],
      ] as const) {
        add({
          template_id: family,
          select_top: tops[1] ?? tops[0]!,
          weighting: "equal",
          rebalance_trigger: "periodic_90d",
          core_weight,
          sleeve_cap,
        });
      }
      break;
    case "partial_hedge_overlay":
    case "beta_hedged_alt_exposure":
      for (const rebalance_trigger of ["periodic_30d", "periodic_90d"] as const) {
        add({
          template_id: family,
          select_top: tops[1] ?? tops[0]!,
          weighting: "equal",
          rebalance_trigger,
        });
      }
      break;
    case "relative_value_pair_trade":
      if (maxTop >= 2) {
        add({
          template_id: family,
          select_top: Math.max(2, tops[0]!),
          weighting: "equal",
        });
      }
      break;
    case "drawdown_based_hedge":
      add({
        template_id: family,
        select_top: tops[1] ?? tops[0]!,
        weighting: "equal",
      });
      break;
  }

  return variants.filter(isValidSweepCandidate);
}

function selectTopSweep(minTop: number, maxTop: number): number[] {
  const mid = Math.round((minTop + maxTop) / 2);
  return Array.from(new Set([minTop, mid, maxTop])).filter(
    (n) => n >= minTop && n <= maxTop,
  );
}

function weightingSweep(objective: Thesis["objective"]): WeightingScheme[] {
  switch (objective) {
    case "growth":
      return ["ranking_proportional", "cap", "equal", "vol_inverse"];
    case "preserve_capital":
    case "income":
      return ["vol_inverse", "equal", "cap", "ranking_proportional"];
    case "balanced_growth":
      return ["equal", "cap", "vol_inverse", "ranking_proportional"];
  }
}

function isValidSweepCandidate(candidate: ProposedCandidate): boolean {
  const family = candidate.template_id;
  const allowsTrigger = (REBALANCE_TRIGGER_FAMILIES as readonly string[]).includes(
    family,
  );
  if (family === "periodic_rebalanced_allocation") {
    return candidate.rebalance_trigger !== undefined;
  }
  if (candidate.rebalance_trigger !== undefined && !allowsTrigger) return false;
  if (candidate.core_weight !== undefined) {
    if (family !== "core_satellite_allocation" && family !== "barbell_allocation") {
      return false;
    }
  }
  if (candidate.sleeve_cap !== undefined && family !== "barbell_allocation") {
    return false;
  }
  return true;
}

function candidateSignature(candidate: ProposedCandidate): string {
  return JSON.stringify({
    template_id: candidate.template_id,
    select_top: candidate.select_top,
    weighting: candidate.weighting,
    rebalance_trigger: candidate.rebalance_trigger ?? null,
    core_weight: candidate.core_weight ?? null,
    sleeve_cap: candidate.sleeve_cap ?? null,
  });
}

function nextSweepId(usedIds: Set<string>, start: number): string {
  let n = start;
  while (usedIds.has(`sw${n}`)) n += 1;
  return `sw${n}`;
}

function nextAttemptNumber(attempts?: Attempt[]): number {
  return (attempts?.length ?? 0) + 1;
}

function lastHint(attempts?: Attempt[]) {
  return attempts && attempts.length > 0
    ? attempts[attempts.length - 1]?.refinement_hint
    : undefined;
}

function buildSystem(attempt: number, lastError: Error | undefined): string {
  if (attempt === 0) return PROPOSE_CANDIDATES_PROMPT;
  const note =
    lastError instanceof ProposalValidationError
      ? SCHEMA_RETRY_NOTE.replace("%ERROR%", lastError.message)
      : PARSE_RETRY_NOTE;
  return `${PROPOSE_CANDIDATES_PROMPT}\n\n${note}`;
}

function buildUserMessage(input: ProposeCandidatesInput): string {
  const payload = {
    run_id: input.run_id,
    thesis: thesisDigest(input.thesis),
    selected_families: input.template_selection?.selected.map((s) => ({
      family: s.family,
      rank: s.rank,
    })),
    universe: {
      coin_ids: input.universe.coin_ids,
      source: input.universe.source,
      size: input.universe.coin_ids.length,
    },
    window: {
      start: input.window.start,
      end: input.window.end,
      horizon_days: input.window.horizon_days,
      window_length_days: input.window.effective.window_length_days,
    },
    prior_attempts: (input.attempts ?? []).map(attemptDigest),
  };
  return JSON.stringify(payload);
}

// The full Thesis is fine to send too, but a digest keeps the prompt
// smaller and tells the model which fields it actually needs to obey.
function thesisDigest(thesis: Thesis) {
  return {
    objective: thesis.objective,
    horizon_days: thesis.horizon_days,
    rebalance_frequency: thesis.rebalance_frequency,
    constraints: thesis.constraints,
    interpretation_notes: thesis.interpretation_notes,
  };
}

function attemptDigest(attempt: Attempt) {
  return {
    attempt_n: attempt.attempt_n,
    proposal: attempt.proposal,
    batch_id: attempt.batch_id,
    validation_summary: attempt.validation_summary,
    refinement_hint: attempt.refinement_hint,
    decision: attempt.decision,
  };
}

function summariseTemplates(candidates: Proposal["candidates"]): string {
  const counts = new Map<string, number>();
  for (const c of candidates) {
    counts.set(c.template_id, (counts.get(c.template_id) ?? 0) + 1);
  }
  return Array.from(counts.entries())
    .map(([id, n]) => `${id}:${n}`)
    .join(",");
}

function parseJson(text: string): unknown {
  const trimmed = text.trim();
  if (!trimmed) throw new Error("empty response");

  try {
    return JSON.parse(trimmed);
  } catch {
    /* fall through */
  }

  const fenced = trimmed.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
  if (fenced?.[1]) {
    try {
      return JSON.parse(fenced[1]);
    } catch {
      /* fall through */
    }
  }

  const embedded = extractLastObject(trimmed);
  if (embedded) return JSON.parse(embedded);

  throw new Error("response did not contain a JSON object");
}

function extractLastObject(text: string): string | null {
  let depth = 0;
  let start = -1;
  let candidate: string | null = null;
  let inString = false;
  let escape = false;

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    if (inString) {
      if (escape) escape = false;
      else if (char === "\\") escape = true;
      else if (char === '"') inString = false;
      continue;
    }
    if (char === '"') {
      inString = true;
      continue;
    }
    if (char === "{") {
      if (depth === 0) start = i;
      depth += 1;
    } else if (char === "}") {
      depth -= 1;
      if (depth === 0 && start >= 0) {
        candidate = text.slice(start, i + 1);
        start = -1;
      }
    }
  }
  return candidate;
}

// Re-export for callers wanting these types from the step entrypoint.
export type { Universe, Window };
