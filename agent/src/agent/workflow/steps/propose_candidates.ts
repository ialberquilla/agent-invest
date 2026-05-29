// propose_candidates -- fourth workflow step. LLM-driven. Takes the
// resolved thesis/universe/window and any prior attempts (with their
// validation failures + refinement hints) and emits a Proposal: a
// small set of allocation-template configurations to backtest next.

import type { LLMClient } from "../llm.ts";
import { createStepLogger, type StepLogger } from "../logging.ts";
import {
  ALLOCATION_TEMPLATES,
  ProposalValidationError,
  REBALANCE_TRIGGERS,
  WEIGHTING_SCHEMES,
  validateProposal,
  type Attempt,
  type Proposal,
  type ProposeCandidatesInput,
  type StepName,
  type Thesis,
  type Universe,
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

const PARSE_RETRY_NOTE =
  "Your previous response could not be parsed as JSON. Reply with only valid JSON matching the Proposal schema. No Markdown fences. No prose.";

const SCHEMA_RETRY_NOTE =
  "Your previous Proposal JSON failed validation: %ERROR%. Re-emit a corrected Proposal JSON object. Output JSON only.";

export const PROPOSE_CANDIDATES_PROMPT = `You are the candidate-proposal step for an investment-strategy workflow.

Inputs you receive in the user message:
- thesis: the constraints, objective, horizon, and weighting mode the brief was translated into.
- selected_families: a ranked strategy-family shortlist (best fit first) chosen by the select_templates step. Treat it as guidance for which configurations to favor. Only the allocation templates below are currently executable, so map each selected family onto the closest executable template (e.g. periodic_rebalanced_allocation -> periodic_rebalance with a periodic trigger; threshold_rebalanced_allocation -> periodic_rebalance with threshold_drift_10pct; synthetic_long_allocation / core_satellite / barbell -> buy_and_hold or periodic_rebalance). May be absent.
- universe: { coin_ids, source } -- the eligible coin set already chosen by the previous step. You may select any subset of these via select_top, but you cannot add coins outside this set.
- window: { start, end, horizon_days } -- the backtest window already chosen by the previous step.
- prior_attempts: an array of prior attempts (may be empty). When present, each entry contains the previous proposal, the validation_summary (which constraints failed and by how much), and a refinement_hint with structured suggested_changes. Treat the hint as a directive: use it to inform this round's candidates.

Your job: emit 3 to 5 candidates spanning the most promising allocation-template configurations for this thesis. The downstream backtest engine requires at least 3 candidates per batch -- always emit at least 3. Emit JSON only -- no prose, no Markdown fences.

Allowed templates (ALLOCATION ONLY for now):
- "buy_and_hold"      -- one-shot allocation, no rebalance_trigger
- "periodic_rebalance" -- requires a rebalance_trigger

Allowed weightings: ${WEIGHTING_SCHEMES.join(", ")}
Allowed rebalance_triggers (only on periodic_rebalance): ${REBALANCE_TRIGGERS.join(", ")}

Proposal schema:
{
  "iteration_hypothesis": "one short sentence on what this attempt tests differently",
  "candidates": [
    {
      "candidate_id": "unique short string (e.g. c1, c2, c3)",
      "template_id": ${ALLOCATION_TEMPLATES.map((t) => `"${t}"`).join(" | ")},
      "select_top": integer in [thesis.constraints.asset_count_min, min(asset_count_max, universe.size)],
      "weighting": one of the allowed weightings,
      "rebalance_trigger": one of the allowed triggers (REQUIRED on periodic_rebalance, FORBIDDEN on buy_and_hold),
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
      logger.exit(NEXT_STEP, {
        candidate_count: parsed.candidates.length,
        template_mix: summariseTemplates(parsed.candidates),
      });
      return { delta: { proposal: parsed }, next: NEXT_STEP };
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
