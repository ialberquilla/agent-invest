// decide -- sixth workflow step. LLM-driven. Examines the iteration
// history (thesis + attempts + their validation summaries) and emits
// exactly one Decision: stop with a winner, stop as no-viable, or go
// back to one of three earlier steps with a structured hint.
//
// This is where iteration memory closes the loop: the hint emitted
// here flows into the next propose_candidates round as part of the
// attempt's `refinement_hint`.

import type { LLMClient } from "../llm.ts";
import { createStepLogger, type StepLogger } from "../logging.ts";
import {
  DEFAULT_DECISION_CAPS,
  DecisionValidationError,
  validateDecision,
  type Attempt,
  type Counters,
  type DecideInput,
  type Decision,
  type DecisionCaps,
  type StepName,
  type Thesis,
} from "../state.ts";

export type DecideDeps = {
  llm: LLMClient;
  logger?: StepLogger;
};

export type DecideResult = {
  delta: { decision: Decision };
  next: StepName;
};

const PARSE_RETRY_NOTE =
  "Your previous response could not be parsed as JSON. Reply with only valid JSON matching the Decision schema. No Markdown fences. No prose.";

const SCHEMA_RETRY_NOTE =
  "Your previous Decision JSON failed validation: %ERROR%. Re-emit a corrected Decision JSON object. Output JSON only.";

export const DECIDE_PROMPT = `You are the decide step for an investment-strategy workflow.

Your job: examine the iteration history (attempts + their validation summaries) and emit exactly ONE Decision. Emit JSON only -- no prose, no Markdown fences.

STEP 0 -- DETERMINE THE ALLOWED ACTION SET (do this BEFORE any strategy reasoning):
Compute which of the five actions are even legal given the run state. The user message gives you attempts, counters, and caps; use them.

- stop_winner: ALLOWED iff the LATEST attempt has at least one entry in validation_summary.passing_candidate_ids. Otherwise FORBIDDEN.
- refine_candidates: ALLOWED iff attempts.length < caps.max_attempts. Otherwise FORBIDDEN.
- broaden_universe: ALLOWED iff counters.broaden_universe < caps.max_broaden_universe. Otherwise FORBIDDEN.
- reinterpret_brief: ALLOWED iff counters.reinterpret_brief < caps.max_reinterpret_brief. Otherwise FORBIDDEN.
- stop_best_effort: ALWAYS ALLOWED.

If only stop_best_effort is allowed (every other action is FORBIDDEN), you MUST pick stop_best_effort. Never propose a FORBIDDEN action; the workflow will reject it and you waste an attempt.

Pick from the ALLOWED set only. If multiple are allowed, then reason about which best matches the failure pattern (rules below).

There is no "give up with nothing" action: the workflow ALWAYS shows the user a strategy. When you cannot fully satisfy the thesis, choose stop_best_effort and a deterministic ranker will surface the closest-fit candidate across all attempts. You never name that candidate -- you only supply the reasons the brief could not be fully satisfied.

The five possible actions:

1. "stop_winner" -- the latest attempt produced at least one candidate that PASSED thesis validation; pick the best one. Use the per-candidate metrics in validation_summary.candidates (sharpe, total_return, max_drawdown, composite_score) to pick the strongest passing candidate, not an arbitrary one.
   {
     "action": "stop_winner",
     "winner_candidate_id": "id from latest attempt's passing_candidate_ids",
     "justification": "one short paragraph on why this candidate is the best, referencing its metrics"
   }

2. "stop_best_effort" -- no candidate fully satisfied the thesis and no further iteration will help (budget exhausted, or failures show a pattern with no clear fix). The ranker will pick the closest-fit candidate to show the user.
   {
     "action": "stop_best_effort",
     "reasons": ["one short reason per array entry on why the thesis could not be fully satisfied"]
   }

3. "refine_candidates" -- try another candidate batch with a structured hint that addresses the specific failures.
   Only allowed if attempts.length < caps.max_attempts.
   {
     "action": "refine_candidates",
     "hint": {
       "failed_constraints": [
         {
           "constraint": "max_drawdown" | "max_weight_per_asset" | "max_cash_weight" | "asset_count_min" | "asset_count_max" | "benchmark_underperformance",
           "observed": number,
           "target": number,
           "candidate_id": "id"
         }
       ],
       "suggested_changes": {
         "tighten_weight_cap_to": optional number,
         "increase_cash_to": optional number,
         "swap_assets": optional { "remove": [...], "consider": [...] },
         "change_rebalance_to": optional "periodic_30d" | "periodic_90d" | "threshold_drift_10pct",
         "change_template_to": optional -- one of the executable strategy-family ids (e.g. "synthetic_long_allocation", "periodic_rebalanced_allocation", "core_satellite_allocation", "volatility_targeted_exposure", ...)
       },
       "rationale": "one short paragraph"
     }
   }

4. "broaden_universe" -- the universe is too narrow given the constraints; go back and loosen filters.
   Only allowed if counters.broaden_universe < caps.max_broaden_universe.
   {
     "action": "broaden_universe",
     "hint": {
       "reason": "too_narrow_after_filters" | "missing_sector" | "horizon_unsatisfiable",
       "loosen": {
         "raise_top_n_to": optional integer,
         "drop_filter": optional ("exclude_stablecoins" | "exclude_wrapped")[],
         "add_sectors": optional string[],
         "lower_market_cap_floor_to": optional number
       },
       "rationale": "one short paragraph"
     }
   }

5. "reinterpret_brief" -- the thesis itself is wrong; go back and re-interpret.
   Only allowed if counters.reinterpret_brief < caps.max_reinterpret_brief.
   {
     "action": "reinterpret_brief",
     "hint": {
       "reason": "constraints_infeasible" | "objective_mismatch" | "horizon_too_long",
       "fields_to_revisit": ["objective" | "horizon_days" | "constraints" | "universe_hints" | ...],
       "rationale": "one short paragraph"
     }
   }

Anti-rationalization rule:
- If action is "stop_winner", winner_candidate_id MUST be in the LATEST attempt's passing_candidate_ids. Do not claim a winner from an earlier attempt or a failed one. The deterministic validator already ran -- you cannot override its judgement.

Cap rules:
- Each backward edge has a per-run cap; the user message lists current counters and caps. Do not propose an action that violates them. If only "stop_*" actions remain, pick the appropriate one with reasons.

When to use which:
- Latest attempt has >= 1 passing candidate -> stop_winner (pick the best one).
- Two consecutive attempts failed on the SAME constraint with no improvement and refining further looks unlikely to help -> stop_best_effort.
- Failure looks fixable by a different candidate configuration (e.g., concentration too high -> increase select_top, or drawdown too high -> tighten weight cap) -> refine_candidates with a specific hint.
- The selected universe is structurally too small to satisfy constraints (e.g., universe size < asset_count_min) -> broaden_universe.
- The brief itself is infeasible (e.g., the user requested a horizon longer than common history allows) -> reinterpret_brief.`;

export async function decide(
  input: DecideInput,
  deps: DecideDeps,
): Promise<DecideResult> {
  const caps: DecisionCaps = { ...DEFAULT_DECISION_CAPS, ...input.caps };
  const logger =
    deps.logger ??
    createStepLogger({
      run_id: input.run_id,
      step: "decide",
      attempt: input.attempts.length,
    });

  logger.enter({
    attempts: input.attempts.length,
    latest_passing: latestPassingCount(input.attempts),
    latest_failing: latestFailingCount(input.attempts),
    counters: input.counters,
    caps,
  });

  if (input.attempts.length === 0) {
    const error = new DecisionValidationError(
      "decide requires at least one attempt with a validation_summary",
    );
    logger.error(error);
    throw error;
  }

  const userMessage = buildUserMessage(input, caps);
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
      validateDecision(parsed, {
        attempts: input.attempts,
        counters: input.counters,
        caps,
      });
      const next = nextStepFor(parsed.action);
      logger.exit(next, {
        action: parsed.action,
        ...exitDigest(parsed),
      });
      return { delta: { decision: parsed }, next };
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      if (attempt === 1) {
        logger.error(lastError, {
          raw_response_chars: response.text.length,
          fallback: "stop_no_viable",
        });
        // Graceful fallback: when the LLM can't emit a parseable /
        // valid Decision after two tries, the right answer is
        // stop_no_viable. The decide step's whole purpose is to choose
        // a next action; if we can't, the run has effectively
        // exhausted what the workflow can do. Surfacing this as
        // stop_no_viable lets the controller finish cleanly with
        // FinalNoViable instead of short-circuiting via step_error.
        const fallback = synthesiseStopBestEffort(input, caps, lastError);
        logger.exit(nextStepFor(fallback.action), {
          action: fallback.action,
          fallback: true,
          ...exitDigest(fallback),
        });
        return {
          delta: { decision: fallback },
          next: nextStepFor(fallback.action),
        };
      }
    }
  }

  throw lastError ?? new Error("decide failed without an error");
}

// Builds a stop_best_effort Decision when the LLM can't produce a valid
// one itself. The workflow always shows a strategy, so even on a decide
// parse failure we fall back to the deterministic ranker rather than
// giving up -- decide is guaranteed at least one attempt with results.
// Includes the parse/validation error AND a compact summary of the
// latest attempt's failed constraints so a human reading the run can see
// why decide fell back.
function synthesiseStopBestEffort(
  input: DecideInput,
  caps: DecisionCaps,
  lastError: Error,
): Decision {
  const reasons: string[] = [
    `decide could not parse a valid Decision after retries (${lastError.message}); showing the closest-fit candidate`,
  ];
  const latest = input.attempts.at(-1);
  const failedConstraints = Array.from(
    new Set(
      latest?.validation_summary?.failing.flatMap((f) =>
        f.violations.map((v) => v.constraint),
      ) ?? [],
    ),
  );
  if (failedConstraints.length > 0) {
    reasons.push(
      `latest attempt failed validation on: ${failedConstraints.join(", ")}`,
    );
  }
  reasons.push(
    `${input.attempts.length}/${caps.max_attempts} candidate-refinement attempts used`,
  );
  return {
    action: "stop_best_effort",
    reasons,
  };
}

export function nextStepFor(action: Decision["action"]): StepName {
  switch (action) {
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

function latestPassingCount(attempts: Attempt[]): number {
  return attempts.at(-1)?.validation_summary?.passing_candidate_ids.length ?? 0;
}

function latestFailingCount(attempts: Attempt[]): number {
  return attempts.at(-1)?.validation_summary?.failing.length ?? 0;
}

function buildSystem(attempt: number, lastError: Error | undefined): string {
  if (attempt === 0) return DECIDE_PROMPT;
  const note =
    lastError instanceof DecisionValidationError
      ? SCHEMA_RETRY_NOTE.replace("%ERROR%", lastError.message)
      : PARSE_RETRY_NOTE;
  return `${DECIDE_PROMPT}\n\n${note}`;
}

function buildUserMessage(input: DecideInput, caps: DecisionCaps): string {
  return JSON.stringify({
    run_id: input.run_id,
    thesis: thesisDigest(input.thesis),
    attempts: input.attempts.map(attemptDigest),
    counters: input.counters,
    caps,
  });
}

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

function exitDigest(decision: Decision): Record<string, unknown> {
  switch (decision.action) {
    case "stop_winner":
      return { winner_candidate_id: decision.winner_candidate_id };
    case "stop_best_effort":
      return { reason_count: decision.reasons.length };
    case "stop_no_viable":
      return { reason_count: decision.reasons.length };
    case "refine_candidates":
      return {
        failed_constraints: decision.hint.failed_constraints.map(
          (fc) => fc.constraint,
        ),
        suggested: Object.keys(decision.hint.suggested_changes),
      };
    case "broaden_universe":
      return {
        reason: decision.hint.reason,
        loosen: Object.keys(decision.hint.loosen),
      };
    case "reinterpret_brief":
      return {
        reason: decision.hint.reason,
        fields_to_revisit: decision.hint.fields_to_revisit,
      };
  }
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

export type { Counters, DecisionCaps };
