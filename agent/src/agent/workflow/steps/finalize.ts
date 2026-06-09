// finalize -- seventh and final workflow step. LLM-driven. Turns a
// validated winner into a user-facing FinalWinner record with a
// structured narrative. Only invoked when `decide` chose stop_winner.

import type { LLMClient } from "../llm.ts";
import { createStepLogger, type StepLogger } from "../logging.ts";
import {
  FinalizeValidationError,
  validateFinalizeNarrative,
  type Attempt,
  type FinalWinner,
  type FinalizeInput,
  type FinalizeNarrative,
  type ProposedCandidate,
  type StepName,
  type Thesis,
  type Universe,
  type Window,
} from "../state.ts";

export type FinalizeDeps = {
  llm: LLMClient;
  logger?: StepLogger;
};

export type FinalizeResult = {
  delta: { final: FinalWinner };
  next: StepName;
};

export const NEXT_STEP: StepName = "complete";

const PARSE_RETRY_NOTE =
  "Your previous response could not be parsed as JSON. Reply with only valid JSON matching the FinalizeNarrative schema. No Markdown fences. No prose outside the JSON.";

const SCHEMA_RETRY_NOTE =
  "Your previous narrative JSON failed validation: %ERROR%. Re-emit a corrected FinalizeNarrative JSON object. Output JSON only.";

export const FINALIZE_PROMPT = `You are the finalize step for an investment-strategy workflow.

A candidate has already been selected. Your job is to emit a user-facing narrative explaining this strategy, its assumptions, its risks, and what the user should do next. Emit JSON only -- no prose, no Markdown fences.

You will receive in the user message:
- thesis: how the user's brief was interpreted (objective, horizon, constraints).
- universe: { coin_ids, source } -- the coin set the strategy uses.
- window: { start, end, horizon_days, effective } -- the backtest window the strategy was scored over.
- winner: the selected candidate config (template, weighting, select_top, rebalance_trigger, rationale).
- is_best_effort: boolean. When FALSE, the winner fully satisfied every thesis constraint. When TRUE, NO candidate fully satisfied the thesis and this is the CLOSEST-FIT fallback we are showing anyway.
- unmet_constraints: present only for best_effort -- the constraints the winner did NOT meet, with observed vs target values.
- attempts: the iteration history that led here. Useful for "why this candidate not the earlier ones" framing.
- decide_justification: the structured reasoning the decide step used when picking this winner. Lean on it but rewrite in user-friendly prose.

BEST-EFFORT HONESTY (when is_best_effort is true):
- The summary MUST state plainly that this strategy did not fully meet the brief, and which constraint(s) it missed (use unmet_constraints, e.g. "its 41% max drawdown exceeds your 35% limit").
- At least one risk entry MUST call out each unmet constraint and its observed-vs-target gap.
- Do NOT describe a best-effort result as if it satisfied the thesis. Frame it as "the closest we could get".
- next_steps SHOULD include relaxing the unmet constraint(s) or rerunning with a different brief if the gap matters to the user.

Output schema (all fields required, all strings non-empty, all arrays non-empty):
{
  "title": "short user-facing label (under ~10 words)",
  "summary": "2-3 sentence plain-English description of the strategy: which assets, how weighted, how often rebalanced, over what horizon. Avoid jargon where possible.",
  "reasoning": "one short paragraph on WHY this candidate was selected over the alternatives. Reference iteration history if relevant.",
  "assumptions": ["explicit assumptions that underlie this recommendation -- each as one short sentence"],
  "risks": ["risks the user should understand -- each as one short sentence. At least one must mention that historical backtests do not guarantee future performance."],
  "next_steps": ["actionable user next steps -- each as one short sentence. Examples: 'rerun with different constraints', 'paper trade for N weeks', 'review with a licensed advisor'."]
}

Rules:
- Do not invent metrics or backtest results that are not visible in the input.
- Do not contradict the thesis constraints. If the winner config has select_top=7 and the thesis says max 10, do not claim "narrow concentration".
- This is not financial advice. Reflect that in the risks and next_steps lists.
- Keep the language concrete: name actual coins from the universe, the actual rebalance cadence, the actual horizon.`;

export async function finalize(
  input: FinalizeInput,
  deps: FinalizeDeps,
): Promise<FinalizeResult> {
  const logger =
    deps.logger ??
    createStepLogger({
      run_id: input.run_id,
      step: "finalize",
      attempt: input.attempts.length,
    });

  // The winner can come from any attempt (a best-effort winner may be
  // from an earlier round), so locate it by attempt_n rather than
  // assuming the latest attempt.
  const winnerAttempt = input.attempts.find(
    (a) => a.attempt_n === input.winner_attempt_n,
  );
  if (!winnerAttempt) {
    const err = new FinalizeValidationError(
      `no attempt with attempt_n ${input.winner_attempt_n}`,
    );
    logger.error(err);
    throw err;
  }
  if (!winnerAttempt.batch_id) {
    const err = new FinalizeValidationError(
      `attempt ${input.winner_attempt_n} is missing batch_id`,
    );
    logger.error(err);
    throw err;
  }
  const winnerCandidate = winnerAttempt.proposal.candidates.find(
    (c) => c.candidate_id === input.winner_candidate_id,
  );
  if (!winnerCandidate) {
    const err = new FinalizeValidationError(
      `winner_candidate_id "${input.winner_candidate_id}" is not in attempt ${input.winner_attempt_n}'s proposal`,
    );
    logger.error(err);
    throw err;
  }
  // A full winner must have actually passed the gate. A best-effort
  // winner did not -- that is the whole point -- so we only check
  // membership in the passing set when this is NOT a best-effort result.
  if (!input.is_best_effort) {
    const passingIds =
      winnerAttempt.validation_summary?.passing_candidate_ids ?? [];
    if (!passingIds.includes(input.winner_candidate_id)) {
      const err = new FinalizeValidationError(
        `winner_candidate_id "${input.winner_candidate_id}" is not in attempt ${input.winner_attempt_n}'s passing_candidate_ids [${passingIds.join(", ")}]`,
      );
      logger.error(err);
      throw err;
    }
  }
  const unmetConstraints = input.is_best_effort
    ? unmetConstraintsFor(winnerAttempt, input.winner_candidate_id)
    : [];

  logger.enter({
    winner_candidate_id: input.winner_candidate_id,
    winner_attempt_n: input.winner_attempt_n,
    is_best_effort: input.is_best_effort,
    batch_id: winnerAttempt.batch_id,
    attempts: input.attempts.length,
  });

  const userMessage = buildUserMessage(
    input,
    winnerCandidate,
    unmetConstraints,
  );
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
      validateFinalizeNarrative(parsed);
      const final = assembleFinal(
        input,
        winnerAttempt.batch_id,
        parsed,
        unmetConstraints,
      );
      logger.exit(NEXT_STEP, {
        title_chars: parsed.title.length,
        summary_chars: parsed.summary.length,
        risk_count: parsed.risks.length,
        next_step_count: parsed.next_steps.length,
      });
      return { delta: { final }, next: NEXT_STEP };
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

  throw lastError ?? new Error("finalize failed without an error");
}

function buildSystem(attempt: number, lastError: Error | undefined): string {
  if (attempt === 0) return FINALIZE_PROMPT;
  const note =
    lastError instanceof FinalizeValidationError
      ? SCHEMA_RETRY_NOTE.replace("%ERROR%", lastError.message)
      : PARSE_RETRY_NOTE;
  return `${FINALIZE_PROMPT}\n\n${note}`;
}

function buildUserMessage(
  input: FinalizeInput,
  winner: ProposedCandidate,
  unmetConstraints: UnmetConstraint[],
): string {
  return JSON.stringify({
    run_id: input.run_id,
    thesis: thesisDigest(input.thesis),
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
    winner,
    // When best_effort, the strategy did NOT fully satisfy the thesis;
    // unmet_constraints lists what it missed so the narrative can be
    // honest about it.
    is_best_effort: input.is_best_effort,
    unmet_constraints: unmetConstraints,
    attempts: input.attempts.map(attemptDigest),
    decide_justification: input.decide_justification,
  });
}

type UnmetConstraint = { constraint: string; observed: number; target: number };

// Pull the winner's own violations from its attempt so the best-effort
// narrative can state exactly which constraints it missed and by how much.
function unmetConstraintsFor(
  attempt: Attempt,
  candidateId: string,
): UnmetConstraint[] {
  const failing = attempt.validation_summary?.failing.find(
    (f) => f.candidate_id === candidateId,
  );
  return (failing?.violations ?? []).map((v) => ({
    constraint: v.constraint,
    observed: v.observed,
    target: v.target,
  }));
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
  };
}

function assembleFinal(
  input: FinalizeInput,
  batchId: string,
  narrative: FinalizeNarrative,
  unmetConstraints: UnmetConstraint[],
): FinalWinner {
  return {
    kind: "winner",
    run_id: input.run_id,
    winner_candidate_id: input.winner_candidate_id,
    winner_attempt_n: input.winner_attempt_n,
    candidate_batch_id: batchId,
    is_best_effort: input.is_best_effort,
    unmet_constraints: unmetConstraints,
    thesis: input.thesis,
    universe: input.universe,
    window: input.window,
    attempts_summary: input.attempts,
    narrative,
    winner_backtest: input.winner_backtest,
  };
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

export type { Universe, Window };
