// interpret_brief — first workflow step. Converts a user brief into a
// typed Thesis. Pure function: (input, deps) -> {delta, next}.

import type { LLMClient } from "../llm.ts";
import { createStepLogger, type StepLogger } from "../logging.ts";
import {
  ThesisValidationError,
  validateThesis,
  type InterpretBriefInput,
  type StepName,
  type Thesis,
} from "../state.ts";

export type InterpretBriefDeps = {
  llm: LLMClient;
  logger?: StepLogger;
};

export type InterpretBriefResult = {
  delta: { thesis: Thesis };
  next: StepName;
};

export const NEXT_STEP: StepName = "select_universe";

const PARSE_RETRY_NOTE =
  "Your previous response could not be parsed as JSON. Reply with only valid JSON matching the Thesis schema. No Markdown fences. No prose.";

const SCHEMA_RETRY_NOTE =
  "Your previous Thesis JSON failed validation: %ERROR%. Re-emit a corrected Thesis JSON object. Output JSON only.";

export const INTERPRET_BRIEF_PROMPT = `You are the brief-interpretation step for an investment-strategy workflow.

Your only job: convert the user brief into a structured Thesis JSON object. Emit JSON only — no prose, no Markdown fences.

Thesis schema (all fields required unless marked optional):
{
  "objective": "balanced_growth" | "growth" | "income" | "preserve_capital",
  "horizon_days": integer >= 30 (forward-looking holding period in days -- how long the user plans to keep capital in this strategy. This is NOT a backtest length; the backtest window is derived automatically by the system and uses much more history than the holding period.),
  "weight_mode": "percentage" | "dollar",
  "universe_hints": {
    "top_n": integer >= 1,
    "top_skip": integer >= 0 (optional; skip the first N market-cap ranks before applying top_n. Use this when the brief says things like "outside the top 5" or "excluding mega-caps". top_skip must be < top_n.),
    "market_cap_min_usd": number (optional),
    "min_history_days": integer >= 0 (optional; minimum days of price history each candidate coin must have. See "How to set min_history_days" below.),
    "exclude_stablecoins": boolean,
    "exclude_wrapped": boolean,
    "sectors_include": string[] (optional),
    "sectors_exclude": string[] (optional),
    "hand_picked_coin_ids": string[] (optional; if present, top_n and market_cap_min_usd are ignored downstream)
  },
  "constraints": {
    "max_weight_per_asset": number in [0, 1],
    "max_cash_weight": number in [0, 1],
    "max_drawdown": number in [0, 1] (positive, e.g. 0.35 for 35%),
    "asset_count_min": integer >= 1,
    "asset_count_max": integer >= asset_count_min
  },
  "rebalance_frequency": "daily" | "weekly" | "monthly" | "quarterly",
  "interpretation_notes": "string explaining assumptions, ambiguities, and how the brief was translated"
}

Feasibility (will be validated):
- asset_count_min <= asset_count_max
- max_weight_per_asset * asset_count_min >= 1 - max_cash_weight  (must be able to fill the non-cash portion)
- horizon_days >= 30
- If hand_picked_coin_ids is set, its length must be within [asset_count_min, asset_count_max]

How to set min_history_days:
- "Static" strategies select coins ONCE at the start of the backtest and hold them. Every selected coin needs enough history to span the whole backtest window. Examples: "buy and hold N coins for a year", "equal-weight basket I won't touch for 12 months", "long-horizon mean reversion on a fixed set of coins".
  -> Set min_history_days to roughly 2 * horizon_days (or larger). This filters out newly-listed coins that would otherwise collapse the realised window to their first-price-date.
- "Dynamic" strategies re-rank the universe at each rebalance and pick coins fresh. Each coin only needs enough history for the ranking lookback (usually ~90 days), not the full backtest. Examples: "momentum strategy rebalanced monthly", "rotate into top movers each quarter", "rank by Sharpe and rebalance".
  -> LEAVE min_history_days UNSET. The universe stays broad; recent listings remain eligible. The backtest window may be shorter for the universe as a whole, but that's fine because the actual coins held at any given point are picked dynamically.
- "Ambiguous" / brief doesn't imply a regime: leave min_history_days UNSET. The downstream workflow will treat the realised window as a signal and the decide step can broaden if needed.
- If hand_picked_coin_ids is set, min_history_days is unnecessary (the user already chose the coins).

How horizon_days flows downstream:
- The system feeds horizon_days to a deterministic backtest-window recommender that targets max(2 * horizon_days, 1460) days of history -- typically 4+ years even for a 1-year horizon, so the strategy is evaluated across multiple market regimes.
- horizon_days is NOT a "minimum backtest length" constraint, and the gate does NOT fail candidates whose realised window equals horizon_days. Pick horizon_days based on the user's stated holding period, not on how much data you think the backtest needs.
- If the realised backtest window comes out shorter than horizon_days, that surfaces as a separate signal for the workflow to act on (broaden the universe, etc.) -- not as a per-candidate validation failure.

Defaults when the brief is silent (explain each in interpretation_notes):
- objective: balanced_growth
- weight_mode: percentage
- rebalance_frequency: monthly
- exclude_stablecoins: true
- exclude_wrapped: true
- top_n: 25 for broad baskets, smaller for narrow themes

If a reinterpret_hint is present in the user message, treat it as a correction from a prior failed attempt: revisit the listed fields and address the hint's rationale before re-emitting the Thesis.`;

export async function interpretBrief(
  input: InterpretBriefInput,
  deps: InterpretBriefDeps,
): Promise<InterpretBriefResult> {
  const logger =
    deps.logger ??
    createStepLogger({ run_id: input.run_id, step: "interpret_brief" });

  logger.enter({
    brief_kind: typeof input.brief === "string" ? "text" : "wizard",
    hint_present: Boolean(input.hint),
  });

  const userMessage = buildUserMessage(input);

  let lastError: Error | undefined;

  for (let attempt = 0; attempt < 2; attempt += 1) {
    const system = buildSystem(attempt, lastError);
    logger.llmRequest({ prompt_chars: system.length + userMessage.length });

    const response = await deps.llm.complete({
      system,
      user: userMessage,
    });
    logger.llmResponse({
      tokens_in: response.tokens_in,
      tokens_out: response.tokens_out,
      response_chars: response.text.length,
    });

    try {
      const parsed = parseJson(response.text);
      validateThesis(parsed);
      logger.exit(NEXT_STEP, {
        objective: parsed.objective,
        horizon_days: parsed.horizon_days,
        top_n: parsed.universe_hints.top_n,
        asset_count_range: [
          parsed.constraints.asset_count_min,
          parsed.constraints.asset_count_max,
        ],
      });
      return { delta: { thesis: parsed }, next: NEXT_STEP };
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

  // Unreachable: the loop either returns or throws.
  throw lastError ?? new Error("interpret_brief failed without an error");
}

function buildSystem(attempt: number, lastError: Error | undefined): string {
  if (attempt === 0) return INTERPRET_BRIEF_PROMPT;
  const note =
    lastError instanceof ThesisValidationError
      ? SCHEMA_RETRY_NOTE.replace("%ERROR%", lastError.message)
      : PARSE_RETRY_NOTE;
  return `${INTERPRET_BRIEF_PROMPT}\n\n${note}`;
}

function buildUserMessage(input: InterpretBriefInput): string {
  const payload: Record<string, unknown> = { brief: input.brief };
  if (input.hint) payload.reinterpret_hint = input.hint;
  return JSON.stringify(payload);
}

function parseJson(text: string): unknown {
  const trimmed = text.trim();
  if (!trimmed) throw new Error("empty response");

  try {
    return JSON.parse(trimmed);
  } catch {
    /* fall through to fenced/embedded extraction */
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
