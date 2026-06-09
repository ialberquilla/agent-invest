// select_templates -- second workflow step. LLM-driven. Takes the
// resolved Thesis and classifies it onto a ranked shortlist of strategy
// families (spec.md section 9). This makes the thesis->strategy mapping
// explicit and auditable, and gives propose_candidates a narrower space
// to parameterize within. Pure function: (input, deps) -> {delta, next}.

import type { LLMClient } from "../llm.ts";
import { createStepLogger, type StepLogger } from "../logging.ts";
import {
  STRATEGY_FAMILIES,
  TemplateSelectionValidationError,
  validateTemplateSelection,
  type SelectTemplatesInput,
  type StepName,
  type TemplateSelection,
  type Thesis,
} from "../state.ts";

export type SelectTemplatesDeps = {
  llm: LLMClient;
  logger?: StepLogger;
};

export type SelectTemplatesResult = {
  delta: { template_selection: TemplateSelection };
  next: StepName;
};

export const NEXT_STEP: StepName = "select_universe";

const PARSE_RETRY_NOTE =
  "Your previous response could not be parsed as JSON. Reply with only valid JSON matching the TemplateSelection schema. No Markdown fences. No prose.";

const SCHEMA_RETRY_NOTE =
  "Your previous TemplateSelection JSON failed validation: %ERROR%. Re-emit a corrected TemplateSelection JSON object. Output JSON only.";

export const SELECT_TEMPLATES_PROMPT = `You are the strategy-template-selection step for an investment-strategy workflow.

Your only job: read the structured Thesis and pick the strategy FAMILIES that best fit it. Emit JSON only -- no prose, no Markdown fences.

The strategy-family catalog (pick from these ids only):
- "synthetic_long_allocation": long-only basket held to target weights. Default for broad long exposure with no rebalance discipline stated.
- "periodic_rebalanced_allocation": long basket rebalanced on a fixed schedule. Use when the thesis emphasizes a rebalance cadence.
- "threshold_rebalanced_allocation": long basket rebalanced only when weights drift past a threshold. Use to avoid overtrading / when the user wants to trim winners.
- "core_satellite_allocation": large BTC/ETH core plus smaller high-growth sleeves. Good for "balanced growth with some alt upside".
- "barbell_allocation": large safe core plus a small, capped speculative sleeve with a stop rule. Good for "mostly safe, small high-risk bet".
- "partial_hedge_overlay": long book with a partial short hedge. REQUIRES SHORTS.
- "trend_following_long_neutral": hold long exposure only while trend is positive, else de-risk. Good for drawdown-aware long exposure.
- "trend_following_long_short": long positive-trend markets, short negative-trend markets. REQUIRES SHORTS. Advanced.
- "relative_momentum_rotation": rotate into the strongest markets each rebalance. Good for momentum / "which assets" questions.
- "relative_value_pair_trade": long one market, short another to express relative outperformance. REQUIRES SHORTS.
- "beta_hedged_alt_exposure": long an alt while shorting BTC/ETH to strip market beta. REQUIRES SHORTS.
- "drawdown_based_hedge": add short hedge as realized drawdown crosses precommitted thresholds. REQUIRES SHORTS. Behavioral guardrail.
- "volatility_targeted_exposure": scale gross exposure to hold a target volatility. Risk-managed overlay on a base long strategy.

TemplateSelection schema:
{
  "rationale": "2-4 sentences explaining the overall mapping from this thesis to the shortlisted families",
  "selected": [
    {
      "family": one of the catalog ids,
      "rank": integer (1 = best fit; ranks must be unique and contiguous starting at 1),
      "rationale": "one short sentence on why this family fits THIS thesis"
    }
  ]
}

Rules:
- selected MUST contain between 1 and 3 families, ranked best-fit first (rank 1, 2, 3).
- family ids must come from the catalog and be unique within the shortlist.
- ranks must be the contiguous set 1..n with no gaps or duplicates.
- SHORTS GATE: only shortlist a family marked "REQUIRES SHORTS" when the thesis interpretation_notes (or objective) clearly opt into shorts or hedging. When in doubt, prefer long-only families.
- Map thesis signals to families:
  - objective preserve_capital / income, low max_drawdown -> prefer core_satellite, threshold_rebalanced, periodic_rebalanced, volatility_targeted. When max_drawdown is TIGHT (<= 0.20), rank a drawdown-aware family (volatility_targeted, or threshold_rebalanced for low turnover) FIRST -- plain calendar rebalancing (periodic_rebalanced) does not control drawdown, so it should not be the rank-1 pick for a tight-drawdown mandate.
  - objective balanced_growth -> core_satellite, periodic_rebalanced, synthetic_long.
  - objective growth, higher drawdown tolerance -> barbell, relative_momentum_rotation, synthetic_long. When the thesis explicitly describes a large core PLUS a small or capped speculative / high-risk sleeve, barbell is the most precise fit -> rank it first, with core_satellite as the close alternative.
  - thesis emphasizing a rebalance cadence -> periodic_rebalanced; emphasizing drift control / not overtrading -> threshold_rebalanced. A stated rebalance cadence is an implementation detail: do not let it displace the primary structural/objective fit at rank 1 unless rebalancing itself is the main intent.
  - thesis mentioning trend / momentum -> trend_following_long_neutral or relative_momentum_rotation (and long_short variants only if shorts allowed).
  - thesis mentioning hedging / downside protection with shorts allowed -> partial_hedge_overlay, drawdown_based_hedge, beta_hedged_alt_exposure.
- Prefer fewer, higher-conviction picks over padding the list to 3.`;

export async function selectTemplates(
  input: SelectTemplatesInput,
  deps: SelectTemplatesDeps,
): Promise<SelectTemplatesResult> {
  const logger =
    deps.logger ??
    createStepLogger({ run_id: input.run_id, step: "select_templates" });

  logger.enter({
    objective: input.thesis.objective,
    horizon_days: input.thesis.horizon_days,
    rebalance_frequency: input.thesis.rebalance_frequency,
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
      validateTemplateSelection(parsed);
      logger.exit(NEXT_STEP, {
        families: parsed.selected.map((s) => s.family),
        top_family: parsed.selected[0]?.family,
      });
      return { delta: { template_selection: parsed }, next: NEXT_STEP };
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
  throw lastError ?? new Error("select_templates failed without an error");
}

function buildSystem(attempt: number, lastError: Error | undefined): string {
  if (attempt === 0) return SELECT_TEMPLATES_PROMPT;
  const note =
    lastError instanceof TemplateSelectionValidationError
      ? SCHEMA_RETRY_NOTE.replace("%ERROR%", lastError.message)
      : PARSE_RETRY_NOTE;
  return `${SELECT_TEMPLATES_PROMPT}\n\n${note}`;
}

function buildUserMessage(input: SelectTemplatesInput): string {
  const payload = {
    run_id: input.run_id,
    thesis: thesisDigest(input.thesis),
    catalog: STRATEGY_FAMILIES,
  };
  return JSON.stringify(payload);
}

// Send only the fields that inform the family choice. The constraint
// numbers and weighting mode don't change which family fits, so they're
// omitted to keep the prompt focused.
function thesisDigest(thesis: Thesis) {
  return {
    objective: thesis.objective,
    horizon_days: thesis.horizon_days,
    rebalance_frequency: thesis.rebalance_frequency,
    max_drawdown: thesis.constraints.max_drawdown,
    asset_count_range: [
      thesis.constraints.asset_count_min,
      thesis.constraints.asset_count_max,
    ],
    interpretation_notes: thesis.interpretation_notes,
  };
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
