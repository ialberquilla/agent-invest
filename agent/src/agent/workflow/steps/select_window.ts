// select_window -- third workflow step. Deterministic. Wraps
// recommend_backtest_window with the universe coin set and the thesis
// horizon. Preserves `thesis.horizon_days` as the intended trade
// horizon (downstream validate_against_thesis depends on it) and
// exposes recommender diagnostics in `effective`.

import { runRecommendBacktestWindow } from "../cli.ts";
import { createStepLogger, type StepLogger } from "../logging.ts";
import type {
  SelectWindowInput,
  StepName,
  Window,
} from "../state.ts";

export type SelectWindowDeps = {
  recommendWindow?: typeof runRecommendBacktestWindow;
  logger?: StepLogger;
};

export type SelectWindowResult = {
  delta: { window: Window };
  next: StepName;
};

export const NEXT_STEP: StepName = "propose_candidates";

export async function selectWindow(
  input: SelectWindowInput,
  deps: SelectWindowDeps = {},
): Promise<SelectWindowResult> {
  const logger =
    deps.logger ??
    createStepLogger({ run_id: input.run_id, step: "select_window" });
  const recommendWindow = deps.recommendWindow ?? runRecommendBacktestWindow;

  logger.enter({
    horizon_days: input.thesis.horizon_days,
    universe_size: input.universe.coin_ids.length,
  });

  try {
    const response = await recommendWindow({
      coin_ids: input.universe.coin_ids,
      horizon_days: input.thesis.horizon_days,
    });

    const constraints = response.history_constraints;
    const drawdowns = Array.isArray(response.covered_drawdowns)
      ? response.covered_drawdowns.length
      : 0;
    const limitingCoin = constraints?.limiting_coin;
    const limitingCoinDates = limitingCoinHistory(
      constraints?.coins,
      limitingCoin,
    );

    const window: Window = {
      start: response.start,
      end: response.end,
      horizon_days: input.thesis.horizon_days,
      effective: {
        window_length_days:
          constraints?.window_length_days ??
          daysBetween(response.start, response.end),
        target_window_length_days:
          constraints?.target_window_length_days ??
          input.thesis.horizon_days * 2,
        rationale: response.rationale,
        limiting_coin: limitingCoin,
        limiting_coin_first_price_date: limitingCoinDates.first ?? undefined,
        limiting_coin_last_price_date: limitingCoinDates.last ?? undefined,
        intersection_start: constraints?.intersection_start,
        intersection_end: constraints?.intersection_end,
        covered_drawdowns_count: drawdowns,
      },
    };

    if (window.effective.window_length_days < input.thesis.horizon_days) {
      // Surface this loudly. Downstream validate_against_thesis would
      // reject any candidate with a realised window shorter than the
      // intended horizon, so we want decide to see this as a clear
      // signal (likely route: broaden_universe or reinterpret_brief).
      logger.capHit({
        cap: "window_length_below_horizon",
        observed: window.effective.window_length_days,
        limit: input.thesis.horizon_days,
      });
    }

    logger.exit(NEXT_STEP, {
      start: window.start,
      end: window.end,
      window_length_days: window.effective.window_length_days,
      limiting_coin: window.effective.limiting_coin ?? null,
      drawdowns: window.effective.covered_drawdowns_count,
    });
    return { delta: { window }, next: NEXT_STEP };
  } catch (error) {
    logger.error(error);
    throw error;
  }
}

// Extracts the limiting coin's first/last price dates from the
// recommender's per-coin history map, so the workflow timeline can
// show concretely why a particular coin is shrinking the window.
function limitingCoinHistory(
  coins: Record<string, unknown> | undefined,
  limitingCoin: string | undefined,
): { first: string | null; last: string | null } {
  if (!coins || !limitingCoin) return { first: null, last: null };
  const entry = coins[limitingCoin];
  if (!entry || typeof entry !== "object") return { first: null, last: null };
  const record = entry as { first_price_date?: unknown; last_price_date?: unknown };
  return {
    first:
      typeof record.first_price_date === "string"
        ? record.first_price_date
        : null,
    last:
      typeof record.last_price_date === "string"
        ? record.last_price_date
        : null,
  };
}

function daysBetween(start: string, end: string): number {
  const startMs = Date.parse(start);
  const endMs = Date.parse(end);
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return 0;
  return Math.max(0, Math.round((endMs - startMs) / 86_400_000));
}
