// select_universe -- second workflow step. Deterministic.
//
// Either short-circuits to a hand-picked basket or calls rank_universe
// with filters derived from the thesis (and any UniverseHint that came
// back from `decide` via the broaden_universe backward edge).

import {
  runRankUniverse,
  type RankUniverseRequest,
  type RankUniverseRow,
} from "../cli.ts";
import { createStepLogger, type StepLogger } from "../logging.ts";
import {
  resolveStrategyMode,
  type AssetExploration,
  type ExploredAsset,
  type Objective,
  type SelectUniverseInput,
  type StepName,
  type Thesis,
  type Universe,
  type UniverseHint,
} from "../state.ts";

// Cap the rejected list so a large ranked pool can't bloat the persisted
// result. The earliest-ranked rejections are the most informative.
const MAX_REJECTED = 30;

export type SelectUniverseDeps = {
  rankUniverse?: typeof runRankUniverse;
  logger?: StepLogger;
};

export type SelectUniverseResult = {
  delta: { universe: Universe };
  next: StepName;
};

export const NEXT_STEP: StepName = "select_window";

export async function selectUniverse(
  input: SelectUniverseInput,
  deps: SelectUniverseDeps = {},
): Promise<SelectUniverseResult> {
  const logger =
    deps.logger ??
    createStepLogger({ run_id: input.run_id, step: "select_universe" });
  const rankUniverse = deps.rankUniverse ?? runRankUniverse;

  logger.enter({
    objective: input.thesis.objective,
    top_n: input.thesis.universe_hints.top_n,
    top_skip: input.thesis.universe_hints.top_skip ?? null,
    hand_picked: Boolean(
      input.thesis.universe_hints.hand_picked_coin_ids?.length,
    ),
    hint_present: Boolean(input.hint),
  });

  try {
    const universe = await resolveUniverse(input, rankUniverse);
    logger.exit(NEXT_STEP, {
      source: universe.source,
      universe_size: universe.coin_ids.length,
      effective_top_n: universe.effective_filters.top_n,
      effective_top_skip: universe.effective_filters.top_skip ?? null,
    });
    return { delta: { universe }, next: NEXT_STEP };
  } catch (error) {
    logger.error(error);
    throw error;
  }
}

async function resolveUniverse(
  input: SelectUniverseInput,
  rankUniverse: typeof runRankUniverse,
): Promise<Universe> {
  const mode = resolveStrategyMode(input.thesis);

  // single_asset with an explicit target: the universe IS that coin. The
  // recipe pins it via config.target_coin_id; a one-coin hand_picked set
  // keeps the window/backtest scoped to it. With no target we fall through
  // to ranking and propose_candidates takes the top-ranked coin.
  if (mode === "single_asset" && input.thesis.target_coin_id) {
    const target = input.thesis.target_coin_id;
    return {
      coin_ids: [target],
      source: "hand_picked",
      effective_filters: {
        top_n: 1,
        exclude_stablecoins: input.thesis.universe_hints.exclude_stablecoins,
        exclude_wrapped: input.thesis.universe_hints.exclude_wrapped,
      },
    };
  }

  // pair_trade with explicit legs: the universe is exactly [long, short].
  if (mode === "pair_trade") {
    const long = input.thesis.long_coin_ids?.[0];
    const short = input.thesis.short_coin_ids?.[0];
    if (long && short && long !== short) {
      return {
        coin_ids: [long, short],
        source: "hand_picked",
        effective_filters: {
          top_n: 2,
          exclude_stablecoins: input.thesis.universe_hints.exclude_stablecoins,
          exclude_wrapped: input.thesis.universe_hints.exclude_wrapped,
        },
      };
    }
  }

  const handPicked = input.thesis.universe_hints.hand_picked_coin_ids ?? [];
  if (handPicked.length > 0) {
    return {
      coin_ids: handPicked,
      source: "hand_picked",
      effective_filters: {
        top_n: handPicked.length,
        exclude_stablecoins: input.thesis.universe_hints.exclude_stablecoins,
        exclude_wrapped: input.thesis.universe_hints.exclude_wrapped,
      },
    };
  }

  const request = buildRankUniverseRequest(input);
  const skip = input.thesis.universe_hints.top_skip ?? 0;
  const targetTopN =
    input.hint?.loosen.raise_top_n_to ?? input.thesis.universe_hints.top_n;
  const droppedFilters = input.hint?.loosen.drop_filter ?? [];

  const ranked = await rankUniverse(request);

  // top_skip excludes coins whose market_cap_rank is in the top `skip`
  // global ranks. We filter by market_cap_rank (not the returned list
  // order) because the brief language is "outside the top N by market
  // cap" and the returned list order depends on the active risk
  // profile (momentum-ranked under `aggressive`, etc.).
  const filtered =
    skip > 0
      ? ranked.filter((row) => {
          const mcr =
            typeof row.market_cap_rank === "number"
              ? row.market_cap_rank
              : Number.POSITIVE_INFINITY;
          return mcr > skip;
        })
      : ranked;

  const coinIds = filtered
    .slice(0, targetTopN)
    .map((row) => row.coin_id)
    .filter((id): id is string => typeof id === "string" && id.length > 0);

  if (coinIds.length === 0) {
    throw new EmptyUniverseError(
      `rank_universe returned 0 coins after top_skip=${skip}; check filters or pass a UniverseHint with raise_top_n_to`,
    );
  }

  return {
    coin_ids: coinIds,
    source: "rank_universe",
    effective_filters: {
      top_n: request.top_n,
      top_skip: skip > 0 ? skip : undefined,
      market_cap_min_usd: request.min_market_cap,
      min_history_days: request.min_history_days,
      exclude_stablecoins: Boolean(request.exclude_stablecoins),
      exclude_wrapped: Boolean(request.exclude_wrapped),
      risk_profile: request.risk_profile,
      dropped_filters: droppedFilters.length > 0 ? droppedFilters : undefined,
    },
    exploration: buildExploration(ranked, coinIds, skip, targetTopN),
  };
}

// Explain the rank_universe selection: which ranked names were dropped by
// top_skip (excluded mega-caps) vs by the top-N cutoff (ranked too low).
// Coins filtered out upstream by rank_universe (stablecoins, short history)
// never come back as rows, so we can only name the rank-stage rejections.
function buildExploration(
  ranked: RankUniverseRow[],
  selected: string[],
  skip: number,
  targetTopN: number,
): AssetExploration {
  const selectedSet = new Set(selected);
  const rejected: ExploredAsset[] = [];

  for (const row of ranked) {
    if (typeof row.coin_id !== "string" || !row.coin_id) continue;
    if (selectedSet.has(row.coin_id)) continue;

    const mcr =
      typeof row.market_cap_rank === "number" ? row.market_cap_rank : undefined;
    if (skip > 0 && mcr !== undefined && mcr <= skip) {
      rejected.push(asset(row, `excluded by top_skip (market-cap rank ${mcr} <= ${skip})`));
      continue;
    }
    // Survived top_skip but didn't make the top-N cut.
    rejected.push(
      asset(row, `ranked below the top ${targetTopN} after filters`),
    );
  }

  return {
    considered_count: ranked.length,
    selected_count: selected.length,
    selected,
    rejected: rejected.slice(0, MAX_REJECTED),
  };
}

function asset(row: RankUniverseRow, reason: string): ExploredAsset {
  return {
    coin_id: row.coin_id,
    ...(typeof row.symbol === "string" ? { symbol: row.symbol } : {}),
    ...(typeof row.market_cap_rank === "number"
      ? { market_cap_rank: row.market_cap_rank }
      : {}),
    reason,
  };
}

function buildRankUniverseRequest(
  input: SelectUniverseInput,
): RankUniverseRequest {
  const hints = input.thesis.universe_hints;
  const loosen = input.hint?.loosen;
  const dropped = new Set(loosen?.drop_filter ?? []);

  const baseTopN = hints.top_n;
  const skip = hints.top_skip ?? 0;
  // Ask the CLI for top_n + top_skip results so we still have top_n
  // coins after we slice off the leading top_skip ranks.
  const effectiveTopN = Math.max(
    1,
    (loosen?.raise_top_n_to ?? baseTopN) + skip,
  );

  // Treat a 0 (or negative) floor as "no floor"; the CLI's market-cap
  // filter code path errors out when the floor is 0 and would also be
  // a no-op semantically.
  const rawFloor =
    loosen?.lower_market_cap_floor_to ?? hints.market_cap_min_usd;
  const marketCapFloor =
    typeof rawFloor === "number" && rawFloor > 0 ? rawFloor : undefined;

  // min_history_days is strategy-dependent and owned by the thesis.
  // interpret_brief sets it (or omits it) based on the brief: static
  // buy-and-hold / long-horizon mean reversion want a long history;
  // momentum / periodic rebalance don't because they sample
  // dynamically each round. `decide` can lower the floor via the
  // UniverseHint loosen path when the universe ends up too small.
  const rawMinHistory =
    loosen?.lower_min_history_days_to ?? hints.min_history_days;
  const minHistoryDays =
    typeof rawMinHistory === "number" && rawMinHistory > 0
      ? rawMinHistory
      : undefined;

  return {
    top_n: effectiveTopN,
    exclude_stablecoins:
      hints.exclude_stablecoins && !dropped.has("exclude_stablecoins"),
    exclude_wrapped: hints.exclude_wrapped && !dropped.has("exclude_wrapped"),
    min_market_cap: marketCapFloor,
    min_history_days: minHistoryDays,
    risk_profile: riskProfileFromObjective(input.thesis.objective),
  };
}

// rank_universe.py currently raises KeyError for the `balanced`,
// `income`, `preserve_capital`, and `high_growth` profiles, so we map
// the workflow objectives to the working CLI values:
//   - balanced_growth  -> undefined (default sort = market_cap_rank)
//   - growth           -> "aggressive"  (momentum-ranked)
//   - income           -> "preserve"    (income is broken; preserve is the closest stable approximation)
//   - preserve_capital -> "preserve"    (preserve_capital is broken)
// TODO: revisit once rank_universe.py is fixed for the missing-column profiles.
export function riskProfileFromObjective(
  objective: Objective,
): RankUniverseRequest["risk_profile"] {
  switch (objective) {
    case "balanced_growth":
      return undefined;
    case "growth":
      return "aggressive";
    case "income":
      return "preserve";
    case "preserve_capital":
      return "preserve";
  }
}

export class EmptyUniverseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EmptyUniverseError";
  }
}

// Exported for tests/evals that want to inspect what would be sent.
export { buildRankUniverseRequest };

// Re-exported for callers that want to inject a custom hint type.
export type { UniverseHint, Thesis };
