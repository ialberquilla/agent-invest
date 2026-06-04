import {
  resolveMarketsBatch as defaultResolveMarketsBatch,
} from "../db/repositories/gmx-markets";
import type { ResolvedMarket } from "../market/gmx";
import {
  rankUniverse as defaultRankUniverse,
  type RankUniverseInput,
  type RankUniverseOutput,
  type RankUniverseRanking,
} from "./rank-universe";

export type ScreenerFactor = "momentum" | "risk_adjusted" | "low_volatility";

export type ScreenMarketsInput = {
  query?: string;
  factor?: ScreenerFactor;
  limit?: number;
  gmxOnly?: boolean;
  asOf?: string;
  timeoutSeconds?: number;
};

export type ScreenerMetric = {
  id: string;
  label: string;
  value: number | null;
  format: "percent" | "number";
};

export type ScreenerRow = {
  rank: number;
  source_rank: number;
  coin_id: string;
  symbol: string;
  market_name: string | null;
  is_gmx_tradeable: boolean;
  factor_values: Record<string, number | null>;
  metrics: ScreenerMetric[];
  actions: {
    long: { enabled: boolean; label: string };
    short: { enabled: boolean; label: string };
  };
  gmx_market: null | {
    chain: "arbitrum";
    market_token: string;
    index_token: string;
    long_token: string;
    short_token: string;
    collateral_token: string;
    collateral_decimals: number;
  };
};

export type ScreenerResult = {
  type: "market_screener";
  version: 1;
  title: string;
  summary: string;
  definition: {
    factor: ScreenerFactor;
    limit: number;
    gmx_only: boolean;
    as_of?: string;
  };
  rows: ScreenerRow[];
  notes: string[];
};

type ScreenMarketsDependencies = {
  rankUniverse?: typeof defaultRankUniverse;
  resolveMarketsBatch?: typeof defaultResolveMarketsBatch;
};

const FACTOR_CONFIG: Record<
  ScreenerFactor,
  {
    title: string;
    summary: string;
    ranking: RankUniverseRanking[];
    metricIds: string[];
  }
> = {
  momentum: {
    title: "GMX momentum screener",
    summary: "Ranked by 180 day return, with Sharpe and volatility as context.",
    ranking: [
      { factor: "return_180d", direction: "high", weight: 1 },
      { factor: "sharpe_180d", direction: "high", weight: 0.35 },
      { factor: "volatility_180d", direction: "low", weight: 0.15 },
    ],
    metricIds: ["return_180d", "sharpe_180d", "volatility_180d"],
  },
  risk_adjusted: {
    title: "GMX risk-adjusted screener",
    summary: "Ranked by 180 day Sharpe, with return and volatility as context.",
    ranking: [
      { factor: "sharpe_180d", direction: "high", weight: 1 },
      { factor: "return_180d", direction: "high", weight: 0.3 },
      { factor: "volatility_180d", direction: "low", weight: 0.2 },
    ],
    metricIds: ["sharpe_180d", "return_180d", "volatility_180d"],
  },
  low_volatility: {
    title: "GMX low-volatility screener",
    summary: "Ranked by lower 180 day volatility, with Sharpe and return as context.",
    ranking: [
      { factor: "volatility_180d", direction: "low", weight: 1 },
      { factor: "sharpe_180d", direction: "high", weight: 0.3 },
      { factor: "return_180d", direction: "high", weight: 0.15 },
    ],
    metricIds: ["volatility_180d", "sharpe_180d", "return_180d"],
  },
};

const METRIC_LABELS: Record<string, { label: string; format: ScreenerMetric["format"] }> = {
  return_180d: { label: "180d return", format: "percent" },
  sharpe_180d: { label: "180d Sharpe", format: "number" },
  volatility_180d: { label: "180d vol", format: "percent" },
};

export function screenerRequestFromMessage(
  message: string,
): ScreenMarketsInput | null {
  const normalized = message.toLowerCase();
  const looksLikeScreener =
    /\b(screen|screener|rank|ranking|top|best|tickers?|assets?|coins?|markets?)\b/.test(
      normalized,
    ) &&
    /\b(momentum|sharpe|risk[ -]?adjusted|low[ -]?vol|volatility)\b/.test(
      normalized,
    );
  if (!looksLikeScreener) return null;

  const limitMatch = /\b(?:top|best)\s+(\d{1,2})\b/.exec(normalized);
  const parsedLimit = limitMatch ? Number(limitMatch[1]) : 10;
  const limit = clampLimit(parsedLimit);
  const factor = normalized.includes("low vol") || normalized.includes("volatility")
    ? "low_volatility"
    : normalized.includes("sharpe") || normalized.includes("risk-adjusted") || normalized.includes("risk adjusted")
      ? "risk_adjusted"
      : "momentum";
  const gmxOnly = !/\b(non-gmx|all assets|research-only|research only|broader)\b/.test(
    normalized,
  );

  return { query: message, factor, limit, gmxOnly };
}

export async function screenMarkets(
  input: ScreenMarketsInput,
  dependencies: ScreenMarketsDependencies = {},
): Promise<ScreenerResult> {
  const factor = input.factor ?? "momentum";
  const limit = clampLimit(input.limit ?? 10);
  const gmxOnly = input.gmxOnly ?? true;
  const config = FACTOR_CONFIG[factor];
  const rankUniverse = dependencies.rankUniverse ?? defaultRankUniverse;
  const resolveMarketsBatch =
    dependencies.resolveMarketsBatch ?? defaultResolveMarketsBatch;
  const rankLimit = gmxOnly ? Math.min(Math.max(limit * 4, limit), 100) : limit;
  const rankInput: RankUniverseInput = {
    universe_selector: { id: "top_n_by_mcap", params: { n: 150 } },
    filters: [{ id: "min_history_days", value: 180 }],
    ranking: config.ranking,
    limit: rankLimit,
    ...(input.asOf ? { asOf: input.asOf } : {}),
    ...(input.timeoutSeconds ? { timeoutSeconds: input.timeoutSeconds } : {}),
  };
  const rankedRows = await rankUniverse(rankInput);
  const { resolved } = await resolveMarketsBatch(
    rankedRows.map((row) => row.coin_id),
  );
  const rows = rankedRows
    .map((row) => toScreenerRow(row, resolved.get(row.coin_id), config.metricIds))
    .filter((row) => (gmxOnly ? row.is_gmx_tradeable : true))
    .slice(0, limit)
    .map((row, index) => ({ ...row, rank: index + 1 }));

  return {
    type: "market_screener",
    version: 1,
    title: config.title,
    summary: `${config.summary} ${
      gmxOnly
        ? "Only rows with executable Arbitrum GMX V2 markets are shown."
        : "Rows without a GMX market are research-only and have no trade actions."
    }`,
    definition: {
      factor,
      limit,
      gmx_only: gmxOnly,
      ...(input.asOf ? { as_of: input.asOf } : {}),
    },
    rows,
    notes: [
      "Long/Short actions open a confirmation ticket; they do not submit a transaction.",
      "GMX perps can lose more quickly than spot exposure. Shorts and leverage require explicit confirmation before signing.",
    ],
  };
}

function toScreenerRow(
  row: RankUniverseOutput[number],
  market: ResolvedMarket | undefined,
  metricIds: string[],
): ScreenerRow {
  const tradeable = Boolean(market);
  const symbol = market?.symbol ?? row.coin_id.toUpperCase();
  return {
    rank: row.rank,
    source_rank: row.rank,
    coin_id: row.coin_id,
    symbol,
    market_name: market?.marketName ?? null,
    is_gmx_tradeable: tradeable,
    factor_values: row.factor_values,
    metrics: metricIds.map((id) => ({
      id,
      label: METRIC_LABELS[id]?.label ?? id,
      value: finiteNumberOrNull(row.factor_values[id]),
      format: METRIC_LABELS[id]?.format ?? "number",
    })),
    actions: {
      long: { enabled: tradeable, label: tradeable ? `Long ${symbol}` : "Research only" },
      short: { enabled: tradeable, label: tradeable ? `Short ${symbol}` : "Research only" },
    },
    gmx_market: market
      ? {
          chain: "arbitrum",
          market_token: market.gmxMarket,
          index_token: market.indexToken,
          long_token: market.longToken,
          short_token: market.shortToken,
          collateral_token: market.collateralToken,
          collateral_decimals: market.collateralDecimals,
        }
      : null,
  };
}

function finiteNumberOrNull(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function clampLimit(value: number) {
  if (!Number.isFinite(value)) return 10;
  return Math.max(1, Math.min(25, Math.trunc(value)));
}
