import { eq, sql } from "drizzle-orm";

import {
  type GmxMarketInfo,
  type GmxTokenInfo,
  resolveMarketFrom,
  type ResolvedMarket,
} from "../../market/gmx.ts";
import { db as defaultDb } from "../client";
import {
  assetSourceMappings,
  gmxMarkets,
  gmxTokens,
  type NewGmxMarketRow,
  type NewGmxTokenRow,
} from "../schema";

type Db = Omit<typeof defaultDb, "$client">;

export async function upsertGmxTokens(
  rows: NewGmxTokenRow[],
  db: Db = defaultDb,
): Promise<number> {
  if (rows.length === 0) return 0;
  await db
    .insert(gmxTokens)
    .values(rows)
    .onConflictDoUpdate({
      target: gmxTokens.symbol,
      set: {
        address: sql`excluded.address`,
        decimals: sql`excluded.decimals`,
        synthetic: sql`excluded.synthetic`,
        updatedAt: sql`now()`,
      },
    });
  return rows.length;
}

export async function upsertGmxMarkets(
  rows: NewGmxMarketRow[],
  db: Db = defaultDb,
): Promise<number> {
  if (rows.length === 0) return 0;
  await db
    .insert(gmxMarkets)
    .values(rows)
    .onConflictDoUpdate({
      target: gmxMarkets.marketToken,
      set: {
        name: sql`excluded.name`,
        indexToken: sql`excluded.index_token`,
        longToken: sql`excluded.long_token`,
        shortToken: sql`excluded.short_token`,
        isListed: sql`excluded.is_listed`,
        listingDate: sql`excluded.listing_date`,
        updatedAt: sql`now()`,
      },
    });
  return rows.length;
}

async function loadTokens(db: Db): Promise<GmxTokenInfo[]> {
  const rows = await db
    .select({
      symbol: gmxTokens.symbol,
      address: gmxTokens.address,
      decimals: gmxTokens.decimals,
      synthetic: gmxTokens.synthetic,
    })
    .from(gmxTokens);
  return rows;
}

async function loadMarkets(db: Db): Promise<GmxMarketInfo[]> {
  const rows = await db
    .select({
      marketToken: gmxMarkets.marketToken,
      name: gmxMarkets.name,
      indexToken: gmxMarkets.indexToken,
      longToken: gmxMarkets.longToken,
      shortToken: gmxMarkets.shortToken,
      isListed: gmxMarkets.isListed,
    })
    .from(gmxMarkets);
  return rows;
}

// CoinGecko coin_id -> GMX symbol bridge (asset_source_mappings, where the GMX
// asset_id is the symbol). Required because workflow/mandate coin_ids are
// CoinGecko ids, not GMX symbols.
async function loadSymbolByCoinId(db: Db): Promise<Record<string, string>> {
  const rows = await db
    .select({
      coinId: assetSourceMappings.sourceAssetId,
      symbol: assetSourceMappings.assetId,
    })
    .from(assetSourceMappings)
    .where(eq(assetSourceMappings.source, "coingecko"));
  const map: Record<string, string> = {};
  for (const row of rows) map[row.coinId] = row.symbol;
  return map;
}

// Resolve one coin_id (= GMX symbol) to an executable market, or throw a
// MarketResolutionError (fail-closed). Loads the full token/market directory
// once; callers resolving many coins should prefer resolveMarketsBatch.
export async function resolveMarket(
  coinId: string,
  db: Db = defaultDb,
): Promise<ResolvedMarket> {
  const [tokens, markets, symbolByCoinId] = await Promise.all([
    loadTokens(db),
    loadMarkets(db),
    loadSymbolByCoinId(db),
  ]);
  return resolveMarketFrom({ tokens, markets, symbolByCoinId }, coinId);
}

// Resolve a set of coin_ids against a single directory load. Returns resolved
// markets and per-coin failures so the caller can abort the whole rebalance on
// any failure (never partially execute).
export async function resolveMarketsBatch(
  coinIds: string[],
  db: Db = defaultDb,
): Promise<{
  resolved: Map<string, ResolvedMarket>;
  failures: Map<string, unknown>;
}> {
  const [tokens, markets, symbolByCoinId] = await Promise.all([
    loadTokens(db),
    loadMarkets(db),
    loadSymbolByCoinId(db),
  ]);
  const data = { tokens, markets, symbolByCoinId };
  const resolved = new Map<string, ResolvedMarket>();
  const failures = new Map<string, unknown>();
  for (const coinId of coinIds) {
    try {
      resolved.set(coinId, resolveMarketFrom(data, coinId));
    } catch (error) {
      failures.set(coinId, error);
    }
  }
  return { resolved, failures };
}

// Every CoinGecko coin_id that resolves to an executable GMX market, with
// its GMX symbol. Drives the strategy-type picker's coin selector so users
// choose from the tradeable set instead of typing a coin_id. Sorted by
// symbol; unresolvable bridge entries are dropped.
export async function listGmxTradeableCoins(
  db: Db = defaultDb,
): Promise<Array<{ coin_id: string; symbol: string }>> {
  const [tokens, markets, symbolByCoinId] = await Promise.all([
    loadTokens(db),
    loadMarkets(db),
    loadSymbolByCoinId(db),
  ]);
  const data = { tokens, markets, symbolByCoinId };
  const out: Array<{ coin_id: string; symbol: string }> = [];
  for (const coinId of Object.keys(symbolByCoinId)) {
    try {
      const market = resolveMarketFrom(data, coinId);
      out.push({ coin_id: coinId, symbol: market.symbol });
    } catch {
      // Bridged but not executable (no token/market) -- skip.
    }
  }
  out.sort((a, b) => a.symbol.localeCompare(b.symbol));
  return out;
}

export async function countGmxMarkets(db: Db = defaultDb): Promise<number> {
  const [row] = await db
    .select({ n: sql<number>`count(*)::int` })
    .from(gmxMarkets);
  return row?.n ?? 0;
}

export async function readGmxMarketByToken(
  marketToken: string,
  db: Db = defaultDb,
) {
  const [row] = await db
    .select()
    .from(gmxMarkets)
    .where(eq(gmxMarkets.marketToken, marketToken));
  return row ?? null;
}
