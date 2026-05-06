import { fileURLToPath } from "node:url";

import { and, eq, inArray, sql } from "drizzle-orm";

import { db, pgPool } from "../db/client";
import { assets, assetMarketCaps, assetSourceMappings } from "../db/schema";
import {
  fetchCoinGeckoMarkets,
  type CoinGeckoMarketRow,
} from "./coingecko-client";
import { refreshAssetUniverseFeatures } from "./feature-view";

type SelectionDatabase = Pick<typeof db, "select">;
type MarketCapWriteDatabase = Pick<typeof db, "insert" | "update">;

export interface CoinGeckoMarketCapOptions {
  symbols: string[] | undefined;
  date: Date;
  dryRun: boolean;
}

export interface SelectedGmxAsset {
  assetId: string;
  symbol: string;
  sourceAssetId: string;
}

export interface MappedGmxAsset extends SelectedGmxAsset {
  coingeckoAssetId: string;
}

export interface MarketCapSelection {
  mapped: MappedGmxAsset[];
  unmapped: SelectedGmxAsset[];
}

export interface AssetMarketCapRow {
  assetId: string;
  timestamp: Date;
  source: "coingecko";
  marketCap: string;
  marketCapRank: number | null;
  metadata: {
    coingeckoAssetId: string;
    coingeckoSymbol: string;
    coingeckoName: string;
    currentPrice: number | null;
    totalVolume: number | null;
    lastUpdated: string | null;
  };
}

export interface ProcessMarketCapRowsResult {
  processedCount: number;
  skippedCount: number;
  missingCoinGeckoIds: string[];
}

export interface CoinGeckoMarketCapSummary {
  dryRun: boolean;
  date: string;
  selectedCount: number;
  mappedCount: number;
  unmappedCount: number;
  fetchedCount: number;
  writtenCount: number;
  featureViewRefreshed: boolean;
  skippedCount: number;
  missingCoinGeckoIds: string[];
}

interface CoinGeckoMarketCapRunnerDependencies {
  selectMappedAssets?: typeof selectMappedGmxAssets;
  fetchMarkets?: typeof fetchCoinGeckoMarkets;
  processRows?: typeof processMarketCapRows;
  refreshFeatureView?: typeof refreshAssetUniverseFeatures;
  writeLog?: typeof writeLog;
}

class CliArgumentError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CliArgumentError";
  }
}

function parseCommaList(flag: string, value: string): string[] {
  const entries = value.split(",").map((entry) => entry.trim());

  if (entries.length === 0 || entries.some((entry) => entry.length === 0)) {
    throw new CliArgumentError(`${flag} must be a comma-separated list`);
  }

  return entries;
}

export function toUtcDayTimestamp(date: Date): Date {
  return new Date(
    Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()),
  );
}

function parseSnapshotDate(value: string): Date {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new CliArgumentError("--date must use YYYY-MM-DD");
  }

  const [yearText, monthText, dayText] = value.split("-");
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const date = new Date(Date.UTC(year, month - 1, day));

  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    throw new CliArgumentError("--date must be a valid calendar day");
  }

  return date;
}

export function parseOptions(
  args: string[],
  now: Date = new Date(),
): CoinGeckoMarketCapOptions {
  const options: CoinGeckoMarketCapOptions = {
    symbols: undefined,
    date: toUtcDayTimestamp(now),
    dryRun: false,
  };

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];

    if (arg === "--") {
      continue;
    }

    if (arg === "--symbols") {
      const value = args[index + 1];
      if (!value || value.startsWith("--")) {
        throw new CliArgumentError("--symbols requires a comma-separated list");
      }
      if (options.symbols !== undefined) {
        throw new CliArgumentError("--symbols can only be provided once");
      }
      options.symbols = parseCommaList("--symbols", value);
      index += 1;
      continue;
    }

    if (arg === "--date") {
      const value = args[index + 1];
      if (!value || value.startsWith("--")) {
        throw new CliArgumentError("--date requires YYYY-MM-DD");
      }
      if (options.date.getTime() !== toUtcDayTimestamp(now).getTime()) {
        throw new CliArgumentError("--date can only be provided once");
      }
      options.date = parseSnapshotDate(value);
      index += 1;
      continue;
    }

    if (arg === "--dry-run") {
      options.dryRun = true;
      continue;
    }

    throw new CliArgumentError(`Unknown argument: ${arg}`);
  }

  return options;
}

export async function selectMappedGmxAssets(
  symbols: string[] | undefined,
  database: SelectionDatabase = db,
): Promise<MarketCapSelection> {
  const whereCondition =
    symbols === undefined
      ? eq(assets.source, "gmx")
      : and(eq(assets.source, "gmx"), inArray(assets.symbol, symbols));

  const rows = await database
    .select({
      assetId: assets.assetId,
      symbol: assets.symbol,
      sourceAssetId: assets.sourceAssetId,
      coingeckoAssetId: assetSourceMappings.sourceAssetId,
    })
    .from(assets)
    .leftJoin(
      assetSourceMappings,
      and(
        eq(assetSourceMappings.assetId, assets.assetId),
        eq(assetSourceMappings.source, "coingecko"),
      ),
    )
    .where(whereCondition);

  const mapped: MappedGmxAsset[] = [];
  const unmapped: SelectedGmxAsset[] = [];

  for (const row of rows) {
    const selected = {
      assetId: row.assetId,
      symbol: row.symbol,
      sourceAssetId: row.sourceAssetId,
    };

    if (row.coingeckoAssetId === null) {
      unmapped.push(selected);
      continue;
    }

    mapped.push({ ...selected, coingeckoAssetId: row.coingeckoAssetId });
  }

  return { mapped, unmapped };
}

export function toAssetMarketCapRow(
  asset: MappedGmxAsset,
  market: CoinGeckoMarketRow,
  timestamp: Date,
): AssetMarketCapRow {
  if (market.market_cap === null || !Number.isFinite(market.market_cap)) {
    throw new Error(`CoinGecko market_cap is required for ${market.id}`);
  }

  return {
    assetId: asset.assetId,
    timestamp,
    source: "coingecko",
    marketCap: String(market.market_cap),
    marketCapRank: market.market_cap_rank,
    metadata: {
      coingeckoAssetId: market.id,
      coingeckoSymbol: market.symbol,
      coingeckoName: market.name,
      currentPrice: market.current_price,
      totalVolume: market.total_volume,
      lastUpdated: market.last_updated,
    },
  };
}

export async function processMarketCapRows(
  mappedAssets: MappedGmxAsset[],
  marketRows: CoinGeckoMarketRow[],
  options: { timestamp: Date; dryRun: boolean },
  database: MarketCapWriteDatabase = db,
): Promise<ProcessMarketCapRowsResult> {
  if (mappedAssets.length === 0 || marketRows.length === 0) {
    return {
      processedCount: 0,
      skippedCount: mappedAssets.length,
      missingCoinGeckoIds: mappedAssets.map((asset) => asset.coingeckoAssetId),
    };
  }

  const marketsById = new Map(marketRows.map((row) => [row.id, row]));
  const missingCoinGeckoIds = mappedAssets
    .filter((asset) => !marketsById.has(asset.coingeckoAssetId))
    .map((asset) => asset.coingeckoAssetId);
  const rows = mappedAssets
    .map((asset) => {
      const market = marketsById.get(asset.coingeckoAssetId);
      return market
        ? toAssetMarketCapRow(asset, market, options.timestamp)
        : null;
    })
    .filter((row): row is AssetMarketCapRow => row !== null);

  if (rows.length === 0 || options.dryRun) {
    return {
      processedCount: rows.length,
      skippedCount: missingCoinGeckoIds.length,
      missingCoinGeckoIds,
    };
  }

  await database
    .insert(assetMarketCaps)
    .values(rows)
    .onConflictDoUpdate({
      target: [
        assetMarketCaps.assetId,
        assetMarketCaps.timestamp,
        assetMarketCaps.source,
      ],
      set: {
        marketCap: sql`excluded.market_cap`,
        marketCapRank: sql`excluded.market_cap_rank`,
        metadata: sql`excluded.metadata`,
      },
    });

  const updatedAt = new Date();
  for (const row of rows) {
    await database
      .update(assets)
      .set({ marketCapRank: row.marketCapRank, updatedAt })
      .where(eq(assets.assetId, row.assetId));
  }

  return {
    processedCount: rows.length,
    skippedCount: missingCoinGeckoIds.length,
    missingCoinGeckoIds,
  };
}

function writeLog(event: string, fields: object = {}): void {
  process.stdout.write(`${JSON.stringify({ event, ...fields })}\n`);
}

export async function runCoinGeckoMarketCapIngestion(
  options: CoinGeckoMarketCapOptions,
  dependencies: CoinGeckoMarketCapRunnerDependencies = {},
): Promise<CoinGeckoMarketCapSummary> {
  const selectMappedAssets =
    dependencies.selectMappedAssets ?? selectMappedGmxAssets;
  const fetchMarkets = dependencies.fetchMarkets ?? fetchCoinGeckoMarkets;
  const processRows = dependencies.processRows ?? processMarketCapRows;
  const refreshFeatureView =
    dependencies.refreshFeatureView ?? refreshAssetUniverseFeatures;
  const log = dependencies.writeLog ?? writeLog;

  log("coingecko_market_caps_started", {
    dryRun: options.dryRun,
    date: options.date.toISOString(),
    symbols: options.symbols,
  });

  const selection = await selectMappedAssets(options.symbols);
  log("coingecko_market_caps_assets_selected", {
    selectedCount: selection.mapped.length + selection.unmapped.length,
    mappedCount: selection.mapped.length,
    unmappedCount: selection.unmapped.length,
    mapped: selection.mapped,
    unmapped: selection.unmapped,
  });

  const coingeckoIds = [
    ...new Set(selection.mapped.map((asset) => asset.coingeckoAssetId)),
  ];
  const marketRows =
    coingeckoIds.length > 0 ? await fetchMarkets({ ids: coingeckoIds }) : [];
  const result = await processRows(selection.mapped, marketRows, {
    timestamp: options.date,
    dryRun: options.dryRun,
  });

  if (!options.dryRun && result.processedCount > 0) {
    log("agent_asset_universe_features_refresh_started");
    await refreshFeatureView();
    log("agent_asset_universe_features_refresh_completed");
  }

  const summary = {
    dryRun: options.dryRun,
    date: options.date.toISOString(),
    selectedCount: selection.mapped.length + selection.unmapped.length,
    mappedCount: selection.mapped.length,
    unmappedCount: selection.unmapped.length,
    fetchedCount: marketRows.length,
    writtenCount: options.dryRun ? 0 : result.processedCount,
    featureViewRefreshed: !options.dryRun && result.processedCount > 0,
    skippedCount: selection.unmapped.length + result.skippedCount,
    missingCoinGeckoIds: result.missingCoinGeckoIds,
  };

  log("coingecko_market_caps_completed", summary);
  log("coingecko_market_caps_summary", summary);

  return summary;
}

export async function main(args = process.argv.slice(2)): Promise<number> {
  try {
    const options = parseOptions(args);
    await runCoinGeckoMarketCapIngestion(options);

    return 0;
  } catch (error) {
    if (error instanceof CliArgumentError) {
      process.stderr.write(`${error.message}\n`);
      return 2;
    }

    process.stderr.write(
      `Fatal initialization error: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    return 2;
  }
}

function isCliEntrypoint() {
  const modulePath = fileURLToPath(import.meta.url);
  return (
    modulePath === process.argv[1] &&
    /[/\\]src[/\\]ingestion[/\\]coingecko-market-caps\.ts$/u.test(modulePath)
  );
}

if (isCliEntrypoint()) {
  try {
    process.exitCode = await main();
  } finally {
    await pgPool.end();
  }
}
