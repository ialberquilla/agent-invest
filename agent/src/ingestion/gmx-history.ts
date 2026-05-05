import { fileURLToPath } from "node:url";

import { and, eq, sql } from "drizzle-orm";

import { db, pgPool } from "../db/client";
import { assetPrices, assets } from "../db/schema";
import {
  fetchCandles,
  fetchTokens,
  type GmxCandle,
  type GmxToken,
} from "./gmx-client";
import { refreshAssetUniverseFeatures } from "./feature-view";

type AssetDatabase = Pick<typeof db, "insert">;
type PriceDatabase = Pick<typeof db, "insert" | "select">;

export interface SymbolSummary {
  symbol: string;
  assetId?: string;
  limit?: number;
  rowCount: number;
  startTimestamp: string | null;
  endTimestamp: string | null;
  dryRun: boolean;
  error?: string;
}

interface GmxHistoryDependencies {
  refreshFeatureView?: typeof refreshAssetUniverseFeatures;
}

export interface GmxHistorySummary {
  dryRun: boolean;
  featureViewRefreshed: boolean;
  symbolCount: number;
  successCount: number;
  failureCount: number;
  symbols: SymbolSummary[];
}

const COLD_START_DAY_LIMIT = 10_000;
const MS_PER_DAY = 24 * 60 * 60 * 1_000;

export interface GmxHistoryOptions {
  symbols: string[] | undefined;
  exclude: string[];
  fullRefresh: boolean;
  dryRun: boolean;
}

const EXCLUDED_SYMBOLS = new Set([
  "XAUT",
  "XAUT.v2",
  "GOLD",
  "SILVER",
  "BRENTOIL",
  "WTIOIL",
  "NATGAS",
]);

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

export function parseOptions(args: string[]): GmxHistoryOptions {
  const options: GmxHistoryOptions = {
    symbols: undefined,
    exclude: [],
    fullRefresh: false,
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

    if (arg === "--exclude") {
      const value = args[index + 1];
      if (!value || value.startsWith("--")) {
        throw new CliArgumentError("--exclude requires a comma-separated list");
      }
      if (options.exclude.length > 0) {
        throw new CliArgumentError("--exclude can only be provided once");
      }
      options.exclude = parseCommaList("--exclude", value);
      index += 1;
      continue;
    }

    if (arg === "--full-refresh") {
      options.fullRefresh = true;
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

export function filterSymbols(
  symbols: string[],
  options: Pick<GmxHistoryOptions, "symbols" | "exclude"> = {
    symbols: undefined,
    exclude: [],
  },
): string[] {
  const included =
    options.symbols === undefined ? undefined : new Set(options.symbols);
  const excluded = new Set(options.exclude);

  return symbols.filter((symbol) => {
    if (included !== undefined && !included.has(symbol)) {
      return false;
    }

    if (excluded.has(symbol)) {
      return false;
    }

    if (symbol.endsWith("_deprecated") || symbol.includes("_deprecated")) {
      return false;
    }

    if (symbol.startsWith("GLV")) {
      return false;
    }

    return !EXCLUDED_SYMBOLS.has(symbol);
  });
}

function getTokenSymbol(token: GmxToken): string | undefined {
  const symbol = token.symbol;

  return typeof symbol === "string" && symbol.length > 0 ? symbol : undefined;
}

function writeLog(event: string, fields: object = {}): void {
  process.stdout.write(`${JSON.stringify({ event, ...fields })}\n`);
}

function getErrorMessage(error: unknown): string {
  if (!(error instanceof Error)) {
    return String(error);
  }

  const cause = error.cause;

  if (cause instanceof Error && cause.message.length > 0) {
    return `${error.message}: ${cause.message}`;
  }

  return error.message;
}

export async function upsertAsset(
  symbol: string,
  database: AssetDatabase = db,
): Promise<string> {
  const assetId = symbol;
  const [asset] = await database
    .insert(assets)
    .values({
      assetId,
      source: "gmx",
      sourceAssetId: symbol,
      symbol,
      name: symbol,
    })
    .onConflictDoUpdate({
      target: [assets.source, assets.sourceAssetId],
      set: { updatedAt: new Date() },
    })
    .returning({ assetId: assets.assetId });

  if (!asset) {
    throw new Error(`Failed to upsert GMX asset: ${symbol}`);
  }

  return asset.assetId;
}

export async function getLatestGmxPriceTimestamp(
  assetId: string,
  database: PriceDatabase = db,
): Promise<Date | null> {
  const [row] = await database
    .select({ lastTs: sql<Date | null>`max(${assetPrices.timestamp})` })
    .from(assetPrices)
    .where(
      and(eq(assetPrices.assetId, assetId), eq(assetPrices.source, "gmx")),
    );

  return row?.lastTs ?? null;
}

export function getNeededCandleDays(
  lastTs: Date | null,
  options: { fullRefresh?: boolean; today?: Date } = {},
): number {
  if (options.fullRefresh || lastTs === null) {
    return COLD_START_DAY_LIMIT;
  }

  const todayDay = toUtcDayNumber(options.today ?? new Date());
  const lastDay = toUtcDayNumber(lastTs);

  return todayDay - lastDay + 2;
}

export async function getNeededGmxCandleDays(
  assetId: string,
  options: { fullRefresh?: boolean; today?: Date } = {},
  database: PriceDatabase = db,
): Promise<number> {
  const lastTs = options.fullRefresh
    ? null
    : await getLatestGmxPriceTimestamp(assetId, database);

  return getNeededCandleDays(lastTs, options);
}

export function getGmxCandleFetchLimit(needDays: number): number | undefined {
  return needDays > 0 ? needDays : undefined;
}

export async function upsertCandles(
  assetId: string,
  candles: GmxCandle[],
  database: PriceDatabase = db,
): Promise<void> {
  if (candles.length === 0) {
    return;
  }

  await database
    .insert(assetPrices)
    .values(candles.map((candle) => toAssetPriceRow(assetId, candle)))
    .onConflictDoUpdate({
      target: [assetPrices.assetId, assetPrices.timestamp, assetPrices.source],
      set: {
        open: sql`excluded.open`,
        high: sql`excluded.high`,
        low: sql`excluded.low`,
        close: sql`excluded.close`,
      },
    });
}

function toUtcDayNumber(date: Date): number {
  return Math.floor(
    Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()) /
      MS_PER_DAY,
  );
}

function toAssetPriceRow(assetId: string, candle: GmxCandle) {
  const [timestamp, open, high, low, close] = Array.isArray(candle)
    ? candle
    : [candle.timestamp, candle.open, candle.high, candle.low, candle.close];

  if (typeof timestamp !== "number") {
    throw new Error("GMX candle timestamp must be a unix timestamp in seconds");
  }

  return {
    assetId,
    timestamp: new Date(timestamp * 1_000),
    source: "gmx",
    open: toNumericString(open, "open"),
    high: toNumericString(high, "high"),
    low: toNumericString(low, "low"),
    close: toNumericString(close, "close"),
    volume: null,
    marketCap: null,
  };
}

function getCandleTimestamp(candle: GmxCandle): Date {
  const timestamp = Array.isArray(candle) ? candle[0] : candle.timestamp;

  if (typeof timestamp !== "number") {
    throw new Error("GMX candle timestamp must be a unix timestamp in seconds");
  }

  return new Date(timestamp * 1_000);
}

function getCandleRange(candles: GmxCandle[]): {
  startTimestamp: string | null;
  endTimestamp: string | null;
} {
  if (candles.length === 0) {
    return { startTimestamp: null, endTimestamp: null };
  }

  const timestamps = candles.map((candle) =>
    getCandleTimestamp(candle).getTime(),
  );

  return {
    startTimestamp: new Date(Math.min(...timestamps)).toISOString(),
    endTimestamp: new Date(Math.max(...timestamps)).toISOString(),
  };
}

async function processSymbol(
  symbol: string,
  options: GmxHistoryOptions,
): Promise<SymbolSummary> {
  const assetId = options.dryRun ? symbol : await upsertAsset(symbol);
  const needDays = options.dryRun
    ? getNeededCandleDays(null)
    : await getNeededGmxCandleDays(assetId, {
        fullRefresh: options.fullRefresh,
      });
  const limit = getGmxCandleFetchLimit(needDays);

  if (limit === undefined) {
    return {
      symbol,
      assetId,
      rowCount: 0,
      startTimestamp: null,
      endTimestamp: null,
      dryRun: options.dryRun,
    };
  }

  const candles = await fetchCandles({ symbol, limit });
  const range = getCandleRange(candles);

  if (!options.dryRun) {
    await upsertCandles(assetId, candles);
  }

  return {
    symbol,
    assetId,
    limit,
    rowCount: candles.length,
    ...range,
    dryRun: options.dryRun,
  };
}

function toNumericString(value: unknown, field: string): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }

  if (typeof value === "string" && value.trim().length > 0) {
    return value;
  }

  throw new Error(`GMX candle ${field} must be numeric`);
}

export async function runGmxHistoryIngestion(
  options: GmxHistoryOptions,
  dependencies: GmxHistoryDependencies = {},
): Promise<GmxHistorySummary> {
  const refreshFeatureView =
    dependencies.refreshFeatureView ?? refreshAssetUniverseFeatures;
  writeLog("gmx_history_started", { options });

  const tokens = await fetchTokens();
  const allSymbols = tokens
    .map((token) => getTokenSymbol(token))
    .filter((symbol): symbol is string => symbol !== undefined);
  const symbols = filterSymbols(allSymbols, options);

  writeLog("gmx_history_symbols_selected", {
    tokenCount: tokens.length,
    symbolCount: symbols.length,
    symbols,
  });

  const summaries: SymbolSummary[] = [];

  for (const symbol of symbols) {
    try {
      writeLog("gmx_history_symbol_started", { symbol });
      const summary = await processSymbol(symbol, options);
      summaries.push(summary);
      writeLog("gmx_history_symbol_completed", summary);
    } catch (error) {
      const summary: SymbolSummary = {
        symbol,
        rowCount: 0,
        startTimestamp: null,
        endTimestamp: null,
        dryRun: options.dryRun,
        error: getErrorMessage(error),
      };
      summaries.push(summary);
      writeLog("gmx_history_symbol_failed", summary);
    }
  }

  const failures = summaries.filter((summary) => summary.error !== undefined);
  const wroteClosePrices = summaries.some(
    (summary) => summary.error === undefined && summary.rowCount > 0,
  );

  if (!options.dryRun && wroteClosePrices) {
    writeLog("agent_asset_universe_features_refresh_started");
    await refreshFeatureView();
    writeLog("agent_asset_universe_features_refresh_completed");
  }

  const summary = {
    dryRun: options.dryRun,
    featureViewRefreshed: !options.dryRun && wroteClosePrices,
    symbolCount: summaries.length,
    successCount: summaries.length - failures.length,
    failureCount: failures.length,
    symbols: summaries,
  };

  writeLog("gmx_history_summary", summary);

  return summary;
}

export async function main(
  args = process.argv.slice(2),
  dependencies: GmxHistoryDependencies = {},
): Promise<number> {
  try {
    const summary = await runGmxHistoryIngestion(
      parseOptions(args),
      dependencies,
    );

    return summary.failureCount > 0 ? 1 : 0;
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

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  try {
    process.exitCode = await main();
  } finally {
    await pgPool.end();
  }
}
