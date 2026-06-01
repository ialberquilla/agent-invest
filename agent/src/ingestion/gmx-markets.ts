// Phase 2 of plans/integrate_contracts.md: ingest the GMX V2 token + market
// directory into gmx_tokens / gmx_markets so a coin_id can be resolved to an
// executable market without a live API round-trip. Mirrors gmx-history.ts:
// typed accessors over the open client shapes, JSON event logs, a --dry-run
// flag, and a CLI entrypoint. Run via `tsx src/ingestion/gmx-markets.ts`.

import { fileURLToPath } from "node:url";

import { db, pgPool } from "../db/client";
import {
  upsertGmxMarkets,
  upsertGmxTokens,
} from "../db/repositories/gmx-markets";
import type { NewGmxMarketRow, NewGmxTokenRow } from "../db/schema";
import {
  fetchMarkets,
  fetchTokens,
  type GmxMarket,
  type GmxToken,
} from "./gmx-client";

type Db = Omit<typeof db, "$client">;

export interface GmxMarketsOptions {
  dryRun: boolean;
}

export interface GmxMarketsSummary {
  dryRun: boolean;
  tokenCount: number;
  marketCount: number;
  skippedTokens: number;
  skippedMarkets: number;
}

interface GmxMarketsDependencies {
  fetchTokens?: typeof fetchTokens;
  fetchMarkets?: typeof fetchMarkets;
  database?: Db;
}

class CliArgumentError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CliArgumentError";
  }
}

export function parseOptions(args: string[]): GmxMarketsOptions {
  const options: GmxMarketsOptions = { dryRun: false };
  for (const arg of args) {
    if (arg === "--" ) continue;
    if (arg === "--dry-run") {
      options.dryRun = true;
      continue;
    }
    throw new CliArgumentError(`Unknown argument: ${arg}`);
  }
  return options;
}

function writeLog(event: string, fields: object = {}): void {
  process.stdout.write(`${JSON.stringify({ event, ...fields })}\n`);
}

// Typed projection of the open GmxToken shape; returns null when the row is
// missing the fields we require (skip rather than poison the table).
export function toTokenRow(token: GmxToken): NewGmxTokenRow | null {
  const symbol = token.symbol;
  const address = token.address;
  const decimals = token.decimals;
  if (
    typeof symbol !== "string" ||
    symbol.length === 0 ||
    typeof address !== "string" ||
    address.length === 0 ||
    typeof decimals !== "number" ||
    !Number.isInteger(decimals)
  ) {
    return null;
  }
  return {
    symbol,
    address,
    decimals,
    synthetic: token.synthetic === true,
  };
}

export function toMarketRow(market: GmxMarket): NewGmxMarketRow | null {
  if (
    typeof market.marketToken !== "string" ||
    market.marketToken.length === 0 ||
    typeof market.indexToken !== "string" ||
    typeof market.longToken !== "string" ||
    typeof market.shortToken !== "string" ||
    typeof market.name !== "string"
  ) {
    return null;
  }
  const listingDate =
    typeof market.listingDate === "string"
      ? new Date(market.listingDate)
      : null;
  return {
    marketToken: market.marketToken,
    name: market.name,
    indexToken: market.indexToken,
    longToken: market.longToken,
    shortToken: market.shortToken,
    isListed: market.isListed !== false,
    listingDate:
      listingDate && !Number.isNaN(listingDate.getTime()) ? listingDate : null,
  };
}

export async function runGmxMarketsIngestion(
  options: GmxMarketsOptions,
  dependencies: GmxMarketsDependencies = {},
): Promise<GmxMarketsSummary> {
  const fetchTokensFn = dependencies.fetchTokens ?? fetchTokens;
  const fetchMarketsFn = dependencies.fetchMarkets ?? fetchMarkets;
  const database = dependencies.database ?? db;

  writeLog("gmx_markets_started", { options });

  const [rawTokens, rawMarkets] = await Promise.all([
    fetchTokensFn(),
    fetchMarketsFn(),
  ]);

  const tokenRows: NewGmxTokenRow[] = [];
  let skippedTokens = 0;
  for (const token of rawTokens) {
    const row = toTokenRow(token);
    if (row) tokenRows.push(row);
    else skippedTokens += 1;
  }

  const marketRows: NewGmxMarketRow[] = [];
  let skippedMarkets = 0;
  for (const market of rawMarkets) {
    const row = toMarketRow(market);
    if (row) marketRows.push(row);
    else skippedMarkets += 1;
  }

  if (!options.dryRun) {
    await upsertGmxTokens(tokenRows, database);
    await upsertGmxMarkets(marketRows, database);
  }

  const summary: GmxMarketsSummary = {
    dryRun: options.dryRun,
    tokenCount: tokenRows.length,
    marketCount: marketRows.length,
    skippedTokens,
    skippedMarkets,
  };
  writeLog("gmx_markets_summary", summary);
  return summary;
}

export async function main(
  args = process.argv.slice(2),
  dependencies: GmxMarketsDependencies = {},
): Promise<number> {
  try {
    await runGmxMarketsIngestion(parseOptions(args), dependencies);
    return 0;
  } catch (error) {
    if (error instanceof CliArgumentError) {
      process.stderr.write(`${error.message}\n`);
      return 2;
    }
    process.stderr.write(
      `Fatal GMX markets ingestion error: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    return 1;
  }
}

function isCliEntrypoint(): boolean {
  const modulePath = fileURLToPath(import.meta.url);
  return (
    modulePath === process.argv[1] &&
    /[/\\]src[/\\]ingestion[/\\]gmx-markets\.ts$/u.test(modulePath)
  );
}

if (isCliEntrypoint()) {
  try {
    process.exitCode = await main();
  } finally {
    await pgPool.end();
  }
}
