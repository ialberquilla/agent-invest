import assert from "node:assert/strict";
import test from "node:test";
import { eq } from "drizzle-orm";
import { PgDialect } from "drizzle-orm/pg-core";

import { assets, assetMarketCaps, assetSourceMappings } from "../src/db/schema";
import { refreshAssetUniverseFeatures } from "../src/ingestion/feature-view";
import {
  parseOptions,
  processMarketCapRows,
  runCoinGeckoMarketCapIngestion,
  selectMappedGmxAssets,
  toAssetMarketCapRow,
  toUtcDayTimestamp,
} from "../src/ingestion/coingecko-market-caps";
import type { CoinGeckoMarketRow } from "../src/ingestion/coingecko-client";

const bitcoinMarketRow: CoinGeckoMarketRow = {
  id: "bitcoin",
  symbol: "btc",
  name: "Bitcoin",
  image: "https://example.com/bitcoin.png",
  current_price: 65_000,
  market_cap: 1_280_000_000_000,
  market_cap_rank: 1,
  fully_diluted_valuation: 1_365_000_000_000,
  total_volume: 20_000_000_000,
  high_24h: 66_000,
  low_24h: 64_000,
  price_change_24h: 100,
  price_change_percentage_24h: 0.15,
  market_cap_change_24h: 1_000_000_000,
  market_cap_change_percentage_24h: 0.08,
  circulating_supply: 19_700_000,
  total_supply: 19_700_000,
  max_supply: 21_000_000,
  ath: 73_000,
  ath_change_percentage: -10,
  ath_date: "2024-03-14T00:00:00.000Z",
  atl: 67,
  atl_change_percentage: 97_000,
  atl_date: "2013-07-06T00:00:00.000Z",
  roi: null,
  last_updated: "2026-05-05T12:00:00.000Z",
};

const pgDialect = new PgDialect();

function renderSql(value: unknown): { sql: string; params: unknown[] } {
  const { sql, params } = pgDialect.sqlToQuery(value as never);
  return { sql, params };
}

test("parseOptions accepts symbols, date, dry-run, and defaults date to current UTC day", () => {
  assert.deepEqual(
    parseOptions(
      ["--symbols", "BTC,ETH,SOL", "--date", "2026-05-04", "--dry-run"],
      new Date("2026-05-05T23:30:00.000Z"),
    ),
    {
      symbols: ["BTC", "ETH", "SOL"],
      date: new Date("2026-05-04T00:00:00.000Z"),
      dryRun: true,
    },
  );

  assert.deepEqual(parseOptions([], new Date("2026-05-05T23:30:00.000Z")), {
    symbols: undefined,
    date: new Date("2026-05-05T00:00:00.000Z"),
    dryRun: false,
  });
});

test("parseOptions rejects unknown and malformed arguments", () => {
  assert.throws(() => parseOptions(["--exclude", "BTC"]), /Unknown argument/);
  assert.throws(() => parseOptions(["--symbols"]), /--symbols requires/);
  assert.throws(() => parseOptions(["--symbols", "BTC,"]), /comma-separated/);
  assert.throws(() => parseOptions(["--date", "2026-5-04"]), /YYYY-MM-DD/);
  assert.throws(
    () => parseOptions(["--date", "2026-02-30"]),
    /valid calendar day/,
  );
});

test("toUtcDayTimestamp normalizes instants to a UTC day timestamp", () => {
  assert.equal(
    toUtcDayTimestamp(new Date("2026-05-05T23:59:59.999Z")).toISOString(),
    "2026-05-05T00:00:00.000Z",
  );
});

test("selectMappedGmxAssets selects GMX assets and separates CoinGecko mappings", async () => {
  const calls: {
    select?: unknown;
    from?: unknown;
    join?: unknown;
    where?: unknown;
  } = {};
  const database = {
    select(selection: unknown) {
      calls.select = selection;

      return {
        from(table: unknown) {
          calls.from = table;

          return {
            leftJoin(table: unknown, condition: unknown) {
              calls.join = { table, condition };

              return {
                where(condition: unknown) {
                  calls.where = condition;
                  return Promise.resolve([
                    {
                      assetId: "BTC",
                      symbol: "BTC",
                      sourceAssetId: "BTC",
                      coingeckoAssetId: "bitcoin",
                    },
                    {
                      assetId: "SOL",
                      symbol: "SOL",
                      sourceAssetId: "SOL",
                      coingeckoAssetId: null,
                    },
                  ]);
                },
              };
            },
          };
        },
      };
    },
  };

  assert.deepEqual(
    await selectMappedGmxAssets(["BTC", "SOL"], database as never),
    {
      mapped: [
        {
          assetId: "BTC",
          symbol: "BTC",
          sourceAssetId: "BTC",
          coingeckoAssetId: "bitcoin",
        },
      ],
      unmapped: [{ assetId: "SOL", symbol: "SOL", sourceAssetId: "SOL" }],
    },
  );
  assert.equal(calls.from, assets);
  assert.deepEqual(calls.select, {
    assetId: assets.assetId,
    symbol: assets.symbol,
    sourceAssetId: assets.sourceAssetId,
    coingeckoAssetId: assetSourceMappings.sourceAssetId,
  });
  assert.ok(calls.join);
  assert.deepEqual(renderSql(calls.where), {
    sql: '("assets"."source" = $1 and "assets"."symbol" in ($2, $3))',
    params: ["gmx", "BTC", "SOL"],
  });
});

test("selectMappedGmxAssets filters all-asset selection to GMX source only", async () => {
  let where: unknown;
  const database = {
    select() {
      return {
        from() {
          return {
            leftJoin() {
              return {
                where(condition: unknown) {
                  where = condition;
                  return Promise.resolve([]);
                },
              };
            },
          };
        },
      };
    },
  };

  await selectMappedGmxAssets(undefined, database as never);

  assert.deepEqual(renderSql(where), {
    sql: '"assets"."source" = $1',
    params: ["gmx"],
  });
});

test("toAssetMarketCapRow converts a mapped CoinGecko market row", () => {
  assert.deepEqual(
    toAssetMarketCapRow(
      {
        assetId: "BTC",
        symbol: "BTC",
        sourceAssetId: "BTC",
        coingeckoAssetId: "bitcoin",
      },
      bitcoinMarketRow,
      new Date("2026-05-05T00:00:00.000Z"),
    ),
    {
      assetId: "BTC",
      timestamp: new Date("2026-05-05T00:00:00.000Z"),
      source: "coingecko",
      marketCap: "1280000000000",
      marketCapRank: 1,
      metadata: {
        coingeckoAssetId: "bitcoin",
        coingeckoSymbol: "btc",
        coingeckoName: "Bitcoin",
        currentPrice: 65_000,
        totalVolume: 20_000_000_000,
        lastUpdated: "2026-05-05T12:00:00.000Z",
      },
    },
  );
});

test("processMarketCapRows upserts snapshots and updates cached asset rank", async () => {
  const calls: {
    insertTable?: unknown;
    values?: unknown;
    conflict?: unknown;
    updateTable?: unknown;
    updateSet?: unknown;
    updateWhere?: unknown;
  } = {};
  const database = {
    insert(table: unknown) {
      calls.insertTable = table;

      return {
        values(values: unknown) {
          calls.values = values;

          return {
            onConflictDoUpdate(conflict: unknown) {
              calls.conflict = conflict;
              return Promise.resolve();
            },
          };
        },
      };
    },
    update(table: unknown) {
      calls.updateTable = table;

      return {
        set(values: unknown) {
          calls.updateSet = values;

          return {
            where(condition: unknown) {
              calls.updateWhere = condition;
              return Promise.resolve();
            },
          };
        },
      };
    },
  };

  assert.deepEqual(
    await processMarketCapRows(
      [
        {
          assetId: "BTC",
          symbol: "BTC",
          sourceAssetId: "BTC",
          coingeckoAssetId: "bitcoin",
        },
      ],
      [bitcoinMarketRow],
      { timestamp: new Date("2026-05-05T00:00:00.000Z"), dryRun: false },
      database as never,
    ),
    { processedCount: 1, skippedCount: 0, missingCoinGeckoIds: [] },
  );

  assert.equal(calls.insertTable, assetMarketCaps);
  assert.deepEqual(calls.values, [
    toAssetMarketCapRow(
      {
        assetId: "BTC",
        symbol: "BTC",
        sourceAssetId: "BTC",
        coingeckoAssetId: "bitcoin",
      },
      bitcoinMarketRow,
      new Date("2026-05-05T00:00:00.000Z"),
    ),
  ]);

  const conflict = calls.conflict as {
    target: unknown;
    set: Record<string, unknown>;
  };
  assert.deepEqual(conflict.target, [
    assetMarketCaps.assetId,
    assetMarketCaps.timestamp,
    assetMarketCaps.source,
  ]);
  assert.deepEqual(Object.keys(conflict.set), [
    "marketCap",
    "marketCapRank",
    "metadata",
  ]);
  assert.equal(calls.updateTable, assets);
  assert.ok(
    (calls.updateSet as { updatedAt: unknown }).updatedAt instanceof Date,
  );
  assert.equal(
    (calls.updateSet as { marketCapRank: unknown }).marketCapRank,
    1,
  );
  assert.ok(calls.updateWhere);
});

test("processMarketCapRows skips unmapped market rows", async () => {
  const database = {
    insert() {
      throw new Error("insert should not be called");
    },
    update() {
      throw new Error("update should not be called");
    },
  };

  assert.deepEqual(
    await processMarketCapRows(
      [
        {
          assetId: "ETH",
          symbol: "ETH",
          sourceAssetId: "ETH",
          coingeckoAssetId: "ethereum",
        },
      ],
      [bitcoinMarketRow],
      { timestamp: new Date("2026-05-05T00:00:00.000Z"), dryRun: false },
      database as never,
    ),
    {
      processedCount: 0,
      skippedCount: 1,
      missingCoinGeckoIds: ["ethereum"],
    },
  );
});

test("processMarketCapRows requires mapped CoinGecko ids instead of symbol matching", async () => {
  const database = {
    insert() {
      throw new Error("insert should not be called");
    },
    update() {
      throw new Error("update should not be called");
    },
  };

  assert.deepEqual(
    await processMarketCapRows(
      [
        {
          assetId: "WBTC",
          symbol: "BTC",
          sourceAssetId: "WBTC",
          coingeckoAssetId: "wrapped-bitcoin",
        },
      ],
      [bitcoinMarketRow],
      { timestamp: new Date("2026-05-05T00:00:00.000Z"), dryRun: false },
      database as never,
    ),
    {
      processedCount: 0,
      skippedCount: 1,
      missingCoinGeckoIds: ["wrapped-bitcoin"],
    },
  );
});

test("processMarketCapRows performs no writes for dry-run or empty input", async () => {
  const database = {
    insert() {
      throw new Error("insert should not be called");
    },
    update() {
      throw new Error("update should not be called");
    },
  };

  assert.deepEqual(
    await processMarketCapRows(
      [
        {
          assetId: "BTC",
          symbol: "BTC",
          sourceAssetId: "BTC",
          coingeckoAssetId: "bitcoin",
        },
      ],
      [bitcoinMarketRow],
      { timestamp: new Date("2026-05-05T00:00:00.000Z"), dryRun: true },
      database as never,
    ),
    { processedCount: 1, skippedCount: 0, missingCoinGeckoIds: [] },
  );

  assert.deepEqual(
    await processMarketCapRows(
      [
        {
          assetId: "BTC",
          symbol: "BTC",
          sourceAssetId: "BTC",
          coingeckoAssetId: "bitcoin",
        },
      ],
      [],
      { timestamp: new Date("2026-05-05T00:00:00.000Z"), dryRun: false },
      database as never,
    ),
    {
      processedCount: 0,
      skippedCount: 1,
      missingCoinGeckoIds: ["bitcoin"],
    },
  );
});

test("runCoinGeckoMarketCapIngestion fetches mapped ids, skips unmapped assets, and logs summary", async () => {
  const logs: { event: string }[] = [];
  const calls: {
    fetchedIds?: string[];
    dryRun?: boolean;
    refreshed?: boolean;
  } = {};

  const summary = await runCoinGeckoMarketCapIngestion(
    {
      symbols: undefined,
      date: new Date("2026-05-05T00:00:00.000Z"),
      dryRun: false,
    },
    {
      selectMappedAssets() {
        return Promise.resolve({
          mapped: [
            {
              assetId: "BTC",
              symbol: "BTC",
              sourceAssetId: "BTC",
              coingeckoAssetId: "bitcoin",
            },
            {
              assetId: "ETH",
              symbol: "ETH",
              sourceAssetId: "ETH",
              coingeckoAssetId: "ethereum",
            },
          ],
          unmapped: [{ assetId: "SOL", symbol: "SOL", sourceAssetId: "SOL" }],
        });
      },
      fetchMarkets({ ids }) {
        calls.fetchedIds = ids;
        return Promise.resolve([bitcoinMarketRow]);
      },
      processRows(mappedAssets, marketRows, options) {
        calls.dryRun = options.dryRun;
        assert.equal(mappedAssets.length, 2);
        assert.deepEqual(marketRows, [bitcoinMarketRow]);
        return Promise.resolve({
          processedCount: 1,
          skippedCount: 1,
          missingCoinGeckoIds: ["ethereum"],
        });
      },
      refreshFeatureView() {
        calls.refreshed = true;
        return Promise.resolve();
      },
      writeLog(event, fields) {
        logs.push({ event, ...fields });
      },
    },
  );

  assert.deepEqual(calls.fetchedIds, ["bitcoin", "ethereum"]);
  assert.equal(calls.dryRun, false);
  assert.equal(calls.refreshed, true);
  assert.deepEqual(
    logs.map((log) => log.event),
    [
      "coingecko_market_caps_started",
      "coingecko_market_caps_assets_selected",
      "agent_asset_universe_features_refresh_started",
      "agent_asset_universe_features_refresh_completed",
      "coingecko_market_caps_completed",
      "coingecko_market_caps_summary",
    ],
  );
  assert.deepEqual(summary, {
    dryRun: false,
    date: "2026-05-05T00:00:00.000Z",
    selectedCount: 3,
    mappedCount: 2,
    unmappedCount: 1,
    fetchedCount: 1,
    writtenCount: 1,
    featureViewRefreshed: true,
    skippedCount: 2,
    missingCoinGeckoIds: ["ethereum"],
  });
});

test("runCoinGeckoMarketCapIngestion dry-run fetches but reports zero writes", async () => {
  const summary = await runCoinGeckoMarketCapIngestion(
    {
      symbols: ["BTC"],
      date: new Date("2026-05-05T00:00:00.000Z"),
      dryRun: true,
    },
    {
      selectMappedAssets() {
        return Promise.resolve({
          mapped: [
            {
              assetId: "BTC",
              symbol: "BTC",
              sourceAssetId: "BTC",
              coingeckoAssetId: "bitcoin",
            },
          ],
          unmapped: [],
        });
      },
      fetchMarkets() {
        return Promise.resolve([bitcoinMarketRow]);
      },
      processRows() {
        return Promise.resolve({
          processedCount: 1,
          skippedCount: 0,
          missingCoinGeckoIds: [],
        });
      },
      writeLog() {},
    },
  );

  assert.equal(summary.writtenCount, 0);
  assert.equal(summary.featureViewRefreshed, false);
  assert.equal(summary.fetchedCount, 1);
});

test("refreshAssetUniverseFeatures refreshes the feature materialized view concurrently", async () => {
  let query: unknown;

  await refreshAssetUniverseFeatures({
    execute(value: unknown) {
      query = value;
      return Promise.resolve({ rows: [] });
    },
  } as never);

  assert.equal(
    renderSql(query).sql,
    'REFRESH MATERIALIZED VIEW CONCURRENTLY "agent_asset_universe_features"',
  );
});

test("market-cap ingestion upserts rows and cascades deletes against local Postgres", async (t) => {
  const hasDatabaseConfig = Boolean(
    process.env.DATABASE_URL || process.env.PGDATABASE || process.env.PGHOST,
  );

  if (!hasDatabaseConfig || !process.env.RUN_MARKETCAP_INGESTION_DB_TESTS) {
    t.skip(
      "Set database configuration and RUN_MARKETCAP_INGESTION_DB_TESTS=1 to run the local Postgres market-cap ingestion test.",
    );
    return;
  }

  const databaseName = process.env.DATABASE_URL
    ? new URL(process.env.DATABASE_URL).pathname.replace(/^\//, "")
    : (process.env.PGDATABASE ?? process.env.PGUSER ?? "postgres");
  if (!/(test|tmp|throwaway)/i.test(databaseName)) {
    throw new Error(
      `Refusing to run market-cap ingestion DB test against non-throwaway database: ${databaseName}`,
    );
  }

  const { db, pgPool } = await import("../src/db/client");
  const assetId = `TEST_MARKETCAP_${process.pid}_${Date.now()}`;
  const symbol = assetId;
  const sourceAssetId = assetId;
  const coingeckoAssetId = `${assetId.toLowerCase()}-coingecko`;
  const timestamp = new Date("2026-05-05T00:00:00.000Z");

  t.after(async () => {
    await db.delete(assets).where(eq(assets.assetId, assetId));
  });

  await db.insert(assets).values({
    assetId,
    source: "gmx",
    sourceAssetId,
    symbol,
    name: "Test Market Cap Asset",
    metadata: {},
  });
  await db.insert(assetSourceMappings).values({
    assetId,
    source: "coingecko",
    sourceAssetId: coingeckoAssetId,
    confidence: "manual",
    metadata: {},
  });

  const makeMarketRow = (
    marketCap: number,
    marketCapRank: number,
  ): CoinGeckoMarketRow => ({
    ...bitcoinMarketRow,
    id: coingeckoAssetId,
    symbol: symbol.toLowerCase(),
    name: "Test Market Cap Asset",
    market_cap: marketCap,
    market_cap_rank: marketCapRank,
  });

  assert.deepEqual(
    await processMarketCapRows(
      [{ assetId, symbol, sourceAssetId, coingeckoAssetId }],
      [makeMarketRow(1_000_000, 42)],
      { timestamp, dryRun: false },
    ),
    { processedCount: 1, skippedCount: 0, missingCoinGeckoIds: [] },
  );

  assert.deepEqual(
    await processMarketCapRows(
      [{ assetId, symbol, sourceAssetId, coingeckoAssetId }],
      [makeMarketRow(2_500_000, 24)],
      { timestamp, dryRun: false },
    ),
    { processedCount: 1, skippedCount: 0, missingCoinGeckoIds: [] },
  );

  const upsertResult = await pgPool.query<{
    market_cap_count: string;
    latest_market_cap: string | null;
    latest_market_cap_rank: number | null;
    cached_market_cap_rank: number | null;
  }>(
    `SELECT
      (SELECT COUNT(*)::text FROM asset_market_caps WHERE asset_id = $1 AND source = 'coingecko') AS market_cap_count,
      (SELECT market_cap::text FROM asset_market_caps WHERE asset_id = $1 AND source = 'coingecko' AND timestamp = $2) AS latest_market_cap,
      (SELECT market_cap_rank FROM asset_market_caps WHERE asset_id = $1 AND source = 'coingecko' AND timestamp = $2) AS latest_market_cap_rank,
      (SELECT market_cap_rank FROM assets WHERE asset_id = $1) AS cached_market_cap_rank`,
    [assetId, timestamp],
  );

  assert.deepEqual(upsertResult.rows[0], {
    market_cap_count: "1",
    latest_market_cap: "2500000",
    latest_market_cap_rank: 24,
    cached_market_cap_rank: 24,
  });

  await db.delete(assets).where(eq(assets.assetId, assetId));

  const cascadeResult = await pgPool.query<{
    asset_count: string;
    mapping_count: string;
    market_cap_count: string;
  }>(
    `SELECT
      (SELECT COUNT(*)::text FROM assets WHERE asset_id = $1) AS asset_count,
      (SELECT COUNT(*)::text FROM asset_source_mappings WHERE asset_id = $1) AS mapping_count,
      (SELECT COUNT(*)::text FROM asset_market_caps WHERE asset_id = $1) AS market_cap_count`,
    [assetId],
  );

  assert.deepEqual(cascadeResult.rows[0], {
    asset_count: "0",
    mapping_count: "0",
    market_cap_count: "0",
  });
});
