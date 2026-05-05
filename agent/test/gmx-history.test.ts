import assert from "node:assert/strict";
import test from "node:test";
import { sql } from "drizzle-orm";

import { db, pgPool } from "../src/db/client";
import { assetPrices, assets } from "../src/db/schema";
import {
  filterSymbols,
  getGmxCandleFetchLimit,
  getLatestGmxPriceTimestamp,
  getNeededCandleDays,
  parseOptions,
  upsertAsset,
  upsertCandles,
} from "../src/ingestion/gmx-history";

const representativeTokenSymbols = [
  "BTC",
  "ETH",
  "ETH_deprecated",
  "SOL_deprecated.v2",
  "GLV.BTC-USDC",
  "XAUT",
  "XAUT.v2",
  "GOLD",
  "SILVER",
  "BRENTOIL",
  "WTIOIL",
  "NATGAS",
  "USDC",
  "USDC.e",
  "USDT",
  "DAI",
  "USDe",
  "WBTC.b",
  "tBTC",
  "wstETH",
  "DOGE",
];

test("filterSymbols drops deprecated, GLV, forex, and commodity symbols", () => {
  const filtered = filterSymbols(representativeTokenSymbols);

  assert.equal(filtered.includes("ETH_deprecated"), false);
  assert.equal(filtered.includes("SOL_deprecated.v2"), false);
  assert.equal(filtered.includes("GLV.BTC-USDC"), false);
  assert.equal(filtered.includes("XAUT"), false);
  assert.equal(filtered.includes("XAUT.v2"), false);
  assert.equal(filtered.includes("GOLD"), false);
  assert.equal(filtered.includes("SILVER"), false);
  assert.equal(filtered.includes("BRENTOIL"), false);
  assert.equal(filtered.includes("WTIOIL"), false);
  assert.equal(filtered.includes("NATGAS"), false);
});

test("filterSymbols keeps stablecoins, wrapped variants, and synthetic feeds", () => {
  const filtered = filterSymbols(representativeTokenSymbols);

  assert.deepEqual(filtered, [
    "BTC",
    "ETH",
    "USDC",
    "USDC.e",
    "USDT",
    "DAI",
    "USDe",
    "WBTC.b",
    "tBTC",
    "wstETH",
    "DOGE",
  ]);
});

test("filterSymbols applies CLI inclusion before exclusion", () => {
  const symbols = ["BTC", "ETH", "SOL", "DOGE", "GOLD"];
  const options = parseOptions([
    "--symbols",
    "BTC,ETH,GOLD",
    "--exclude",
    "ETH",
  ]);

  assert.deepEqual(filterSymbols(symbols, options), ["BTC"]);
});

test("parseOptions ignores a forwarded argument separator", () => {
  assert.deepEqual(parseOptions(["--", "--dry-run", "--symbols", "BTC"]), {
    symbols: ["BTC"],
    exclude: [],
    fullRefresh: false,
    dryRun: true,
  });
});

test("upsertAsset creates or updates a stable GMX asset and returns its id", async () => {
  const calls: {
    table?: unknown;
    values?: unknown;
    conflict?: unknown;
    returning?: unknown;
  } = {};

  const database = {
    insert(table: unknown) {
      calls.table = table;

      return {
        values(values: unknown) {
          calls.values = values;

          return {
            onConflictDoUpdate(conflict: unknown) {
              calls.conflict = conflict;

              return {
                returning(returning: unknown) {
                  calls.returning = returning;
                  return Promise.resolve([{ assetId: "BTC" }]);
                },
              };
            },
          };
        },
      };
    },
  };

  const assetId = await upsertAsset("BTC", database as never);

  assert.equal(assetId, "BTC");
  assert.equal(calls.table, assets);
  assert.deepEqual(calls.values, {
    assetId: "BTC",
    source: "gmx",
    sourceAssetId: "BTC",
    symbol: "BTC",
    name: "BTC",
  });
  assert.deepEqual(calls.returning, { assetId: assets.assetId });

  const conflict = calls.conflict as {
    target: unknown;
    set: { updatedAt: unknown };
  };
  assert.deepEqual(conflict.target, [assets.source, assets.sourceAssetId]);
  assert.ok(conflict.set.updatedAt instanceof Date);
});

test("getNeededCandleDays returns cold start limit and full refresh limit", () => {
  assert.equal(getNeededCandleDays(null), 10_000);
  assert.equal(
    getNeededCandleDays(new Date("2026-05-01T00:00:00.000Z"), {
      fullRefresh: true,
      today: new Date("2026-05-05T12:00:00.000Z"),
    }),
    10_000,
  );
});

test("getNeededCandleDays includes overlap from latest candle date", () => {
  assert.equal(
    getNeededCandleDays(new Date("2026-05-03T23:00:00.000Z"), {
      today: new Date("2026-05-05T01:00:00.000Z"),
    }),
    4,
  );
});

test("getNeededCandleDays can signal no incremental work", () => {
  assert.equal(
    getNeededCandleDays(new Date("2026-05-08T00:00:00.000Z"), {
      today: new Date("2026-05-05T00:00:00.000Z"),
    }),
    -1,
  );
});

test("getGmxCandleFetchLimit skips non-positive need days", () => {
  assert.equal(getGmxCandleFetchLimit(4), 4);
  assert.equal(getGmxCandleFetchLimit(0), undefined);
  assert.equal(getGmxCandleFetchLimit(-1), undefined);
});

test("getLatestGmxPriceTimestamp reads max GMX timestamp for an asset", async () => {
  const lastTs = new Date("2026-05-04T00:00:00.000Z");
  const calls: { select?: unknown; from?: unknown; where?: unknown } = {};
  const database = {
    select(selection: unknown) {
      calls.select = selection;

      return {
        from(table: unknown) {
          calls.from = table;

          return {
            where(condition: unknown) {
              calls.where = condition;
              return Promise.resolve([{ lastTs }]);
            },
          };
        },
      };
    },
  };

  assert.equal(
    await getLatestGmxPriceTimestamp("BTC", database as never),
    lastTs,
  );
  assert.equal(calls.from, assetPrices);
  assert.ok(calls.select);
  assert.ok(calls.where);
});

test("upsertCandles skips empty candle batches", async () => {
  const database = {
    insert() {
      throw new Error("insert should not be called");
    },
  };

  await upsertCandles("BTC", [], database as never);
});

test("upsertCandles inserts GMX OHLC rows and updates conflicts", async () => {
  const calls: { table?: unknown; values?: unknown; conflict?: unknown } = {};
  const database = {
    insert(table: unknown) {
      calls.table = table;

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
  };

  await upsertCandles(
    "BTC",
    [[1_714_521_600, 60_000, 61_000, 59_000, 60_500] as never],
    database as never,
  );

  assert.equal(calls.table, assetPrices);
  assert.deepEqual(calls.values, [
    {
      assetId: "BTC",
      timestamp: new Date("2024-05-01T00:00:00.000Z"),
      source: "gmx",
      open: "60000",
      high: "61000",
      low: "59000",
      close: "60500",
      volume: null,
      marketCap: null,
    },
  ]);

  const conflict = calls.conflict as {
    target: unknown;
    set: Record<string, unknown>;
  };
  assert.deepEqual(conflict.target, [
    assetPrices.assetId,
    assetPrices.timestamp,
    assetPrices.source,
  ]);
  assert.deepEqual(Object.keys(conflict.set), ["open", "high", "low", "close"]);
});

test("GMX ingestion is idempotent against local Postgres", async (t) => {
  if (!process.env.DATABASE_URL || !process.env.RUN_GMX_INGESTION_DB_TESTS) {
    t.skip(
      "Set DATABASE_URL and RUN_GMX_INGESTION_DB_TESTS=1 to run the local Postgres GMX ingestion test.",
    );
    return;
  }

  const databaseName = new URL(process.env.DATABASE_URL).pathname.replace(
    /^\//,
    "",
  );
  if (!/(test|tmp|throwaway)/i.test(databaseName)) {
    throw new Error(
      `Refusing to run GMX ingestion DB test against non-throwaway database: ${databaseName}`,
    );
  }

  const symbol = `TEST_GMX_${process.pid}_${Date.now()}`;
  const assetId = symbol;
  const firstDay = 1_714_521_600;
  const secondDay = firstDay + 86_400;
  const coldStartCandles = [
    [firstDay, 60_000, 61_000, 59_000, 60_500],
    [secondDay, 61_000, 62_000, 60_000, 61_500],
  ] as const;
  const refreshedCandles = [
    [firstDay, 60_000, 61_000, 59_000, 60_500],
    [secondDay, 61_000, 62_000, 60_000, 61_750],
  ] as const;
  const nextDayCandles = [
    ...refreshedCandles,
    [secondDay + 86_400, 62_000, 63_000, 61_000, 62_500],
  ] as const;

  t.after(async () => {
    await db.delete(assets).where(sql`${assets.assetId} = ${assetId}`);
  });

  const countRows = async () => {
    const result = await pgPool.query<{
      asset_count: string;
      price_count: string;
      latest_close: string | null;
    }>(
      `SELECT
        (SELECT COUNT(*)::text FROM assets WHERE asset_id = $1) AS asset_count,
        (SELECT COUNT(*)::text FROM asset_prices WHERE asset_id = $1 AND source = 'gmx') AS price_count,
        (SELECT close::text FROM asset_prices WHERE asset_id = $1 AND source = 'gmx' ORDER BY timestamp DESC LIMIT 1) AS latest_close`,
      [assetId],
    );

    const row = result.rows[0];
    assert.ok(row);

    return {
      assetCount: Number(row.asset_count),
      priceCount: Number(row.price_count),
      latestClose: row.latest_close,
    };
  };

  assert.equal(await upsertAsset(symbol), assetId);
  await upsertCandles(assetId, coldStartCandles as never);

  assert.deepEqual(await countRows(), {
    assetCount: 1,
    priceCount: coldStartCandles.length,
    latestClose: "61500",
  });

  assert.equal(await upsertAsset(symbol), assetId);
  await upsertCandles(assetId, refreshedCandles as never);

  assert.deepEqual(await countRows(), {
    assetCount: 1,
    priceCount: coldStartCandles.length,
    latestClose: "61750",
  });

  await upsertCandles(assetId, nextDayCandles as never);

  assert.deepEqual(await countRows(), {
    assetCount: 1,
    priceCount: coldStartCandles.length + 1,
    latestClose: "62500",
  });
});
