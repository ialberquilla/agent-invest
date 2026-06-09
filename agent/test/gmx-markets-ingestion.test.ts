import assert from "node:assert/strict";
import test from "node:test";

import {
  runGmxMarketsIngestion,
  toMarketRow,
  toTokenRow,
} from "../src/ingestion/gmx-markets.ts";

test("toTokenRow projects valid tokens and flags synthetic", () => {
  assert.deepEqual(
    toTokenRow({ symbol: "ETH", address: "0xabc", decimals: 18 }),
    { symbol: "ETH", address: "0xabc", decimals: 18, synthetic: false },
  );
  assert.deepEqual(
    toTokenRow({ symbol: "APT", address: "0xdef", decimals: 8, synthetic: true }),
    { symbol: "APT", address: "0xdef", decimals: 8, synthetic: true },
  );
});

test("toTokenRow skips rows missing required fields", () => {
  assert.equal(toTokenRow({ symbol: "ETH", address: "0xabc" }), null);
  assert.equal(toTokenRow({ symbol: "", address: "0xabc", decimals: 18 }), null);
  assert.equal(
    toTokenRow({ symbol: "ETH", address: "0xabc", decimals: "18" }),
    null,
  );
});

test("toMarketRow parses listingDate and defaults isListed to true", () => {
  const row = toMarketRow({
    name: "ETH/USD [ETH-USDC]",
    marketToken: "0xMarket",
    indexToken: "0xidx",
    longToken: "0xlong",
    shortToken: "0xshort",
    isListed: true,
    listingDate: "2023-08-08T00:00:00.000Z",
  });
  assert.equal(row?.marketToken, "0xMarket");
  assert.equal(row?.isListed, true);
  assert.ok(row?.listingDate instanceof Date);
});

test("toMarketRow tolerates a missing listingDate", () => {
  const row = toMarketRow({
    name: "ETH/USD [ETH-USDC]",
    marketToken: "0xMarket",
    indexToken: "0xidx",
    longToken: "0xlong",
    shortToken: "0xshort",
    isListed: false,
  });
  assert.equal(row?.listingDate, null);
  assert.equal(row?.isListed, false);
});

test("runGmxMarketsIngestion dry-run projects and skips without writing", async () => {
  let wrote = false;
  const database = {
    insert() {
      wrote = true;
      return { values: () => ({ onConflictDoUpdate: async () => undefined }) };
    },
    select() {
      return { from: async () => [] };
    },
  } as never;

  const summary = await runGmxMarketsIngestion(
    { dryRun: true },
    {
      database,
      fetchTokens: async () => [
        { symbol: "ETH", address: "0xabc", decimals: 18 },
        { symbol: "BAD" }, // skipped
      ],
      fetchMarkets: async () => [
        {
          name: "ETH/USD [ETH-USDC]",
          marketToken: "0xMarket",
          indexToken: "0xidx",
          longToken: "0xlong",
          shortToken: "0xshort",
          isListed: true,
        },
        { name: "broken" } as never, // skipped
      ],
    },
  );

  assert.equal(summary.tokenCount, 1);
  assert.equal(summary.skippedTokens, 1);
  assert.equal(summary.marketCount, 1);
  assert.equal(summary.skippedMarkets, 1);
  assert.equal(wrote, false, "dry-run must not write");
});
