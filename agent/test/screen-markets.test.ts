import assert from "node:assert/strict";
import test from "node:test";

import { screenMarkets } from "../src/tools/screen-markets";
import type { ResolvedMarket } from "../src/market/gmx";

function market(coinId: string, symbol: string): ResolvedMarket {
  return {
    coinId,
    symbol,
    gmxMarket: `0x${symbol.toLowerCase()}market`,
    marketName: `${symbol}/USD [${symbol}-USDC]`,
    indexToken: `0x${symbol.toLowerCase()}index`,
    indexTokenDecimals: 18,
    isSynthetic: false,
    longToken: `0x${symbol.toLowerCase()}long`,
    shortToken: "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    collateralToken: "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    collateralDecimals: 6,
  };
}

test("screenMarkets filters to GMX-tradeable rows and gates actions", async () => {
  const result = await screenMarkets(
    { factor: "momentum", limit: 2, gmxOnly: true },
    {
      async rankUniverse(input) {
        assert.equal(input.limit, 8);
        return [
          {
            coin_id: "unlisted-coin",
            rank: 1,
            factor_values: {
              return_180d: 1.2,
              sharpe_180d: 1.1,
              volatility_180d: 0.8,
            },
          },
          {
            coin_id: "bitcoin",
            rank: 2,
            factor_values: {
              return_180d: 0.4,
              sharpe_180d: 0.9,
              volatility_180d: 0.5,
            },
          },
          {
            coin_id: "ethereum",
            rank: 3,
            factor_values: {
              return_180d: 0.3,
              sharpe_180d: 0.8,
              volatility_180d: 0.6,
            },
          },
        ];
      },
      async resolveMarketsBatch() {
        return {
          resolved: new Map([
            ["bitcoin", market("bitcoin", "BTC")],
            ["ethereum", market("ethereum", "ETH")],
          ]),
          failures: new Map([["unlisted-coin", new Error("no market")]]),
        };
      },
    },
  );

  assert.equal(result.type, "market_screener");
  assert.equal(result.rows.length, 2);
  assert.deepEqual(
    result.rows.map((row) => [row.rank, row.source_rank, row.symbol]),
    [
      [1, 2, "BTC"],
      [2, 3, "ETH"],
    ],
  );
  assert.equal(result.rows[0].is_gmx_tradeable, true);
  assert.equal(result.rows[0].actions.long.enabled, true);
  assert.equal(result.rows[0].gmx_market?.chain, "arbitrum");
});

test("screenMarkets can include research-only rows without actions", async () => {
  const result = await screenMarkets(
    { factor: "risk_adjusted", limit: 1, gmxOnly: false },
    {
      async rankUniverse() {
        return [
          {
            coin_id: "unlisted-coin",
            rank: 1,
            factor_values: {
              sharpe_180d: 2.1,
              return_180d: 0.7,
              volatility_180d: 0.4,
            },
          },
        ];
      },
      async resolveMarketsBatch() {
        return { resolved: new Map(), failures: new Map() };
      },
    },
  );

  assert.equal(result.rows[0].is_gmx_tradeable, false);
  assert.equal(result.rows[0].actions.long.enabled, false);
  assert.equal(result.rows[0].actions.short.enabled, false);
  assert.equal(result.rows[0].gmx_market, null);
});
