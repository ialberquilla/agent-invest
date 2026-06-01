import assert from "node:assert/strict";
import test from "node:test";

import {
  acceptablePriceForIncrease,
  ARBITRUM_USDC,
  type GmxMarketInfo,
  type GmxTokenInfo,
  MarketResolutionError,
  resolveMarketFrom,
  selectCanonicalMarket,
  selectIncreaseOraclePrice,
  ZERO_ADDRESS,
} from "../src/market/gmx.ts";

// Fixtures lifted from the real GMX Arbitrum /tokens + /markets payloads
// (spike, 2026-06-01), including the genuinely ambiguous ETH and LINK cases.
const ETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1";
const BTC = "0x47904963fc8b2340414262125aF798B9655E58Cd";
const LINK = "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4";
const APT = "0x3f8f0dCE4dCE4d0D1d0871941e79CDA82cA50d0B";
const WSTETH = "0x5979D7b546E38E414F7E9822514be443A4800529";
const USDE = "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34";

const tokens: GmxTokenInfo[] = [
  { symbol: "ETH", address: ETH, decimals: 18, synthetic: false },
  { symbol: "BTC", address: BTC, decimals: 8, synthetic: false },
  { symbol: "LINK", address: LINK, decimals: 18, synthetic: false },
  { symbol: "APT", address: APT, decimals: 8, synthetic: true },
  { symbol: "USDC", address: ARBITRUM_USDC, decimals: 6, synthetic: false },
];

const markets: GmxMarketInfo[] = [
  // ETH: three markets -- native USDC, alt-collateral, and ETH-ETH.
  {
    marketToken: "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
    name: "ETH/USD [ETH-USDC]",
    indexToken: ETH,
    longToken: ETH,
    shortToken: ARBITRUM_USDC,
    isListed: true,
  },
  {
    marketToken: "0xMarketEthWstethUsde",
    name: "ETH/USD [wstETH-USDe]",
    indexToken: ETH,
    longToken: WSTETH,
    shortToken: USDE,
    isListed: true,
  },
  {
    marketToken: "0xMarketEthEth",
    name: "ETH/USD [ETH-ETH]",
    indexToken: ETH,
    longToken: ETH,
    shortToken: ETH,
    isListed: true,
  },
  // BTC: single native market.
  {
    marketToken: "0x47c031236e19d024b42f8AE6780E44A573170703",
    name: "BTC/USD [BTC-USDC]",
    indexToken: BTC,
    longToken: BTC,
    shortToken: ARBITRUM_USDC,
    isListed: true,
  },
  // LINK: native + alt-collateral, both USDC short.
  {
    marketToken: "0x7f1fa204bb700853D36994DA19F830b6Ad18455C",
    name: "LINK/USD [LINK-USDC]",
    indexToken: LINK,
    longToken: LINK,
    shortToken: ARBITRUM_USDC,
    isListed: true,
  },
  {
    marketToken: "0xMarketLinkEthUsdc",
    name: "LINK/USD [ETH-USDC]",
    indexToken: LINK,
    longToken: ETH,
    shortToken: ARBITRUM_USDC,
    isListed: true,
  },
  // APT: synthetic index, ETH-collateralized, USDC short.
  {
    marketToken: "0xMarketApt",
    name: "APT/USD [ETH-USDC]",
    indexToken: APT,
    longToken: ETH,
    shortToken: ARBITRUM_USDC,
    isListed: true,
  },
  // A swap-only market (zero index) that must never be selected.
  {
    marketToken: "0xSwapOnly",
    name: "SWAP-ONLY [ETH-USDC]",
    indexToken: ZERO_ADDRESS,
    longToken: ETH,
    shortToken: ARBITRUM_USDC,
    isListed: true,
  },
];

// CoinGecko coin_id -> GMX symbol bridge (asset_source_mappings).
const symbolByCoinId: Record<string, string> = {
  bitcoin: "BTC",
  ethereum: "ETH",
  chainlink: "LINK",
  aptos: "APT",
};

const data = { tokens, markets, symbolByCoinId };

test("selectCanonicalMarket prefers native USDC market when ambiguous (ETH)", () => {
  const m = selectCanonicalMarket("ETH", ETH, markets);
  assert.equal(m?.name, "ETH/USD [ETH-USDC]");
});

test("selectCanonicalMarket picks native over alt-collateral when both USDC (LINK)", () => {
  const m = selectCanonicalMarket("LINK", LINK, markets);
  assert.equal(m?.name, "LINK/USD [LINK-USDC]");
});

test("selectCanonicalMarket never returns a swap-only (zero index) market", () => {
  const m = selectCanonicalMarket("ETH", ZERO_ADDRESS, markets);
  assert.equal(m, null);
});

test("resolveMarketFrom resolves a coingecko coin_id to a native market end to end", () => {
  const r = resolveMarketFrom(data, "bitcoin");
  assert.equal(r.coinId, "bitcoin");
  assert.equal(r.symbol, "BTC");
  assert.equal(r.gmxMarket, "0x47c031236e19d024b42f8AE6780E44A573170703");
  assert.equal(r.indexToken, BTC);
  assert.equal(r.indexTokenDecimals, 8);
  assert.equal(r.isSynthetic, false);
  assert.equal(r.collateralToken, ARBITRUM_USDC);
  assert.equal(r.collateralDecimals, 6);
});

test("resolveMarketFrom marks synthetic index tokens and still resolves USDC collateral", () => {
  const r = resolveMarketFrom(data, "aptos");
  assert.equal(r.symbol, "APT");
  assert.equal(r.isSynthetic, true);
  assert.equal(r.indexTokenDecimals, 8);
  assert.equal(r.collateralToken, ARBITRUM_USDC);
  assert.equal(r.collateralDecimals, 6);
});

test("resolveMarketFrom fails closed on a coin_id with no GMX symbol mapping", () => {
  assert.throws(
    () => resolveMarketFrom(data, "dogecoin"),
    (err: unknown) =>
      err instanceof MarketResolutionError && err.failure === "unknown_coin_id",
  );
});

test("resolveMarketFrom fails closed when the mapped symbol has no token", () => {
  const broken = {
    tokens,
    markets,
    symbolByCoinId: { ...symbolByCoinId, "ghost-coin": "GHOST" },
  };
  assert.throws(
    () => resolveMarketFrom(broken, "ghost-coin"),
    (err: unknown) =>
      err instanceof MarketResolutionError && err.failure === "unknown_symbol",
  );
});

test("resolveMarketFrom fails closed when no listed market has the index token", () => {
  const orphan = {
    tokens: [
      { symbol: "ORPHAN", address: "0xOrphan", decimals: 18, synthetic: false },
      ...tokens,
    ],
    markets,
    symbolByCoinId: { ...symbolByCoinId, "orphan-coin": "ORPHAN" },
  };
  assert.throws(
    () => resolveMarketFrom(orphan, "orphan-coin"),
    (err: unknown) =>
      err instanceof MarketResolutionError &&
      err.failure === "no_listed_market",
  );
});

test("resolveMarketFrom fails closed when collateral decimals are unknown", () => {
  // Market whose short token is not in the token directory.
  const m: GmxMarketInfo[] = [
    {
      marketToken: "0xMarketWeird",
      name: "ETH/USD [ETH-XYZ]",
      indexToken: ETH,
      longToken: ETH,
      shortToken: "0xUnknownStable",
      isListed: true,
    },
  ];
  assert.throws(
    () => resolveMarketFrom({ tokens, markets: m, symbolByCoinId }, "ethereum"),
    (err: unknown) =>
      err instanceof MarketResolutionError &&
      err.failure === "unknown_collateral_decimals",
  );
});

test("resolveMarketFrom skips delisted markets", () => {
  const m: GmxMarketInfo[] = [
    {
      marketToken: "0xDelisted",
      name: "ETH/USD [ETH-USDC]",
      indexToken: ETH,
      longToken: ETH,
      shortToken: ARBITRUM_USDC,
      isListed: false,
    },
  ];
  assert.throws(
    () => resolveMarketFrom({ tokens, markets: m, symbolByCoinId }, "ethereum"),
    (err: unknown) =>
      err instanceof MarketResolutionError &&
      err.failure === "no_listed_market",
  );
});

test("selectIncreaseOraclePrice picks ask for long, bid for short", () => {
  const ticker = {
    tokenAddress: ETH,
    tokenSymbol: "ETH",
    minPrice: "1000",
    maxPrice: "1010",
  };
  assert.equal(selectIncreaseOraclePrice(ticker, true), 1010n);
  assert.equal(selectIncreaseOraclePrice(ticker, false), 1000n);
});

test("acceptablePriceForIncrease widens the worse direction by slippage bps", () => {
  // 50 bps = 0.5%
  assert.equal(acceptablePriceForIncrease(10_000n, true, 50), 10_050n);
  assert.equal(acceptablePriceForIncrease(10_000n, false, 50), 9_950n);
  assert.equal(acceptablePriceForIncrease(10_000n, true, 0), 10_000n);
});

test("acceptablePriceForIncrease rejects invalid slippage", () => {
  assert.throws(() => acceptablePriceForIncrease(1n, true, -1));
  assert.throws(() => acceptablePriceForIncrease(1n, true, 1.5));
});
