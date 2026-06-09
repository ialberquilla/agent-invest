// Phase 2 of plans/integrate_contracts.md: turn a workflow coin_id into
// something executable on GMX V2. Pure domain logic with no DB or network --
// the DB-backed wrapper lives in db/repositories/gmx-markets.ts, ingestion in
// ingestion/gmx-markets.ts.
//
// Verified facts (spike, 2026-06-01; coin_id format corrected during Phase 3):
//   - A workflow coin_id is the CoinGecko id (e.g. "bitcoin", "ethereum",
//     "aptos") -- that is what daily_prices()/the feature view and therefore
//     universe.coin_ids/mandate.coin_ids carry. It is NOT the GMX symbol.
//   - The CoinGecko id -> GMX symbol bridge is asset_source_mappings
//     (source='coingecko', source_asset_id=coin_id, asset_id=symbol). GMX
//     assets store asset_id == symbol, e.g. "bitcoin" -> "BTC".
//   - /tokens gives { symbol, address, decimals, synthetic? }.
//   - /markets gives { name, marketToken, indexToken, longToken, shortToken,
//     isListed, listingDate }. marketToken is the GM market address the order
//     needs; indexToken identifies the traded asset; short/long are collateral.
//   - 12/116 index tokens expose multiple markets; 5 markets are swap-only
//     (indexToken == zero address). Selection must be deterministic + fail-closed.

// Native USDC on Arbitrum One -- the canonical USD short collateral.
export const ARBITRUM_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831";
export const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";

export type GmxTokenInfo = {
  symbol: string;
  address: string;
  decimals: number;
  synthetic: boolean;
};

export type GmxMarketInfo = {
  marketToken: string;
  name: string;
  indexToken: string;
  longToken: string;
  shortToken: string;
  isListed: boolean;
};

// Everything the rebalance engine/adapter needs to size and submit a GMX order
// for one asset. collateralToken is the USD collateral (USDC) for long-only
// sizing; longToken is exposed for families that collateralize in the index.
export type ResolvedMarket = {
  coinId: string;
  symbol: string;
  gmxMarket: string; // marketToken (the `market` arg of a GMX order)
  marketName: string;
  indexToken: string;
  indexTokenDecimals: number;
  isSynthetic: boolean;
  longToken: string;
  shortToken: string;
  collateralToken: string; // = shortToken (USDC) for long-only exposure
  collateralDecimals: number;
};

export type MarketResolutionFailure =
  | "unknown_coin_id"
  | "unknown_symbol"
  | "no_listed_market"
  | "unknown_collateral_decimals";

// Fail-closed: any unresolved coin_id raises this so the caller aborts the
// rebalance (never silently skips an asset and distorts the weight vector).
export class MarketResolutionError extends Error {
  constructor(
    public readonly coinId: string,
    public readonly failure: MarketResolutionFailure,
    message: string,
  ) {
    super(message);
    this.name = "MarketResolutionError";
  }
}

const eqAddr = (a: string, b: string): boolean =>
  a.toLowerCase() === b.toLowerCase();

const isUsdc = (addr: string): boolean => eqAddr(addr, ARBITRUM_USDC);

// Deterministic canonical-market pick for one index token. Higher score wins;
// marketToken breaks ties so the choice is stable across ingest runs.
function scoreMarket(symbol: string, m: GmxMarketInfo): number {
  let score = 0;
  if (isUsdc(m.shortToken)) score += 4; // USD-quoted collateral (preferred)
  if (eqAddr(m.longToken, m.indexToken)) score += 2; // native, deepest market
  if (m.name.includes(`[${symbol}-USDC]`)) score += 1; // canonical naming
  return score;
}

export function selectCanonicalMarket(
  symbol: string,
  indexTokenAddress: string,
  markets: GmxMarketInfo[],
): GmxMarketInfo | null {
  const candidates = markets.filter(
    (m) =>
      m.isListed &&
      !eqAddr(m.indexToken, ZERO_ADDRESS) &&
      eqAddr(m.indexToken, indexTokenAddress),
  );
  if (candidates.length === 0) return null;

  return candidates.sort((a, b) => {
    const byScore = scoreMarket(symbol, b) - scoreMarket(symbol, a);
    if (byScore !== 0) return byScore;
    return a.marketToken.toLowerCase() < b.marketToken.toLowerCase() ? -1 : 1;
  })[0];
}

export type GmxResolutionData = {
  tokens: GmxTokenInfo[];
  markets: GmxMarketInfo[];
  // CoinGecko coin_id -> GMX symbol (asset_source_mappings, source='coingecko').
  symbolByCoinId: Record<string, string>;
};

// Pure resolution: coin_id (CoinGecko id) -> ResolvedMarket, or throws a
// MarketResolutionError. Bridges coin_id -> GMX symbol -> token -> market. The
// DB repository loads `data` and delegates here so the live path and the tests
// exercise identical logic.
export function resolveMarketFrom(
  data: GmxResolutionData,
  coinId: string,
): ResolvedMarket {
  const symbol = data.symbolByCoinId[coinId];
  if (!symbol) {
    throw new MarketResolutionError(
      coinId,
      "unknown_coin_id",
      `no GMX symbol mapped for coin_id "${coinId}"`,
    );
  }

  const token = data.tokens.find((t) => t.symbol === symbol);
  if (!token) {
    throw new MarketResolutionError(
      coinId,
      "unknown_symbol",
      `no GMX token for symbol "${symbol}" (coin_id "${coinId}")`,
    );
  }

  const market = selectCanonicalMarket(symbol, token.address, data.markets);
  if (!market) {
    throw new MarketResolutionError(
      coinId,
      "no_listed_market",
      `no listed GMX market with index token ${token.address} for "${coinId}"`,
    );
  }

  const collateralToken = market.shortToken;
  const collateral = data.tokens.find((t) =>
    eqAddr(t.address, collateralToken),
  );
  if (!collateral) {
    throw new MarketResolutionError(
      coinId,
      "unknown_collateral_decimals",
      `collateral token ${collateralToken} for "${coinId}" not in token set`,
    );
  }

  return {
    coinId,
    symbol: token.symbol,
    gmxMarket: market.marketToken,
    marketName: market.name,
    indexToken: market.indexToken,
    indexTokenDecimals: token.decimals,
    isSynthetic: token.synthetic,
    longToken: market.longToken,
    shortToken: market.shortToken,
    collateralToken,
    collateralDecimals: collateral.decimals,
  };
}

// ---------------------------------------------------------------------------
// Pricing helpers (2.2). GMX oracle prices are integers (min/max) at
// 10^(30 - tokenDecimals) precision. acceptablePrice bounds slippage on an
// increase order: a long pays up to maxPrice*(1+slip), a short down to
// minPrice*(1-slip). executionFee depends on on-chain gas params and is
// deferred to Phase 5 (execution adapter); it is not computable from this data.
// ---------------------------------------------------------------------------

export type GmxTicker = {
  tokenAddress: string;
  tokenSymbol: string;
  minPrice: string;
  maxPrice: string;
};

// Oracle price to use as the basis for an increase order, before slippage.
// Long increase fills at the ask (maxPrice); short increase at the bid (min).
export function selectIncreaseOraclePrice(
  ticker: GmxTicker,
  isLong: boolean,
): bigint {
  return BigInt(isLong ? ticker.maxPrice : ticker.minPrice);
}

// Apply a slippage tolerance (bps) in the direction that is worse for the
// trader, yielding the acceptablePrice bound for an increase order.
export function acceptablePriceForIncrease(
  oraclePrice: bigint,
  isLong: boolean,
  slippageBps: number,
): bigint {
  if (!Number.isInteger(slippageBps) || slippageBps < 0) {
    throw new Error(`slippageBps must be a non-negative integer, got ${slippageBps}`);
  }
  const bps = BigInt(slippageBps);
  const denom = 10_000n;
  return isLong
    ? (oraclePrice * (denom + bps)) / denom
    : (oraclePrice * (denom - bps)) / denom;
}
