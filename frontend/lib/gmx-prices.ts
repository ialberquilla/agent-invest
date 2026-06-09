import type { createPublicClient } from "viem";

// Live GMX v2 pricing helpers shared by the vault execution flow. These replace the old static
// NEXT_PUBLIC_GMX_* env values, which froze orders on mainnet because the acceptable price drifted.

const GMX_API = "https://arbitrum-api.gmxinfra.io";

// GMX market-increase execution is paid to the keeper out of the WNT escrowed at order creation.
// We over-estimate (gas price * a generous gas limit * buffer) and floor it — overpayment is safe
// now that refunds land in the vault gas tank and are sweepable via withdrawNative.
const EXECUTION_GAS_LIMIT = 3_000_000n;
const EXECUTION_FEE_BUFFER_BPS = 15_000n; // 1.5x
const MIN_EXECUTION_FEE_WEI = 200_000_000_000_000n; // 0.0002 ETH floor

type MarketMeta = { marketToken: string; indexToken: string };
let marketIndexCache: Map<string, string> | null = null;

async function loadMarketIndexMap(): Promise<Map<string, string>> {
  const response = await fetch(`${GMX_API}/markets`, { cache: "no-store" });
  if (!response.ok) throw new Error("Unable to fetch GMX markets");
  const payload = (await response.json()) as { markets?: MarketMeta[] };
  const map = new Map<string, string>();
  for (const market of payload.markets ?? []) {
    if (market.marketToken && market.indexToken) {
      map.set(market.marketToken.toLowerCase(), market.indexToken);
    }
  }
  if (map.size === 0) throw new Error("GMX markets response was empty");
  return map;
}

// Resolve a GMX market token -> its index token (the priced asset). The vault allocation only
// carries the market token, so we look up the index token from GMX's market metadata.
export async function getMarketIndexToken(
  marketToken: string,
): Promise<string> {
  if (!marketIndexCache) marketIndexCache = await loadMarketIndexMap();
  const indexToken = marketIndexCache.get(marketToken.toLowerCase());
  if (!indexToken) throw new Error(`No GMX market metadata for ${marketToken}`);
  return indexToken;
}

// Acceptable price for a market order: the live index price nudged by slippage in the direction
// that protects the trader (higher for longs, lower for shorts). Returned in GMX's 30-decimal format.
export async function getAcceptablePrice(
  indexToken: string,
  isLong: boolean,
  slippageBps: number,
): Promise<bigint> {
  const ticker = await getTicker(indexToken, slippageBps);
  const basePrice = BigInt(isLong ? ticker.maxPrice : ticker.minPrice);
  const bps = BigInt(Math.round(slippageBps));
  return isLong
    ? (basePrice * (10_000n + bps)) / 10_000n
    : (basePrice * (10_000n - bps)) / 10_000n;
}

// Acceptable price for a market-decrease order. Closing a long sells the index asset, so lower
// prices are worse; closing a short buys it back, so higher prices are worse.
export async function getAcceptablePriceForDecrease(
  indexToken: string,
  isLong: boolean,
  slippageBps: number,
): Promise<bigint> {
  const ticker = await getTicker(indexToken, slippageBps);
  const basePrice = BigInt(isLong ? ticker.minPrice : ticker.maxPrice);
  const bps = BigInt(Math.round(slippageBps));
  return isLong
    ? (basePrice * (10_000n - bps)) / 10_000n
    : (basePrice * (10_000n + bps)) / 10_000n;
}

async function getTicker(
  indexToken: string,
  slippageBps: number,
): Promise<{ minPrice: string; maxPrice: string }> {
  if (!Number.isFinite(slippageBps) || slippageBps < 0 || slippageBps > 5_000) {
    throw new Error("Slippage must be between 0% and 50%");
  }

  const response = await fetch(`${GMX_API}/prices/tickers`, {
    cache: "no-store",
  });
  if (!response.ok) throw new Error("Unable to fetch GMX prices");

  const tickers = (await response.json()) as Array<{
    tokenAddress?: string;
    minPrice?: string;
    maxPrice?: string;
  }>;
  const ticker = tickers.find(
    (item) => item.tokenAddress?.toLowerCase() === indexToken.toLowerCase(),
  );
  if (!ticker?.minPrice || !ticker.maxPrice) {
    throw new Error(`GMX price unavailable for ${indexToken}`);
  }
  return { minPrice: ticker.minPrice, maxPrice: ticker.maxPrice };
}

// Per-order GMX keeper execution fee, in wei. Scales with current gas price and is floored so a
// quiet-gas estimate never under-funds the keeper.
export async function estimateExecutionFee(
  publicClient: ReturnType<typeof createPublicClient>,
): Promise<bigint> {
  const envOverride = BigInt(
    process.env.NEXT_PUBLIC_GMX_EXECUTION_FEE_WEI ?? "0",
  );
  if (envOverride > 0n) return envOverride;

  const gasPrice = await publicClient.getGasPrice();
  const estimate =
    (gasPrice * EXECUTION_GAS_LIMIT * EXECUTION_FEE_BUFFER_BPS) / 10_000n;
  return estimate > MIN_EXECUTION_FEE_WEI ? estimate : MIN_EXECUTION_FEE_WEI;
}
