const DEFAULT_COINGECKO_API_BASE_URL = "https://api.coingecko.com/api/v3/";
const DEFAULT_TIMEOUT_MS = 30_000;
const MAX_RETRIES = 3;
const BASE_RETRY_DELAY_MS = 500;
const DEFAULT_API_KEY_HEADER = "x-cg-demo-api-key";

export interface CoinGeckoMarketRow {
  id: string;
  symbol: string;
  name: string;
  image: string;
  current_price: number | null;
  market_cap: number | null;
  market_cap_rank: number | null;
  fully_diluted_valuation: number | null;
  total_volume: number | null;
  high_24h: number | null;
  low_24h: number | null;
  price_change_24h: number | null;
  price_change_percentage_24h: number | null;
  market_cap_change_24h: number | null;
  market_cap_change_percentage_24h: number | null;
  circulating_supply: number | null;
  total_supply: number | null;
  max_supply: number | null;
  ath: number | null;
  ath_change_percentage: number | null;
  ath_date: string | null;
  atl: number | null;
  atl_change_percentage: number | null;
  atl_date: string | null;
  roi: unknown;
  last_updated: string | null;
}

export interface FetchCoinGeckoMarketsOptions {
  ids: string[];
}

export class CoinGeckoHttpError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly statusText: string,
    public readonly url: string,
    public readonly responseBody: string,
  ) {
    super(message);
    this.name = "CoinGeckoHttpError";
  }
}

async function requestJson<T>(
  path: string,
  query?: URLSearchParams,
): Promise<T> {
  const url = new URL(
    path,
    process.env.COINGECKO_API_BASE_URL ?? DEFAULT_COINGECKO_API_BASE_URL,
  );

  if (query) {
    url.search = query.toString();
  }

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);

    try {
      const response = await fetch(url, {
        headers: getApiKeyHeaders(),
        signal: controller.signal,
      });

      if (response.ok) {
        return (await response.json()) as T;
      }

      if (isRetryableStatus(response.status) && attempt < MAX_RETRIES) {
        await delay(getRetryDelayMs(response, attempt));
        continue;
      }

      const responseBody = await response.text();

      throw new CoinGeckoHttpError(
        `CoinGecko API request failed with ${response.status} ${response.statusText}: ${url.toString()}`,
        response.status,
        response.statusText,
        url.toString(),
        responseBody,
      );
    } catch (error) {
      if (error instanceof CoinGeckoHttpError) {
        throw error;
      }

      if (attempt < MAX_RETRIES) {
        await delay(getExponentialBackoffMs(attempt));
        continue;
      }

      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }

  throw new Error(
    `CoinGecko API request failed after retries: ${url.toString()}`,
  );
}

function getApiKeyHeaders(): Record<string, string> | undefined {
  const apiKey = process.env.COINGECKO_API_KEY;

  if (!apiKey) {
    return undefined;
  }

  return {
    [process.env.COINGECKO_API_KEY_HEADER ?? DEFAULT_API_KEY_HEADER]: apiKey,
  };
}

function isRetryableStatus(status: number): boolean {
  return status === 429 || status >= 500;
}

function getRetryDelayMs(response: Response, attempt: number): number {
  const retryAfter = response.headers.get("retry-after");

  if (retryAfter) {
    const delayMs = parseRetryAfterMs(retryAfter);

    if (delayMs !== undefined) {
      return delayMs;
    }
  }

  return getExponentialBackoffMs(attempt);
}

function parseRetryAfterMs(value: string): number | undefined {
  const seconds = Number(value);

  if (Number.isFinite(seconds)) {
    return Math.max(0, seconds * 1_000);
  }

  const retryAt = Date.parse(value);

  if (Number.isNaN(retryAt)) {
    return undefined;
  }

  return Math.max(0, retryAt - Date.now());
}

function getExponentialBackoffMs(attempt: number): number {
  return BASE_RETRY_DELAY_MS * 2 ** attempt;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export async function fetchCoinGeckoMarkets({
  ids,
}: FetchCoinGeckoMarketsOptions): Promise<CoinGeckoMarketRow[]> {
  const query = new URLSearchParams({
    vs_currency: "usd",
    ids: ids.join(","),
    order: "market_cap_desc",
    per_page: "250",
    page: "1",
    sparkline: "false",
  });

  return requestJson<CoinGeckoMarketRow[]>("coins/markets", query);
}
