const GMX_API_BASE_URL = "https://arbitrum-api.gmxinfra.io";
const CANDLE_PERIOD = "1d";
const DEFAULT_TIMEOUT_MS = 30_000;
const MAX_RETRIES = 3;
const BASE_RETRY_DELAY_MS = 500;

export interface GmxToken {
  [key: string]: unknown;
}

interface FetchTokensResponse {
  tokens: GmxToken[];
}

export interface GmxCandle {
  [key: string]: unknown;
}

interface FetchCandlesResponse {
  candles: GmxCandle[];
}

export interface FetchCandlesOptions {
  symbol: string;
  limit: number;
}

export class GmxHttpError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly statusText: string,
    public readonly url: string,
    public readonly responseBody: string,
  ) {
    super(message);
    this.name = "GmxHttpError";
  }
}

async function requestJson<T>(
  path: string,
  query?: URLSearchParams,
): Promise<T> {
  const url = new URL(path, GMX_API_BASE_URL);

  if (query) {
    url.search = query.toString();
  }

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);

    try {
      const response = await fetch(url, { signal: controller.signal });

      if (response.ok) {
        return (await response.json()) as T;
      }

      if (isRetryableStatus(response.status) && attempt < MAX_RETRIES) {
        await delay(getRetryDelayMs(response, attempt));
        continue;
      }

      const responseBody = await response.text();

      throw new GmxHttpError(
        `GMX API request failed with ${response.status} ${response.statusText}: ${url.toString()}`,
        response.status,
        response.statusText,
        url.toString(),
        responseBody,
      );
    } catch (error) {
      if (error instanceof GmxHttpError) {
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

  throw new Error(`GMX API request failed after retries: ${url.toString()}`);
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

export async function fetchTokens(): Promise<GmxToken[]> {
  const response = await requestJson<GmxToken[] | FetchTokensResponse>(
    "/tokens",
  );

  return Array.isArray(response) ? response : response.tokens;
}

export async function fetchCandles({
  symbol,
  limit,
}: FetchCandlesOptions): Promise<GmxCandle[]> {
  const query = new URLSearchParams({
    tokenSymbol: symbol,
    period: CANDLE_PERIOD,
    limit: String(limit),
  });

  const response = await requestJson<GmxCandle[] | FetchCandlesResponse>(
    "/prices/candles",
    query,
  );

  return Array.isArray(response) ? response : response.candles;
}
