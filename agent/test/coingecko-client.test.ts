import assert from "node:assert/strict";
import test from "node:test";

import {
  CoinGeckoHttpError,
  fetchCoinGeckoMarkets,
} from "../src/ingestion/coingecko-client";

test("fetchCoinGeckoMarkets requests markets with required query parameters", async () => {
  const originalFetch = globalThis.fetch;
  const calls: URL[] = [];

  globalThis.fetch = ((input: string | URL | Request) => {
    calls.push(new URL(input.toString()));

    return Promise.resolve(new Response("[]", { status: 200 }));
  }) as typeof fetch;

  try {
    await fetchCoinGeckoMarkets({ ids: ["bitcoin", "ethereum"] });
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.origin, "https://api.coingecko.com");
  assert.equal(calls[0]?.pathname, "/api/v3/coins/markets");
  assert.equal(calls[0]?.searchParams.get("vs_currency"), "usd");
  assert.equal(calls[0]?.searchParams.get("ids"), "bitcoin,ethereum");
  assert.equal(calls[0]?.searchParams.get("order"), "market_cap_desc");
  assert.equal(calls[0]?.searchParams.get("per_page"), "250");
  assert.equal(calls[0]?.searchParams.get("page"), "1");
  assert.equal(calls[0]?.searchParams.get("sparkline"), "false");
});

test("fetchCoinGeckoMarkets returns empty successful responses", async () => {
  const originalFetch = globalThis.fetch;

  globalThis.fetch = (() => {
    return Promise.resolve(new Response("[]", { status: 200 }));
  }) as typeof fetch;

  try {
    const rows = await fetchCoinGeckoMarkets({ ids: ["bitcoin"] });

    assert.deepEqual(rows, []);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("fetchCoinGeckoMarkets returns CoinGecko market response rows", async () => {
  const originalFetch = globalThis.fetch;
  const row = {
    id: "bitcoin",
    symbol: "btc",
    name: "Bitcoin",
    image: "https://assets.coingecko.com/coins/images/1/large/bitcoin.png",
    current_price: 100_000,
    market_cap: 1_900_000_000_000,
    market_cap_rank: 1,
    fully_diluted_valuation: 2_100_000_000_000,
    total_volume: 50_000_000_000,
    high_24h: 101_000,
    low_24h: 99_000,
    price_change_24h: 1_000,
    price_change_percentage_24h: 1,
    market_cap_change_24h: 19_000_000_000,
    market_cap_change_percentage_24h: 1,
    circulating_supply: 19_000_000,
    total_supply: 21_000_000,
    max_supply: 21_000_000,
    ath: 110_000,
    ath_change_percentage: -9.09,
    ath_date: "2026-01-01T00:00:00.000Z",
    atl: 67.81,
    atl_change_percentage: 147_000,
    atl_date: "2013-07-06T00:00:00.000Z",
    roi: null,
    last_updated: "2026-05-05T00:00:00.000Z",
  };

  globalThis.fetch = (() => {
    return Promise.resolve(
      new Response(JSON.stringify([row]), { status: 200 }),
    );
  }) as typeof fetch;

  try {
    const rows = await fetchCoinGeckoMarkets({ ids: ["bitcoin"] });

    assert.deepEqual(rows, [row]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("fetchCoinGeckoMarkets only sends API key header when configured", async () => {
  const originalFetch = globalThis.fetch;
  const originalApiKey = process.env.COINGECKO_API_KEY;
  const originalApiKeyHeader = process.env.COINGECKO_API_KEY_HEADER;
  const headers: unknown[] = [];

  globalThis.fetch = ((_: string | URL | Request, init?: RequestInit) => {
    headers.push(init?.headers);

    return Promise.resolve(new Response("[]", { status: 200 }));
  }) as typeof fetch;

  try {
    delete process.env.COINGECKO_API_KEY;
    delete process.env.COINGECKO_API_KEY_HEADER;
    await fetchCoinGeckoMarkets({ ids: ["bitcoin"] });

    process.env.COINGECKO_API_KEY = "demo-key";
    await fetchCoinGeckoMarkets({ ids: ["bitcoin"] });

    process.env.COINGECKO_API_KEY_HEADER = "x-custom-key";
    await fetchCoinGeckoMarkets({ ids: ["bitcoin"] });
  } finally {
    globalThis.fetch = originalFetch;
    restoreEnv("COINGECKO_API_KEY", originalApiKey);
    restoreEnv("COINGECKO_API_KEY_HEADER", originalApiKeyHeader);
  }

  assert.equal(headers[0], undefined);
  assert.deepEqual(headers[1], { "x-cg-demo-api-key": "demo-key" });
  assert.deepEqual(headers[2], { "x-custom-key": "demo-key" });
});

test("fetchCoinGeckoMarkets supports configurable API base URL", async () => {
  const originalFetch = globalThis.fetch;
  const originalApiBaseUrl = process.env.COINGECKO_API_BASE_URL;
  const calls: URL[] = [];

  globalThis.fetch = ((input: string | URL | Request) => {
    calls.push(new URL(input.toString()));

    return Promise.resolve(new Response("[]", { status: 200 }));
  }) as typeof fetch;

  try {
    process.env.COINGECKO_API_BASE_URL =
      "https://pro-api.coingecko.com/api/v3/";
    await fetchCoinGeckoMarkets({ ids: ["bitcoin"] });
  } finally {
    globalThis.fetch = originalFetch;
    restoreEnv("COINGECKO_API_BASE_URL", originalApiBaseUrl);
  }

  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.origin, "https://pro-api.coingecko.com");
  assert.equal(calls[0]?.pathname, "/api/v3/coins/markets");
});

test("fetchCoinGeckoMarkets retries 429 and 5xx responses", async () => {
  const originalFetch = globalThis.fetch;
  const statuses = [429, 500, 200];
  let calls = 0;

  globalThis.fetch = (() => {
    const status = statuses[calls] ?? 200;
    calls += 1;

    return Promise.resolve(
      new Response(status === 200 ? "[]" : "try later", {
        headers: { "retry-after": "0" },
        status,
        statusText: status === 200 ? "OK" : "Retryable",
      }),
    );
  }) as typeof fetch;

  try {
    await fetchCoinGeckoMarkets({ ids: ["bitcoin"] });
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.equal(calls, 3);
});

test("fetchCoinGeckoMarkets surfaces non-retryable status context", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;

  globalThis.fetch = (() => {
    calls += 1;

    return Promise.resolve(
      new Response("bad request", { status: 400, statusText: "Bad Request" }),
    );
  }) as typeof fetch;

  try {
    await assert.rejects(
      fetchCoinGeckoMarkets({ ids: ["not-a-coin"] }),
      (error) => {
        assert.ok(error instanceof CoinGeckoHttpError);
        assert.equal(error.status, 400);
        assert.equal(error.statusText, "Bad Request");
        assert.equal(error.responseBody, "bad request");
        assert.match(error.url, /\/coins\/markets/);
        return true;
      },
    );
  } finally {
    globalThis.fetch = originalFetch;
  }

  assert.equal(calls, 1);
});

function restoreEnv(name: string, value: string | undefined): void {
  if (value === undefined) {
    delete process.env[name];
    return;
  }

  process.env[name] = value;
}
