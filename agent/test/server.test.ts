import assert from "node:assert/strict";
import {
  mkdir,
  mkdtemp,
  readFile,
  rm,
  utimes,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

import type {
  OpencodePromptResult,
  OpencodeTurnClient,
} from "../src/agent/session";
import { buildServer } from "../src/api/server";

type StrategyState = {
  userId: string;
  opencodeSessionId: string;
  title: string;
  lastUsedAt: string;
};

type RunState = {
  strategyId: string;
  status: string;
  startedAt: string;
  endedAt: string | null;
  exitCode: number | null;
  reply: string | null;
  error: string | null;
};

type Deferred<T> = {
  promise: Promise<T>;
  resolve(value: T | PromiseLike<T>): void;
  reject(reason?: unknown): void;
};

function createState() {
  return {
    runs: new Map<string, RunState>(),
    strategies: new Map<string, StrategyState>(),
    users: new Set<string>(),
  };
}

function createDeferred<T = void>(): Deferred<T> {
  let resolve!: Deferred<T>["resolve"];
  let reject!: Deferred<T>["reject"];
  const promise = new Promise<T>((nextResolve, nextReject) => {
    resolve = nextResolve;
    reject = nextReject;
  });

  return { promise, reject, resolve };
}

function completedPromptResult(
  text: string,
  sessionId: string,
): OpencodePromptResult {
  return {
    info: {
      cost: 0,
      id: `assistant-${sessionId}`,
      mode: "chat",
      modelID: "gpt-5",
      parentID: `run-${sessionId}`,
      path: { cwd: "/tmp", root: "/tmp" },
      providerID: "openai",
      role: "assistant",
      sessionID: sessionId,
      time: { completed: Date.now(), created: Date.now() },
      tokens: {
        cache: { read: 0, write: 0 },
        input: 0,
        output: 0,
        reasoning: 0,
      },
    },
    parts: [
      {
        id: `text-${sessionId}`,
        messageID: `assistant-${sessionId}`,
        sessionID: sessionId,
        text,
        type: "text" as const,
      },
    ],
  } as unknown as OpencodePromptResult;
}

function toolErrorPromptResult(
  message: string,
  sessionId: string,
): OpencodePromptResult {
  return {
    info: {
      cost: 0,
      id: `assistant-${sessionId}`,
      mode: "chat",
      modelID: "gpt-5",
      parentID: `run-${sessionId}`,
      path: { cwd: "/tmp", root: "/tmp" },
      providerID: "openai",
      role: "assistant",
      sessionID: sessionId,
      time: { completed: Date.now(), created: Date.now() },
      tokens: {
        cache: { read: 0, write: 0 },
        input: 0,
        output: 0,
        reasoning: 0,
      },
    },
    parts: [
      {
        callID: "call-tool-error",
        id: `tool-${sessionId}`,
        messageID: `assistant-${sessionId}`,
        sessionID: sessionId,
        state: {
          error: message,
          status: "error" as const,
        },
        tool: "bash",
        type: "tool" as const,
      },
    ],
  } as unknown as OpencodePromptResult;
}

function parseSseEvents(body: string) {
  return body
    .trim()
    .split("\n\n")
    .filter(Boolean)
    .map((chunk) => {
      const lines = chunk.split("\n");
      const event = lines
        .find((line) => line.startsWith("event: "))
        ?.slice("event: ".length);
      const data = lines
        .find((line) => line.startsWith("data: "))
        ?.slice("data: ".length);

      return { data: data ? JSON.parse(data) : null, event };
    });
}

function createOpencodeClientDouble(
  title: string,
  promptResult: OpencodePromptResult,
): OpencodeTurnClient {
  return {
    async getSession() {
      return { title };
    },
    async prompt() {
      return promptResult;
    },
    async subscribeEvents() {
      return {
        [Symbol.asyncIterator]() {
          return {
            async next() {
              return { done: true, value: undefined };
            },
          };
        },
      };
    },
  };
}

function createRepositoryDouble(state: ReturnType<typeof createState>) {
  return {
    async createRun(runId: string, strategyId: string) {
      state.runs.set(runId, {
        endedAt: null,
        error: null,
        exitCode: null,
        reply: null,
        startedAt: new Date().toISOString(),
        status: "running",
        strategyId,
      });
    },
    async ensureStrategy(userId: string, strategyId: string) {
      state.users.add(userId);
      if (!state.strategies.has(strategyId)) {
        state.strategies.set(strategyId, {
          lastUsedAt: new Date().toISOString(),
          opencodeSessionId: "",
          title: "",
          userId,
        });
      }

      const strategy = state.strategies.get(strategyId);
      return strategy ? { userId: strategy.userId } : null;
    },
    async markRunCompleted(runId: string, reply: string) {
      const run = state.runs.get(runId);
      if (!run) return;
      run.status = "completed";
      run.endedAt = new Date().toISOString();
      run.exitCode = 0;
      run.reply = reply;
      run.error = null;
    },
    async markRunFailed(runId: string, error: string) {
      const run = state.runs.get(runId);
      if (!run) return;
      run.status = "failed";
      run.endedAt = new Date().toISOString();
      run.exitCode = 1;
      run.reply = null;
      run.error = error;
    },
    async readRun(runId: string) {
      const run = state.runs.get(runId);
      return run ? { ...run, runId } : null;
    },
    async touchStrategy(strategyId: string) {
      const strategy = state.strategies.get(strategyId);
      if (strategy) strategy.lastUsedAt = new Date().toISOString();
    },
    async updateStrategyTitleIfBlank(strategyId: string, title: string) {
      const strategy = state.strategies.get(strategyId);
      const normalizedTitle = title.trim();
      if (strategy && normalizedTitle && !strategy.title.trim()) {
        strategy.title = normalizedTitle;
      }
    },
  };
}

test("POST /strategies creates a new strategy row and returns its id", async () => {
  const state = createState();
  const app = buildServer({
    repositories: createRepositoryDouble(state),
  });

  try {
    const firstResponse = await app.inject({
      method: "POST",
      payload: { user_id: "user-1" },
      url: "/strategies",
    });

    assert.equal(firstResponse.statusCode, 200);
    assert.match(
      firstResponse.json().strategy_id,
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
    );

    const firstStrategyId = firstResponse.json().strategy_id as string;
    const firstStrategy = state.strategies.get(firstStrategyId);

    assert.ok(firstStrategy);
    assert.ok(state.users.has("user-1"));
    assert.equal(firstStrategy.userId, "user-1");
    assert.equal(firstStrategy.opencodeSessionId, "");
    assert.equal(firstStrategy.title, "");

    const secondResponse = await app.inject({
      method: "POST",
      payload: { user_id: "user-1" },
      url: "/strategies",
    });

    assert.equal(secondResponse.statusCode, 200);
    assert.notEqual(secondResponse.json().strategy_id, firstStrategyId);
    assert.ok(state.strategies.has(secondResponse.json().strategy_id));
  } finally {
    await app.close();
  }
});

test("POST /strategies rejects a missing user_id", async () => {
  const app = buildServer({
    repositories: createRepositoryDouble(createState()),
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: {},
      url: "/strategies",
    });

    assert.equal(response.statusCode, 400);
    assert.deepEqual(response.json(), {
      error: "Bad Request",
      message: "Request body must include a non-empty 'user_id' field",
      statusCode: 400,
    });
  } finally {
    await app.close();
  }
});

test("POST /ingestion/gmx runs the GMX loader with API options", async () => {
  const state = createState();
  let capturedOptions: unknown;
  const app = buildServer({
    repositories: createRepositoryDouble(state),
    ingestionRunners: {
      async gmx(options) {
        capturedOptions = options;
        return {
          dryRun: options.dryRun,
          featureViewRefreshed: false,
          symbolCount: 1,
          successCount: 1,
          failureCount: 0,
          symbols: [
            {
              symbol: "BTC",
              assetId: "BTC",
              limit: 4,
              rowCount: 4,
              startTimestamp: "2026-05-01T00:00:00.000Z",
              endTimestamp: "2026-05-04T00:00:00.000Z",
              dryRun: options.dryRun,
            },
          ],
        };
      },
    },
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: {
        symbols: ["BTC", "ETH"],
        exclude: "ETH",
        full_refresh: true,
        dry_run: true,
      },
      url: "/ingestion/gmx",
    });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(capturedOptions, {
      symbols: ["BTC", "ETH"],
      exclude: ["ETH"],
      fullRefresh: true,
      dryRun: true,
    });
    assert.deepEqual(response.json().summary, {
      dryRun: true,
      featureViewRefreshed: false,
      symbolCount: 1,
      successCount: 1,
      failureCount: 0,
      symbols: [
        {
          symbol: "BTC",
          assetId: "BTC",
          limit: 4,
          rowCount: 4,
          startTimestamp: "2026-05-01T00:00:00.000Z",
          endTimestamp: "2026-05-04T00:00:00.000Z",
          dryRun: true,
        },
      ],
    });
    assert.equal(response.json().loader, "gmx");
    assert.equal(response.json().status, "completed");
  } finally {
    await app.close();
  }
});

test("POST /ingestion/coingecko-market-caps runs market-cap loader", async () => {
  const state = createState();
  let capturedOptions: { symbols?: string[]; date?: Date; dryRun?: boolean } =
    {};
  const app = buildServer({
    repositories: createRepositoryDouble(state),
    ingestionRunners: {
      async coingeckoMarketCaps(options) {
        capturedOptions = options;
        return {
          dryRun: options.dryRun,
          date: options.date.toISOString(),
          selectedCount: 2,
          mappedCount: 2,
          unmappedCount: 0,
          fetchedCount: 2,
          writtenCount: 0,
          featureViewRefreshed: false,
          skippedCount: 0,
          missingCoinGeckoIds: [],
        };
      },
    },
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: {
        symbols: "BTC, ETH",
        date: "2026-05-01",
        dry_run: true,
      },
      url: "/ingestion/coingecko-market-caps",
    });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(capturedOptions.symbols, ["BTC", "ETH"]);
    assert.equal(
      capturedOptions.date?.toISOString(),
      "2026-05-01T00:00:00.000Z",
    );
    assert.equal(capturedOptions.dryRun, true);
    assert.equal(response.json().loader, "coingecko-market-caps");
    assert.equal(response.json().status, "completed");
    assert.deepEqual(response.json().summary, {
      dryRun: true,
      date: "2026-05-01T00:00:00.000Z",
      selectedCount: 2,
      mappedCount: 2,
      unmappedCount: 0,
      fetchedCount: 2,
      writtenCount: 0,
      featureViewRefreshed: false,
      skippedCount: 0,
      missingCoinGeckoIds: [],
    });
  } finally {
    await app.close();
  }
});

test("POST /ingestion rejects unknown loaders", async () => {
  const app = buildServer({
    repositories: createRepositoryDouble(createState()),
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: {},
      url: "/ingestion/unknown",
    });

    assert.equal(response.statusCode, 404);
    assert.deepEqual(response.json(), {
      error: "Not Found",
      message: "Ingestion loader not found",
      statusCode: 404,
    });
  } finally {
    await app.close();
  }
});

test("POST /maintenance/storage/cleanup deletes old storage files", async () => {
  const storageRoot = await mkdtemp(
    path.join(tmpdir(), "agent-invest-cleanup-"),
  );
  const previousStorageRoot = process.env.STORAGE_ROOT;
  const previousToken = process.env.MAINTENANCE_TOKEN;
  process.env.STORAGE_ROOT = storageRoot;
  process.env.MAINTENANCE_TOKEN = "secret-token";

  const app = buildServer({
    repositories: createRepositoryDouble(createState()),
  });
  const oldPath = path.join(storageRoot, "users", "user-1", "old.md");
  const freshPath = path.join(storageRoot, "artifacts", "runs", "fresh.json");

  try {
    await mkdir(path.dirname(oldPath), { recursive: true });
    await mkdir(path.dirname(freshPath), { recursive: true });
    await writeFile(oldPath, "old");
    await writeFile(freshPath, "fresh");
    const oldTime = new Date(Date.now() - 2 * 60 * 60 * 1000);
    await utimes(oldPath, oldTime, oldTime);

    const unauthorized = await app.inject({
      method: "POST",
      payload: { max_age_hours: 1 },
      url: "/maintenance/storage/cleanup",
    });
    assert.equal(unauthorized.statusCode, 401);

    const response = await app.inject({
      headers: { authorization: "Bearer secret-token" },
      method: "POST",
      payload: { max_age_hours: 1 },
      url: "/maintenance/storage/cleanup",
    });

    assert.equal(response.statusCode, 200);
    assert.equal(response.json().deleted_files, 1);
    assert.equal(await readFile(freshPath, "utf8"), "fresh");
    await assert.rejects(
      () => readFile(oldPath, "utf8"),
      (error) => {
        return (
          typeof error === "object" &&
          error !== null &&
          "code" in error &&
          error.code === "ENOENT"
        );
      },
    );
  } finally {
    await app.close();
    if (previousStorageRoot === undefined) delete process.env.STORAGE_ROOT;
    else process.env.STORAGE_ROOT = previousStorageRoot;
    if (previousToken === undefined) delete process.env.MAINTENANCE_TOKEN;
    else process.env.MAINTENANCE_TOKEN = previousToken;
    await rm(storageRoot, { force: true, recursive: true });
  }
});

test("POST /messages returns the completed run and auto-creates the strategy", async () => {
  const state = createState();
  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    repositories: createRepositoryDouble(state),
    getOpencodeClient: async () =>
      createOpencodeClientDouble(
        "Momentum explorer",
        completedPromptResult("Here is the agent reply.", "session-1"),
      ),
    getSessionId: async (strategyId) => {
      const strategy = state.strategies.get(strategyId);

      assert.ok(strategy);
      strategy.opencodeSessionId ||= "session-1";

      return strategy.opencodeSessionId;
    },
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: {
        strategy_id: "strategy-1",
        text: "Build me a crypto momentum strategy",
        user_id: "user-1",
      },
      url: "/messages",
    });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(response.json(), {
      ended_at: state.runs.get(response.json().run_id)?.endedAt ?? null,
      error: null,
      exit_code: 0,
      reply: "Here is the agent reply.",
      run_id: response.json().run_id,
      started_at: state.runs.get(response.json().run_id)?.startedAt ?? null,
      status: "completed",
    });

    const strategy = state.strategies.get("strategy-1");
    const run = state.runs.get(response.json().run_id);

    assert.ok(strategy);
    assert.equal(strategy.userId, "user-1");
    assert.equal(strategy.opencodeSessionId, "session-1");
    assert.equal(strategy.title, "Momentum explorer");
    assert.ok(run);
    assert.equal(run.status, "completed");
    assert.equal(run.exitCode, 0);
  } finally {
    await app.close();
  }
});

test("GET /runs/:id returns the persisted run", async () => {
  const state = createState();
  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    repositories: createRepositoryDouble(state),
    getOpencodeClient: async () =>
      createOpencodeClientDouble(
        "Mean reversion scout",
        completedPromptResult("Async reply completed.", "session-2"),
      ),
    getSessionId: async (strategyId) => {
      const strategy = state.strategies.get(strategyId);

      assert.ok(strategy);
      strategy.opencodeSessionId ||= "session-2";

      return strategy.opencodeSessionId;
    },
  });

  try {
    const createResponse = await app.inject({
      method: "POST",
      payload: {
        strategy_id: "strategy-2",
        text: "Try a mean reversion approach",
        user_id: "user-1",
      },
      url: "/messages",
    });

    assert.equal(createResponse.statusCode, 200);

    const { run_id: runId } = createResponse.json() as { run_id: string };
    const runResponse = await app.inject({
      method: "GET",
      url: `/runs/${runId}`,
    });

    assert.equal(runResponse.statusCode, 200);
    assert.deepEqual(runResponse.json(), {
      ended_at: state.runs.get(runId)?.endedAt ?? null,
      error: null,
      exit_code: 0,
      reply: "Async reply completed.",
      run_id: runId,
      started_at: state.runs.get(runId)?.startedAt ?? null,
      status: "completed",
    });
  } finally {
    await app.close();
  }
});

test("GET /runs/:id returns 404 for unknown runs", async () => {
  const app = buildServer({
    repositories: createRepositoryDouble(createState()),
  });

  try {
    const response = await app.inject({
      method: "GET",
      url: "/runs/missing-run",
    });

    assert.equal(response.statusCode, 404);
    assert.deepEqual(response.json(), {
      error: "Not Found",
      message: "Run not found",
      statusCode: 404,
    });
  } finally {
    await app.close();
  }
});

test("different users can build prompts in parallel", async () => {
  const state = createState();
  const bothBuildsStarted = createDeferred<void>();
  const releaseBuilds = createDeferred<void>();
  const buildOrder: string[] = [];
  const app = buildServer({
    buildSystemPrompt: async ({ userId }) => {
      buildOrder.push(userId);

      if (buildOrder.length === 2) {
        bothBuildsStarted.resolve();
      }

      await releaseBuilds.promise;

      return `system prompt for ${userId}`;
    },
    repositories: createRepositoryDouble(state),
    getOpencodeClient: async () =>
      createOpencodeClientDouble(
        "Parallel strategy",
        completedPromptResult("Parallel reply.", "session-parallel"),
      ),
    getSessionId: async (strategyId) => {
      const strategy = state.strategies.get(strategyId);

      assert.ok(strategy);
      strategy.opencodeSessionId ||= `session-${strategyId}`;

      return strategy.opencodeSessionId;
    },
  });

  try {
    const firstResponse = app.inject({
      method: "POST",
      payload: {
        strategy_id: "strategy-1",
        text: "User one turn",
        user_id: "user-1",
      },
      url: "/messages",
    });
    const secondResponse = app.inject({
      method: "POST",
      payload: {
        strategy_id: "strategy-2",
        text: "User two turn",
        user_id: "user-2",
      },
      url: "/messages",
    });

    await bothBuildsStarted.promise;
    assert.deepEqual([...buildOrder].sort(), ["user-1", "user-2"]);

    releaseBuilds.resolve();

    const [first, second] = await Promise.all([firstResponse, secondResponse]);

    assert.equal(first.statusCode, 200);
    assert.equal(second.statusCode, 200);
  } finally {
    releaseBuilds.resolve();
    await app.close();
  }
});

test("unexpected errors return a 500 response", async () => {
  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    repositories: {
      async ensureStrategy() {
        throw new Error("database offline");
      },
    },
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: {
        strategy_id: "strategy-error",
        text: "Trigger an unexpected failure",
        user_id: "user-1",
      },
      url: "/messages",
    });

    assert.equal(response.statusCode, 500);
    assert.equal(response.json().statusCode, 500);
  } finally {
    await app.close();
  }
});

test("assistant tool error persists a failed run", async () => {
  const state = createState();
  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    repositories: createRepositoryDouble(state),
    getOpencodeClient: async () =>
      createOpencodeClientDouble(
        "Timeout strategy",
        toolErrorPromptResult("Script timed out after 1s", "session-timeout"),
      ),
    getSessionId: async (strategyId) => {
      const strategy = state.strategies.get(strategyId);

      assert.ok(strategy);
      strategy.opencodeSessionId ||= `session-${strategyId}`;

      return strategy.opencodeSessionId;
    },
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: {
        strategy_id: "strategy-timeout",
        text: "Run the long fixture",
        user_id: "user-1",
      },
      url: "/messages",
    });

    assert.equal(response.statusCode, 500);
    assert.equal(state.runs.size, 1);

    const [runId, run] = [...state.runs.entries()][0] ?? [];

    assert.equal(typeof runId, "string");
    assert.ok(run);
    assert.equal(run.status, "failed");
    assert.equal(run.exitCode, 1);
    assert.equal(run.error, "Script timed out after 1s");
    assert.equal(run.reply, null);
  } finally {
    await app.close();
  }
});

test("POST /messages/stream includes parsed structured_result on completion", async () => {
  const state = createState();
  const structuredResult = {
    allocation: [{ asset: "Bitcoin", weight: 1 }],
    assumptions: ["Liquidity remains stable"],
    kpis: { sharpe_ratio: 1.4 },
    next_steps: ["Monitor drawdown"],
    reasoning: "Trend following fits the requested universe.",
    risks: ["Whipsaw risk"],
    summary: "A BTC trend strategy.",
    title: "BTC Trend",
  };
  const reply = `Here is the strategy.\n\n\`\`\`strategy_result\n${JSON.stringify(
    structuredResult,
  )}\n\`\`\``;
  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    repositories: createRepositoryDouble(state),
    getOpencodeClient: async () =>
      createOpencodeClientDouble(
        "BTC trend strategy",
        completedPromptResult(reply, "session-stream"),
      ),
    getSessionId: async (strategyId) => {
      const strategy = state.strategies.get(strategyId);

      assert.ok(strategy);
      strategy.opencodeSessionId ||= "session-stream";

      return strategy.opencodeSessionId;
    },
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: {
        strategy_id: "strategy-stream",
        text: "Build a BTC trend strategy",
        user_id: "user-1",
      },
      url: "/messages/stream",
    });

    assert.equal(response.statusCode, 200);

    const events = parseSseEvents(response.body);
    const finalizing = events.find((event) => event.event === "run.finalizing");
    const completed = events.find((event) => event.event === "run.completed");

    assert.ok(finalizing);
    assert.equal(
      finalizing.data.message,
      "Creating structured report and charts",
    );
    assert.ok(completed);
    assert.deepEqual(completed.data.structured_result, structuredResult);

    const [run] = state.runs.values();

    assert.ok(run);
    assert.equal(run.reply, reply);
    assert.equal(
      Object.hasOwn(
        run as unknown as Record<string, unknown>,
        "structured_result",
      ),
      false,
    );
  } finally {
    await app.close();
  }
});

test("POST /messages/stream enriches structured_result from JSON artifacts", async () => {
  const state = createState();
  const storageRoot = await mkdtemp(
    path.join(tmpdir(), "agent-invest-storage-"),
  );
  const previousStorageRoot = process.env.STORAGE_ROOT;
  process.env.STORAGE_ROOT = storageRoot;

  const structuredResult = {
    allocation: [{ asset: "Bitcoin", rationale: "Model", weight: 1 }],
    assumptions: ["Liquidity remains stable"],
    charts: {
      equity_curve: [{ date: "2024-01-01", strategy_equity: 999 }],
    },
    kpis: { cagr: 0.1, sharpe_ratio: 0.5 },
    next_steps: ["Monitor drawdown"],
    reasoning: "Trend following fits the requested universe.",
    risks: ["Whipsaw risk"],
    summary: "A BTC trend strategy.",
    title: "BTC Trend",
  };
  const reply = `Here is the strategy.\n\n\`\`\`strategy_result\n${JSON.stringify(
    structuredResult,
  )}\n\`\`\``;

  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    repositories: createRepositoryDouble(state),
    getOpencodeClient: async () => ({
      ...createOpencodeClientDouble(
        "BTC trend strategy",
        completedPromptResult(reply, "session-stream-artifacts"),
      ),
      async prompt() {
        const artifactDir = path.join(
          storageRoot,
          "artifacts",
          "run_backtest",
          "btc-trend",
        );
        await mkdir(artifactDir, { recursive: true });
        await writeFile(
          path.join(artifactDir, "report.json"),
          JSON.stringify({ kpis: { cagr: 0.25, sharpe_ratio: 1.7 } }),
        );
        await writeFile(
          path.join(artifactDir, "equity_curve.json"),
          JSON.stringify([
            {
              bitcoin_equity_usd: 1100,
              date: "2024-01-01",
              equity_usd: 1200,
            },
          ]),
        );
        await writeFile(
          path.join(artifactDir, "drawdown.json"),
          JSON.stringify([
            {
              bitcoin_drawdown: -0.2,
              date: "2024-01-01",
              drawdown: -0.1,
            },
          ]),
        );
        await writeFile(
          path.join(artifactDir, "allocation.json"),
          JSON.stringify([
            { coin_id: "bitcoin", date: "2024-01-01", weight: 1 },
          ]),
        );
        return completedPromptResult(reply, "session-stream-artifacts");
      },
    }),
    getSessionId: async (strategyId) => {
      const strategy = state.strategies.get(strategyId);

      assert.ok(strategy);
      strategy.opencodeSessionId ||= "session-stream-artifacts";

      return strategy.opencodeSessionId;
    },
  });

  try {
    await mkdir(path.join(storageRoot, "artifacts"), { recursive: true });
    await writeFile(path.join(storageRoot, "artifacts", ".keep"), "", {
      flag: "wx",
    });
  } catch {
    // The artifacts directory may be created by another setup path.
  }

  try {
    const response = await app.inject({
      method: "POST",
      payload: {
        strategy_id: "strategy-stream-artifacts",
        text: "Build a BTC trend strategy",
        user_id: "user-1",
      },
      url: "/messages/stream",
    });

    assert.equal(response.statusCode, 200);

    const events = parseSseEvents(response.body);
    const completed = events.find((event) => event.event === "run.completed");

    assert.ok(completed);
    assert.deepEqual(completed.data.structured_result, {
      ...structuredResult,
      charts: {
        final_allocation: [{ asset: "bitcoin", weight: 1 }],
        drawdown: [
          {
            benchmark_drawdown: -0.2,
            date: "2024-01-01",
            strategy_drawdown: -0.1,
          },
        ],
        equity_curve: [
          {
            benchmark_equity: 1100,
            date: "2024-01-01",
            strategy_equity: 1200,
          },
        ],
      },
      kpis: { cagr: 0.25, sharpe_ratio: 1.7 },
    });

    const [run] = state.runs.values();
    assert.ok(run);
    assert.equal(run.reply, reply);
  } finally {
    await app.close();
    if (previousStorageRoot === undefined) delete process.env.STORAGE_ROOT;
    else process.env.STORAGE_ROOT = previousStorageRoot;
    await rm(storageRoot, { force: true, recursive: true });
  }
});

test("POST /messages/stream omits structured_result for plain text completion", async () => {
  const state = createState();
  const reply = "Plain text strategy reply.";
  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    repositories: createRepositoryDouble(state),
    getOpencodeClient: async () =>
      createOpencodeClientDouble(
        "Plain text strategy",
        completedPromptResult(reply, "session-stream-plain"),
      ),
    getSessionId: async (strategyId) => {
      const strategy = state.strategies.get(strategyId);

      assert.ok(strategy);
      strategy.opencodeSessionId ||= "session-stream-plain";

      return strategy.opencodeSessionId;
    },
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: {
        strategy_id: "strategy-stream-plain",
        text: "Build a plain text strategy",
        user_id: "user-1",
      },
      url: "/messages/stream",
    });

    assert.equal(response.statusCode, 200);

    const events = parseSseEvents(response.body);
    const completed = events.find((event) => event.event === "run.completed");

    assert.ok(completed);
    assert.equal(Object.hasOwn(completed.data, "structured_result"), false);
    assert.equal(completed.data.reply, reply);

    const [run] = state.runs.values();

    assert.ok(run);
    assert.equal(run.reply, reply);
  } finally {
    await app.close();
  }
});
