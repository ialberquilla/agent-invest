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

type StageRunState = {
  stageRunId: string;
  runId: string;
  stage: string;
  round: number;
  status: string;
  opencodeSessionId: string | null;
  model: string;
  input: unknown;
  output: unknown;
  error: string | null;
  startedAt: Date;
  endedAt: Date | null;
  tokensIn: number | null;
  tokensOut: number | null;
};

type AgentEventState = {
  eventId: string;
  threadId: string | null;
  messageId: string | null;
  runId: string | null;
  eventType: string;
  payload: unknown;
  createdAt: Date;
};

type Deferred<T> = {
  promise: Promise<T>;
  resolve(value: T | PromiseLike<T>): void;
  reject(reason?: unknown): void;
};

function createState() {
  return {
    runs: new Map<string, RunState>(),
    events: new Map<string, AgentEventState>(),
    stageRuns: new Map<string, StageRunState>(),
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

async function readSseEvents(
  response: Response,
  count: number,
  afterFirstEvent?: () => void,
) {
  assert.ok(response.body);
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let body = "";
  let didRunAfterFirstEvent = false;

  while (parseSseEvents(body).length < count) {
    const { done, value } = await reader.read();
    if (done) break;
    body += decoder.decode(value, { stream: true });

    if (!didRunAfterFirstEvent && parseSseEvents(body).length > 0) {
      didRunAfterFirstEvent = true;
      afterFirstEvent?.();
    }
  }

  reader.releaseLock();
  return parseSseEvents(body);
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
    async getStageRun(stageRunId: string) {
      return state.stageRuns.get(stageRunId) ?? null;
    },
    async listStageRunsByRunId(runId: string) {
      return [...state.stageRuns.values()]
        .filter((stageRun) => stageRun.runId === runId)
        .sort((left, right) => {
          return (
            left.stage.localeCompare(right.stage) ||
            left.round - right.round ||
            left.startedAt.getTime() - right.startedAt.getTime()
          );
        });
    },
    async listStageEventsByRunId(
      runId: string,
      filters: { stage?: string; round?: number } = {},
    ) {
      return [...state.events.values()]
        .filter((event) => event.runId === runId)
        .filter((event) => event.eventType.startsWith("stage."))
        .filter((event) => {
          if (!filters.stage && filters.round === undefined) return true;
          const payload = event.payload as Record<string, unknown>;
          return (
            (filters.stage === undefined || payload.stage === filters.stage) &&
            (filters.round === undefined || payload.round === filters.round)
          );
        })
        .sort((left, right) => {
          return (
            left.createdAt.getTime() - right.createdAt.getTime() ||
            left.eventId.localeCompare(right.eventId)
          );
        });
    },
    async readRun(runId: string) {
      const run = state.runs.get(runId);
      return run ? { ...run, runId } : null;
    },
    async touchStrategy(strategyId: string) {
      const strategy = state.strategies.get(strategyId);
      if (strategy) strategy.lastUsedAt = new Date().toISOString();
    },
  };
}

test("agent API key rejects requests without the x-api-key header", async () => {
  const app = buildServer({
    apiKey: "test-agent-key",
    repositories: createRepositoryDouble(createState()),
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: { user_id: "user-1" },
      url: "/strategies",
    });

    assert.equal(response.statusCode, 401);
    assert.deepEqual(response.json(), {
      error: "Unauthorized",
      message: "Unauthorized",
      statusCode: 401,
    });
  } finally {
    await app.close();
  }
});

test("agent API key accepts requests with a valid x-api-key header", async () => {
  const app = buildServer({
    apiKey: "test-agent-key",
    repositories: createRepositoryDouble(createState()),
  });

  try {
    const response = await app.inject({
      headers: { "x-api-key": "test-agent-key" },
      method: "POST",
      payload: { user_id: "user-1" },
      url: "/strategies",
    });

    assert.equal(response.statusCode, 200);
    assert.match(
      response.json().strategy_id,
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
    );
  } finally {
    await app.close();
  }
});

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

test("POST /messages returns the completed pipeline run", async () => {
  const state = createState();
  const app = buildServer({
    repositories: createRepositoryDouble(state),
    runPipeline: async () => ({ result_id: "result-1" }),
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
      reply: "result-1",
      run_id: response.json().run_id,
      started_at: state.runs.get(response.json().run_id)?.startedAt ?? null,
      status: "completed",
    });

    const strategy = state.strategies.get("strategy-1");
    const run = state.runs.get(response.json().run_id);

    assert.ok(strategy);
    assert.equal(strategy.userId, "user-1");
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
    repositories: createRepositoryDouble(state),
    runPipeline: async () => ({ result_id: "result-2" }),
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
      reply: "result-2",
      run_id: runId,
      started_at: state.runs.get(runId)?.startedAt ?? null,
      stages: [],
      status: "completed",
    });
  } finally {
    await app.close();
  }
});

test("GET /runs/:id includes stage run summaries without payloads", async () => {
  const state = createState();
  state.runs.set("run-with-stages", {
    endedAt: "2026-05-14T00:01:00.000Z",
    error: null,
    exitCode: 0,
    reply: "result-with-stages",
    startedAt: "2026-05-14T00:00:00.000Z",
    status: "completed",
    strategyId: "strategy-1",
  });
  state.stageRuns.set("stage-run-1", {
    endedAt: new Date("2026-05-14T00:00:30.000Z"),
    error: null,
    input: { heavy: "input" },
    model: "gpt-test",
    opencodeSessionId: "session-1",
    output: { heavy: "output" },
    round: 1,
    runId: "run-with-stages",
    stage: "thesis",
    stageRunId: "stage-run-1",
    startedAt: new Date("2026-05-14T00:00:10.000Z"),
    status: "completed",
    tokensIn: 12,
    tokensOut: 34,
  });
  const app = buildServer({ repositories: createRepositoryDouble(state) });

  try {
    const response = await app.inject({
      method: "GET",
      url: "/runs/run-with-stages",
    });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(response.json().stages, [
      {
        ended_at: "2026-05-14T00:00:30.000Z",
        model: "gpt-test",
        round: 1,
        stage: "thesis",
        stage_run_id: "stage-run-1",
        started_at: "2026-05-14T00:00:10.000Z",
        status: "completed",
        tokens: { input: 12, output: 34 },
      },
    ]);
    assert.equal("input" in response.json().stages[0], false);
    assert.equal("output" in response.json().stages[0], false);
  } finally {
    await app.close();
  }
});

test("GET /runs/:id/stream emits snapshot and filtered stage deltas", async () => {
  const state = createState();
  state.runs.set("run-stream", {
    endedAt: null,
    error: null,
    exitCode: null,
    reply: null,
    startedAt: new Date("2026-01-01T00:00:00.000Z").toISOString(),
    status: "running",
    strategyId: "strategy-1",
  });
  state.stageRuns.set("stage-run-1", {
    endedAt: null,
    error: null,
    input: { prompt: "input" },
    model: "gpt-test",
    opencodeSessionId: "session-1",
    output: null,
    round: 1,
    runId: "run-stream",
    stage: "designer",
    stageRunId: "stage-run-1",
    startedAt: new Date("2026-01-01T00:00:01.000Z"),
    status: "running",
    tokensIn: 12,
    tokensOut: null,
  });

  let listener:
    | ((delta: {
        round: number;
        run_id: string;
        stage: string;
        stage_run_id: string;
        status: string;
      }) => void)
    | null = null;
  let cleanupCount = 0;
  const app = buildServer({
    repositories: createRepositoryDouble(state),
    async subscribeToStageRunChanges(onDelta) {
      listener = onDelta;
      return async () => {
        cleanupCount += 1;
        listener = null;
      };
    },
  });
  const abortController = new AbortController();

  try {
    const address = await app.listen({ host: "127.0.0.1", port: 0 });
    const response = await fetch(`${address}/runs/run-stream/stream`, {
      signal: abortController.signal,
    });

    assert.equal(response.status, 200);
    assert.equal(response.headers.get("content-type"), "text/event-stream");

    const events = await readSseEvents(response, 2, () => {
      assert.ok(listener);
      listener({
        round: 1,
        run_id: "other-run",
        stage: "designer",
        stage_run_id: "stage-run-other",
        status: "completed",
      });
      listener({
        round: 1,
        run_id: "run-stream",
        stage: "designer",
        stage_run_id: "stage-run-1",
        status: "completed",
      });
    });

    assert.deepEqual(events, [
      {
        event: "snapshot",
        data: [
          {
            ended_at: null,
            model: "gpt-test",
            round: 1,
            stage: "designer",
            stage_run_id: "stage-run-1",
            started_at: "2026-01-01T00:00:01.000Z",
            status: "running",
            tokens: { input: 12, output: null },
          },
        ],
      },
      {
        event: "delta",
        data: {
          round: 1,
          run_id: "run-stream",
          stage: "designer",
          stage_run_id: "stage-run-1",
          status: "completed",
        },
      },
    ]);

    abortController.abort();
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(cleanupCount, 1);
    assert.equal(listener, null);
  } finally {
    abortController.abort();
    await app.close();
  }
});

test("GET /runs/:id/stages/:stage_run_id returns the full stage run", async () => {
  const state = createState();
  state.runs.set("run-stage-detail", {
    endedAt: null,
    error: null,
    exitCode: null,
    reply: null,
    startedAt: "2026-05-14T00:00:00.000Z",
    status: "running",
    strategyId: "strategy-1",
  });
  state.stageRuns.set("stage-run-detail", {
    endedAt: null,
    error: null,
    input: { prompt: "design" },
    model: "gpt-test",
    opencodeSessionId: null,
    output: { answer: "allocation" },
    round: 2,
    runId: "run-stage-detail",
    stage: "designer",
    stageRunId: "stage-run-detail",
    startedAt: new Date("2026-05-14T00:00:10.000Z"),
    status: "running",
    tokensIn: null,
    tokensOut: null,
  });
  const app = buildServer({ repositories: createRepositoryDouble(state) });

  try {
    const response = await app.inject({
      method: "GET",
      url: "/runs/run-stage-detail/stages/stage-run-detail",
    });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(response.json(), {
      ended_at: null,
      error: null,
      input: { prompt: "design" },
      model: "gpt-test",
      opencode_session_id: null,
      output: { answer: "allocation" },
      round: 2,
      run_id: "run-stage-detail",
      stage: "designer",
      stage_run_id: "stage-run-detail",
      started_at: "2026-05-14T00:00:10.000Z",
      status: "running",
      tokens: { input: null, output: null },
    });
  } finally {
    await app.close();
  }
});

test("GET /runs/:id/stages/:stage_run_id returns 404 for missing stage runs", async () => {
  const state = createState();
  state.runs.set("run-stage-missing", {
    endedAt: null,
    error: null,
    exitCode: null,
    reply: null,
    startedAt: "2026-05-14T00:00:00.000Z",
    status: "running",
    strategyId: "strategy-1",
  });
  const app = buildServer({ repositories: createRepositoryDouble(state) });

  try {
    const response = await app.inject({
      method: "GET",
      url: "/runs/run-stage-missing/stages/missing-stage-run",
    });

    assert.equal(response.statusCode, 404);
    assert.deepEqual(response.json(), {
      error: "Not Found",
      message: "Stage run not found",
      statusCode: 404,
    });
  } finally {
    await app.close();
  }
});

test("GET /runs/:id/stages/:stage_run_id returns 404 for cross-run stage ids", async () => {
  const state = createState();
  state.runs.set("run-a", {
    endedAt: null,
    error: null,
    exitCode: null,
    reply: null,
    startedAt: "2026-05-14T00:00:00.000Z",
    status: "running",
    strategyId: "strategy-1",
  });
  state.runs.set("run-b", {
    endedAt: null,
    error: null,
    exitCode: null,
    reply: null,
    startedAt: "2026-05-14T00:00:00.000Z",
    status: "running",
    strategyId: "strategy-1",
  });
  state.stageRuns.set("stage-run-b", {
    endedAt: null,
    error: null,
    input: {},
    model: "gpt-test",
    opencodeSessionId: null,
    output: null,
    round: 1,
    runId: "run-b",
    stage: "thesis",
    stageRunId: "stage-run-b",
    startedAt: new Date("2026-05-14T00:00:10.000Z"),
    status: "running",
    tokensIn: null,
    tokensOut: null,
  });
  const app = buildServer({ repositories: createRepositoryDouble(state) });

  try {
    const response = await app.inject({
      method: "GET",
      url: "/runs/run-a/stages/stage-run-b",
    });

    assert.equal(response.statusCode, 404);
    assert.deepEqual(response.json(), {
      error: "Not Found",
      message: "Stage run not found",
      statusCode: 404,
    });
  } finally {
    await app.close();
  }
});

test("GET /runs/:id/events returns all stage events ordered by creation time", async () => {
  const state = createState();
  state.runs.set("run-events", {
    endedAt: null,
    error: null,
    exitCode: null,
    reply: null,
    startedAt: "2026-05-14T00:00:00.000Z",
    status: "running",
    strategyId: "strategy-1",
  });
  state.events.set("later", {
    createdAt: new Date("2026-05-14T00:00:03.000Z"),
    eventId: "later",
    eventType: "stage.completed",
    messageId: null,
    payload: { round: 1, stage: "thesis" },
    runId: "run-events",
    threadId: null,
  });
  state.events.set("non-stage", {
    createdAt: new Date("2026-05-14T00:00:01.000Z"),
    eventId: "non-stage",
    eventType: "run.started",
    messageId: null,
    payload: { round: 1, stage: "thesis" },
    runId: "run-events",
    threadId: null,
  });
  state.events.set("earlier", {
    createdAt: new Date("2026-05-14T00:00:02.000Z"),
    eventId: "earlier",
    eventType: "stage.started",
    messageId: null,
    payload: { round: 1, stage: "thesis" },
    runId: "run-events",
    threadId: null,
  });
  const app = buildServer({ repositories: createRepositoryDouble(state) });

  try {
    const response = await app.inject({
      method: "GET",
      url: "/runs/run-events/events",
    });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(response.json(), [
      {
        created_at: "2026-05-14T00:00:02.000Z",
        event_id: "earlier",
        event_type: "stage.started",
        payload: { round: 1, stage: "thesis" },
      },
      {
        created_at: "2026-05-14T00:00:03.000Z",
        event_id: "later",
        event_type: "stage.completed",
        payload: { round: 1, stage: "thesis" },
      },
    ]);
  } finally {
    await app.close();
  }
});

test("GET /runs/:id/events filters stage events by stage", async () => {
  const state = createState();
  state.runs.set("run-events", {
    endedAt: null,
    error: null,
    exitCode: null,
    reply: null,
    startedAt: "2026-05-14T00:00:00.000Z",
    status: "running",
    strategyId: "strategy-1",
  });
  for (const event of [
    { eventId: "thesis", payload: { round: 1, stage: "thesis" } },
    { eventId: "designer", payload: { round: 1, stage: "designer" } },
  ]) {
    state.events.set(event.eventId, {
      createdAt: new Date(`2026-05-14T00:00:0${state.events.size}.000Z`),
      eventId: event.eventId,
      eventType: "stage.started",
      messageId: null,
      payload: event.payload,
      runId: "run-events",
      threadId: null,
    });
  }
  const app = buildServer({ repositories: createRepositoryDouble(state) });

  try {
    const response = await app.inject({
      method: "GET",
      url: "/runs/run-events/events?stage=designer",
    });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(
      response.json().map((event: { event_id: string }) => event.event_id),
      ["designer"],
    );
  } finally {
    await app.close();
  }
});

test("GET /runs/:id/events filters stage events by stage and round", async () => {
  const state = createState();
  state.runs.set("run-events", {
    endedAt: null,
    error: null,
    exitCode: null,
    reply: null,
    startedAt: "2026-05-14T00:00:00.000Z",
    status: "running",
    strategyId: "strategy-1",
  });
  for (const event of [
    { eventId: "designer-r1", payload: { round: 1, stage: "designer" } },
    { eventId: "designer-r2", payload: { round: 2, stage: "designer" } },
    { eventId: "thesis-r2", payload: { round: 2, stage: "thesis" } },
  ]) {
    state.events.set(event.eventId, {
      createdAt: new Date(`2026-05-14T00:00:0${state.events.size}.000Z`),
      eventId: event.eventId,
      eventType: "stage.started",
      messageId: null,
      payload: event.payload,
      runId: "run-events",
      threadId: null,
    });
  }
  const app = buildServer({ repositories: createRepositoryDouble(state) });

  try {
    const response = await app.inject({
      method: "GET",
      url: "/runs/run-events/events?stage=designer&round=2",
    });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(
      response.json().map((event: { event_id: string }) => event.event_id),
      ["designer-r2"],
    );
  } finally {
    await app.close();
  }
});

test("GET /runs/:id/events returns an empty array when no events match", async () => {
  const state = createState();
  state.runs.set("run-events", {
    endedAt: null,
    error: null,
    exitCode: null,
    reply: null,
    startedAt: "2026-05-14T00:00:00.000Z",
    status: "running",
    strategyId: "strategy-1",
  });
  const app = buildServer({ repositories: createRepositoryDouble(state) });

  try {
    const response = await app.inject({
      method: "GET",
      url: "/runs/run-events/events?stage=reporter&round=3",
    });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(response.json(), []);
  } finally {
    await app.close();
  }
});

test("GET /runs/:id/events rejects invalid stage and round query params", async () => {
  const state = createState();
  state.runs.set("run-events", {
    endedAt: null,
    error: null,
    exitCode: null,
    reply: null,
    startedAt: "2026-05-14T00:00:00.000Z",
    status: "running",
    strategyId: "strategy-1",
  });
  const app = buildServer({ repositories: createRepositoryDouble(state) });

  try {
    for (const url of [
      "/runs/run-events/events?stage=unknown",
      "/runs/run-events/events?round=two",
      "/runs/run-events/events?round=4",
    ]) {
      const response = await app.inject({ method: "GET", url });
      assert.equal(response.statusCode, 400);
    }
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

test("different users can run pipelines in parallel", async () => {
  const state = createState();
  const bothRunsStarted = createDeferred<void>();
  const releaseRuns = createDeferred<void>();
  const runOrder: string[] = [];
  const app = buildServer({
    runPipeline: async (_runId, brief) => {
      runOrder.push(String(brief));

      if (runOrder.length === 2) {
        bothRunsStarted.resolve();
      }

      await releaseRuns.promise;

      return { result_id: "parallel-result" };
    },
    repositories: createRepositoryDouble(state),
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

    await bothRunsStarted.promise;
    assert.deepEqual([...runOrder].sort(), ["User one turn", "User two turn"]);

    releaseRuns.resolve();

    const [first, second] = await Promise.all([firstResponse, secondResponse]);

    assert.equal(first.statusCode, 200);
    assert.equal(second.statusCode, 200);
  } finally {
    releaseRuns.resolve();
    await app.close();
  }
});

test("unexpected errors return a 500 response", async () => {
  const app = buildServer({
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

test("pipeline error persists a failed run", async () => {
  const state = createState();
  const app = buildServer({
    repositories: createRepositoryDouble(state),
    async runPipeline() {
      throw new Error("Script timed out after 1s");
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

test("POST /messages/stream completes with pipeline result", async () => {
  const state = createState();
  const app = buildServer({
    repositories: createRepositoryDouble(state),
    runPipeline: async () => ({ result_id: "stream-result" }),
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
    assert.equal(completed.data.reply, "stream-result");

    const [run] = state.runs.values();

    assert.ok(run);
    assert.equal(run.reply, "stream-result");
  } finally {
    await app.close();
  }
});

test.skip("POST /messages/stream enriches structured_result from JSON artifacts", async () => {
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
    repositories: createRepositoryDouble(state),
    async runPipeline() {
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
        JSON.stringify([{ coin_id: "bitcoin", date: "2024-01-01", weight: 1 }]),
      );
      return { result_id: reply };
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
    repositories: createRepositoryDouble(state),
    runPipeline: async () => ({ result_id: reply }),
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
