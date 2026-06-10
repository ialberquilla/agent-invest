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
import type { WorkflowState } from "../src/agent/workflow/state";

// Stub WorkflowState mirroring a winning run. Tests that don't care
// about the specifics of the run use this; tests that do care
// customize via the partial override.
function stubWorkflowState(
  resultLabel: string,
  overrides: Partial<WorkflowState> = {},
): WorkflowState {
  return {
    run_id: "stub",
    brief: "stub",
    workflow_version: "test",
    attempts: [],
    counters: { reinterpret_brief: 0, broaden_universe: 0 },
    final: {
      kind: "winner",
      run_id: "stub",
      winner_candidate_id: "c1",
      winner_attempt_n: 1,
      candidate_batch_id: `batch_${resultLabel}`,
      is_best_effort: false,
      unmet_constraints: [],
      thesis: {} as never,
      universe: {} as never,
      window: {} as never,
      attempts_summary: [],
      narrative: {
        title: resultLabel,
        summary: "",
        reasoning: "",
        assumptions: [],
        risks: [],
        next_steps: [],
      },
    },
    ...overrides,
  };
}

function stubWorkflow(resultLabel: string) {
  return async () => stubWorkflowState(resultLabel);
}

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
    path.join(tmpdir(), "pond3r-portfolio-cleanup-"),
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

test("POST /messages forwards strategy-type overrides to the workflow", async () => {
  const state = createState();
  let captured: unknown;
  const app = buildServer({
    repositories: createRepositoryDouble(state),
    runWorkflow: (async (
      _runId: string,
      _brief: unknown,
      _deps: unknown,
      options: unknown,
    ) => {
      captured = options;
      return stubWorkflowState("ov");
    }) as never,
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: {
        strategy_id: "strategy-ov",
        text: "single asset",
        user_id: "user-ov",
        overrides: {
          strategy_mode: "single_asset",
          asset_count_min: 1,
          asset_count_max: 1,
          target_coin_id: "bitcoin",
        },
      },
      url: "/messages",
    });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(captured, {
      overrides: {
        strategy_mode: "single_asset",
        asset_count_min: 1,
        asset_count_max: 1,
        target_coin_id: "bitcoin",
      },
    });
  } finally {
    await app.close();
  }
});

test("POST /messages returns the completed pipeline run", async () => {
  const state = createState();
  const app = buildServer({
    repositories: createRepositoryDouble(state),
    runWorkflow: stubWorkflow("result-1"),
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
    const body = response.json();
    assert.equal(body.status, "completed");
    assert.equal(body.exit_code, 0);
    assert.equal(body.error, null);
    assert.equal(body.reply, "result-1");
    assert.match(body.run_id, /^[0-9a-f-]{36}$/);
    assert.ok(body.structured_result);
    assert.equal(body.structured_result.title, "result-1");
    assert.equal(body.structured_result.winner_candidate_id, "c1");

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
    runWorkflow: stubWorkflow("result-2"),
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

test("GET /runs/:id/events/stream emits snapshot then live events", async () => {
  const state = createState();
  state.runs.set("run-events", {
    endedAt: null,
    error: null,
    exitCode: null,
    reply: null,
    startedAt: new Date("2026-01-01T00:00:00.000Z").toISOString(),
    status: "running",
    strategyId: "strategy-1",
  });
  state.events.set("ev-snap", {
    eventId: "ev-snap",
    threadId: null,
    messageId: null,
    runId: "run-events",
    eventType: "stage.started",
    payload: { stage: "interpret_brief" },
    createdAt: new Date("2026-01-01T00:00:01.000Z"),
  });

  let listener: ((event: AgentEventState) => void) | null = null;
  let unsubscribeCount = 0;
  const app = buildServer({
    repositories: createRepositoryDouble(state),
    subscribeAgentEvents: ((
      runId: string,
      onEvent: (event: unknown) => void,
    ) => {
      listener = (event) => {
        if (event.runId === runId) onEvent(event);
      };
      return () => {
        unsubscribeCount += 1;
        listener = null;
      };
    }) as never,
  });
  const abortController = new AbortController();

  try {
    const address = await app.listen({ host: "127.0.0.1", port: 0 });
    const response = await fetch(`${address}/runs/run-events/events/stream`, {
      signal: abortController.signal,
    });

    assert.equal(response.status, 200);
    assert.equal(response.headers.get("content-type"), "text/event-stream");

    const events = await readSseEvents(response, 2, () => {
      assert.ok(listener);
      // Emitted for a different run -- must NOT be forwarded.
      listener({
        eventId: "ev-other",
        threadId: null,
        messageId: null,
        runId: "run-different",
        eventType: "stage.started",
        payload: { stage: "interpret_brief" },
        createdAt: new Date("2026-01-01T00:00:02.000Z"),
      });
      // Matches the subscribed run.
      listener({
        eventId: "ev-live",
        threadId: null,
        messageId: null,
        runId: "run-events",
        eventType: "stage.completed",
        payload: { stage: "interpret_brief", next: "select_universe" },
        createdAt: new Date("2026-01-01T00:00:03.000Z"),
      });
    });

    assert.deepEqual(events, [
      {
        event: "snapshot",
        data: [
          {
            event_id: "ev-snap",
            event_type: "stage.started",
            payload: { stage: "interpret_brief" },
            created_at: "2026-01-01T00:00:01.000Z",
          },
        ],
      },
      {
        event: "event",
        data: {
          event_id: "ev-live",
          event_type: "stage.completed",
          payload: { stage: "interpret_brief", next: "select_universe" },
          created_at: "2026-01-01T00:00:03.000Z",
        },
      },
    ]);
  } finally {
    abortController.abort();
    await app.close();
  }

  // Give the close handlers a tick to run so unsubscribe fires.
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(unsubscribeCount, 1);
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
    runWorkflow: async (_runId: string, brief) => {
      runOrder.push(String(brief));

      if (runOrder.length === 2) {
        bothRunsStarted.resolve();
      }

      await releaseRuns.promise;

      return stubWorkflowState("parallel-result");
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
    async runWorkflow() {
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
    runWorkflow: stubWorkflow("stream-result"),
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
    assert.equal(finalizing.data.message, "Assembling structured result");
    assert.ok(completed);
    assert.equal(completed.data.reply, "stream-result");

    const [run] = state.runs.values();

    assert.ok(run);
    assert.equal(run.reply, "stream-result");
  } finally {
    await app.close();
  }
});

test("POST /messages/stream starts wizard runs without chat agent", async () => {
  const state = createState();
  let receivedRunId: string | undefined;
  let receivedBrief: string | Record<string, unknown> | undefined;
  const app = buildServer({
    repositories: createRepositoryDouble(state),
    async runWorkflow(runId: string, brief) {
      receivedRunId = runId;
      receivedBrief = brief;
      return stubWorkflowState("wizard-result");
    },
    chatAgent: {
      async run() {
        throw new Error("chat agent should not run for wizard submissions");
      },
    },
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: {
        strategy_id: "strategy-wizard-stream",
        user_id: "user-1",
        wizard_params: {
          universe: "top25",
          exclusions: ["stablecoins", "wrapped"],
          minimumMarketCap: "1b",
          concentrationLimit: "20",
          maxDrawdown: "35",
          riskPreference: "balanced",
          horizon: "1y",
          rebalance: "monthly",
          initialCapitalUsd: "10000",
          cashAllocation: "10",
          targetAssets: "5-10",
        },
      },
      url: "/messages/stream",
    });

    assert.equal(response.statusCode, 200);
    assert.equal(state.runs.size, 1);
    assert.ok(receivedRunId);
    assert.equal(state.runs.has(receivedRunId), true);
    assert.equal(typeof receivedBrief, "string");
    assert.match(receivedBrief as string, /User brief:/);
    assert.match(receivedBrief as string, /Universe: top 25 cryptoassets/);

    const events = parseSseEvents(response.body);
    const started = events.find((event) => event.event === "run.started");
    assert.ok(started);
    assert.equal(started.data.run_id, receivedRunId);

    const [run] = state.runs.values();
    assert.ok(run);
    assert.equal(run.status, "completed");
    assert.equal(run.reply, "wizard-result");
  } finally {
    await app.close();
  }
});

test("POST /chat/messages invokes chat agent and returns JSON response", async () => {
  let receivedInput: unknown;
  const app = buildServer({
    chatAgent: {
      async run(input) {
        receivedInput = input;
        return {
          content: "I can help with that.",
          opencode_session_id: "opencode-chat-1",
        };
      },
    },
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: {
        chat_session_id: "chat-1",
        message: "Explain momentum.",
      },
      url: "/chat/messages",
    });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(receivedInput, {
      chatSessionId: "chat-1",
      message: "Explain momentum.",
      userId: "chat-1",
    });
    assert.deepEqual(response.json(), {
      chat_session_id: "chat-1",
      content: "I can help with that.",
      opencode_session_id: "opencode-chat-1",
    });
  } finally {
    await app.close();
  }
});

test("POST /chat/messages/stream streams chat agent events", async () => {
  let receivedInput: unknown;
  const app = buildServer({
    chatAgent: {
      async run(input) {
        receivedInput = input;
        return {
          content: "I can help with that.",
          opencode_session_id: "opencode-chat-1",
        };
      },
    },
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: {
        chat_session_id: "chat-1",
        message: "Explain momentum.",
      },
      url: "/chat/messages/stream",
    });

    assert.equal(response.statusCode, 200);
    assert.equal(response.headers["content-type"], "text/event-stream");
    assert.deepEqual(receivedInput, {
      chatSessionId: "chat-1",
      message: "Explain momentum.",
      userId: "chat-1",
    });
    assert.deepEqual(parseSseEvents(response.body), [
      {
        data: { type: "chat.delta", content: "I can help with that." },
        event: "chat.delta",
      },
      {
        data: {
          type: "chat.completed",
          content: "I can help with that.",
          opencode_session_id: "opencode-chat-1",
        },
        event: "chat.completed",
      },
    ]);
  } finally {
    await app.close();
  }
});

test("POST /chat/messages includes run_id when chat agent starts pipeline", async () => {
  const app = buildServer({
    chatAgent: {
      async run() {
        return {
          content: "Started the strategy pipeline.",
          opencode_session_id: "opencode-chat-2",
          run_id: "run-chat-1",
        };
      },
    },
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: {
        chat_session_id: "chat-2",
        message: "Build and test a BTC momentum strategy.",
      },
      url: "/chat/messages",
    });

    assert.equal(response.statusCode, 200);
    assert.equal(response.json().run_id, "run-chat-1");
  } finally {
    await app.close();
  }
});

test("POST /screeners/markets invokes screener and returns structured payload", async () => {
  let receivedInput: unknown;
  const app = buildServer({
    async screenMarkets(input) {
      receivedInput = input;
      return {
        type: "market_screener",
        version: 1,
        title: "GMX momentum screener",
        summary: "Only GMX rows.",
        definition: { factor: "momentum", limit: 3, gmx_only: true },
        rows: [],
        notes: [],
      };
    },
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: {
        factor: "momentum",
        limit: 3,
        gmxOnly: true,
      },
      url: "/screeners/markets",
    });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(receivedInput, {
      factor: "momentum",
      limit: 3,
      gmxOnly: true,
    });
    assert.equal(response.json().type, "market_screener");
  } finally {
    await app.close();
  }
});

test("GET /runs/:id/vault/allocation is executable when GMX markets resolve", async () => {
  const app = buildServer({
    repositories: {
      async readMandatesForRun() {
        return [
          {
            mandateId: "mandate-1",
            runId: "run-1",
            version: 1,
            status: "active",
            templateId: "top_n_momentum",
            spec: {
              allowed_sides: "long_only",
              initial_target_allocation: [{ coin_id: "bitcoin", weight: 1 }],
              template_id: "top_n_momentum",
            },
            createdAt: new Date(),
            updatedAt: new Date(),
          } as never,
        ];
      },
      async readVaultForMandate() {
        return {
          assetAddress: "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
          chainId: 42161,
          createdAt: new Date(),
          mandateId: "mandate-1",
          status: "active",
          updatedAt: new Date(),
          vaultAddress: "0x0000000000000000000000000000000000000001",
        } as never;
      },
      async resolveMarketsBatch() {
        return {
          failures: new Map(),
          resolved: new Map([
            [
              "bitcoin",
              {
                coinId: "bitcoin",
                collateralDecimals: 6,
                collateralToken: "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                gmxMarket: "0x47c031236e19d024b42f8AE6780E44A573170703",
                indexToken: "0x47904963fc8b2340414262125aF798B9655E58Cd",
                indexTokenDecimals: 8,
                isSynthetic: false,
                longToken: "0x47904963fc8b2340414262125aF798B9655E58Cd",
                marketName: "BTC/USD [BTC-USDC]",
                shortToken: "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                symbol: "BTC",
              },
            ],
          ]),
        };
      },
    },
  });

  try {
    const response = await app.inject({
      method: "GET",
      url: "/runs/run-1/vault/allocation",
    });

    assert.equal(response.statusCode, 200);
    assert.equal(response.json().executable, true);
    assert.deepEqual(response.json().missing, []);
    assert.equal(
      response.json().target_allocation[0].gmx_market.market_token,
      "0x47c031236e19d024b42f8AE6780E44A573170703",
    );
  } finally {
    await app.close();
  }
});

test("GET /runs/:id/vault/allocation blocks unresolved GMX markets", async () => {
  const app = buildServer({
    repositories: {
      async readMandatesForRun() {
        return [
          {
            mandateId: "mandate-1",
            runId: "run-1",
            version: 1,
            status: "active",
            templateId: "top_n_momentum",
            spec: {
              initial_target_allocation: [{ coin_id: "bitcoin", weight: 1 }],
              template_id: "top_n_momentum",
            },
            createdAt: new Date(),
            updatedAt: new Date(),
          } as never,
        ];
      },
      async readVaultForMandate() {
        return {
          assetAddress: "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
          chainId: 42161,
          createdAt: new Date(),
          mandateId: "mandate-1",
          status: "active",
          updatedAt: new Date(),
          vaultAddress: "0x0000000000000000000000000000000000000001",
        } as never;
      },
      async resolveMarketsBatch() {
        return {
          failures: new Map([["bitcoin", new Error("missing")]]),
          resolved: new Map(),
        };
      },
    },
  });

  try {
    const response = await app.inject({
      method: "GET",
      url: "/runs/run-1/vault/allocation",
    });

    assert.equal(response.statusCode, 200);
    assert.equal(response.json().executable, false);
    assert.deepEqual(response.json().missing, [
      "GMX market resolution for bitcoin",
    ]);
    assert.equal(response.json().target_allocation[0].gmx_market, null);
  } finally {
    await app.close();
  }
});
