import assert from "node:assert/strict";
import test from "node:test";
import type { QueryResultRow } from "pg";

import type {
  OpencodePromptResult,
  OpencodeTurnClient,
} from "../src/agent/session";
import { buildServer } from "../src/api/server";

type QueryResult<TRow extends QueryResultRow> = {
  rowCount: number | null;
  rows: TRow[];
};

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

type ClientState = {
  id: number;
  lockTimeoutMs: number;
  lockedUserIds: Set<string>;
};

type LockWaiter = {
  client: ClientState;
  reject(reason?: unknown): void;
  resolve(): void;
  timer?: NodeJS.Timeout;
};

function createState() {
  return {
    nextClientId: 1,
    runs: new Map<string, RunState>(),
    strategies: new Map<string, StrategyState>(),
    userLockQueues: new Map<string, LockWaiter[]>(),
    userLocks: new Map<string, number>(),
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

function sleep(milliseconds: number) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
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

function createDatabaseDouble(state: ReturnType<typeof createState>) {
  async function acquireUserLock(client: ClientState, userId: string) {
    const currentOwner = state.userLocks.get(userId);

    if (currentOwner === undefined || currentOwner === client.id) {
      state.userLocks.set(userId, client.id);
      client.lockedUserIds.add(userId);
      return;
    }

    await new Promise<void>((resolve, reject) => {
      const waiter: LockWaiter = {
        client,
        reject,
        resolve: () => {
          if (waiter.timer) {
            clearTimeout(waiter.timer);
          }

          state.userLocks.set(userId, client.id);
          client.lockedUserIds.add(userId);
          resolve();
        },
      };
      const queue = state.userLockQueues.get(userId) ?? [];

      if (client.lockTimeoutMs > 0) {
        waiter.timer = setTimeout(() => {
          state.userLockQueues.set(
            userId,
            (state.userLockQueues.get(userId) ?? []).filter(
              (queuedWaiter) => queuedWaiter !== waiter,
            ),
          );

          reject(
            Object.assign(
              new Error("canceling statement due to lock timeout"),
              {
                code: "55P03",
              },
            ),
          );
        }, client.lockTimeoutMs);
      }

      queue.push(waiter);
      state.userLockQueues.set(userId, queue);
    });
  }

  function releaseUserLocks(client: ClientState) {
    for (const userId of client.lockedUserIds) {
      if (state.userLocks.get(userId) !== client.id) {
        continue;
      }

      const queue = state.userLockQueues.get(userId) ?? [];
      const nextWaiter = queue.shift();

      if (queue.length > 0) {
        state.userLockQueues.set(userId, queue);
      } else {
        state.userLockQueues.delete(userId);
      }

      if (!nextWaiter) {
        state.userLocks.delete(userId);
        continue;
      }

      nextWaiter.resolve();
    }

    client.lockedUserIds.clear();
  }

  async function executeQuery<TRow extends QueryResultRow = QueryResultRow>(
    text: string,
    values: unknown[] = [],
    client?: ClientState,
  ): Promise<QueryResult<TRow>> {
    if (text === "BEGIN") {
      return { rowCount: null, rows: [] as TRow[] };
    }

    if (text === "COMMIT" || text === "ROLLBACK") {
      if (client) {
        releaseUserLocks(client);
      }

      return { rowCount: null, rows: [] as TRow[] };
    }

    if (text === "SELECT set_config('lock_timeout', $1, true)") {
      if (client) {
        const rawTimeout = String(values[0]);
        client.lockTimeoutMs = Number.parseInt(
          rawTimeout.replace(/ms$/, ""),
          10,
        );
      }

      return { rowCount: 1, rows: [] as TRow[] };
    }

    if (text === "SELECT pg_advisory_xact_lock(0, hashtext($1))") {
      assert.ok(client);
      await acquireUserLock(client, String(values[0]));
      return { rowCount: 1, rows: [] as TRow[] };
    }

    if (text.startsWith("INSERT INTO users")) {
      state.users.add(String(values[0]));
      return { rowCount: 1, rows: [] as TRow[] };
    }

    if (text.startsWith("INSERT INTO strategies")) {
      const strategyId = String(values[0]);

      if (!state.strategies.has(strategyId)) {
        state.strategies.set(strategyId, {
          lastUsedAt: new Date().toISOString(),
          opencodeSessionId: String(values[2]),
          title: String(values[3]),
          userId: String(values[1]),
        });
      }

      return { rowCount: 1, rows: [] as TRow[] };
    }

    if (text === "SELECT user_id FROM strategies WHERE strategy_id = $1") {
      const strategy = state.strategies.get(String(values[0]));

      return {
        rowCount: strategy ? 1 : 0,
        rows: strategy
          ? ([{ user_id: strategy.userId }] as unknown as TRow[])
          : ([] as TRow[]),
      };
    }

    if (text.startsWith("UPDATE strategies SET last_used_at = NOW()")) {
      const strategy = state.strategies.get(String(values[0]));

      if (strategy) {
        strategy.lastUsedAt = new Date().toISOString();
      }

      return { rowCount: strategy ? 1 : 0, rows: [] as TRow[] };
    }

    if (
      text ===
      "INSERT INTO runs (run_id, strategy_id, status) VALUES ($1, $2, $3)"
    ) {
      state.runs.set(String(values[0]), {
        endedAt: null,
        error: null,
        exitCode: null,
        reply: null,
        startedAt: new Date().toISOString(),
        status: String(values[2]),
        strategyId: String(values[1]),
      });

      return { rowCount: 1, rows: [] as TRow[] };
    }

    if (
      text ===
      "UPDATE runs SET status = $2, ended_at = NOW(), exit_code = $3, reply = $4, error = NULL WHERE run_id = $1"
    ) {
      const run = state.runs.get(String(values[0]));

      if (run) {
        run.status = String(values[1]);
        run.endedAt = new Date().toISOString();
        run.exitCode = Number(values[2]);
        run.reply = String(values[3]);
        run.error = null;
      }

      return { rowCount: run ? 1 : 0, rows: [] as TRow[] };
    }

    if (
      text ===
      "UPDATE runs SET status = $2, ended_at = NOW(), exit_code = $3, reply = NULL, error = $4 WHERE run_id = $1"
    ) {
      const run = state.runs.get(String(values[0]));

      if (run) {
        run.status = String(values[1]);
        run.endedAt = new Date().toISOString();
        run.exitCode = Number(values[2]);
        run.reply = null;
        run.error = String(values[3]);
      }

      return { rowCount: run ? 1 : 0, rows: [] as TRow[] };
    }

    if (text.startsWith("UPDATE strategies SET title = $2")) {
      const strategy = state.strategies.get(String(values[0]));

      if (strategy && !strategy.title.trim()) {
        strategy.title = String(values[1]);
      }

      return { rowCount: strategy ? 1 : 0, rows: [] as TRow[] };
    }

    if (
      text ===
      "SELECT run_id, status, started_at, ended_at, exit_code, reply, error FROM runs WHERE run_id = $1"
    ) {
      const runId = String(values[0]);
      const run = state.runs.get(runId);

      return {
        rowCount: run ? 1 : 0,
        rows: run
          ? ([
              {
                ended_at: run.endedAt,
                error: run.error,
                exit_code: run.exitCode,
                reply: run.reply,
                run_id: runId,
                started_at: run.startedAt,
                status: run.status,
              },
            ] as unknown as TRow[])
          : ([] as TRow[]),
      };
    }

    throw new Error(`Unexpected query: ${text}`);
  }

  return {
    async connect() {
      const clientState: ClientState = {
        id: state.nextClientId,
        lockedUserIds: new Set<string>(),
        lockTimeoutMs: 0,
      };

      state.nextClientId += 1;

      return {
        async query<TRow extends QueryResultRow = QueryResultRow>(
          text: string,
          values: unknown[] = [],
        ) {
          return executeQuery<TRow>(text, values, clientState);
        },
        release() {
          releaseUserLocks(clientState);
        },
      };
    },
    async query<TRow extends QueryResultRow = QueryResultRow>(
      text: string,
      values: unknown[] = [],
    ) {
      return executeQuery<TRow>(text, values);
    },
  };
}

test("POST /strategies creates a new strategy row and returns its id", async () => {
  const state = createState();
  const app = buildServer({
    db: createDatabaseDouble(state),
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
    db: createDatabaseDouble(createState()),
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

test("POST /messages returns the completed run and auto-creates the strategy", async () => {
  const state = createState();
  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    db: createDatabaseDouble(state),
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
    db: createDatabaseDouble(state),
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
    db: createDatabaseDouble(createState()),
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

test("same-user turns serialize across strategies", async () => {
  const state = createState();
  const firstBuildStarted = createDeferred<void>();
  const releaseFirstBuild = createDeferred<void>();
  const buildOrder: string[] = [];
  const app = buildServer({
    buildSystemPrompt: async ({ strategyId, userId }) => {
      buildOrder.push(`${userId}/${strategyId}`);

      if (strategyId === "strategy-1") {
        firstBuildStarted.resolve();
        await releaseFirstBuild.promise;
      }

      return `system prompt for ${strategyId}`;
    },
    db: createDatabaseDouble(state),
    getOpencodeClient: async () =>
      createOpencodeClientDouble(
        "Serialized strategy",
        completedPromptResult("Serialized reply.", "session-serialized"),
      ),
    getSessionId: async (strategyId) => {
      const strategy = state.strategies.get(strategyId);

      assert.ok(strategy);
      strategy.opencodeSessionId ||= `session-${strategyId}`;

      return strategy.opencodeSessionId;
    },
    turnLockTimeoutMs: 200,
  });

  try {
    const firstResponse = app.inject({
      method: "POST",
      payload: {
        strategy_id: "strategy-1",
        text: "Run the first turn",
        user_id: "user-1",
      },
      url: "/messages",
    });

    await firstBuildStarted.promise;

    const secondResponse = app.inject({
      method: "POST",
      payload: {
        strategy_id: "strategy-2",
        text: "Run the second turn",
        user_id: "user-1",
      },
      url: "/messages",
    });

    await sleep(25);
    assert.deepEqual(buildOrder, ["user-1/strategy-1"]);

    releaseFirstBuild.resolve();

    const [first, second] = await Promise.all([firstResponse, secondResponse]);

    assert.equal(first.statusCode, 200);
    assert.equal(second.statusCode, 200);
    assert.deepEqual(buildOrder, ["user-1/strategy-1", "user-1/strategy-2"]);
  } finally {
    releaseFirstBuild.resolve();
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
    db: createDatabaseDouble(state),
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

test("lock timeout returns a conflict error", async () => {
  const state = createState();
  const firstBuildStarted = createDeferred<void>();
  const releaseFirstBuild = createDeferred<void>();
  const app = buildServer({
    buildSystemPrompt: async ({ strategyId }) => {
      if (strategyId === "strategy-1") {
        firstBuildStarted.resolve();
        await releaseFirstBuild.promise;
      }

      return `system prompt for ${strategyId}`;
    },
    db: createDatabaseDouble(state),
    getOpencodeClient: async () =>
      createOpencodeClientDouble(
        "Timeout strategy",
        completedPromptResult("Timeout reply.", "session-timeout"),
      ),
    getSessionId: async (strategyId) => {
      const strategy = state.strategies.get(strategyId);

      assert.ok(strategy);
      strategy.opencodeSessionId ||= `session-${strategyId}`;

      return strategy.opencodeSessionId;
    },
    turnLockTimeoutMs: 20,
  });

  try {
    const firstResponse = app.inject({
      method: "POST",
      payload: {
        strategy_id: "strategy-1",
        text: "Hold the lock",
        user_id: "user-1",
      },
      url: "/messages",
    });

    await firstBuildStarted.promise;

    const secondResponse = await app.inject({
      method: "POST",
      payload: {
        strategy_id: "strategy-2",
        text: "This should time out",
        user_id: "user-1",
      },
      url: "/messages",
    });

    assert.equal(secondResponse.statusCode, 409);
    assert.deepEqual(secondResponse.json(), {
      error: "Conflict",
      message: "Timed out after 20ms waiting for the user turn lock",
      statusCode: 409,
    });
    assert.equal(state.runs.size, 1);

    releaseFirstBuild.resolve();
    await firstResponse;
  } finally {
    releaseFirstBuild.resolve();
    await app.close();
  }
});

test("unexpected errors return a 500 response", async () => {
  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    db: {
      async connect() {
        throw new Error("database offline");
      },
      async query() {
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
    db: createDatabaseDouble(state),
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
