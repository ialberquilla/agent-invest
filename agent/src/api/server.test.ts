import assert from "node:assert/strict";
import test from "node:test";
import type { Event as OpencodeEvent } from "@opencode-ai/sdk";
import type { QueryResultRow } from "pg";

import { buildServer } from "./server.js";

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

type EventState = {
  seq: number;
  type: string;
  payload: Record<string, unknown>;
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

function createClosedEventStream() {
  return {
    [Symbol.asyncIterator]() {
      return {
        async next(): Promise<IteratorResult<OpencodeEvent, undefined>> {
          return { done: true, value: undefined };
        },
      };
    },
  };
}

function createAsyncEventStream() {
  const queue: OpencodeEvent[] = [];
  let ended = false;
  let resolveNext:
    | ((result: IteratorResult<OpencodeEvent, undefined>) => void)
    | undefined;

  function flush(value: IteratorResult<OpencodeEvent, undefined>) {
    if (!resolveNext) {
      return false;
    }

    const resolve = resolveNext;

    resolveNext = undefined;
    resolve(value);
    return true;
  }

  return {
    emit(event: OpencodeEvent) {
      if (ended) {
        throw new Error("Event stream already ended");
      }

      if (flush({ done: false, value: event })) {
        return;
      }

      queue.push(event);
    },
    end() {
      ended = true;
      flush({ done: true, value: undefined });
    },
    stream(signal?: AbortSignal) {
      return {
        [Symbol.asyncIterator]() {
          return this;
        },
        async next(): Promise<IteratorResult<OpencodeEvent, undefined>> {
          if (queue.length > 0) {
            return { done: false, value: queue.shift()! };
          }

          if (ended || signal?.aborted) {
            return { done: true, value: undefined };
          }

          return new Promise((resolve) => {
            const onAbort = () => {
              signal?.removeEventListener("abort", onAbort);
              flush({ done: true, value: undefined });
            };

            signal?.addEventListener("abort", onAbort, { once: true });
            resolveNext = (result) => {
              signal?.removeEventListener("abort", onAbort);
              resolve(result);
            };
          });
        },
      };
    },
  };
}

function parseSseEvents(body: string) {
  return body
    .trim()
    .split("\n\n")
    .filter(Boolean)
    .map((chunk) => {
      const lines = chunk.split("\n");
      const id = lines.find((line) => line.startsWith("id: "))?.slice(4) ?? "0";
      const event =
        lines.find((line) => line.startsWith("event: "))?.slice(7) ?? "message";
      const data =
        lines.find((line) => line.startsWith("data: "))?.slice(6) ?? "{}";

      return {
        data: JSON.parse(data) as Record<string, unknown>,
        event,
        id: Number(id),
      };
    });
}

function opencodeEvent(
  type: OpencodeEvent["type"],
  properties: Record<string, unknown>,
): OpencodeEvent {
  return {
    properties,
    type,
  } as OpencodeEvent;
}

function createState() {
  return {
    events: new Map<string, EventState[]>(),
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

async function withEnv(
  overrides: Partial<NodeJS.ProcessEnv>,
  callback: () => Promise<void>,
) {
  const previousValues = new Map<string, string | undefined>();

  for (const [key, value] of Object.entries(overrides)) {
    previousValues.set(key, process.env[key]);

    if (value === undefined) {
      delete process.env[key];
      continue;
    }

    process.env[key] = value;
  }

  try {
    await callback();
  } finally {
    for (const [key, value] of previousValues) {
      if (value === undefined) {
        delete process.env[key];
        continue;
      }

      process.env[key] = value;
    }
  }
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

    if (text === "SELECT user_id FROM users WHERE user_id = $1 FOR UPDATE") {
      assert.ok(client);
      await acquireUserLock(client, String(values[0]));

      return {
        rowCount: state.users.has(String(values[0])) ? 1 : 0,
        rows: state.users.has(String(values[0]))
          ? ([{ user_id: String(values[0]) }] as unknown as TRow[])
          : ([] as TRow[]),
      };
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
      "INSERT INTO events (run_id, seq, type, payload) VALUES ($1, $2, $3, $4::jsonb)"
    ) {
      const runId = String(values[0]);
      const payload = JSON.parse(String(values[3])) as Record<string, unknown>;
      const events = state.events.get(runId) ?? [];

      events.push({
        payload,
        seq: Number(values[1]),
        type: String(values[2]),
      });
      state.events.set(runId, events);

      return { rowCount: 1, rows: [] as TRow[] };
    }

    if (
      text ===
      "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq FROM events WHERE run_id = $1"
    ) {
      const runId = String(values[0]);
      const nextSeq =
        Math.max(0, ...(state.events.get(runId) ?? []).map(({ seq }) => seq)) +
        1;

      return {
        rowCount: 1,
        rows: [{ next_seq: nextSeq }] as unknown as TRow[],
      };
    }

    if (
      text ===
      "UPDATE runs SET status = $2, ended_at = NOW(), exit_code = $3 WHERE run_id = $1"
    ) {
      const run = state.runs.get(String(values[0]));

      if (run) {
        run.status = String(values[1]);
        run.endedAt = new Date().toISOString();
        run.exitCode = Number(values[2]);
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
      text.startsWith(
        "SELECT r.run_id, r.status, r.started_at, r.ended_at, r.exit_code",
      )
    ) {
      const [userId, strategyId, runId] = values.map(String);
      const strategy = state.strategies.get(strategyId);
      const run = state.runs.get(runId);

      if (
        !strategy ||
        !run ||
        strategy.userId !== userId ||
        run.strategyId !== strategyId
      ) {
        return { rowCount: 0, rows: [] as TRow[] };
      }

      return {
        rowCount: 1,
        rows: [
          {
            ended_at: run.endedAt,
            exit_code: run.exitCode,
            run_id: runId,
            started_at: run.startedAt,
            status: run.status,
          },
        ] as unknown as TRow[],
      };
    }

    if (text.startsWith("SELECT type, payload FROM events")) {
      const runId = String(values[0]);
      const event = [...(state.events.get(runId) ?? [])]
        .filter(({ type }) => type === "run.completed" || type === "run.failed")
        .sort((left, right) => right.seq - left.seq)[0];

      return {
        rowCount: event ? 1 : 0,
        rows: event
          ? ([
              { payload: event.payload, type: event.type },
            ] as unknown as TRow[])
          : ([] as TRow[]),
      };
    }

    if (text.startsWith("SELECT seq, type, payload FROM events")) {
      const runId = String(values[0]);
      const lastEventId = Number(values[1]);
      const events = [...(state.events.get(runId) ?? [])]
        .filter(({ seq }) => seq > lastEventId)
        .sort((left, right) => left.seq - right.seq)
        .map(({ payload, seq, type }) => ({ payload, seq, type }));

      return {
        rowCount: events.length,
        rows: events as unknown as TRow[],
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

test("POST /messages?wait returns the completed run and auto-creates the strategy", async () => {
  const state = createState();
  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    db: createDatabaseDouble(state),
    getOpencodeClient: async () => ({
      async getSession() {
        return {
          title: "Momentum explorer",
        };
      },
      async prompt() {
        return {
          info: {
            cost: 0,
            id: "assistant-1",
            mode: "chat",
            modelID: "gpt-5",
            parentID: "run-1",
            path: { cwd: "/tmp", root: "/tmp" },
            providerID: "openai",
            role: "assistant",
            sessionID: "session-1",
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
              id: "text-1",
              messageID: "assistant-1",
              sessionID: "session-1",
              text: "Here is the agent reply.",
              type: "text",
            },
          ],
        };
      },
      async subscribeEvents() {
        return createClosedEventStream();
      },
    }),
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
      payload: { text: "Build me a crypto momentum strategy" },
      url: "/users/user-1/strategies/strategy-1/messages?wait=1",
    });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(response.json(), {
      ended_at: state.runs.get(response.json().run_id)?.endedAt ?? null,
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

test("GET /runs/{run_id}?wait waits for async completion and returns the reply", async () => {
  const state = createState();
  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    db: createDatabaseDouble(state),
    getOpencodeClient: async () => ({
      async getSession() {
        return {
          title: "Mean reversion scout",
        };
      },
      async prompt() {
        await new Promise((resolve) => setTimeout(resolve, 50));

        return {
          info: {
            cost: 0,
            id: "assistant-2",
            mode: "chat",
            modelID: "gpt-5",
            parentID: "run-2",
            path: { cwd: "/tmp", root: "/tmp" },
            providerID: "openai",
            role: "assistant",
            sessionID: "session-2",
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
              id: "text-2",
              messageID: "assistant-2",
              sessionID: "session-2",
              text: "Async reply completed.",
              type: "text",
            },
          ],
        };
      },
      async subscribeEvents() {
        return createClosedEventStream();
      },
    }),
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
      payload: { text: "Try a mean reversion approach" },
      url: "/users/user-1/strategies/strategy-2/messages",
    });

    assert.equal(createResponse.statusCode, 202);

    const { run_id: runId } = createResponse.json() as { run_id: string };
    const runResponse = await app.inject({
      method: "GET",
      url: `/users/user-1/strategies/strategy-2/runs/${runId}?wait=1`,
    });

    assert.equal(runResponse.statusCode, 200);
    assert.deepEqual(runResponse.json(), {
      ended_at: state.runs.get(runId)?.endedAt ?? null,
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
    getOpencodeClient: async () => ({
      async getSession() {
        return {
          title: "Serialized strategy",
        };
      },
      async prompt() {
        return {
          info: {
            cost: 0,
            id: "assistant-serialized",
            mode: "chat",
            modelID: "gpt-5",
            parentID: "run-serialized",
            path: { cwd: "/tmp", root: "/tmp" },
            providerID: "openai",
            role: "assistant",
            sessionID: "session-serialized",
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
              id: "text-serialized",
              messageID: "assistant-serialized",
              sessionID: "session-serialized",
              text: "Serialized reply.",
              type: "text",
            },
          ],
        };
      },
      async subscribeEvents() {
        return createClosedEventStream();
      },
    }),
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
      payload: { text: "Run the first turn" },
      url: "/users/user-1/strategies/strategy-1/messages?wait=1",
    });

    await firstBuildStarted.promise;

    const secondResponse = app.inject({
      method: "POST",
      payload: { text: "Run the second turn" },
      url: "/users/user-1/strategies/strategy-2/messages?wait=1",
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
    getOpencodeClient: async () => ({
      async getSession() {
        return {
          title: "Parallel strategy",
        };
      },
      async prompt() {
        return {
          info: {
            cost: 0,
            id: "assistant-parallel",
            mode: "chat",
            modelID: "gpt-5",
            parentID: "run-parallel",
            path: { cwd: "/tmp", root: "/tmp" },
            providerID: "openai",
            role: "assistant",
            sessionID: "session-parallel",
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
              id: "text-parallel",
              messageID: "assistant-parallel",
              sessionID: "session-parallel",
              text: "Parallel reply.",
              type: "text",
            },
          ],
        };
      },
      async subscribeEvents() {
        return createClosedEventStream();
      },
    }),
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
      payload: { text: "User one turn" },
      url: "/users/user-1/strategies/strategy-1/messages?wait=1",
    });
    const secondResponse = app.inject({
      method: "POST",
      payload: { text: "User two turn" },
      url: "/users/user-2/strategies/strategy-2/messages?wait=1",
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

test("lock timeout returns a typed conflict error", async () => {
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
    getOpencodeClient: async () => ({
      async getSession() {
        return {
          title: "Timeout strategy",
        };
      },
      async prompt() {
        return {
          info: {
            cost: 0,
            id: "assistant-timeout",
            mode: "chat",
            modelID: "gpt-5",
            parentID: "run-timeout",
            path: { cwd: "/tmp", root: "/tmp" },
            providerID: "openai",
            role: "assistant",
            sessionID: "session-timeout",
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
              id: "text-timeout",
              messageID: "assistant-timeout",
              sessionID: "session-timeout",
              text: "Timeout reply.",
              type: "text",
            },
          ],
        };
      },
      async subscribeEvents() {
        return createClosedEventStream();
      },
    }),
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
      payload: { text: "Hold the lock" },
      url: "/users/user-1/strategies/strategy-1/messages?wait=1",
    });

    await firstBuildStarted.promise;

    const secondResponse = await app.inject({
      method: "POST",
      payload: { text: "This should time out" },
      url: "/users/user-1/strategies/strategy-2/messages",
    });

    assert.equal(secondResponse.statusCode, 409);
    assert.deepEqual(secondResponse.json(), {
      error: {
        code: "turn_lock_timeout",
        message: "Timed out after 20ms waiting for the user turn lock",
      },
    });
    assert.equal(state.runs.size, 1);

    releaseFirstBuild.resolve();
    await firstResponse;
  } finally {
    releaseFirstBuild.resolve();
    await app.close();
  }
});

test("unexpected errors return the structured error envelope", async () => {
  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    db: {
      async connect() {
        throw new Error("connect should not be called");
      },
      async query() {
        throw new Error("database offline");
      },
    },
  });

  try {
    const response = await app.inject({
      method: "POST",
      payload: { text: "Trigger an unexpected failure" },
      url: "/users/user-1/strategies/strategy-error/messages",
    });

    assert.equal(response.statusCode, 500);
    assert.deepEqual(response.json(), {
      error: {
        code: "internal_error",
        message: "Internal server error",
      },
    });
  } finally {
    await app.close();
  }
});

test("POST /messages applies rate limiting and returns Retry-After", async () => {
  await withEnv(
    {
      MESSAGES_RATE_LIMIT_MAX: "1",
      MESSAGES_RATE_LIMIT_WINDOW_MS: "60000",
    },
    async () => {
      const state = createState();
      const app = buildServer({
        buildSystemPrompt: async () => "system prompt",
        db: createDatabaseDouble(state),
        getOpencodeClient: async () => ({
          async getSession() {
            return {
              title: "Rate limited strategy",
            };
          },
          async prompt() {
            return {
              info: {
                cost: 0,
                id: "assistant-rate-limit",
                mode: "chat",
                modelID: "gpt-5",
                parentID: "run-rate-limit",
                path: { cwd: "/tmp", root: "/tmp" },
                providerID: "openai",
                role: "assistant",
                sessionID: "session-rate-limit",
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
                  id: "text-rate-limit",
                  messageID: "assistant-rate-limit",
                  sessionID: "session-rate-limit",
                  text: "Rate limited reply.",
                  type: "text",
                },
              ],
            };
          },
          async subscribeEvents() {
            return createClosedEventStream();
          },
        }),
        getSessionId: async (strategyId) => {
          const strategy = state.strategies.get(strategyId);

          assert.ok(strategy);
          strategy.opencodeSessionId ||= `session-${strategyId}`;

          return strategy.opencodeSessionId;
        },
      });

      try {
        const firstResponse = await app.inject({
          method: "POST",
          payload: { text: "First turn" },
          url: "/users/user-1/strategies/strategy-rate-limit/messages?wait=1",
        });

        assert.equal(firstResponse.statusCode, 200);

        const secondResponse = await app.inject({
          method: "POST",
          payload: { text: "Second turn" },
          url: "/users/user-1/strategies/strategy-rate-limit/messages",
        });

        assert.equal(secondResponse.statusCode, 429);
        assert.deepEqual(secondResponse.json(), {
          error: {
            code: "rate_limited",
            message: "Rate limit exceeded, retry in 1 minute",
          },
        });
        assert.ok(secondResponse.headers["retry-after"]);
      } finally {
        await app.close();
      }
    },
  );
});

test("assistant tool error marks the run as failed", async () => {
  const state = createState();
  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    db: createDatabaseDouble(state),
    getOpencodeClient: async () => ({
      async getSession() {
        return {
          title: "Timeout strategy",
        };
      },
      async prompt() {
        return {
          info: {
            cost: 0,
            error: {
              data: {
                isRetryable: false,
                message: "Script timed out after 1s",
              },
              name: "APIError",
            },
            id: "assistant-script-timeout",
            mode: "chat",
            modelID: "gpt-5",
            parentID: "run-script-timeout",
            path: { cwd: "/tmp", root: "/tmp" },
            providerID: "openai",
            role: "assistant",
            sessionID: "session-script-timeout",
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
              callID: "call-script-timeout",
              id: "tool-script-timeout",
              messageID: "assistant-script-timeout",
              sessionID: "session-script-timeout",
              state: {
                input: {
                  command:
                    "uv run --project agent/scripts python -m agent_invest_scripts.test_fixtures.sleep --seconds 30",
                },
                metadata: {},
                output: "Script timed out after 1s",
                status: "completed",
                time: { end: Date.now(), start: Date.now() },
                title: "Run sleep fixture",
              },
              tool: "bash",
              type: "tool",
            },
          ],
        };
      },
      async subscribeEvents() {
        return createClosedEventStream();
      },
    }),
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
      payload: { text: "Run the long fixture" },
      url: "/users/user-1/strategies/strategy-timeout/messages?wait=1",
    });

    assert.equal(response.statusCode, 200);
    assert.equal(response.json().status, "failed");
    assert.equal(response.json().error, "Script timed out after 1s");
  } finally {
    await app.close();
  }
});

test("GET /runs/{run_id}/events streams persisted and live events until completion", async () => {
  const state = createState();
  const eventStream = createAsyncEventStream();
  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    db: createDatabaseDouble(state),
    getOpencodeClient: async () => ({
      async getSession() {
        return {
          title: "Streaming strategist",
        };
      },
      async prompt({ messageId }) {
        const promptMessageId = messageId ?? "run-stream";

        await new Promise((resolve) => setTimeout(resolve, 10));
        eventStream.emit(
          opencodeEvent("message.updated", {
            info: {
              agent: "main",
              id: promptMessageId,
              model: { modelID: "gpt-5", providerID: "openai" },
              role: "user",
              sessionID: "session-stream",
              time: { created: Date.now() },
            },
          }),
        );

        await new Promise((resolve) => setTimeout(resolve, 10));
        eventStream.emit(
          opencodeEvent("message.updated", {
            info: {
              cost: 0,
              id: "assistant-stream",
              mode: "chat",
              modelID: "gpt-5",
              parentID: promptMessageId,
              path: { cwd: "/tmp", root: "/tmp" },
              providerID: "openai",
              role: "assistant",
              sessionID: "session-stream",
              time: { completed: Date.now(), created: Date.now() },
              tokens: {
                cache: { read: 0, write: 0 },
                input: 0,
                output: 0,
                reasoning: 0,
              },
            },
          }),
        );

        await new Promise((resolve) => setTimeout(resolve, 10));
        eventStream.emit(
          opencodeEvent("message.part.updated", {
            part: {
              id: "part-stream",
              messageID: "assistant-stream",
              sessionID: "session-stream",
              text: "Streaming reply.",
              type: "text",
            },
          }),
        );

        await new Promise((resolve) => setTimeout(resolve, 10));
        eventStream.emit(
          opencodeEvent("session.idle", {
            sessionID: "session-stream",
          }),
        );
        eventStream.end();

        return {
          info: {
            cost: 0,
            id: "assistant-stream",
            mode: "chat",
            modelID: "gpt-5",
            parentID: promptMessageId,
            path: { cwd: "/tmp", root: "/tmp" },
            providerID: "openai",
            role: "assistant",
            sessionID: "session-stream",
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
              id: "text-stream",
              messageID: "assistant-stream",
              sessionID: "session-stream",
              text: "Streaming reply.",
              type: "text",
            },
          ],
        };
      },
      async subscribeEvents(options) {
        return eventStream.stream(options?.signal);
      },
    }),
    getSessionId: async (strategyId) => {
      const strategy = state.strategies.get(strategyId);

      assert.ok(strategy);
      strategy.opencodeSessionId ||= "session-stream";

      return strategy.opencodeSessionId;
    },
  });

  try {
    const createResponse = await app.inject({
      method: "POST",
      payload: { text: "Start streaming" },
      url: "/users/user-1/strategies/strategy-stream/messages",
    });

    assert.equal(createResponse.statusCode, 202);

    const { run_id: runId } = createResponse.json() as { run_id: string };
    const eventsResponse = await app.inject({
      method: "GET",
      url: `/users/user-1/strategies/strategy-stream/runs/${runId}/events`,
    });

    assert.equal(eventsResponse.statusCode, 200);
    assert.match(
      String(eventsResponse.headers["content-type"]),
      /text\/event-stream/,
    );

    const events = parseSseEvents(eventsResponse.body);

    assert.deepEqual(
      events.map(({ event, id }) => ({ event, id })),
      [
        { event: "run.started", id: 1 },
        { event: "message.updated", id: 2 },
        { event: "message.updated", id: 3 },
        { event: "message.part.updated", id: 4 },
        { event: "session.idle", id: 5 },
        { event: "run.completed", id: 6 },
      ],
    );
    assert.deepEqual(events.at(-1)?.data, {
      message_id: "assistant-stream",
      reply: "Streaming reply.",
      session_id: "session-stream",
    });
  } finally {
    await app.close();
  }
});

test("GET /runs/{run_id}/events replays missed events from Last-Event-ID and resumes live streaming", async () => {
  const state = createState();
  const eventStream = createAsyncEventStream();
  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    db: createDatabaseDouble(state),
    getOpencodeClient: async () => ({
      async getSession() {
        return {
          title: "Replay strategist",
        };
      },
      async prompt({ messageId }) {
        const promptMessageId = messageId ?? "run-replay";

        await new Promise((resolve) => setTimeout(resolve, 5));
        eventStream.emit(
          opencodeEvent("message.updated", {
            info: {
              agent: "main",
              id: promptMessageId,
              model: { modelID: "gpt-5", providerID: "openai" },
              role: "user",
              sessionID: "session-replay",
              time: { created: Date.now() },
            },
          }),
        );

        await new Promise((resolve) => setTimeout(resolve, 10));
        eventStream.emit(
          opencodeEvent("message.updated", {
            info: {
              cost: 0,
              id: "assistant-replay",
              mode: "chat",
              modelID: "gpt-5",
              parentID: promptMessageId,
              path: { cwd: "/tmp", root: "/tmp" },
              providerID: "openai",
              role: "assistant",
              sessionID: "session-replay",
              time: { completed: Date.now(), created: Date.now() },
              tokens: {
                cache: { read: 0, write: 0 },
                input: 0,
                output: 0,
                reasoning: 0,
              },
            },
          }),
        );

        await new Promise((resolve) => setTimeout(resolve, 25));
        eventStream.emit(
          opencodeEvent("message.part.updated", {
            part: {
              id: "part-replay",
              messageID: "assistant-replay",
              sessionID: "session-replay",
              text: "Replay reply.",
              type: "text",
            },
          }),
        );

        await new Promise((resolve) => setTimeout(resolve, 10));
        eventStream.emit(
          opencodeEvent("session.idle", {
            sessionID: "session-replay",
          }),
        );
        eventStream.end();

        return {
          info: {
            cost: 0,
            id: "assistant-replay",
            mode: "chat",
            modelID: "gpt-5",
            parentID: promptMessageId,
            path: { cwd: "/tmp", root: "/tmp" },
            providerID: "openai",
            role: "assistant",
            sessionID: "session-replay",
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
              id: "text-replay",
              messageID: "assistant-replay",
              sessionID: "session-replay",
              text: "Replay reply.",
              type: "text",
            },
          ],
        };
      },
      async subscribeEvents(options) {
        return eventStream.stream(options?.signal);
      },
    }),
    getSessionId: async (strategyId) => {
      const strategy = state.strategies.get(strategyId);

      assert.ok(strategy);
      strategy.opencodeSessionId ||= "session-replay";

      return strategy.opencodeSessionId;
    },
  });

  try {
    const createResponse = await app.inject({
      method: "POST",
      payload: { text: "Start replayable stream" },
      url: "/users/user-1/strategies/strategy-replay/messages",
    });

    assert.equal(createResponse.statusCode, 202);

    const { run_id: runId } = createResponse.json() as { run_id: string };

    await new Promise((resolve) => setTimeout(resolve, 20));

    const eventsResponse = await app.inject({
      headers: {
        "last-event-id": "2",
      },
      method: "GET",
      url: `/users/user-1/strategies/strategy-replay/runs/${runId}/events`,
    });

    assert.equal(eventsResponse.statusCode, 200);

    const events = parseSseEvents(eventsResponse.body);

    assert.deepEqual(
      events.map(({ event, id }) => ({ event, id })),
      [
        { event: "message.updated", id: 3 },
        { event: "message.part.updated", id: 4 },
        { event: "session.idle", id: 5 },
        { event: "run.completed", id: 6 },
      ],
    );
    assert.deepEqual(events.at(-1)?.data, {
      message_id: "assistant-replay",
      reply: "Replay reply.",
      session_id: "session-replay",
    });
  } finally {
    await app.close();
  }
});
