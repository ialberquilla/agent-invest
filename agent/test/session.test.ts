import assert from "node:assert/strict";
import test from "node:test";

import {
  DEFAULT_OPENCODE_MODEL,
  createSessionManager,
  resolveOpencodeModel,
  type DatabaseClient,
  type SessionClient,
} from "../src/agent/session";
import type { readStrategySession } from "../src/db/repositories/strategies";

type StrategyState = {
  opencodeSessionId: string | null;
  title: string;
};

function createDbDouble() {
  let transaction = Promise.resolve();
  let transactionCalls = 0;

  const db = {
    async transaction<T>(callback: (tx: DatabaseClient) => Promise<T>) {
      transactionCalls += 1;
      const run = transaction.then(() => callback({} as DatabaseClient));
      transaction = run.then(
        () => undefined,
        () => undefined,
      );
      return run;
    },
  } as DatabaseClient;

  return { db, getTransactionCalls: () => transactionCalls };
}

function createRepositoryDoubles(state: StrategyState) {
  const lockRequests: boolean[] = [];
  const read: typeof readStrategySession = async (
    _strategyId,
    options = {},
  ) => {
    lockRequests.push(options.lockForUpdate ?? false);
    return state;
  };
  const update = async (_strategyId: string, opencodeSessionId: string) => {
    state.opencodeSessionId = opencodeSessionId;
    return 1;
  };

  return { lockRequests, read, update };
}

test("getOrCreateSession creates and persists a session on first use", async () => {
  const state: StrategyState = {
    opencodeSessionId: null,
    title: "Trend strategy",
  };
  const repositories = createRepositoryDoubles(state);
  const createdTitles: string[] = [];
  const sessionClient: SessionClient = {
    async createSession(title) {
      createdTitles.push(title);
      return "session-123";
    },
  };
  const manager = createSessionManager({
    getOpencodeClient: async () => sessionClient,
    db: createDbDouble().db,
    readStrategySession: repositories.read,
    updateStrategySession: repositories.update,
  });

  const sessionId = await manager.getOrCreateSession("strategy-1");

  assert.equal(sessionId, "session-123");
  assert.deepEqual(createdTitles, ["Trend strategy"]);
  assert.equal(state.opencodeSessionId, "session-123");
  assert.deepEqual(repositories.lockRequests, [true]);
});

test("getOrCreateSession reuses the persisted session id", async () => {
  const state: StrategyState = {
    opencodeSessionId: "session-456",
    title: "Mean reversion",
  };
  const repositories = createRepositoryDoubles(state);
  let createCalls = 0;
  const sessionClient: SessionClient = {
    async createSession() {
      createCalls += 1;
      return "session-new";
    },
  };
  const manager = createSessionManager({
    getOpencodeClient: async () => sessionClient,
    db: createDbDouble().db,
    readStrategySession: repositories.read,
    updateStrategySession: repositories.update,
  });

  const sessionId = await manager.getOrCreateSession("strategy-2");

  assert.equal(sessionId, "session-456");
  assert.equal(createCalls, 0);
  assert.equal(state.opencodeSessionId, "session-456");
  assert.deepEqual(repositories.lockRequests, [true]);
});

test("getOrCreateSession serializes concurrent creation for a strategy", async () => {
  const state: StrategyState = {
    opencodeSessionId: null,
    title: "Concurrent strategy",
  };
  const repositories = createRepositoryDoubles(state);
  let createCalls = 0;
  const sessionClient: SessionClient = {
    async createSession() {
      createCalls += 1;
      return "session-concurrent";
    },
  };
  const manager = createSessionManager({
    getOpencodeClient: async () => sessionClient,
    db: createDbDouble().db,
    readStrategySession: repositories.read,
    updateStrategySession: repositories.update,
  });

  const [firstSessionId, secondSessionId] = await Promise.all([
    manager.getOrCreateSession("strategy-3"),
    manager.getOrCreateSession("strategy-3"),
  ]);

  assert.equal(firstSessionId, "session-concurrent");
  assert.equal(secondSessionId, "session-concurrent");
  assert.equal(createCalls, 1);
  assert.equal(state.opencodeSessionId, "session-concurrent");
  assert.deepEqual(repositories.lockRequests, [true, true]);
});

test("getOrCreateSession reuses caller transaction without requesting a row lock", async () => {
  const state: StrategyState = {
    opencodeSessionId: null,
    title: "Nested transaction strategy",
  };
  const repositories = createRepositoryDoubles(state);
  const db = createDbDouble();
  const manager = createSessionManager({
    getOpencodeClient: async () => ({
      async createSession() {
        return "session-nested";
      },
    }),
    db: db.db,
    readStrategySession: repositories.read,
    updateStrategySession: repositories.update,
  });

  const sessionId = await manager.getOrCreateSession(
    "strategy-4",
    {} as DatabaseClient,
  );

  assert.equal(sessionId, "session-nested");
  assert.equal(db.getTransactionCalls(), 0);
  assert.deepEqual(repositories.lockRequests, [false]);
});

test("getOrCreateSession reports missing strategies", async () => {
  const db = createDbDouble();
  const manager = createSessionManager({
    db: db.db,
    readStrategySession: async () => null,
  });

  await assert.rejects(
    () => manager.getOrCreateSession("missing-strategy"),
    /Strategy not found: missing-strategy/,
  );
  assert.equal(db.getTransactionCalls(), 1);
});

test("resolveOpencodeModel prefers env and falls back to the default", () => {
  assert.equal(
    resolveOpencodeModel({ OPENCODE_MODEL: "anthropic/claude-sonnet-4" }),
    "anthropic/claude-sonnet-4",
  );
  assert.equal(resolveOpencodeModel({}), DEFAULT_OPENCODE_MODEL);
});
