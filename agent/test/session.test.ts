import assert from "node:assert/strict";
import test from "node:test";
import type { QueryResultRow } from "pg";

import {
  DEFAULT_OPENCODE_MODEL,
  createSessionManager,
  resolveOpencodeModel,
  type DatabaseClient,
  type DatabasePool,
  type SessionClient,
} from "../src/agent/session";

type StrategyState = {
  opencode_session_id: string | null;
  title: string;
};

function createPoolDouble(state: StrategyState): DatabasePool {
  return {
    async connect() {
      const client: DatabaseClient = {
        async query<TRow extends QueryResultRow = QueryResultRow>(
          text: string,
          values = [],
        ) {
          if (text === "BEGIN" || text === "COMMIT" || text === "ROLLBACK") {
            return { rowCount: null, rows: [] as TRow[] };
          }

          if (text.includes("SELECT title, opencode_session_id")) {
            return {
              rowCount: 1,
              rows: [
                {
                  opencode_session_id: state.opencode_session_id,
                  title: state.title,
                },
              ] as unknown as TRow[],
            };
          }

          if (text.includes("UPDATE strategies")) {
            state.opencode_session_id = String(values[1]);
            return { rowCount: 1, rows: [] as TRow[] };
          }

          throw new Error(`Unexpected query: ${text}`);
        },
        release() {},
      };

      return client;
    },
  };
}

test("getOrCreateSession creates and persists a session on first use", async () => {
  const state: StrategyState = {
    opencode_session_id: "",
    title: "Trend strategy",
  };
  const createdTitles: string[] = [];
  const sessionClient: SessionClient = {
    async createSession(title) {
      createdTitles.push(title);
      return "session-123";
    },
  };
  const manager = createSessionManager({
    getOpencodeClient: async () => sessionClient,
    pool: createPoolDouble(state),
  });

  const sessionId = await manager.getOrCreateSession("strategy-1");

  assert.equal(sessionId, "session-123");
  assert.deepEqual(createdTitles, ["Trend strategy"]);
  assert.equal(state.opencode_session_id, "session-123");
});

test("getOrCreateSession reuses the persisted session id", async () => {
  const state: StrategyState = {
    opencode_session_id: "session-456",
    title: "Mean reversion",
  };
  let createCalls = 0;
  const sessionClient: SessionClient = {
    async createSession() {
      createCalls += 1;
      return "session-new";
    },
  };
  const manager = createSessionManager({
    getOpencodeClient: async () => sessionClient,
    pool: createPoolDouble(state),
  });

  const sessionId = await manager.getOrCreateSession("strategy-2");

  assert.equal(sessionId, "session-456");
  assert.equal(createCalls, 0);
  assert.equal(state.opencode_session_id, "session-456");
});

test("resolveOpencodeModel prefers env and falls back to the default", () => {
  assert.equal(
    resolveOpencodeModel({ OPENCODE_MODEL: "anthropic/claude-sonnet-4" }),
    "anthropic/claude-sonnet-4",
  );
  assert.equal(resolveOpencodeModel({}), DEFAULT_OPENCODE_MODEL);
});
