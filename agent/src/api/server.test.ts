import assert from "node:assert/strict";
import test from "node:test";
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
};

type EventState = {
  seq: number;
  type: string;
  payload: Record<string, unknown>;
};

function createState() {
  return {
    users: new Set<string>(),
    strategies: new Map<string, StrategyState>(),
    runs: new Map<string, RunState>(),
    events: new Map<string, EventState[]>(),
  };
}

function createDatabaseDouble(state: ReturnType<typeof createState>) {
  return {
    async query<TRow extends QueryResultRow = QueryResultRow>(
      text: string,
      values: unknown[] = [],
    ): Promise<QueryResult<TRow>> {
      if (text.startsWith("INSERT INTO users")) {
        state.users.add(String(values[0]));
        return { rowCount: 1, rows: [] as TRow[] };
      }

      if (text.startsWith("INSERT INTO strategies")) {
        const strategyId = String(values[0]);

        if (!state.strategies.has(strategyId)) {
          state.strategies.set(strategyId, {
            userId: String(values[1]),
            opencodeSessionId: String(values[2]),
            title: String(values[3]),
            lastUsedAt: new Date().toISOString(),
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
          strategyId: String(values[1]),
          status: String(values[2]),
          startedAt: new Date().toISOString(),
          endedAt: null,
          exitCode: null,
        });

        return { rowCount: 1, rows: [] as TRow[] };
      }

      if (
        text ===
        "INSERT INTO events (run_id, seq, type, payload) VALUES ($1, $2, $3, $4::jsonb)"
      ) {
        const runId = String(values[0]);
        const payload = JSON.parse(String(values[3])) as Record<
          string,
          unknown
        >;
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
          .filter(
            ({ type }) => type === "run.completed" || type === "run.failed",
          )
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

      throw new Error(`Unexpected query: ${text}`);
    },
  };
}

test("POST /messages?wait returns the completed run and auto-creates the strategy", async () => {
  const state = createState();
  const app = buildServer({
    buildSystemPrompt: async () => "system prompt",
    db: createDatabaseDouble(state),
    getOpencodeTurnClient: async () => ({
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
            time: { created: Date.now(), completed: Date.now() },
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
    getOpencodeTurnClient: async () => ({
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
            time: { created: Date.now(), completed: Date.now() },
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
