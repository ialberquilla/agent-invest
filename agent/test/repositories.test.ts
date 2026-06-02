import assert from "node:assert/strict";
import test from "node:test";

import {
  appendEvent,
  subscribeAgentEvents,
} from "../src/db/repositories/agent-events";
import { createArtifact } from "../src/db/repositories/artifacts";
import { createRequest, storeResult } from "../src/db/repositories/backtests";
import {
  appendMessage,
  createThread,
} from "../src/db/repositories/conversations";
import { markRunCompleted, markRunFailed } from "../src/db/repositories/runs";
import { insertMandate } from "../src/db/repositories/strategy-mandates";
import {
  bindVaultToMandate,
  insertVault,
} from "../src/db/repositories/vaults";
import {
  ensureStrategy,
  readStrategySession,
  updateStrategySession,
  updateStrategyTitleIfBlank,
} from "../src/db/repositories/strategies";
import { ensureUser } from "../src/db/repositories/users";

type InsertCall = {
  table: unknown;
  values: unknown;
  conflictIgnored: boolean;
};

type UpdateCall = {
  table: unknown;
  set: Record<string, unknown>;
  where: unknown;
};

function createDbDouble(options: { selectRows?: unknown[] } = {}) {
  const insertCalls: InsertCall[] = [];
  const lockModes: string[] = [];
  const updateCalls: UpdateCall[] = [];

  const db = {
    insert(table: unknown) {
      const call: InsertCall = {
        conflictIgnored: false,
        table,
        values: undefined,
      };
      insertCalls.push(call);

      return {
        values(values: unknown) {
          call.values = values;
          return {
            onConflictDoNothing() {
              call.conflictIgnored = true;
              return {
                async returning() {
                  return [values];
                },
                then<TResult1 = undefined, TResult2 = never>(
                  onfulfilled?:
                    | ((value: undefined) => TResult1 | PromiseLike<TResult1>)
                    | null,
                  onrejected?:
                    | ((reason: unknown) => TResult2 | PromiseLike<TResult2>)
                    | null,
                ) {
                  return Promise.resolve(undefined).then(
                    onfulfilled,
                    onrejected,
                  );
                },
              };
            },
            async returning() {
              return [values];
            },
          };
        },
      };
    },
    select() {
      return {
        from() {
          return {
            where() {
              const rows = options.selectRows ?? [];
              return {
                async for(mode: string) {
                  lockModes.push(mode);
                  return rows;
                },
                then<TResult1 = unknown[], TResult2 = never>(
                  onfulfilled?:
                    | ((value: unknown[]) => TResult1 | PromiseLike<TResult1>)
                    | null,
                  onrejected?:
                    | ((reason: unknown) => TResult2 | PromiseLike<TResult2>)
                    | null,
                ) {
                  return Promise.resolve(rows).then(onfulfilled, onrejected);
                },
              };
            },
          };
        },
      };
    },
    update(table: unknown) {
      return {
        set(values: Record<string, unknown>) {
          const call: UpdateCall = { set: values, table, where: undefined };
          updateCalls.push(call);

          return {
            async where(where: unknown) {
              call.where = where;
              return { rowCount: 1 };
            },
          };
        },
      };
    },
  };

  return { db: db as never, insertCalls, lockModes, updateCalls };
}

test("ensureUser inserts with conflict ignored", async () => {
  const { db, insertCalls } = createDbDouble();

  await ensureUser("user-1", db);

  assert.equal(insertCalls.length, 1);
  assert.deepEqual(insertCalls[0]?.values, { userId: "user-1" });
  assert.equal(insertCalls[0]?.conflictIgnored, true);
});

test("ensureStrategy creates owning user and strategy defaults", async () => {
  const { db, insertCalls } = createDbDouble({
    selectRows: [{ userId: "user-1" }],
  });

  const ownership = await ensureStrategy("user-1", "strategy-1", db);

  assert.deepEqual(ownership, { userId: "user-1" });
  assert.equal(insertCalls.length, 2);
  assert.deepEqual(insertCalls[0]?.values, { userId: "user-1" });
  assert.deepEqual(insertCalls[1]?.values, {
    opencodeSessionId: "",
    strategyId: "strategy-1",
    title: "",
    userId: "user-1",
  });
});

test("updateStrategyTitleIfBlank skips blank titles", async () => {
  const { db, updateCalls } = createDbDouble();

  await updateStrategyTitleIfBlank("strategy-1", "   ", db);

  assert.equal(updateCalls.length, 0);
});

test("updateStrategyTitleIfBlank trims title before persisting", async () => {
  const { db, updateCalls } = createDbDouble();

  await updateStrategyTitleIfBlank("strategy-1", "  Momentum  ", db);

  assert.equal(updateCalls.length, 1);
  assert.equal(updateCalls[0]?.set.title, "Momentum");
});

test("readStrategySession returns strategy session without locking by default", async () => {
  const { db, lockModes } = createDbDouble({
    selectRows: [{ opencodeSessionId: "session-1", title: "Momentum" }],
  });

  const strategy = await readStrategySession("strategy-1", {}, db);

  assert.deepEqual(strategy, {
    opencodeSessionId: "session-1",
    title: "Momentum",
  });
  assert.deepEqual(lockModes, []);
});

test("readStrategySession requests a row lock when asked", async () => {
  const { db, lockModes } = createDbDouble({
    selectRows: [{ opencodeSessionId: "session-1", title: "Momentum" }],
  });

  const strategy = await readStrategySession(
    "strategy-1",
    { lockForUpdate: true },
    db,
  );

  assert.equal(strategy?.opencodeSessionId, "session-1");
  assert.deepEqual(lockModes, ["update"]);
});

test("updateStrategySession reports affected row count", async () => {
  const { db, updateCalls } = createDbDouble();

  const updatedRows = await updateStrategySession(
    "strategy-1",
    "session-1",
    db,
  );

  assert.equal(updatedRows, 1);
  assert.equal(updateCalls.length, 1);
  assert.deepEqual(updateCalls[0]?.set, { opencodeSessionId: "session-1" });
});

test("run terminal updates preserve completed and failed payloads", async () => {
  const { db, updateCalls } = createDbDouble();

  await markRunCompleted("run-1", "reply", db);
  await markRunFailed("run-2", "boom", db);

  assert.equal(updateCalls.length, 2);
  assert.deepEqual(
    {
      error: updateCalls[0]?.set.error,
      exitCode: updateCalls[0]?.set.exitCode,
      reply: updateCalls[0]?.set.reply,
      status: updateCalls[0]?.set.status,
    },
    { error: null, exitCode: 0, reply: "reply", status: "completed" },
  );
  assert.deepEqual(
    {
      error: updateCalls[1]?.set.error,
      exitCode: updateCalls[1]?.set.exitCode,
      reply: updateCalls[1]?.set.reply,
      status: updateCalls[1]?.set.status,
    },
    { error: "boom", exitCode: 1, reply: null, status: "failed" },
  );
});

test("createThread inserts a conversation thread and returns its id", async () => {
  const { db, insertCalls } = createDbDouble();

  const thread = await createThread(
    {
      provider: "opencode",
      providerSessionId: "session-1",
      strategyId: "strategy-1",
      threadId: "thread-1",
      title: "Plan",
      userId: "user-1",
    },
    db,
  );

  assert.equal(insertCalls.length, 1);
  assert.equal(thread.threadId, "thread-1");
  assert.deepEqual(insertCalls[0]?.values, {
    provider: "opencode",
    providerSessionId: "session-1",
    strategyId: "strategy-1",
    threadId: "thread-1",
    title: "Plan",
    userId: "user-1",
  });
});

test("appendMessage inserts a message and touches its thread", async () => {
  const { db, insertCalls, updateCalls } = createDbDouble();

  const message = await appendMessage(
    {
      content: "hello",
      messageId: "message-1",
      metadata: { source: "test" },
      role: "user",
      threadId: "thread-1",
    },
    db,
  );

  assert.equal(insertCalls.length, 1);
  assert.equal(updateCalls.length, 1);
  assert.equal(message.messageId, "message-1");
  assert.deepEqual(insertCalls[0]?.values, {
    content: "hello",
    messageId: "message-1",
    metadata: { source: "test" },
    role: "user",
    threadId: "thread-1",
  });
  assert.ok(updateCalls[0]?.set.updatedAt);
});

test("appendEvent inserts an agent event", async () => {
  const { db, insertCalls } = createDbDouble();

  const event = await appendEvent(
    {
      eventId: "event-1",
      eventType: "tool_call",
      payload: { tool: "read" },
      threadId: "thread-1",
    },
    db,
  );

  assert.equal(insertCalls.length, 1);
  assert.equal(event.eventId, "event-1");
  assert.deepEqual(insertCalls[0]?.values, {
    eventId: "event-1",
    eventType: "tool_call",
    payload: { tool: "read" },
    threadId: "thread-1",
  });
});

test("subscribeAgentEvents notifies SSE listeners for the matching run only", async () => {
  const { db } = createDbDouble();
  const received: string[] = [];
  const unsubscribe = subscribeAgentEvents("run-A", (event) => {
    received.push(event.eventId);
  });

  await appendEvent(
    { eventId: "ev-a", eventType: "stage.started", runId: "run-A" },
    db,
  );
  await appendEvent(
    { eventId: "ev-b", eventType: "stage.started", runId: "run-B" },
    db,
  );
  await appendEvent(
    { eventId: "ev-c", eventType: "stage.completed", runId: "run-A" },
    db,
  );

  assert.deepEqual(received, ["ev-a", "ev-c"]);

  unsubscribe();
  await appendEvent(
    { eventId: "ev-d", eventType: "stage.started", runId: "run-A" },
    db,
  );
  assert.deepEqual(received, ["ev-a", "ev-c"]);
});

test("createRequest and storeResult persist backtest rows", async () => {
  const { db, insertCalls } = createDbDouble();

  const request = await createRequest(
    {
      allocation: { BTC: 1 },
      backtestId: "backtest-1",
      initialCapitalUsd: "1000",
      rebalance: "monthly",
      runId: "run-1",
      strategyId: "strategy-1",
    },
    db,
  );
  const result = await storeResult(
    {
      backtestId: "backtest-1",
      cagr: "0.12",
      report: { ok: true },
      runId: "run-1",
    },
    db,
  );

  assert.equal(insertCalls.length, 2);
  assert.equal(request.backtestId, "backtest-1");
  assert.equal(result.backtestId, "backtest-1");
  assert.deepEqual(insertCalls[0]?.values, {
    allocation: { BTC: 1 },
    backtestId: "backtest-1",
    initialCapitalUsd: "1000",
    rebalance: "monthly",
    runId: "run-1",
    strategyId: "strategy-1",
  });
  assert.deepEqual(insertCalls[1]?.values, {
    backtestId: "backtest-1",
    cagr: "0.12",
    report: { ok: true },
    runId: "run-1",
  });
});

test("insertMandate promotes index columns and stores the full spec", async () => {
  const { db, insertCalls } = createDbDouble();

  const mandate = {
    mandate_id: "mandate-1",
    run_id: "run-1",
    version: 1,
    created_at: "2026-06-01T00:00:00.000Z",
    template_id: "synthetic_long_allocation",
    select_top: 5,
    weighting: "equal",
    objective: "growth",
    rebalance_frequency: "monthly",
    universe_hints: { top_n: 10, exclude_stablecoins: true, exclude_wrapped: true },
    coin_ids: ["bitcoin", "ethereum"],
    dynamic_universe: false,
    constraints: { max_weight_per_asset: 0.3, max_cash_weight: 0.2, max_drawdown: 0.4 },
    allowed_sides: "long_only",
    initial_target_allocation: [{ coin_id: "bitcoin", weight: 1 }],
    status: "pending",
  } as never;

  await insertMandate(mandate, db);

  assert.equal(insertCalls.length, 1);
  assert.deepEqual(insertCalls[0]?.values, {
    mandateId: "mandate-1",
    runId: "run-1",
    version: 1,
    status: "pending",
    templateId: "synthetic_long_allocation",
    spec: mandate,
  });
});

test("insertVault writes a deployed vault row with conflict ignored", async () => {
  const { db, insertCalls } = createDbDouble();

  await insertVault(
    {
      chainId: 42161,
      vaultAddress: "0xVault",
      mandateId: "mandate-1",
      assetAddress: "0xUSDC",
    },
    db,
  );

  assert.equal(insertCalls.length, 1);
  assert.equal(insertCalls[0]?.conflictIgnored, true);
  assert.deepEqual(insertCalls[0]?.values, {
    chainId: 42161,
    vaultAddress: "0xVault",
    mandateId: "mandate-1",
    assetAddress: "0xUSDC",
    status: "active",
  });
});

test("bindVaultToMandate inserts the vault and promotes the mandate to active", async () => {
  const { db, insertCalls, updateCalls } = createDbDouble();

  await bindVaultToMandate(
    {
      chainId: 42161,
      vaultAddress: "0xVault",
      mandateId: "mandate-1",
      assetAddress: "0xUSDC",
    },
    db,
  );

  assert.equal(insertCalls.length, 1);
  assert.equal(insertCalls[0]?.values && (insertCalls[0].values as { mandateId: string }).mandateId, "mandate-1");
  assert.equal(updateCalls.length, 1);
  assert.equal(updateCalls[0]?.set.status, "active");
});

test("createArtifact inserts an artifact row", async () => {
  const { db, insertCalls } = createDbDouble();

  const artifact = await createArtifact(
    {
      artifactId: "artifact-1",
      contentType: "application/json",
      kind: "backtest_report",
      metadata: { source: "test" },
      runId: "run-1",
      sizeBytes: 42,
      storageKey: "runs/run-1/report.json",
    },
    db,
  );

  assert.equal(insertCalls.length, 1);
  assert.equal(artifact.artifactId, "artifact-1");
  assert.deepEqual(insertCalls[0]?.values, {
    artifactId: "artifact-1",
    contentType: "application/json",
    kind: "backtest_report",
    metadata: { source: "test" },
    runId: "run-1",
    sizeBytes: 42,
    storageKey: "runs/run-1/report.json",
  });
});
