import { randomUUID } from "node:crypto";
import Fastify, { type FastifyReply, type FastifyRequest } from "fastify";
import pino from "pino";
import { fileURLToPath } from "node:url";
import type { QueryResultRow } from "pg";

import { buildSystemPrompt as defaultBuildSystemPrompt } from "../agent/prompt.js";
import {
  createOpencodeTurnClient,
  getOrCreateSession,
  type OpencodePromptResult,
  type OpencodeTurnClient,
} from "../agent/session.js";
import { pg } from "../db/client.js";

const NOT_IMPLEMENTED_RESPONSE = {
  error: {
    code: "not_implemented",
    message: "Not implemented",
  },
} as const;

const POLL_INTERVAL_MS = 100;
const DEFAULT_TURN_LOCK_TIMEOUT_MS = 5_000;

type QueryResult<TRow extends QueryResultRow> = {
  rowCount: number | null;
  rows: TRow[];
};

type DatabaseQueryable = {
  query<TRow extends QueryResultRow = QueryResultRow>(
    text: string,
    values?: unknown[],
  ): Promise<QueryResult<TRow>>;
};

type DatabaseClient = DatabaseQueryable & {
  release(): void;
};

type DatabasePool = DatabaseQueryable & {
  connect(): Promise<DatabaseClient>;
};

type RouteParams = {
  user_id: string;
  strategy_id: string;
};

type RunRouteParams = RouteParams & {
  run_id: string;
};

type WaitQuery = {
  wait?: string;
};

type MessageBody = {
  text: string;
};

type StrategyOwnershipRow = {
  user_id: string;
};

type RunRow = {
  run_id: string;
  status: string;
  started_at: Date | string;
  ended_at: Date | string | null;
  exit_code: number | null;
};

type EventRow = {
  type: string;
  payload: RunTerminalPayload | null;
};

type NextSeqRow = {
  next_seq: number | string;
};

type PersistedRunEventRow = {
  seq: number | string;
  type: string;
  payload: Record<string, unknown> | null;
};

type PersistedRunEvent = {
  runId: string;
  seq: number;
  type: string;
  payload: Record<string, unknown>;
};

type RunEventSubscription = {
  next(): Promise<PersistedRunEvent | undefined>;
  close(): void;
};

type RunEventListener = (event: PersistedRunEvent) => void;

type RunTerminalPayload = {
  reply?: string;
  error?: string;
  session_id?: string;
  message_id?: string;
};

type RunState = {
  runId: string;
  status: string;
  startedAt: Date | string;
  endedAt: Date | string | null;
  exitCode: number | null;
  reply?: string;
  error?: string;
};

type ServerDependencies = {
  db?: DatabasePool;
  buildSystemPrompt?: typeof defaultBuildSystemPrompt;
  getSessionId?: (strategyId: string) => Promise<string>;
  getOpencodeTurnClient?: () => Promise<OpencodeTurnClient>;
  turnLockTimeoutMs?: number;
};

class RunEventStore {
  private readonly appendQueues = new Map<string, Promise<void>>();
  private readonly listeners = new Map<string, Set<RunEventListener>>();
  private readonly nextSeqByRun = new Map<string, number>();

  async append(
    db: DatabaseQueryable,
    runId: string,
    type: string,
    payload: Record<string, unknown>,
  ) {
    let persisted: PersistedRunEvent | undefined;
    const previous = this.appendQueues.get(runId) ?? Promise.resolve();
    const next = previous.then(async () => {
      const cachedNextSeq = this.nextSeqByRun.get(runId);
      const seq =
        cachedNextSeq ?? (await this.loadNextSequenceNumber(db, runId));

      await db.query(
        "INSERT INTO events (run_id, seq, type, payload) VALUES ($1, $2, $3, $4::jsonb)",
        [runId, seq, type, JSON.stringify(payload)],
      );

      this.nextSeqByRun.set(runId, seq + 1);
      persisted = {
        payload,
        runId,
        seq,
        type,
      };

      for (const listener of this.listeners.get(runId) ?? []) {
        listener(persisted);
      }

      if (isTerminalEventType(type)) {
        this.nextSeqByRun.delete(runId);
      }
    });

    const queued = next.catch(() => undefined);

    void queued.finally(() => {
      if (this.appendQueues.get(runId) === queued) {
        this.appendQueues.delete(runId);
      }
    });

    this.appendQueues.set(runId, queued);

    await next;

    return persisted as PersistedRunEvent;
  }

  subscribe(runId: string): RunEventSubscription {
    const listeners = this.listeners.get(runId) ?? new Set<RunEventListener>();
    const queue: PersistedRunEvent[] = [];
    let resolveNext:
      | ((event: PersistedRunEvent | undefined) => void)
      | undefined;
    let closed = false;

    const listener: RunEventListener = (event) => {
      if (closed) {
        return;
      }

      if (resolveNext) {
        const resolve = resolveNext;

        resolveNext = undefined;
        resolve(event);
        return;
      }

      queue.push(event);
    };

    listeners.add(listener);
    this.listeners.set(runId, listeners);

    return {
      close: () => {
        if (closed) {
          return;
        }

        closed = true;
        listeners.delete(listener);

        if (listeners.size === 0) {
          this.listeners.delete(runId);
        }

        if (resolveNext) {
          const resolve = resolveNext;

          resolveNext = undefined;
          resolve(undefined);
        }
      },
      next: async () => {
        if (queue.length > 0) {
          return queue.shift();
        }

        if (closed) {
          return undefined;
        }

        return new Promise((resolve) => {
          resolveNext = resolve;
        });
      },
    };
  }

  private async loadNextSequenceNumber(db: DatabaseQueryable, runId: string) {
    const result = await db.query<NextSeqRow>(
      "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq FROM events WHERE run_id = $1",
      [runId],
    );

    return Number(result.rows[0]?.next_seq ?? 1);
  }
}

class RouteError extends Error {
  constructor(
    readonly statusCode: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

function getPort() {
  const rawPort = process.env.PORT ?? "3000";
  const port = Number.parseInt(rawPort, 10);

  if (!Number.isInteger(port) || port <= 0) {
    throw new Error(`Invalid PORT value: ${rawPort}`);
  }

  return port;
}

function resolveTurnLockTimeoutMs(env: NodeJS.ProcessEnv = process.env) {
  const rawTimeout = env.TURN_LOCK_TIMEOUT_MS?.trim();

  if (!rawTimeout) {
    return DEFAULT_TURN_LOCK_TIMEOUT_MS;
  }

  const timeoutMs = Number.parseInt(rawTimeout, 10);

  if (!Number.isInteger(timeoutMs) || timeoutMs <= 0) {
    throw new Error(`Invalid TURN_LOCK_TIMEOUT_MS value: ${rawTimeout}`);
  }

  return timeoutMs;
}

async function notImplemented(_request: FastifyRequest, reply: FastifyReply) {
  return reply.code(501).send(NOT_IMPLEMENTED_RESPONSE);
}

function errorResponse(code: string, message: string) {
  return {
    error: {
      code,
      message,
    },
  } as const;
}

function getErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

function getPostgresErrorCode(error: unknown) {
  if (typeof error !== "object" || error === null) {
    return undefined;
  }

  const { code } = error as { code?: unknown };

  return typeof code === "string" ? code : undefined;
}

function formatTurnLockTimeoutMessage(timeoutMs: number) {
  return `Timed out after ${timeoutMs}ms waiting for the user turn lock`;
}

function toRouteError(error: unknown, turnLockTimeoutMs: number) {
  if (error instanceof RouteError) {
    return error;
  }

  if (getPostgresErrorCode(error) === "55P03") {
    return new RouteError(
      409,
      "turn_lock_timeout",
      formatTurnLockTimeoutMessage(turnLockTimeoutMs),
    );
  }

  return error;
}

function isAbortError(error: unknown) {
  return error instanceof Error && error.name === "AbortError";
}

function getAssistantErrorMessage(result: OpencodePromptResult) {
  const error = result.info.error;

  if (!error) {
    return undefined;
  }

  const message = error.data?.message;

  return typeof message === "string" && message.trim() ? message : error.name;
}

function extractReplyText(parts: OpencodePromptResult["parts"]) {
  return parts.reduce((reply, part) => {
    if (part.type !== "text" || part.ignored) {
      return reply;
    }

    return `${reply}${part.text}`;
  }, "");
}

function isTerminalStatus(status: string) {
  return status === "completed" || status === "failed";
}

function isTerminalEventType(type: string) {
  return type === "run.completed" || type === "run.failed";
}

function serializeTimestamp(value: Date | string | null) {
  if (!value) {
    return null;
  }

  return value instanceof Date ? value.toISOString() : value;
}

function sleep(milliseconds: number) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function readString(
  record: Record<string, unknown> | undefined,
  key: string,
): string | undefined {
  const value = record?.[key];

  return typeof value === "string" && value.trim() ? value : undefined;
}

function normalizeEventPayload(
  payload: Record<string, unknown> | null | undefined,
) {
  return payload ?? {};
}

function parseLastEventId(rawLastEventId: string | string[] | undefined) {
  if (rawLastEventId === undefined) {
    return 0;
  }

  const lastEventId = Array.isArray(rawLastEventId)
    ? rawLastEventId[rawLastEventId.length - 1]
    : rawLastEventId;
  const normalized = lastEventId.trim();

  if (!/^\d+$/.test(normalized)) {
    throw new RouteError(
      400,
      "invalid_last_event_id",
      "Header 'Last-Event-ID' must be a non-negative integer",
    );
  }

  return Number(normalized);
}

function formatSseEvent(event: PersistedRunEvent) {
  return [
    `id: ${event.seq}`,
    `event: ${event.type}`,
    `data: ${JSON.stringify(event.payload)}`,
    "",
  ].join("\n");
}

function writeSseEvent(reply: FastifyReply, event: PersistedRunEvent) {
  reply.raw.write(`${formatSseEvent(event)}\n`);
}

function beginSseStream(reply: FastifyReply) {
  reply.hijack();
  reply.raw.statusCode = 200;
  reply.raw.setHeader("cache-control", "no-cache, no-transform");
  reply.raw.setHeader("connection", "keep-alive");
  reply.raw.setHeader("content-type", "text/event-stream; charset=utf-8");
  reply.raw.setHeader("x-accel-buffering", "no");
  reply.raw.flushHeaders?.();
}

function createDisconnectPromise(request: FastifyRequest) {
  let disconnected = false;
  let resolveDisconnected: (() => void) | undefined;

  const done = new Promise<void>((resolve) => {
    resolveDisconnected = resolve;
  });
  const onClose = () => {
    disconnected = true;
    resolveDisconnected?.();
  };

  request.raw.once("close", onClose);

  return {
    cleanup() {
      request.raw.off("close", onClose);
    },
    done,
    isDisconnected() {
      return disconnected;
    },
  };
}

async function loadRunEvents(
  db: DatabaseQueryable,
  runId: string,
  lastEventId: number,
): Promise<PersistedRunEvent[]> {
  const result = await db.query<PersistedRunEventRow>(
    [
      "SELECT seq, type, payload",
      "FROM events",
      "WHERE run_id = $1 AND seq > $2",
      "ORDER BY seq ASC",
    ].join(" "),
    [runId, lastEventId],
  );

  return result.rows.map((row) => ({
    payload: normalizeEventPayload(row.payload),
    runId,
    seq: Number(row.seq),
    type: row.type,
  }));
}

function getOpencodeEventDetails(event: unknown) {
  if (!isRecord(event)) {
    return null;
  }

  const type = readString(event, "type");
  const properties = isRecord(event.properties) ? event.properties : undefined;

  if (!type || !properties) {
    return null;
  }

  return { properties, type };
}

function getOpencodeEventSessionId(
  type: string,
  properties: Record<string, unknown>,
) {
  if (type === "message.updated") {
    return readString(
      isRecord(properties.info) ? properties.info : undefined,
      "sessionID",
    );
  }

  if (type === "message.part.updated") {
    return readString(
      isRecord(properties.part) ? properties.part : undefined,
      "sessionID",
    );
  }

  return readString(properties, "sessionID");
}

function getOpencodeEventMessageId(
  type: string,
  properties: Record<string, unknown>,
) {
  if (type === "message.updated") {
    return readString(
      isRecord(properties.info) ? properties.info : undefined,
      "id",
    );
  }

  if (type === "message.part.updated") {
    return readString(
      isRecord(properties.part) ? properties.part : undefined,
      "messageID",
    );
  }

  return readString(properties, "messageID");
}

function getOpencodeEventParentMessageId(
  type: string,
  properties: Record<string, unknown>,
) {
  if (type !== "message.updated") {
    return undefined;
  }

  return readString(
    isRecord(properties.info) ? properties.info : undefined,
    "parentID",
  );
}

function shouldPersistOpencodeEvent(
  event: unknown,
  sessionId: string,
  relatedMessageIds: Set<string>,
) {
  const details = getOpencodeEventDetails(event);

  if (!details) {
    return false;
  }

  const eventSessionId = getOpencodeEventSessionId(
    details.type,
    details.properties,
  );

  if (eventSessionId !== sessionId) {
    return false;
  }

  const messageId = getOpencodeEventMessageId(details.type, details.properties);
  const parentMessageId = getOpencodeEventParentMessageId(
    details.type,
    details.properties,
  );

  if (!messageId) {
    return true;
  }

  if (relatedMessageIds.has(messageId)) {
    return true;
  }

  if (parentMessageId && relatedMessageIds.has(parentMessageId)) {
    relatedMessageIds.add(messageId);
    return true;
  }

  return false;
}

function extractOpencodeEventPayload(event: unknown) {
  const details = getOpencodeEventDetails(event);

  return details
    ? { payload: details.properties, type: details.type }
    : undefined;
}

async function streamOpencodeEvents(options: {
  app: ReturnType<typeof Fastify>;
  db: DatabaseQueryable;
  getOpencodeTurnClient: () => Promise<OpencodeTurnClient>;
  runEvents: RunEventStore;
  runId: string;
  sessionId: string;
}) {
  const { app, db, getOpencodeTurnClient, runEvents, runId, sessionId } =
    options;
  const relatedMessageIds = new Set<string>([runId]);
  const abortController = new AbortController();
  let resolveIdle: (() => void) | undefined;
  const idlePromise = new Promise<void>((resolve) => {
    resolveIdle = resolve;
  });
  const streamTask = (async () => {
    try {
      const opencode = await getOpencodeTurnClient();
      const stream = await opencode.subscribeEvents({
        signal: abortController.signal,
      });

      for await (const globalEvent of stream) {
        const event = globalEvent;

        if (!shouldPersistOpencodeEvent(event, sessionId, relatedMessageIds)) {
          continue;
        }

        const persistedEvent = extractOpencodeEventPayload(event);

        if (!persistedEvent) {
          continue;
        }

        await runEvents.append(
          db,
          runId,
          persistedEvent.type,
          persistedEvent.payload,
        );

        if (persistedEvent.type === "session.idle") {
          resolveIdle?.();
        }
      }
    } catch (error) {
      if (isAbortError(error)) {
        return;
      }

      app.log.warn(
        {
          error: getErrorMessage(error),
          runId,
          sessionId,
        },
        "Failed to forward opencode events for run",
      );
    } finally {
      resolveIdle?.();
    }
  })();

  return {
    async stop() {
      abortController.abort();
      await streamTask;
    },
    waitForIdle(timeoutMs: number) {
      return Promise.race([idlePromise, sleep(timeoutMs)]).then(
        () => undefined,
      );
    },
  };
}

function parseWaitMilliseconds(rawWait: string | undefined) {
  if (rawWait === undefined) {
    return undefined;
  }

  const seconds = Number(rawWait);

  if (!Number.isFinite(seconds) || seconds < 0) {
    throw new RouteError(
      400,
      "invalid_wait",
      "Query parameter 'wait' must be a non-negative number of seconds",
    );
  }

  return Math.round(seconds * 1000);
}

function parseMessageText(body: unknown) {
  if (
    typeof body !== "object" ||
    body === null ||
    typeof (body as Record<string, unknown>).text !== "string"
  ) {
    throw new RouteError(
      400,
      "invalid_body",
      "Request body must include a non-empty 'text' field",
    );
  }

  const text = (body as MessageBody).text.trim();

  if (!text) {
    throw new RouteError(
      400,
      "invalid_body",
      "Request body must include a non-empty 'text' field",
    );
  }

  return text;
}

async function ensureStrategyExists(
  db: DatabaseQueryable,
  userId: string,
  strategyId: string,
) {
  await db.query(
    "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING",
    [userId],
  );
  await db.query(
    [
      "INSERT INTO strategies (strategy_id, user_id, opencode_session_id, title)",
      "VALUES ($1, $2, $3, $4)",
      "ON CONFLICT (strategy_id) DO NOTHING",
    ].join(" "),
    [strategyId, userId, "", ""],
  );

  const result = await db.query<StrategyOwnershipRow>(
    "SELECT user_id FROM strategies WHERE strategy_id = $1",
    [strategyId],
  );
  const strategy = result.rows[0];

  if (!strategy || strategy.user_id !== userId) {
    throw new RouteError(404, "strategy_not_found", "Strategy not found");
  }
}

async function acquireUserTurnClient(
  db: DatabasePool,
  userId: string,
  turnLockTimeoutMs: number,
) {
  const client = await db.connect();
  let inTransaction = false;

  try {
    await client.query("BEGIN");
    inTransaction = true;
    await client.query("SELECT set_config('lock_timeout', $1, true)", [
      `${turnLockTimeoutMs}ms`,
    ]);
    await client.query(
      "SELECT user_id FROM users WHERE user_id = $1 FOR UPDATE",
      [userId],
    );

    return client;
  } catch (error) {
    if (inTransaction) {
      try {
        await client.query("ROLLBACK");
      } catch {
        // Prefer the original lock acquisition error.
      }
    }

    client.release();
    throw toRouteError(error, turnLockTimeoutMs);
  }
}

async function insertRunStart(
  db: DatabaseQueryable,
  runEvents: RunEventStore,
  runId: string,
  strategyId: string,
) {
  await db.query(
    "UPDATE strategies SET last_used_at = NOW() WHERE strategy_id = $1",
    [strategyId],
  );
  await db.query(
    "INSERT INTO runs (run_id, strategy_id, status) VALUES ($1, $2, $3)",
    [runId, strategyId, "running"],
  );
  await runEvents.append(db, runId, "run.started", {});
}

async function markRunCompleted(
  db: DatabaseQueryable,
  runEvents: RunEventStore,
  runId: string,
  sessionId: string,
  messageId: string,
  reply: string,
) {
  await db.query(
    "UPDATE runs SET status = $2, ended_at = NOW(), exit_code = $3 WHERE run_id = $1",
    [runId, "completed", 0],
  );
  await runEvents.append(db, runId, "run.completed", {
    message_id: messageId,
    reply,
    session_id: sessionId,
  });
}

async function markRunFailed(
  db: DatabaseQueryable,
  runEvents: RunEventStore,
  runId: string,
  error: string,
) {
  await db.query(
    "UPDATE runs SET status = $2, ended_at = NOW(), exit_code = $3 WHERE run_id = $1",
    [runId, "failed", 1],
  );
  await runEvents.append(db, runId, "run.failed", { error });
}

async function maybeUpdateStrategyTitle(
  db: DatabaseQueryable,
  strategyId: string,
  title: string,
) {
  const normalizedTitle = title.trim();

  if (!normalizedTitle) {
    return;
  }

  await db.query(
    [
      "UPDATE strategies",
      "SET title = $2",
      "WHERE strategy_id = $1 AND btrim(title) = ''",
    ].join(" "),
    [strategyId, normalizedTitle],
  );
}

async function loadRunState(
  db: DatabaseQueryable,
  userId: string,
  strategyId: string,
  runId: string,
): Promise<RunState | null> {
  const runResult = await db.query<RunRow>(
    [
      "SELECT r.run_id, r.status, r.started_at, r.ended_at, r.exit_code",
      "FROM runs r",
      "JOIN strategies s ON s.strategy_id = r.strategy_id",
      "WHERE s.user_id = $1 AND r.strategy_id = $2 AND r.run_id = $3",
    ].join(" "),
    [userId, strategyId, runId],
  );
  const run = runResult.rows[0];

  if (!run) {
    return null;
  }

  let terminalPayload: RunTerminalPayload | null = null;

  if (isTerminalStatus(run.status)) {
    const eventResult = await db.query<EventRow>(
      [
        "SELECT type, payload",
        "FROM events",
        "WHERE run_id = $1 AND type IN ('run.completed', 'run.failed')",
        "ORDER BY seq DESC",
        "LIMIT 1",
      ].join(" "),
      [runId],
    );
    terminalPayload = eventResult.rows[0]?.payload ?? null;
  }

  return {
    runId: run.run_id,
    status: run.status,
    startedAt: run.started_at,
    endedAt: run.ended_at,
    exitCode: run.exit_code,
    reply: terminalPayload?.reply,
    error: terminalPayload?.error,
  };
}

async function waitForRun(
  db: DatabaseQueryable,
  userId: string,
  strategyId: string,
  runId: string,
  waitMs: number,
) {
  const deadline = Date.now() + waitMs;
  let currentRun = await loadRunState(db, userId, strategyId, runId);

  while (
    currentRun &&
    !isTerminalStatus(currentRun.status) &&
    Date.now() < deadline
  ) {
    await sleep(Math.min(POLL_INTERVAL_MS, deadline - Date.now()));
    currentRun = await loadRunState(db, userId, strategyId, runId);
  }

  return currentRun;
}

function sendRunResponse(reply: FastifyReply, run: RunState) {
  const payload = {
    run_id: run.runId,
    status: run.status,
    started_at: serializeTimestamp(run.startedAt),
    ended_at: serializeTimestamp(run.endedAt),
    exit_code: run.exitCode,
    ...(run.reply !== undefined ? { reply: run.reply } : {}),
    ...(run.error !== undefined ? { error: run.error } : {}),
  };

  if (isTerminalStatus(run.status)) {
    return reply.code(200).send(payload);
  }

  return reply.code(202).send(payload);
}

async function executeRun(options: {
  app: ReturnType<typeof Fastify>;
  buildSystemPrompt: typeof defaultBuildSystemPrompt;
  db: DatabasePool;
  getOpencodeTurnClient: () => Promise<OpencodeTurnClient>;
  getSessionId: (strategyId: string) => Promise<string>;
  runEvents: RunEventStore;
  runId: string;
  strategyId: string;
  text: string;
  turnClient: DatabaseClient;
  userId: string;
}) {
  const {
    app,
    buildSystemPrompt,
    db,
    getOpencodeTurnClient,
    getSessionId,
    runEvents,
    runId,
    strategyId,
    text,
    turnClient,
    userId,
  } = options;
  let inTransaction = true;

  try {
    const systemPrompt = await buildSystemPrompt({ userId, strategyId });
    const sessionId = await getSessionId(strategyId);
    const opencode = await getOpencodeTurnClient();
    const opencodeEvents = await streamOpencodeEvents({
      app,
      db,
      getOpencodeTurnClient,
      runEvents,
      runId,
      sessionId,
    });

    try {
      const result = await opencode.prompt({
        messageId: runId,
        sessionId,
        system: systemPrompt,
        text,
      });
      const assistantError = getAssistantErrorMessage(result);

      if (assistantError) {
        throw new Error(assistantError);
      }

      await opencodeEvents.waitForIdle(1_000);

      try {
        const session = await opencode.getSession(sessionId);
        await maybeUpdateStrategyTitle(turnClient, strategyId, session.title);
      } catch (error) {
        app.log.warn(
          {
            error: getErrorMessage(error),
            runId,
            strategyId,
          },
          "Failed to refresh strategy title after run completion",
        );
      }

      await turnClient.query("COMMIT");
      inTransaction = false;

      await markRunCompleted(
        db,
        runEvents,
        runId,
        sessionId,
        result.info.id,
        extractReplyText(result.parts),
      );
    } finally {
      await opencodeEvents.stop();
    }
  } catch (error) {
    const message = getErrorMessage(error);

    if (inTransaction) {
      try {
        await turnClient.query("ROLLBACK");
        inTransaction = false;
      } catch (rollbackError) {
        app.log.error(
          {
            error: getErrorMessage(rollbackError),
            runId,
            strategyId,
          },
          "Failed to roll back turn transaction",
        );
      }
    }

    try {
      await markRunFailed(db, runEvents, runId, message);
    } catch (updateError) {
      app.log.error(
        {
          error: getErrorMessage(updateError),
          runId,
          strategyId,
        },
        "Failed to persist run failure state",
      );
    }

    app.log.error(
      {
        error: message,
        runId,
        strategyId,
      },
      "Failed to execute agent turn",
    );
  } finally {
    turnClient.release();
  }
}

export function buildServer(dependencies: ServerDependencies = {}) {
  const db = dependencies.db ?? (pg as unknown as DatabasePool);
  const buildSystemPrompt =
    dependencies.buildSystemPrompt ?? defaultBuildSystemPrompt;
  const getSessionId = dependencies.getSessionId ?? getOrCreateSession;
  const getOpencodeTurnClient =
    dependencies.getOpencodeTurnClient ?? createOpencodeTurnClient;
  const runEvents = new RunEventStore();
  const turnLockTimeoutMs =
    dependencies.turnLockTimeoutMs ?? resolveTurnLockTimeoutMs();
  const app = Fastify({
    loggerInstance: pino(),
  });

  app.get("/health", async () => ({ ok: true }));

  app.post("/users/:user_id/strategies", notImplemented);
  app.get("/users/:user_id/strategies", notImplemented);
  app.get("/users/:user_id/strategies/:strategy_id", notImplemented);
  app.post<{
    Body: MessageBody;
    Params: RouteParams;
    Querystring: WaitQuery;
  }>(
    "/users/:user_id/strategies/:strategy_id/messages",
    async (request, reply) => {
      try {
        const waitMs = parseWaitMilliseconds(request.query.wait);
        const text = parseMessageText(request.body);
        const { strategy_id: strategyId, user_id: userId } = request.params;

        await ensureStrategyExists(db, userId, strategyId);

        const turnClient = await acquireUserTurnClient(
          db,
          userId,
          turnLockTimeoutMs,
        );

        const runId = randomUUID();

        try {
          await insertRunStart(db, runEvents, runId, strategyId);
        } catch (error) {
          try {
            await turnClient.query("ROLLBACK");
          } finally {
            turnClient.release();
          }

          throw error;
        }

        void executeRun({
          app,
          buildSystemPrompt,
          db,
          getOpencodeTurnClient,
          getSessionId,
          runEvents,
          runId,
          strategyId,
          text,
          turnClient,
          userId,
        });

        if (waitMs === undefined) {
          return reply.code(202).send({ run_id: runId });
        }

        const run = await waitForRun(db, userId, strategyId, runId, waitMs);

        if (!run) {
          return reply
            .code(404)
            .send(errorResponse("run_not_found", "Run not found"));
        }

        return sendRunResponse(reply, run);
      } catch (error) {
        const routeError = toRouteError(error, turnLockTimeoutMs);

        if (routeError instanceof RouteError) {
          return reply
            .code(routeError.statusCode)
            .send(errorResponse(routeError.code, routeError.message));
        }

        request.log.error(error);
        return reply
          .code(500)
          .send(errorResponse("internal_error", "Internal server error"));
      }
    },
  );
  app.get<{
    Params: RunRouteParams;
    Querystring: WaitQuery;
  }>(
    "/users/:user_id/strategies/:strategy_id/runs/:run_id",
    async (request, reply) => {
      try {
        const waitMs = parseWaitMilliseconds(request.query.wait) ?? 0;
        const {
          run_id: runId,
          strategy_id: strategyId,
          user_id: userId,
        } = request.params;
        const run = await waitForRun(db, userId, strategyId, runId, waitMs);

        if (!run) {
          return reply
            .code(404)
            .send(errorResponse("run_not_found", "Run not found"));
        }

        return sendRunResponse(reply, run);
      } catch (error) {
        const routeError = toRouteError(error, turnLockTimeoutMs);

        if (routeError instanceof RouteError) {
          return reply
            .code(routeError.statusCode)
            .send(errorResponse(routeError.code, routeError.message));
        }

        request.log.error(error);
        return reply
          .code(500)
          .send(errorResponse("internal_error", "Internal server error"));
      }
    },
  );
  app.get<{
    Params: RunRouteParams;
  }>(
    "/users/:user_id/strategies/:strategy_id/runs/:run_id/events",
    async (request, reply) => {
      const disconnect = createDisconnectPromise(request);

      try {
        const lastEventId = parseLastEventId(request.headers["last-event-id"]);
        const {
          run_id: runId,
          strategy_id: strategyId,
          user_id: userId,
        } = request.params;
        const run = await loadRunState(db, userId, strategyId, runId);

        if (!run) {
          return reply
            .code(404)
            .send(errorResponse("run_not_found", "Run not found"));
        }

        const subscription = runEvents.subscribe(runId);

        beginSseStream(reply);

        let latestEventId = lastEventId;
        let terminalSeen = false;

        try {
          const replayedEvents = await loadRunEvents(db, runId, lastEventId);

          for (const event of replayedEvents) {
            if (event.seq <= latestEventId) {
              continue;
            }

            writeSseEvent(reply, event);
            latestEventId = event.seq;
            terminalSeen = isTerminalEventType(event.type);
          }

          while (!terminalSeen && !disconnect.isDisconnected()) {
            const nextResult = await Promise.race([
              subscription
                .next()
                .then((event) => ({ event, type: "event" as const })),
              disconnect.done.then(() => ({ type: "disconnect" as const })),
            ]);

            if (nextResult.type === "disconnect") {
              break;
            }

            const event = nextResult.event;

            if (!event || event.seq <= latestEventId) {
              continue;
            }

            writeSseEvent(reply, event);
            latestEventId = event.seq;
            terminalSeen = isTerminalEventType(event.type);
          }
        } finally {
          subscription.close();

          if (!reply.raw.writableEnded) {
            reply.raw.end();
          }
        }

        return reply;
      } catch (error) {
        const routeError = toRouteError(error, turnLockTimeoutMs);

        if (routeError instanceof RouteError) {
          return reply
            .code(routeError.statusCode)
            .send(errorResponse(routeError.code, routeError.message));
        }

        request.log.error(error);

        if (!reply.sent) {
          return reply
            .code(500)
            .send(errorResponse("internal_error", "Internal server error"));
        }

        if (!reply.raw.destroyed && !reply.raw.writableEnded) {
          reply.raw.end();
        }

        return reply;
      } finally {
        disconnect.cleanup();
      }
    },
  );

  return app;
}

export async function startServer() {
  const app = buildServer();

  try {
    await app.listen({
      host: process.env.HOST ?? "0.0.0.0",
      port: getPort(),
    });
  } catch (error) {
    app.log.error(error);
    process.exitCode = 1;
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  void startServer();
}
