import "../env";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import Fastify from "fastify";
import pino from "pino";

import { buildSystemPrompt as defaultBuildSystemPrompt } from "../agent/prompt";
import {
  createOpencodeClient,
  getOrCreateSession,
  type DatabaseClient,
  type OpencodePromptResult,
  type OpencodeTurnClient,
} from "../agent/session";
import { pg } from "../db/client";

type DatabaseQueryable = Pick<DatabaseClient, "query">;
type ServerDependencies = {
  db?: DatabaseQueryable;
  buildSystemPrompt?: typeof defaultBuildSystemPrompt;
  getSessionId?: (
    strategyId: string,
    client?: DatabaseClient,
  ) => Promise<string>;
  getOpencodeClient?: () => Promise<OpencodeTurnClient>;
};
type StrategyOwnershipRow = { user_id: string };
type RunRow = {
  run_id: string;
  status: string;
  started_at: Date | string;
  ended_at: Date | string | null;
  exit_code: number | null;
  reply: string | null;
  error: string | null;
};

function getPort() {
  const raw = process.env.PORT ?? "3000";
  const port = Number.parseInt(raw, 10);
  if (!Number.isInteger(port) || port <= 0)
    throw new Error(`Invalid PORT value: ${raw}`);
  return port;
}

function httpError(statusCode: number, message: string) {
  return Object.assign(new Error(message), { statusCode });
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

function requiredText(body: Record<string, unknown>, key: string) {
  const value = body[key];
  if (typeof value !== "string" || !value.trim()) {
    throw httpError(
      400,
      `Request body must include a non-empty '${key}' field`,
    );
  }
  return value.trim();
}

function toIso(value: Date | string | null) {
  return value instanceof Date ? value.toISOString() : value;
}

function replyText(parts: OpencodePromptResult["parts"]) {
  return parts.reduce(
    (reply, part) =>
      part.type === "text" && !part.ignored ? `${reply}${part.text}` : reply,
    "",
  );
}

function promptFailure(result: OpencodePromptResult) {
  for (const part of result.parts) {
    if (part.type !== "tool" || part.state.status !== "error") continue;
    if (typeof part.state.error === "string" && part.state.error.trim()) {
      return part.state.error;
    }
  }

  const error = result.info.error;
  if (!error) return undefined;
  return typeof error.data?.message === "string" && error.data.message.trim()
    ? error.data.message
    : error.name;
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

  const strategy = (
    await db.query<StrategyOwnershipRow>(
      "SELECT user_id FROM strategies WHERE strategy_id = $1",
      [strategyId],
    )
  ).rows[0];
  if (!strategy || strategy.user_id !== userId)
    throw httpError(404, "Strategy not found");
}

async function maybeUpdateStrategyTitle(
  db: DatabaseQueryable,
  strategyId: string,
  title: string,
) {
  if (!title.trim()) return;
  await db.query(
    "UPDATE strategies SET title = $2 WHERE strategy_id = $1 AND btrim(title) = ''",
    [strategyId, title.trim()],
  );
}

async function readRun(db: DatabaseQueryable, runId: string) {
  return (
    (
      await db.query<RunRow>(
        "SELECT run_id, status, started_at, ended_at, exit_code, reply, error FROM runs WHERE run_id = $1",
        [runId],
      )
    ).rows[0] ?? null
  );
}

function runResponse(run: RunRow) {
  return {
    run_id: run.run_id,
    status: run.status,
    started_at: toIso(run.started_at),
    ended_at: toIso(run.ended_at),
    exit_code: run.exit_code,
    reply: run.reply,
    error: run.error,
  };
}

export function buildServer(dependencies: ServerDependencies = {}) {
  const db = dependencies.db ?? (pg as unknown as DatabaseQueryable);
  const buildSystemPrompt =
    dependencies.buildSystemPrompt ?? defaultBuildSystemPrompt;
  const getSessionId = dependencies.getSessionId ?? getOrCreateSession;
  const getOpencodeClient =
    dependencies.getOpencodeClient ?? createOpencodeClient;
  const app = Fastify({ loggerInstance: pino() });

  app.get("/health", async () => ({ ok: true }));

  app.post("/strategies", async (request) => {
    const body = (request.body ?? {}) as Record<string, unknown>;
    const userId = requiredText(body, "user_id");
    const strategyId = randomUUID();

    await ensureStrategyExists(db, userId, strategyId);
    return { strategy_id: strategyId };
  });

  app.post("/messages", async (request) => {
    const body = (request.body ?? {}) as Record<string, unknown>;
    const userId = requiredText(body, "user_id");
    const strategyId = requiredText(body, "strategy_id");
    const text = requiredText(body, "text");
    const runId = randomUUID();

    await ensureStrategyExists(db, userId, strategyId);
    await db.query(
      "UPDATE strategies SET last_used_at = NOW() WHERE strategy_id = $1",
      [strategyId],
    );
    await db.query(
      "INSERT INTO runs (run_id, strategy_id, status) VALUES ($1, $2, $3)",
      [runId, strategyId, "running"],
    );

    request.log.info({ runId, strategyId }, "resolving opencode session");
    const sessionId = await getSessionId(strategyId);
    request.log.info(
      { runId, strategyId, sessionId },
      "opencode session resolved",
    );

    let promptError: unknown;
    let result: OpencodePromptResult | undefined;
    try {
      const system = await buildSystemPrompt({ userId, strategyId });
      const opencode = await getOpencodeClient();
      request.log.info(
        { runId, sessionId, textLength: text.length },
        "calling opencode prompt",
      );
      const promptStart = Date.now();
      result = await opencode.prompt({
        messageId: `msg_${runId.replace(/-/g, "")}`,
        sessionId,
        system,
        text,
      });
      request.log.info(
        {
          runId,
          sessionId,
          durationMs: Date.now() - promptStart,
          parts: result.parts?.length,
        },
        "opencode prompt returned",
      );
      if (!result.parts) throw new Error("opencode prompt returned no parts");
      const failure = promptFailure(result);
      if (failure) throw new Error(failure);

      try {
        const session = await opencode.getSession(sessionId);
        await maybeUpdateStrategyTitle(db, strategyId, session.title);
      } catch (error) {
        request.log.warn(
          { error: errorMessage(error), runId, strategyId },
          "Failed to refresh strategy title after run completion",
        );
      }
    } catch (error) {
      promptError = error;
    }

    if (promptError) {
      await db.query(
        "UPDATE runs SET status = $2, ended_at = NOW(), exit_code = $3, reply = NULL, error = $4 WHERE run_id = $1",
        [runId, "failed", 1, errorMessage(promptError)],
      );
    } else {
      await db.query(
        "UPDATE runs SET status = $2, ended_at = NOW(), exit_code = $3, reply = $4, error = NULL WHERE run_id = $1",
        [runId, "completed", 0, replyText(result!.parts)],
      );
    }

    const run = await readRun(db, runId);
    if (!run) throw new Error(`Run missing after execution: ${runId}`);
    if (promptError) throw promptError;
    return runResponse(run);
  });

  app.get<{ Params: { id: string } }>("/runs/:id", async (request) => {
    const run = await readRun(db, request.params.id);
    if (!run) throw httpError(404, "Run not found");
    return runResponse(run);
  });

  return app;
}

export async function startServer() {
  const app = buildServer();
  try {
    await app.listen({ host: process.env.HOST ?? "0.0.0.0", port: getPort() });
  } catch (error) {
    app.log.error(error);
    process.exitCode = 1;
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1])
  void startServer();
