import "../env";
import { randomUUID } from "node:crypto";
import { createReadStream, existsSync } from "node:fs";
import { readdir, stat } from "node:fs/promises";
import path from "node:path";
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

type ArtifactRef = { kind: string; path: string };

const ARTIFACT_EXTENSIONS = ["png", "jpg", "jpeg", "svg", "gif", "webp", "json", "csv"];

const ARTIFACT_CONTENT_TYPES: Record<string, string> = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".svg": "image/svg+xml",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".json": "application/json; charset=utf-8",
  ".csv": "text/csv; charset=utf-8",
};

const KNOWN_ARTIFACT_KINDS: Record<string, string> = {
  "equity_curve.png": "equity_curve_png",
  "drawdown.png": "drawdown_png",
  "report.json": "report_json",
};

function resolveStorageRoot() {
  const explicit = process.env.STORAGE_ROOT?.trim();
  if (explicit) return path.resolve(explicit);

  let current = path.dirname(fileURLToPath(import.meta.url));
  while (true) {
    const candidate = path.join(current, ".data", "storage");
    if (existsSync(candidate)) return candidate;
    const parent = path.dirname(current);
    if (parent === current) return undefined;
    current = parent;
  }
}

function artifactsDir() {
  const root = resolveStorageRoot();
  return root ? path.join(root, "artifacts") : undefined;
}

function relativeArtifactPath(absolute: string): string | null {
  const dir = artifactsDir();
  if (!dir) return null;
  const resolved = path.resolve(absolute);
  const relative = path.relative(dir, resolved);
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) {
    return null;
  }
  return relative.split(path.sep).join("/");
}

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function kindFromPath(relative: string): string {
  const basename = relative.split("/").pop() ?? relative;
  return KNOWN_ARTIFACT_KINDS[basename] ?? basename;
}

type ArtifactSnapshot = Map<string, number>;

const ARTIFACT_EXTENSION_SET = new Set(
  ARTIFACT_EXTENSIONS.map((extension) => `.${extension}`),
);

async function walkArtifacts(
  current: string,
  out: ArtifactSnapshot,
): Promise<void> {
  let entries;
  try {
    entries = await readdir(current, { withFileTypes: true });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    throw error;
  }
  for (const entry of entries) {
    const full = path.join(current, entry.name);
    if (entry.isDirectory()) {
      await walkArtifacts(full, out);
      continue;
    }
    if (!entry.isFile()) continue;
    const extension = path.extname(entry.name).toLowerCase();
    if (!ARTIFACT_EXTENSION_SET.has(extension)) continue;
    const info = await stat(full).catch(() => null);
    if (info) out.set(full, info.mtimeMs);
  }
}

async function snapshotArtifacts(): Promise<ArtifactSnapshot> {
  const dir = artifactsDir();
  const snapshot: ArtifactSnapshot = new Map();
  if (!dir) return snapshot;
  await walkArtifacts(dir, snapshot);
  return snapshot;
}

function diffArtifactSnapshots(
  before: ArtifactSnapshot,
  after: ArtifactSnapshot,
): string[] {
  const changed: string[] = [];
  for (const [absolutePath, mtime] of after) {
    const previous = before.get(absolutePath);
    if (previous === undefined || previous !== mtime) {
      changed.push(absolutePath);
    }
  }
  return changed.sort();
}

function artifactsFromAbsolutePaths(absolutePaths: string[]): ArtifactRef[] {
  const seen = new Set<string>();
  const artifacts: ArtifactRef[] = [];
  for (const absolutePath of absolutePaths) {
    const relative = relativeArtifactPath(absolutePath);
    if (!relative || seen.has(relative)) continue;
    seen.add(relative);
    artifacts.push({ kind: kindFromPath(relative), path: relative });
  }
  return artifacts;
}

function mergeArtifacts(...sources: ArtifactRef[][]): ArtifactRef[] {
  const seen = new Set<string>();
  const merged: ArtifactRef[] = [];
  for (const source of sources) {
    for (const artifact of source) {
      if (seen.has(artifact.path)) continue;
      seen.add(artifact.path);
      merged.push(artifact);
    }
  }
  return merged;
}

function extractArtifactsFromParts(
  parts: OpencodePromptResult["parts"],
): ArtifactRef[] {
  const dir = artifactsDir();
  if (!dir) return [];

  const pattern = new RegExp(
    `${escapeRegExp(dir)}/[\\w\\-./]+?\\.(?:${ARTIFACT_EXTENSIONS.join("|")})`,
    "g",
  );

  const seen = new Set<string>();
  const artifacts: ArtifactRef[] = [];

  const recordPath = (absolutePath: string) => {
    const relative = relativeArtifactPath(absolutePath);
    if (!relative || seen.has(relative)) return;
    seen.add(relative);
    artifacts.push({ kind: kindFromPath(relative), path: relative });
  };

  const scan = (text: string) => {
    for (const match of text.matchAll(pattern)) recordPath(match[0]);
  };

  for (const part of parts ?? []) {
    if (part.type === "tool") {
      const output = (
        part.state as { metadata?: { output?: unknown } } | undefined
      )?.metadata?.output;
      if (typeof output === "string") scan(output);
      continue;
    }
    if (part.type === "text" || part.type === "reasoning") {
      const text = (part as { text?: unknown }).text;
      if (typeof text === "string") scan(text);
    }
  }

  return artifacts;
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

  app.get<{ Params: { "*": string } }>("/artifacts/*", async (request, reply) => {
    const dir = artifactsDir();
    if (!dir) throw httpError(500, "STORAGE_ROOT is not configured");

    const requested = request.params["*"] ?? "";
    if (!requested) throw httpError(404, "Artifact not found");

    const resolved = path.resolve(dir, requested);
    const dirWithSeparator = dir.endsWith(path.sep) ? dir : `${dir}${path.sep}`;
    if (!resolved.startsWith(dirWithSeparator)) {
      throw httpError(403, "Forbidden");
    }

    const info = await stat(resolved).catch(() => null);
    if (!info || !info.isFile()) throw httpError(404, "Artifact not found");

    const extension = path.extname(resolved).toLowerCase();
    const contentType =
      ARTIFACT_CONTENT_TYPES[extension] ?? "application/octet-stream";
    reply.header("Cache-Control", "private, max-age=300");
    reply.type(contentType);
    return reply.send(createReadStream(resolved));
  });

  app.post("/messages/stream", async (request, reply) => {
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

    request.log.info({ runId, strategyId }, "stream: resolving opencode session");
    const sessionId = await getSessionId(strategyId);
    request.log.info(
      { runId, strategyId, sessionId },
      "stream: opencode session resolved",
    );

    reply.hijack();
    const raw = reply.raw;
    raw.setHeader("Content-Type", "text/event-stream");
    raw.setHeader("Cache-Control", "no-cache, no-transform");
    raw.setHeader("Connection", "keep-alive");
    raw.setHeader("X-Accel-Buffering", "no");
    raw.flushHeaders?.();

    let clientGone = false;
    let runCompletedSent = false;
    raw.on("close", () => {
      clientGone = true;
    });

    const send = (eventName: string, payload: unknown) => {
      if (clientGone || raw.writableEnded) return;
      raw.write(`event: ${eventName}\n`);
      raw.write(`data: ${JSON.stringify(payload)}\n\n`);
    };

    const finalize = (
      runPayload: Record<string, unknown>,
      artifacts: ArtifactRef[],
    ) => {
      if (runCompletedSent) return;
      runCompletedSent = true;
      send("run.completed", { ...runPayload, artifacts });
      if (!clientGone && !raw.writableEnded) raw.end();
    };

    send("run.started", { run_id: runId, session_id: sessionId });

    const beforeSnapshot = await snapshotArtifacts().catch((error) => {
      request.log.warn(
        { error: errorMessage(error), runId },
        "stream: failed to snapshot artifacts before prompt",
      );
      return new Map() as ArtifactSnapshot;
    });

    const opencode = await getOpencodeClient();
    const eventsAbort = new AbortController();
    let eventCount = 0;

    const eventLoop = (async () => {
      try {
        const events = await opencode.subscribeEvents({
          signal: eventsAbort.signal,
        });
        for await (const event of events) {
          if (clientGone || eventsAbort.signal.aborted) break;
          const properties = (event as { properties?: { sessionID?: string } })
            .properties;
          if (properties?.sessionID && properties.sessionID !== sessionId) {
            continue;
          }
          eventCount += 1;
          send(event.type, event);
        }
      } catch (error) {
        if (eventsAbort.signal.aborted || clientGone) return;
        request.log.warn(
          { error: errorMessage(error), runId },
          "stream: event loop failed",
        );
      }
    })();

    let promptError: unknown;
    let result: OpencodePromptResult | undefined;
    try {
      const system = await buildSystemPrompt({ userId, strategyId });
      const promptStart = Date.now();
      request.log.info({ runId, sessionId }, "stream: calling opencode prompt");
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
          eventCount,
        },
        "stream: prompt returned",
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
          "stream: failed to refresh strategy title",
        );
      }
    } catch (error) {
      promptError = error;
    }

    eventsAbort.abort();
    const eventLoopWithTimeout = Promise.race([
      eventLoop,
      new Promise<void>((resolve) => setTimeout(resolve, 1000)),
    ]);
    await eventLoopWithTimeout.catch(() => undefined);

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

    const afterSnapshot = await snapshotArtifacts().catch((error) => {
      request.log.warn(
        { error: errorMessage(error), runId },
        "stream: failed to snapshot artifacts after prompt",
      );
      return new Map() as ArtifactSnapshot;
    });
    const changedAbsolutePaths = diffArtifactSnapshots(
      beforeSnapshot,
      afterSnapshot,
    );
    const filesystemArtifacts = artifactsFromAbsolutePaths(changedAbsolutePaths);
    const textArtifacts = result?.parts
      ? extractArtifactsFromParts(result.parts)
      : [];
    const artifacts = mergeArtifacts(filesystemArtifacts, textArtifacts);
    request.log.info(
      {
        runId,
        artifactCount: artifacts.length,
        filesystemArtifactCount: filesystemArtifacts.length,
        textArtifactCount: textArtifacts.length,
        storageRoot: process.env.STORAGE_ROOT ?? null,
        artifactsDir: artifactsDir() ?? null,
      },
      "stream: extracted artifacts",
    );

    const run = await readRun(db, runId);
    if (run) {
      finalize(runResponse(run), artifacts);
    } else {
      finalize(
        {
          run_id: runId,
          status: "failed",
          error: "Run missing after execution",
        },
        artifacts,
      );
    }
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
