import "../env";
import { randomUUID } from "node:crypto";
import { createReadStream, existsSync } from "node:fs";
import { readFile, readdir, stat } from "node:fs/promises";
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
import { parseStrategyResultBlock } from "../agent/strategy-result";
import {
  createRun as defaultCreateRun,
  markRunCompleted as defaultMarkRunCompleted,
  markRunFailed as defaultMarkRunFailed,
  readRun as defaultReadRun,
  type RunRow,
} from "../db/repositories/runs";
import {
  ensureStrategy as defaultEnsureStrategy,
  touchStrategy as defaultTouchStrategy,
  updateStrategyTitleIfBlank as defaultUpdateStrategyTitleIfBlank,
} from "../db/repositories/strategies";
import {
  runCoinGeckoMarketCapIngestion,
  toUtcDayTimestamp,
  type CoinGeckoMarketCapOptions,
  type CoinGeckoMarketCapSummary,
} from "../ingestion/coingecko-market-caps";
import {
  runGmxHistoryIngestion,
  type GmxHistoryOptions,
  type GmxHistorySummary,
} from "../ingestion/gmx-history";

type Repositories = {
  createRun: typeof defaultCreateRun;
  ensureStrategy: typeof defaultEnsureStrategy;
  markRunCompleted: typeof defaultMarkRunCompleted;
  markRunFailed: typeof defaultMarkRunFailed;
  readRun: typeof defaultReadRun;
  touchStrategy: typeof defaultTouchStrategy;
  updateStrategyTitleIfBlank: typeof defaultUpdateStrategyTitleIfBlank;
};
type IngestionRunners = {
  gmx: (options: GmxHistoryOptions) => Promise<GmxHistorySummary>;
  coingeckoMarketCaps: (
    options: CoinGeckoMarketCapOptions,
  ) => Promise<CoinGeckoMarketCapSummary>;
};
type ServerDependencies = {
  buildSystemPrompt?: typeof defaultBuildSystemPrompt;
  getSessionId?: (
    strategyId: string,
    client?: DatabaseClient,
  ) => Promise<string>;
  getOpencodeClient?: () => Promise<OpencodeTurnClient>;
  ingestionRunners?: Partial<IngestionRunners>;
  repositories?: Partial<Repositories>;
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

function optionalStringList(body: Record<string, unknown>, key: string) {
  const value = body[key];
  if (value === undefined || value === null) return undefined;

  const normalize = (entry: string) => entry.trim();
  const entries =
    typeof value === "string"
      ? value.split(",").map(normalize)
      : Array.isArray(value) &&
          value.every((entry) => typeof entry === "string")
        ? value.map(normalize)
        : null;

  if (!entries || entries.length === 0 || entries.some((entry) => !entry)) {
    throw httpError(
      400,
      `Request body field '${key}' must be a non-empty string array or comma-separated string`,
    );
  }

  return entries;
}

function optionalBoolean(
  body: Record<string, unknown>,
  key: string,
  fallback = false,
) {
  const value = body[key];
  if (value === undefined || value === null) return fallback;
  if (typeof value === "boolean") return value;
  throw httpError(400, `Request body field '${key}' must be a boolean`);
}

function optionalFullRefresh(body: Record<string, unknown>) {
  if (body.full_refresh !== undefined && body.fullRefresh !== undefined) {
    throw httpError(
      400,
      "Request body must not include both 'full_refresh' and 'fullRefresh'",
    );
  }

  return body.fullRefresh === undefined
    ? optionalBoolean(body, "full_refresh")
    : optionalBoolean(body, "fullRefresh");
}

function optionalMarketCapDate(body: Record<string, unknown>) {
  const value = body.date;
  if (value === undefined || value === null)
    return toUtcDayTimestamp(new Date());
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw httpError(400, "Request body field 'date' must use YYYY-MM-DD");
  }

  const [yearText, monthText, dayText] = value.split("-");
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const date = new Date(Date.UTC(year, month - 1, day));

  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    throw httpError(
      400,
      "Request body field 'date' must be a valid calendar day",
    );
  }

  return date;
}

function parseGmxIngestionOptions(
  body: Record<string, unknown>,
): GmxHistoryOptions {
  return {
    symbols: optionalStringList(body, "symbols"),
    exclude: optionalStringList(body, "exclude") ?? [],
    fullRefresh: optionalFullRefresh(body),
    dryRun: optionalBoolean(body, "dry_run"),
  };
}

function parseCoinGeckoMarketCapOptions(
  body: Record<string, unknown>,
): CoinGeckoMarketCapOptions {
  return {
    symbols: optionalStringList(body, "symbols"),
    date: optionalMarketCapDate(body),
    dryRun: optionalBoolean(body, "dry_run"),
  };
}

function normalizeIngestionLoader(loader: string) {
  if (loader === "gmx" || loader === "gmx-history") return "gmx";
  if (loader === "coingecko-market-caps") return "coingecko-market-caps";
  return null;
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

function runResponse(run: RunRow, structuredResult: unknown = null) {
  const response = {
    run_id: run.runId,
    status: run.status,
    started_at: toIso(run.startedAt),
    ended_at: toIso(run.endedAt),
    exit_code: run.exitCode,
    reply: run.reply,
    error: run.error,
  };

  return structuredResult === null
    ? response
    : { ...response, structured_result: structuredResult };
}

type ArtifactRef = { kind: string; path: string };

const ARTIFACT_EXTENSIONS = [
  "png",
  "jpg",
  "jpeg",
  "svg",
  "gif",
  "webp",
  "json",
  "csv",
];

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
  "equity_curve.json": "equity_curve_json",
  "drawdown.png": "drawdown_png",
  "drawdown.json": "drawdown_json",
  "allocation.json": "allocation_json",
  "report.json": "report_json",
  "strategy_result.json": "strategy_result_json",
};

const JSON_ARTIFACT_FILENAMES = new Set([
  "report.json",
  "equity_curve.json",
  "drawdown.json",
  "allocation.json",
  "strategy_result.json",
]);

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

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function toNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function normalizeEquityPoint(value: unknown) {
  if (!isRecord(value) || typeof value.date !== "string") return null;
  const strategyEquity = toNumber(value.equity_usd) ?? toNumber(value.equity);
  if (strategyEquity === null) return null;

  return {
    date: value.date,
    strategy_equity: strategyEquity,
    benchmark_equity:
      toNumber(value.bitcoin_equity_usd) ?? toNumber(value.bitcoin_equity),
  };
}

function normalizeDrawdownPoint(value: unknown) {
  if (!isRecord(value) || typeof value.date !== "string") return null;
  const strategyDrawdown = toNumber(value.drawdown);
  if (strategyDrawdown === null) return null;

  return {
    date: value.date,
    strategy_drawdown: strategyDrawdown,
    benchmark_drawdown: toNumber(value.bitcoin_drawdown),
  };
}

function normalizeAllocationPoint(value: unknown) {
  if (!isRecord(value)) return null;
  const weight = toNumber(value.weight);
  if (weight === null) return null;
  const asset =
    typeof value.asset === "string"
      ? value.asset
      : typeof value.coin_id === "string"
        ? value.coin_id
        : null;
  if (!asset) return null;

  return { asset, weight };
}

function normalizeArray<T>(
  value: unknown,
  normalize: (item: unknown) => T | null,
): T[] | null {
  if (!Array.isArray(value)) return null;
  const normalized = value.flatMap((item) => {
    const next = normalize(item);
    return next ? [next] : [];
  });

  return normalized.length > 0 ? normalized : null;
}

function mergeArtifactPayload(
  structuredResult: unknown,
  filename: string,
  payload: unknown,
): unknown {
  if (!isRecord(structuredResult)) return structuredResult;
  const enriched: Record<string, unknown> = { ...structuredResult };

  if (filename === "report.json" && isRecord(payload)) {
    if (isRecord(payload.kpis)) {
      enriched.kpis = {
        ...(isRecord(enriched.kpis) ? enriched.kpis : {}),
        ...payload.kpis,
      };
    }
    if (isRecord(payload.summary)) {
      enriched.backtest = {
        ...(isRecord(enriched.backtest) ? enriched.backtest : {}),
        ...payload.summary,
      };
    }
  }

  const charts = isRecord(enriched.charts) ? { ...enriched.charts } : {};

  if (filename === "equity_curve.json") {
    const equity = normalizeArray(payload, normalizeEquityPoint);
    if (equity) charts.equity_curve = equity;
  }

  if (filename === "drawdown.json") {
    const drawdown = normalizeArray(payload, normalizeDrawdownPoint);
    if (drawdown) charts.drawdown = drawdown;
  }

  if (filename === "allocation.json") {
    const allocation = normalizeArray(payload, normalizeAllocationPoint);
    if (allocation) charts.allocation = allocation;
  }

  if (Object.keys(charts).length > 0) enriched.charts = charts;
  return enriched;
}

async function enrichStructuredResultFromArtifacts(
  structuredResult: unknown,
  artifacts: ArtifactRef[],
): Promise<unknown> {
  if (!isRecord(structuredResult)) return structuredResult;
  const dir = artifactsDir();
  if (!dir) return structuredResult;

  let enriched: unknown = structuredResult;
  for (const artifact of artifacts) {
    const filename = artifact.path.split("/").pop() ?? artifact.path;
    if (!JSON_ARTIFACT_FILENAMES.has(filename)) continue;

    try {
      const absolutePath = path.resolve(dir, artifact.path);
      const dirWithSeparator = dir.endsWith(path.sep)
        ? dir
        : `${dir}${path.sep}`;
      if (!absolutePath.startsWith(dirWithSeparator)) continue;
      const payload: unknown = JSON.parse(
        await readFile(absolutePath, "utf-8"),
      );
      enriched = mergeArtifactPayload(enriched, filename, payload);
    } catch {
      continue;
    }
  }

  return enriched;
}

async function readFinalizedStructuredResult(
  artifacts: ArtifactRef[],
): Promise<unknown> {
  const dir = artifactsDir();
  if (!dir) return null;

  const finalized = artifacts.find(
    (artifact) =>
      (artifact.path.split("/").pop() ?? artifact.path) ===
      "strategy_result.json",
  );
  if (!finalized) return null;

  try {
    const absolutePath = path.resolve(dir, finalized.path);
    const dirWithSeparator = dir.endsWith(path.sep) ? dir : `${dir}${path.sep}`;
    if (!absolutePath.startsWith(dirWithSeparator)) return null;
    return JSON.parse(await readFile(absolutePath, "utf-8"));
  } catch {
    return null;
  }
}

async function structuredResultFromArtifacts(
  reply: string,
  artifacts: ArtifactRef[],
): Promise<unknown> {
  const hasStructuredArtifact = artifacts.some((artifact) =>
    JSON_ARTIFACT_FILENAMES.has(
      artifact.path.split("/").pop() ?? artifact.path,
    ),
  );
  if (!hasStructuredArtifact) return null;

  const summary = reply.trim().split("\n").find(Boolean) ?? "Strategy result";
  const base = {
    title: "Strategy result",
    summary,
    reasoning: summary,
    allocation: [],
    kpis: {},
    assumptions: [],
    risks: [],
    next_steps: [],
  };

  return enrichStructuredResultFromArtifacts(base, artifacts);
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
  const buildSystemPrompt =
    dependencies.buildSystemPrompt ?? defaultBuildSystemPrompt;
  const getSessionId = dependencies.getSessionId ?? getOrCreateSession;
  const getOpencodeClient =
    dependencies.getOpencodeClient ?? createOpencodeClient;
  const repositories: Repositories = {
    createRun: defaultCreateRun,
    ensureStrategy: defaultEnsureStrategy,
    markRunCompleted: defaultMarkRunCompleted,
    markRunFailed: defaultMarkRunFailed,
    readRun: defaultReadRun,
    touchStrategy: defaultTouchStrategy,
    updateStrategyTitleIfBlank: defaultUpdateStrategyTitleIfBlank,
    ...dependencies.repositories,
  };
  const ingestionRunners: IngestionRunners = {
    gmx: runGmxHistoryIngestion,
    coingeckoMarketCaps: runCoinGeckoMarketCapIngestion,
    ...dependencies.ingestionRunners,
  };
  const runningIngestions = new Set<string>();
  const app = Fastify({ loggerInstance: pino() });

  app.get("/health", async () => ({ ok: true }));

  app.post<{ Params: { loader: string } }>(
    "/ingestion/:loader",
    async (request, reply) => {
      const loader = normalizeIngestionLoader(request.params.loader);
      if (!loader) throw httpError(404, "Ingestion loader not found");

      if (runningIngestions.has(loader)) {
        throw httpError(409, `Ingestion loader '${loader}' is already running`);
      }

      const body = (request.body ?? {}) as Record<string, unknown>;
      if (!isRecord(body)) {
        throw httpError(400, "Request body must be a JSON object");
      }

      const startedAt = new Date();
      runningIngestions.add(loader);
      request.log.info({ loader }, "ingestion started");

      try {
        if (loader === "gmx") {
          const options = parseGmxIngestionOptions(body);
          const summary = await ingestionRunners.gmx(options);
          const status = summary.failureCount > 0 ? "failed" : "completed";
          if (status === "failed") reply.code(500);

          return {
            loader,
            status,
            started_at: startedAt.toISOString(),
            ended_at: new Date().toISOString(),
            summary,
          };
        }

        const options = parseCoinGeckoMarketCapOptions(body);
        const summary = await ingestionRunners.coingeckoMarketCaps(options);

        return {
          loader,
          status: "completed",
          started_at: startedAt.toISOString(),
          ended_at: new Date().toISOString(),
          summary,
        };
      } finally {
        runningIngestions.delete(loader);
        request.log.info({ loader }, "ingestion finished");
      }
    },
  );

  app.post("/strategies", async (request) => {
    const body = (request.body ?? {}) as Record<string, unknown>;
    const userId = requiredText(body, "user_id");
    const strategyId = randomUUID();

    const strategy = await repositories.ensureStrategy(userId, strategyId);
    if (!strategy || strategy.userId !== userId)
      throw httpError(404, "Strategy not found");
    return { strategy_id: strategyId };
  });

  app.post("/messages", async (request) => {
    const body = (request.body ?? {}) as Record<string, unknown>;
    const userId = requiredText(body, "user_id");
    const strategyId = requiredText(body, "strategy_id");
    const text = requiredText(body, "text");
    const runId = randomUUID();

    const strategy = await repositories.ensureStrategy(userId, strategyId);
    if (!strategy || strategy.userId !== userId)
      throw httpError(404, "Strategy not found");
    await repositories.touchStrategy(strategyId);
    await repositories.createRun(runId, strategyId);

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
        await repositories.updateStrategyTitleIfBlank(
          strategyId,
          session.title,
        );
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
      await repositories.markRunFailed(runId, errorMessage(promptError));
    } else {
      await repositories.markRunCompleted(runId, replyText(result!.parts));
    }

    const run = await repositories.readRun(runId);
    if (!run) throw new Error(`Run missing after execution: ${runId}`);
    if (promptError) throw promptError;
    return runResponse(run);
  });

  app.get<{ Params: { id: string } }>("/runs/:id", async (request) => {
    const run = await repositories.readRun(request.params.id);
    if (!run) throw httpError(404, "Run not found");
    return runResponse(run);
  });

  app.get<{ Params: { "*": string } }>(
    "/artifacts/*",
    async (request, reply) => {
      const dir = artifactsDir();
      if (!dir) throw httpError(500, "STORAGE_ROOT is not configured");

      const requested = request.params["*"] ?? "";
      if (!requested) throw httpError(404, "Artifact not found");

      const resolved = path.resolve(dir, requested);
      const dirWithSeparator = dir.endsWith(path.sep)
        ? dir
        : `${dir}${path.sep}`;
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
    },
  );

  app.post("/messages/stream", async (request, reply) => {
    const body = (request.body ?? {}) as Record<string, unknown>;
    const userId = requiredText(body, "user_id");
    const strategyId = requiredText(body, "strategy_id");
    const text = requiredText(body, "text");
    const runId = randomUUID();

    const strategy = await repositories.ensureStrategy(userId, strategyId);
    if (!strategy || strategy.userId !== userId)
      throw httpError(404, "Strategy not found");
    await repositories.touchStrategy(strategyId);
    await repositories.createRun(runId, strategyId);

    request.log.info(
      { runId, strategyId },
      "stream: resolving opencode session",
    );
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
        await repositories.updateStrategyTitleIfBlank(
          strategyId,
          session.title,
        );
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
      await repositories.markRunFailed(runId, errorMessage(promptError));
    } else {
      await repositories.markRunCompleted(runId, replyText(result!.parts));
    }

    send("run.finalizing", {
      message: "Creating structured report and charts",
      run_id: runId,
    });

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
    const filesystemArtifacts =
      artifactsFromAbsolutePaths(changedAbsolutePaths);
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

    const run = await repositories.readRun(runId);
    if (run) {
      const structuredResult =
        (await readFinalizedStructuredResult(artifacts)) ??
        parseStrategyResultBlock(run.reply ?? "") ??
        (await structuredResultFromArtifacts(run.reply ?? "", artifacts));
      const enrichedStructuredResult =
        await enrichStructuredResultFromArtifacts(structuredResult, artifacts);
      finalize(runResponse(run, enrichedStructuredResult), artifacts);
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
