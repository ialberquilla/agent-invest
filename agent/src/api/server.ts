import "../env";
import { randomUUID, timingSafeEqual } from "node:crypto";
import { createReadStream, existsSync } from "node:fs";
import { readFile, readdir, rm, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Fastify from "fastify";
import type { Notification } from "pg";
import pino from "pino";

import {
  buildAllocationWizardPrompt,
  type AllocationWizardParams,
} from "../agent/prompt";
import { chatAgent, type ChatAgentResponse } from "../agent/chat";
import { createOpencodeClient } from "../agent/session";
import { runWorkflow } from "../agent/workflow/controller";
import { createOpencodeLLMClient, type LLMClient } from "../agent/workflow/llm";
import { workflowStateToStructuredResult } from "../agent/workflow/persist";
import type {
  WizardBrief,
  WorkflowState,
} from "../agent/workflow/state";
import { pgPool } from "../db/client";
import {
  listStageEventsByRunId as defaultListStageEventsByRunId,
  subscribeAgentEvents as defaultSubscribeAgentEvents,
  type ListStageEventsFilters,
} from "../db/repositories/agent-events";
import {
  createRun as defaultCreateRun,
  markRunCompleted as defaultMarkRunCompleted,
  markRunFailed as defaultMarkRunFailed,
  readRun as defaultReadRun,
  type RunRow,
} from "../db/repositories/runs";
import {
  getStageRun as defaultGetStageRun,
  listStageRunsByRunId as defaultListStageRunsByRunId,
} from "../db/repositories/stage-runs";
import {
  getEvalRun as defaultGetEvalRun,
  listEvalRuns as defaultListEvalRuns,
} from "../db/repositories/stage-eval-runs";
import type { StageRun } from "../db/schema";
import {
  ensureStrategy as defaultEnsureStrategy,
  claimStrategies as defaultClaimStrategies,
  touchStrategy as defaultTouchStrategy,
} from "../db/repositories/strategies";
import { readMandatesForRun as defaultReadMandatesForRun } from "../db/repositories/strategy-mandates";
import {
  bindVaultToMandate as defaultBindVaultToMandate,
  readVaultForMandate as defaultReadVaultForMandate,
} from "../db/repositories/vaults";
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
import { isStorageDisabled } from "../storage/local";

type Repositories = {
  createRun: typeof defaultCreateRun;
  ensureStrategy: typeof defaultEnsureStrategy;
  claimStrategies: typeof defaultClaimStrategies;
  markRunCompleted: typeof defaultMarkRunCompleted;
  markRunFailed: typeof defaultMarkRunFailed;
  getStageRun: typeof defaultGetStageRun;
  getEvalRun: typeof defaultGetEvalRun;
  listEvalRuns: typeof defaultListEvalRuns;
  listStageEventsByRunId: typeof defaultListStageEventsByRunId;
  listStageRunsByRunId: typeof defaultListStageRunsByRunId;
  readRun: typeof defaultReadRun;
  touchStrategy: typeof defaultTouchStrategy;
  readMandatesForRun: typeof defaultReadMandatesForRun;
  bindVaultToMandate: typeof defaultBindVaultToMandate;
  readVaultForMandate: typeof defaultReadVaultForMandate;
};
type IngestionRunners = {
  gmx: (options: GmxHistoryOptions) => Promise<GmxHistorySummary>;
  coingeckoMarketCaps: (
    options: CoinGeckoMarketCapOptions,
  ) => Promise<CoinGeckoMarketCapSummary>;
};
type StageRunNotification = {
  round: number;
  run_id: string;
  stage: string;
  stage_run_id: string;
  status: string;
};
type SubscribeToStageRunChanges = (
  onDelta: (delta: StageRunNotification) => void,
) => Promise<() => Promise<void>>;
type ServerDependencies = {
  runWorkflow?: typeof runWorkflow;
  llm?: LLMClient;
  subscribeAgentEvents?: typeof defaultSubscribeAgentEvents;
  chatAgent?: {
    run(input: {
      chatSessionId: string;
      userId: string;
      message: string;
    }): Promise<ChatAgentResponse>;
  };
  apiKey?: string | null;
  ingestionRunners?: Partial<IngestionRunners>;
  repositories?: Partial<Repositories>;
  subscribeToStageRunChanges?: SubscribeToStageRunChanges;
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

function isProduction() {
  return process.env.NODE_ENV === "production";
}

function configuredAgentApiKey() {
  return process.env.AGENT_API_KEY?.trim();
}

function secureEquals(left: string, right: string) {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);
  return (
    leftBuffer.length === rightBuffer.length &&
    timingSafeEqual(leftBuffer, rightBuffer)
  );
}

function headerValue(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value;
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

function requiredNumber(body: Record<string, unknown>, key: string) {
  const value = body[key];
  const parsed = typeof value === "string" ? Number(value) : value;
  if (typeof parsed !== "number" || !Number.isFinite(parsed)) {
    throw httpError(400, `Request body field '${key}' must be a number`);
  }
  return parsed;
}

function requiredChoice<T extends readonly string[]>(
  body: Record<string, unknown>,
  key: string,
  choices: T,
): T[number] {
  const value = body[key];
  if (typeof value === "string" && choices.includes(value)) return value;
  throw httpError(400, `Request body field '${key}' is invalid`);
}

function requiredStringArray(body: Record<string, unknown>, key: string) {
  const value = body[key];
  if (
    Array.isArray(value) &&
    value.every((entry) => typeof entry === "string")
  ) {
    return value;
  }
  throw httpError(400, `Request body field '${key}' must be a string array`);
}

function parseAllocationWizardParams(value: unknown): AllocationWizardParams {
  if (!isRecord(value)) {
    throw httpError(
      400,
      "Request body field 'wizard_params' must be an object",
    );
  }

  const initialCapitalUsd = value.initialCapitalUsd;
  if (typeof initialCapitalUsd !== "string") {
    throw httpError(
      400,
      "Request body field 'wizard_params.initialCapitalUsd' must be a string",
    );
  }

  return {
    universe: requiredChoice(value, "universe", [
      "top10",
      "top25",
      "top50",
      "all",
    ] as const),
    exclusions: requiredStringArray(value, "exclusions"),
    minimumMarketCap: requiredChoice(value, "minimumMarketCap", [
      "none",
      "100m",
      "500m",
      "1b",
      "10b",
    ] as const),
    concentrationLimit: requiredChoice(value, "concentrationLimit", [
      "20",
      "30",
      "agent",
    ] as const),
    maxDrawdown: requiredChoice(value, "maxDrawdown", [
      "10",
      "20",
      "35",
      "50",
      "moreThan50",
    ] as const),
    riskPreference: requiredChoice(value, "riskPreference", [
      "preserve",
      "balanced",
      "aggressive",
      "maxUpside",
    ] as const),
    horizon: requiredChoice(value, "horizon", [
      "3m",
      "6m",
      "1y",
      "3yPlus",
    ] as const),
    rebalance: requiredChoice(value, "rebalance", [
      "none",
      "monthly",
      "weekly",
      "agent",
    ] as const),
    initialCapitalUsd,
    cashAllocation: requiredChoice(value, "cashAllocation", [
      "none",
      "10",
      "25",
      "agent",
    ] as const),
    targetAssets: requiredChoice(value, "targetAssets", [
      "3-5",
      "5-10",
      "10-20",
      "agent",
    ] as const),
  };
}

function resolveMessageText(body: Record<string, unknown>) {
  if (body.wizard_params !== undefined) {
    return buildAllocationWizardPrompt(
      parseAllocationWizardParams(body.wizard_params),
    );
  }
  return requiredText(body, "text");
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

function optionalPositiveNumber(
  body: Record<string, unknown>,
  key: string,
  fallback: number,
) {
  const value = body[key];
  if (value === undefined || value === null) return fallback;
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    throw httpError(
      400,
      `Request body field '${key}' must be a positive number`,
    );
  }
  return value;
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

const STAGE_NAMES = ["thesis", "designer", "adjudicator", "reporter"] as const;
type StageName = (typeof STAGE_NAMES)[number];

function queryStringValue(
  query: Record<string, unknown>,
  key: string,
): string | undefined {
  const value = query[key];
  if (value === undefined) return undefined;
  if (typeof value !== "string")
    throw httpError(400, `Query param '${key}' is invalid`);
  return value;
}

function parseStageEventsFilters(
  query: Record<string, unknown>,
): ListStageEventsFilters {
  const filters: ListStageEventsFilters = {};
  const stage = queryStringValue(query, "stage");
  if (stage !== undefined) {
    if (!STAGE_NAMES.includes(stage as StageName)) {
      throw httpError(400, "Query param 'stage' is invalid");
    }
    filters.stage = stage;
  }

  const rawRound = queryStringValue(query, "round");
  if (rawRound !== undefined) {
    const round = Number.parseInt(rawRound, 10);
    if (!/^\d+$/.test(rawRound) || round < 1 || round > 3) {
      throw httpError(400, "Query param 'round' is invalid");
    }
    filters.round = round;
  }

  return filters;
}

function parseEvalRunsFilters(query: Record<string, unknown>) {
  const stage = queryStringValue(query, "stage");
  if (stage !== undefined && !STAGE_NAMES.includes(stage as StageName)) {
    throw httpError(400, "Query param 'stage' is invalid");
  }

  const fixtureId = queryStringValue(query, "fixture_id");
  const rawLimit = queryStringValue(query, "limit");
  let limit = 50;
  if (rawLimit !== undefined) {
    limit = Number.parseInt(rawLimit, 10);
    if (!/^\d+$/.test(rawLimit) || limit < 1 || limit > 200) {
      throw httpError(400, "Query param 'limit' is invalid");
    }
  }

  return { stage, fixtureId, limit };
}

function evalRunSummaryResponse(
  evalRun: Awaited<ReturnType<typeof defaultListEvalRuns>>[number],
) {
  return {
    eval_run_id: evalRun.evalRunId,
    stage: evalRun.stage,
    fixture_id: evalRun.fixtureId,
    model: evalRun.model,
    passed: evalRun.passed,
    score: evalRun.score,
    duration_ms: evalRun.durationMs,
    created_at: toIso(evalRun.createdAt),
  };
}

async function fixtureExpectations(stage: string, fixtureId: string) {
  let current = path.dirname(fileURLToPath(import.meta.url));
  let fixturePath: string | null = null;
  while (true) {
    const candidate = path.join(
      current,
      "evals",
      "fixtures",
      stage,
      `${fixtureId}.json`,
    );
    if (existsSync(candidate)) {
      fixturePath = candidate;
      break;
    }
    const parent = path.dirname(current);
    if (parent === current) break;
    current = parent;
  }

  if (!fixturePath) return null;
  try {
    const payload = JSON.parse(await readFile(fixturePath, "utf-8")) as unknown;
    return isRecord(payload) ? (payload.expectations ?? null) : null;
  } catch {
    return null;
  }
}

async function evalRunDetailResponse(
  evalRun: NonNullable<Awaited<ReturnType<typeof defaultGetEvalRun>>>,
) {
  return {
    ...evalRunSummaryResponse(evalRun),
    diagnostics: evalRun.diagnostics,
    output: evalRun.output,
    expectations: await fixtureExpectations(evalRun.stage, evalRun.fixtureId),
  };
}

function stageEventResponse(
  event: Awaited<ReturnType<typeof defaultListStageEventsByRunId>>[number],
) {
  return {
    event_id: event.eventId,
    event_type: event.eventType,
    payload: event.payload,
    created_at: toIso(event.createdAt),
  };
}

function stageRunSummaryResponse(stageRun: StageRun) {
  return {
    stage_run_id: stageRun.stageRunId,
    stage: stageRun.stage,
    round: stageRun.round,
    status: stageRun.status,
    started_at: toIso(stageRun.startedAt),
    ended_at: toIso(stageRun.endedAt),
    model: stageRun.model,
    tokens: {
      input: stageRun.tokensIn,
      output: stageRun.tokensOut,
    },
  };
}

function stageRunDetailResponse(stageRun: StageRun) {
  return {
    ...stageRunSummaryResponse(stageRun),
    run_id: stageRun.runId,
    opencode_session_id: stageRun.opencodeSessionId,
    input: stageRun.input,
    output: stageRun.output,
    error: stageRun.error,
  };
}

function isStageRunNotification(value: unknown): value is StageRunNotification {
  return (
    isRecord(value) &&
    typeof value.run_id === "string" &&
    typeof value.stage_run_id === "string" &&
    typeof value.stage === "string" &&
    typeof value.status === "string" &&
    typeof value.round === "number"
  );
}

function sseFrame(event: string, data: unknown) {
  return `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
}

async function subscribeToStageRunChanges(
  onDelta: (delta: StageRunNotification) => void,
) {
  const client = await pgPool.connect();

  const onNotification = (notification: Notification) => {
    if (notification.channel !== "stage_runs_changed") return;
    if (!notification.payload) return;

    try {
      const payload = JSON.parse(notification.payload) as unknown;
      if (isStageRunNotification(payload)) onDelta(payload);
    } catch {
      // Ignore malformed notifications from this shared channel.
    }
  };

  client.on("notification", onNotification);
  try {
    await client.query("LISTEN stage_runs_changed");
  } catch (error) {
    client.off("notification", onNotification);
    client.release();
    throw error;
  }

  return async () => {
    client.off("notification", onNotification);
    try {
      await client.query("UNLISTEN stage_runs_changed");
    } finally {
      client.release();
    }
  };
}

function runResponse(
  run: RunRow,
  structuredResult: unknown = null,
  stages?: StageRun[],
) {
  const response = {
    run_id: run.runId,
    status: run.status,
    started_at: toIso(run.startedAt),
    ended_at: toIso(run.endedAt),
    exit_code: run.exitCode,
    reply: run.reply,
    error: run.error,
  };
  const responseWithStages = stages
    ? { ...response, stages: stages.map(stageRunSummaryResponse) }
    : response;

  return structuredResult === null
    ? responseWithStages
    : { ...responseWithStages, structured_result: structuredResult };
}

// Content types for artifacts served from disk via GET /artifacts/*.
// The workflow no longer writes new artifacts, but pre-existing
// artifacts (e.g. from older runs) can still be served as long as the
// storage dir is mounted.
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

function resolveStorageRoot() {
  if (isStorageDisabled()) return undefined;

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

function storageRootDir() {
  return resolveStorageRoot();
}

// Pulls a short user-facing reply out of the structured workflow
// result. Used as the `reply` column on the runs row when marking the
// run completed. Falls back to a stable placeholder when no
// structured result is available.
function replyFromStructuredResult(structuredResult: unknown): string {
  if (!isRecord(structuredResult)) return "Strategy run completed.";
  const parts: string[] = [];
  if (typeof structuredResult.title === "string" && structuredResult.title) {
    parts.push(structuredResult.title);
  }
  if (typeof structuredResult.summary === "string" && structuredResult.summary) {
    parts.push(structuredResult.summary);
  }
  return parts.join("\n\n") || "Strategy run completed.";
}

async function cleanupArtifactDirectory(
  dir: string,
  cutoffMs: number,
): Promise<{ deletedBytes: number; deletedFiles: number }> {
  let deletedBytes = 0;
  let deletedFiles = 0;

  async function visit(current: string): Promise<boolean> {
    const entries = await readdir(current, { withFileTypes: true }).catch(
      (error) => {
        if (
          typeof error === "object" &&
          error !== null &&
          "code" in error &&
          error.code === "ENOENT"
        ) {
          return [];
        }
        throw error;
      },
    );
    let isEmpty = true;

    for (const entry of entries) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) {
        const childEmpty = await visit(full);
        if (childEmpty && full !== dir) {
          await rm(full, { force: true, recursive: true });
        } else isEmpty = false;
        continue;
      }

      if (!entry.isFile()) {
        isEmpty = false;
        continue;
      }

      const info = await stat(full).catch(() => null);
      if (!info) continue;
      if (info.mtimeMs >= cutoffMs) {
        isEmpty = false;
        continue;
      }

      await rm(full, { force: true });
      deletedBytes += info.size;
      deletedFiles += 1;
    }

    return isEmpty;
  }

  await visit(dir);
  return { deletedBytes, deletedFiles };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function runSummaryFields(structuredResult: unknown) {
  if (!isRecord(structuredResult)) return undefined;
  return {
    winnerTemplateId:
      typeof structuredResult.template_id === "string"
        ? structuredResult.template_id
        : null,
    winnersByDimension: structuredResult.winners_by_dimension ?? null,
    roundHistory: structuredResult.round_history ?? null,
    refinementReasons: structuredResult.refinement_reasons ?? null,
    metadata: {
      candidate_batch_id: isRecord(structuredResult.backtest)
        ? (structuredResult.backtest.candidate_batch_id ?? null)
        : null,
      winner_candidate_id: structuredResult.winner_candidate_id ?? null,
      // Persist the full structured result so a page reload / the run
      // inspector can recover the report card (KPIs + charts) without
      // re-running the workflow. The streaming path only delivers it
      // over SSE otherwise.
      structured_result: structuredResult,
    },
  };
}

export function buildServer(dependencies: ServerDependencies = {}) {
  const executeWorkflow = dependencies.runWorkflow ?? runWorkflow;
  const workflowLLM = dependencies.llm ?? createOpencodeLLMClient();
  const subscribeAgentEvents =
    dependencies.subscribeAgentEvents ?? defaultSubscribeAgentEvents;
  const executeChatAgent = dependencies.chatAgent ?? chatAgent;
  const subscribeStageRunChanges =
    dependencies.subscribeToStageRunChanges ?? subscribeToStageRunChanges;
  const repositories: Repositories = {
    createRun: defaultCreateRun,
    ensureStrategy: defaultEnsureStrategy,
    claimStrategies: defaultClaimStrategies,
    markRunCompleted: defaultMarkRunCompleted,
    markRunFailed: defaultMarkRunFailed,
    getEvalRun: defaultGetEvalRun,
    getStageRun: defaultGetStageRun,
    listEvalRuns: defaultListEvalRuns,
    listStageEventsByRunId: defaultListStageEventsByRunId,
    listStageRunsByRunId: defaultListStageRunsByRunId,
    readRun: defaultReadRun,
    touchStrategy: defaultTouchStrategy,
    readMandatesForRun: defaultReadMandatesForRun,
    bindVaultToMandate: defaultBindVaultToMandate,
    readVaultForMandate: defaultReadVaultForMandate,
    ...dependencies.repositories,
  };
  const ingestionRunners: IngestionRunners = {
    gmx: runGmxHistoryIngestion,
    coingeckoMarketCaps: runCoinGeckoMarketCapIngestion,
    ...dependencies.ingestionRunners,
  };
  const runningIngestions = new Set<string>();
  const app = Fastify({ loggerInstance: pino() });
  const apiKey =
    dependencies.apiKey === undefined
      ? process.env.NODE_ENV === "test"
        ? undefined
        : configuredAgentApiKey()
      : (dependencies.apiKey ?? undefined);

  app.addHook("onRequest", async (request) => {
    if (!apiKey) {
      if (isProduction()) throw httpError(500, "AGENT_API_KEY is not set");
      return;
    }

    const requestApiKey = headerValue(request.headers["x-api-key"])?.trim();
    if (!requestApiKey || !secureEquals(requestApiKey, apiKey)) {
      throw httpError(401, "Unauthorized");
    }
  });

  app.get("/health", async () => ({ ok: true }));

  app.post<{ Params: { id: string } }>("/users/:id/claim", async (request) => {
    const body = (request.body ?? {}) as Record<string, unknown>;
    if (!isRecord(body)) {
      throw httpError(400, "Request body must be a JSON object");
    }
    const targetUserId = request.params.id.trim();
    const anonymousUserId = requiredText(body, "anonymous_user_id");
    if (!targetUserId.startsWith("privy:")) {
      throw httpError(400, "Target user must be a Privy user");
    }
    if (!anonymousUserId.startsWith("anon:")) {
      throw httpError(400, "Anonymous user id is invalid");
    }

    const claimed = await repositories.claimStrategies(
      targetUserId,
      anonymousUserId,
    );
    return { claimed };
  });

  app.get<{
    Querystring: { stage?: string; fixture_id?: string; limit?: string };
  }>("/dev/evals", async (request) => {
    const filters = parseEvalRunsFilters(request.query);
    const evalRuns = await repositories.listEvalRuns(filters);
    return evalRuns.map(evalRunSummaryResponse);
  });

  app.get<{ Params: { id: string } }>("/dev/evals/:id", async (request) => {
    const evalRun = await repositories.getEvalRun(request.params.id);
    if (!evalRun) throw httpError(404, "Eval run not found");
    return evalRunDetailResponse(evalRun);
  });

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

  // Bind a deployed StrategyVault to the run's finalized mandate and promote it
  // to `active`. The on-chain deploy happens client-side (the broadcast is wired
  // later via Privy); this just persists the resulting address + chain.
  app.post<{ Params: { id: string } }>(
    "/runs/:id/vault",
    async (request) => {
      const runId = request.params.id;
      const body = (request.body ?? {}) as Record<string, unknown>;
      const chainId = requiredNumber(body, "chain_id");
      const vaultAddress = requiredText(body, "vault_address");
      const assetAddress = requiredText(body, "asset_address");

      const mandates = await repositories.readMandatesForRun(runId);
      const mandate = mandates[0];
      if (!mandate)
        throw httpError(404, "No strategy mandate for this run yet");

      const existing = await repositories.readVaultForMandate(
        mandate.mandateId,
      );
      if (existing)
        throw httpError(409, "This strategy is already bound to a vault");

      await repositories.bindVaultToMandate({
        chainId,
        vaultAddress,
        mandateId: mandate.mandateId,
        assetAddress,
      });

      return {
        mandate_id: mandate.mandateId,
        chain_id: chainId,
        vault_address: vaultAddress,
        asset_address: assetAddress,
        status: "active",
      };
    },
  );

  app.post("/messages", async (request) => {
    const body = (request.body ?? {}) as Record<string, unknown>;
    const userId = requiredText(body, "user_id");
    const strategyId = requiredText(body, "strategy_id");
    const text = resolveMessageText(body);
    const runId = randomUUID();

    const strategy = await repositories.ensureStrategy(userId, strategyId);
    if (!strategy || strategy.userId !== userId)
      throw httpError(404, "Strategy not found");
    await repositories.touchStrategy(strategyId);
    await repositories.createRun(runId, strategyId);

    let workflowError: unknown;
    let workflowState: WorkflowState | undefined;
    try {
      request.log.info({ runId, strategyId }, "calling workflow");
      workflowState = await executeWorkflow(runId, text as string | WizardBrief, {
        llm: workflowLLM,
      });
      request.log.info(
        { runId, final_kind: workflowState.final?.kind ?? null },
        "workflow returned",
      );
    } catch (error) {
      workflowError = error;
    }

    const structuredResult = workflowState
      ? workflowStateToStructuredResult(workflowState)
      : null;

    if (workflowError) {
      await repositories.markRunFailed(runId, errorMessage(workflowError));
    } else {
      await repositories.markRunCompleted(
        runId,
        replyFromStructuredResult(structuredResult),
        runSummaryFields(structuredResult),
      );
    }

    const run = await repositories.readRun(runId);
    if (!run) throw new Error(`Run missing after execution: ${runId}`);
    if (workflowError) throw workflowError;
    return runResponse(run, structuredResult);
  });

  app.post("/chat/messages", async (request) => {
    const body = (request.body ?? {}) as Record<string, unknown>;
    if (!isRecord(body)) {
      throw httpError(400, "Request body must be a JSON object");
    }

    const chatSessionId = requiredText(body, "chat_session_id");
    const message = requiredText(body, "message");
    const userId =
      typeof body.user_id === "string" && body.user_id.trim()
        ? body.user_id.trim()
        : chatSessionId;

    const response = await executeChatAgent.run({
      chatSessionId,
      message,
      userId,
    });

    return {
      chat_session_id: chatSessionId,
      content: response.content,
      opencode_session_id: response.opencode_session_id,
      ...(response.run_id ? { run_id: response.run_id } : {}),
    };
  });

  app.get<{ Params: { id: string } }>("/runs/:id", async (request) => {
    const run = await repositories.readRun(request.params.id);
    if (!run) throw httpError(404, "Run not found");
    const stages = await repositories.listStageRunsByRunId(request.params.id);
    return runResponse(run, null, stages);
  });

  app.get<{ Params: { id: string } }>(
    "/runs/:id/stream",
    async (request, reply) => {
      const run = await repositories.readRun(request.params.id);
      if (!run) throw httpError(404, "Run not found");

      const stages = await repositories.listStageRunsByRunId(request.params.id);
      const cleanup = await subscribeStageRunChanges(
        (delta: StageRunNotification) => {
          if (delta.run_id !== request.params.id) return;
          reply.raw.write(sseFrame("delta", delta));
        },
      );
      let cleanedUp = false;
      const close = async () => {
        if (cleanedUp) return;
        cleanedUp = true;
        clearInterval(heartbeat);
        await cleanup().catch((error: unknown) => {
          request.log.warn(
            { error },
            "failed to clean up stage stream listener",
          );
        });
      };
      const heartbeat = setInterval(() => {
        reply.raw.write(": heartbeat\n\n");
      }, 15_000);

      request.raw.on("aborted", close);
      reply.raw.on("close", close);
      reply.raw.on("error", close);

      reply.hijack();
      reply.raw.writeHead(200, {
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive",
        "Content-Type": "text/event-stream",
      });
      reply.raw.write(
        sseFrame("snapshot", stages.map(stageRunSummaryResponse)),
      );
    },
  );

  app.get<{
    Params: { id: string };
    Querystring: { stage?: string; round?: string };
  }>("/runs/:id/events", async (request) => {
    const run = await repositories.readRun(request.params.id);
    if (!run) throw httpError(404, "Run not found");

    const filters = parseStageEventsFilters(request.query);
    const events = await repositories.listStageEventsByRunId(
      request.params.id,
      filters,
    );
    return events.map(stageEventResponse);
  });

  // SSE channel for agent_events writes. The frontend opens one of
  // these per run page and stops polling /events entirely. We send a
  // `snapshot` event with everything written so far, then `event`
  // frames as new rows are appended via the in-process emitter in
  // agent-events.ts.
  app.get<{ Params: { id: string } }>(
    "/runs/:id/events/stream",
    async (request, reply) => {
      const runId = request.params.id;
      const run = await repositories.readRun(runId);
      if (!run) throw httpError(404, "Run not found");

      const snapshot = await repositories.listStageEventsByRunId(runId, {});

      reply.hijack();
      reply.raw.writeHead(200, {
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive",
        "Content-Type": "text/event-stream",
        "X-Accel-Buffering": "no",
      });
      reply.raw.write(sseFrame("snapshot", snapshot.map(stageEventResponse)));

      const heartbeat = setInterval(() => {
        if (!reply.raw.writableEnded) reply.raw.write(": heartbeat\n\n");
      }, 15_000);

      const unsubscribe = subscribeAgentEvents(runId, (event) => {
        if (reply.raw.writableEnded) return;
        reply.raw.write(sseFrame("event", stageEventResponse(event)));
      });

      let cleanedUp = false;
      const close = () => {
        if (cleanedUp) return;
        cleanedUp = true;
        clearInterval(heartbeat);
        unsubscribe();
      };

      request.raw.on("aborted", close);
      reply.raw.on("close", close);
      reply.raw.on("error", close);
    },
  );

  app.get<{ Params: { id: string; stage_run_id: string } }>(
    "/runs/:id/stages/:stage_run_id",
    async (request) => {
      const run = await repositories.readRun(request.params.id);
      if (!run) throw httpError(404, "Run not found");

      const stageRun = await repositories.getStageRun(
        request.params.stage_run_id,
      );
      if (!stageRun || stageRun.runId !== request.params.id) {
        throw httpError(404, "Stage run not found");
      }

      return stageRunDetailResponse(stageRun);
    },
  );

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

  app.post("/maintenance/storage/cleanup", async (request) => {
    const token = process.env.MAINTENANCE_TOKEN?.trim();
    if (token) {
      const authorization = request.headers.authorization;
      const headerToken = Array.isArray(request.headers["x-maintenance-token"])
        ? request.headers["x-maintenance-token"][0]
        : request.headers["x-maintenance-token"];
      const bearerToken = authorization?.startsWith("Bearer ")
        ? authorization.slice("Bearer ".length).trim()
        : undefined;
      if (bearerToken !== token && headerToken !== token) {
        throw httpError(401, "Unauthorized");
      }
    }

    const dir = storageRootDir();
    if (!dir) throw httpError(500, "STORAGE_ROOT is not configured");

    const body = (request.body ?? {}) as Record<string, unknown>;
    const maxAgeHours = optionalPositiveNumber(body, "max_age_hours", 24);
    const cutoffMs = Date.now() - maxAgeHours * 60 * 60 * 1000;
    const { deletedBytes, deletedFiles } = await cleanupArtifactDirectory(
      dir,
      cutoffMs,
    );

    request.log.info(
      { deletedBytes, deletedFiles, maxAgeHours, storageRoot: dir },
      "storage cleanup completed",
    );

    return {
      storage_root: dir,
      deleted_bytes: deletedBytes,
      deleted_files: deletedFiles,
      max_age_hours: maxAgeHours,
    };
  });

  app.post("/messages/stream", async (request, reply) => {
    const body = (request.body ?? {}) as Record<string, unknown>;
    const userId = requiredText(body, "user_id");
    const strategyId = requiredText(body, "strategy_id");
    const text = resolveMessageText(body);
    const runId = randomUUID();

    const strategy = await repositories.ensureStrategy(userId, strategyId);
    if (!strategy || strategy.userId !== userId)
      throw httpError(404, "Strategy not found");
    await repositories.touchStrategy(strategyId);
    await repositories.createRun(runId, strategyId);

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

    const finalize = (runPayload: Record<string, unknown>) => {
      if (runCompletedSent) return;
      runCompletedSent = true;
      send("run.completed", { ...runPayload, artifacts: [] });
      if (!clientGone && !raw.writableEnded) raw.end();
    };

    send("run.started", { run_id: runId });

    const sentStageEventIds = new Set<string>();
    const flushStageEvents = async () => {
      const events = await repositories.listStageEventsByRunId(runId);
      for (const event of events) {
        if (sentStageEventIds.has(event.eventId)) continue;
        sentStageEventIds.add(event.eventId);
        send(event.eventType, stageEventResponse(event));
      }
    };
    const stageEventPoller = setInterval(() => {
      void flushStageEvents().catch((error: unknown) => {
        request.log.warn(
          { error: errorMessage(error), runId },
          "stream: failed to flush stage events",
        );
      });
    }, 500);

    let workflowError: unknown;
    let workflowState: WorkflowState | undefined;
    try {
      request.log.info({ runId, strategyId }, "stream: calling workflow");
      workflowState = await executeWorkflow(runId, text as string | WizardBrief, {
        llm: workflowLLM,
      });
      request.log.info(
        { runId, final_kind: workflowState.final?.kind ?? null },
        "stream: workflow returned",
      );
    } catch (error) {
      workflowError = error;
    } finally {
      clearInterval(stageEventPoller);
      await flushStageEvents().catch((error: unknown) => {
        request.log.warn(
          { error: errorMessage(error), runId },
          "stream: failed to flush final stage events",
        );
      });
    }

    send("run.finalizing", {
      message: "Assembling structured result",
      run_id: runId,
    });

    const structuredResult = workflowState
      ? workflowStateToStructuredResult(workflowState)
      : null;

    if (workflowError) {
      await repositories.markRunFailed(runId, errorMessage(workflowError));
    } else {
      await repositories.markRunCompleted(
        runId,
        replyFromStructuredResult(structuredResult),
        runSummaryFields(structuredResult),
      );
    }

    const run = await repositories.readRun(runId);
    if (run) {
      finalize(runResponse(run, structuredResult));
    } else {
      finalize({
        run_id: runId,
        status: "failed",
        error: "Run missing after execution",
      });
    }
  });

  return app;
}

export async function startServer() {
  const app = buildServer();
  try {
    await createOpencodeClient();
    await app.listen({ host: process.env.HOST ?? "0.0.0.0", port: getPort() });
  } catch (error) {
    app.log.error(error);
    process.exitCode = 1;
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1])
  void startServer();
