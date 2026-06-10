import "../env";
import { randomUUID, timingSafeEqual } from "node:crypto";
import { createReadStream, existsSync } from "node:fs";
import { readFile, readdir, rm, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Fastify, { type FastifyReply, type FastifyRequest } from "fastify";
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
import { buildMandate } from "../agent/workflow/mandate";
import { workflowStateToStructuredResult } from "../agent/workflow/persist";
import type {
  FinalWinner,
  StrategyRunOverrides,
  WizardBrief,
  WorkflowState,
} from "../agent/workflow/state";
import { pgPool } from "../db/client";
import {
  screenMarkets as defaultScreenMarkets,
  type ScreenMarketsInput,
} from "../tools/screen-markets";
import { runResearchCode as defaultRunResearchCode } from "../tools/run-research-code";
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
import {
  insertMandate as defaultInsertMandate,
  readMandatesForRun as defaultReadMandatesForRun,
} from "../db/repositories/strategy-mandates";
import {
  bindVaultToMandate as defaultBindVaultToMandate,
  readVaultForMandate as defaultReadVaultForMandate,
} from "../db/repositories/vaults";
import {
  listGmxTradeableCoins as defaultListGmxTradeableCoins,
  resolveMarketsBatch as defaultResolveMarketsBatch,
} from "../db/repositories/gmx-markets";
import {
  deletePinnedScreener as defaultDeletePinnedScreener,
  listPinnedScreeners as defaultListPinnedScreeners,
  markPinnedScreenerRefreshed as defaultMarkPinnedScreenerRefreshed,
  readPinnedScreener as defaultReadPinnedScreener,
  upsertPinnedScreener as defaultUpsertPinnedScreener,
  type PinnedScreenerDefinition,
} from "../db/repositories/pinned-screeners";
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
  insertMandate: typeof defaultInsertMandate;
  bindVaultToMandate: typeof defaultBindVaultToMandate;
  readVaultForMandate: typeof defaultReadVaultForMandate;
  resolveMarketsBatch: typeof defaultResolveMarketsBatch;
  listGmxTradeableCoins: typeof defaultListGmxTradeableCoins;
  listPinnedScreeners: typeof defaultListPinnedScreeners;
  upsertPinnedScreener: typeof defaultUpsertPinnedScreener;
  readPinnedScreener: typeof defaultReadPinnedScreener;
  deletePinnedScreener: typeof defaultDeletePinnedScreener;
  markPinnedScreenerRefreshed: typeof defaultMarkPinnedScreenerRefreshed;
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
    runStream?(
      input: { chatSessionId: string; userId: string; message: string },
      onEvent: (event: unknown) => void | Promise<void>,
    ): Promise<ChatAgentResponse>;
    runStrategyPipeline?(
      input: {
        brief: string;
        based_on_run_id?: string;
        overrides?: StrategyRunOverrides;
        data_as_of?: string;
      },
      chatSessionId?: string,
    ): Promise<{ run_id: string }>;
  };
  screenMarkets?: typeof defaultScreenMarkets;
  apiKey?: string | null;
  ingestionRunners?: Partial<IngestionRunners>;
  repositories?: Partial<Repositories>;
  subscribeToStageRunChanges?: SubscribeToStageRunChanges;
};

const ARBITRUM_ONE_CHAIN_ID = 42161;
const ARBITRUM_SEPOLIA_CHAIN_ID = 421614;

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

// Shape-validate the optional `overrides` object from a strategy-pipeline
// request. Type checks only -- feasibility (e.g. asset_count_min <= max)
// is enforced later by applyOverrides against the interpreted thesis.
// Returns undefined when no overrides were sent.
function parseStrategyRunOverrides(
  raw: unknown,
): StrategyRunOverrides | undefined {
  if (raw === undefined || raw === null) return undefined;
  if (!isRecord(raw)) {
    throw httpError(400, "Request body field 'overrides' must be an object");
  }

  const out: Record<string, unknown> = {};
  const numberFields = [
    "asset_count_min",
    "asset_count_max",
    "max_weight_per_asset",
    "max_cash_weight",
    "max_drawdown",
    "horizon_days",
    "top_n",
    "top_skip",
  ] as const;
  for (const key of numberFields) {
    if (raw[key] === undefined) continue;
    const value =
      typeof raw[key] === "string" ? Number(raw[key]) : raw[key];
    if (typeof value !== "number" || !Number.isFinite(value)) {
      throw httpError(400, `overrides.${key} must be a number`);
    }
    out[key] = value;
  }

  for (const key of ["exclude_stablecoins", "exclude_wrapped"] as const) {
    if (raw[key] === undefined) continue;
    if (typeof raw[key] !== "boolean") {
      throw httpError(400, `overrides.${key} must be a boolean`);
    }
    out[key] = raw[key];
  }

  if (raw.rebalance_frequency !== undefined) {
    const allowed = ["daily", "weekly", "monthly", "quarterly"];
    if (!allowed.includes(raw.rebalance_frequency as string)) {
      throw httpError(
        400,
        `overrides.rebalance_frequency must be one of ${allowed.join(", ")}`,
      );
    }
    out.rebalance_frequency = raw.rebalance_frequency;
  }

  if (raw.hand_picked_coin_ids !== undefined) {
    if (
      !Array.isArray(raw.hand_picked_coin_ids) ||
      !raw.hand_picked_coin_ids.every((id) => typeof id === "string")
    ) {
      throw httpError(
        400,
        "overrides.hand_picked_coin_ids must be an array of strings",
      );
    }
    out.hand_picked_coin_ids = raw.hand_picked_coin_ids;
  }

  if (raw.strategy_mode !== undefined) {
    const allowed = [
      "single_asset",
      "pair_trade",
      "hedge_overlay",
      "basket_allocation",
      "momentum_rotation",
      "long_short_portfolio",
    ];
    if (!allowed.includes(raw.strategy_mode as string)) {
      throw httpError(
        400,
        `overrides.strategy_mode must be one of ${allowed.join(", ")}`,
      );
    }
    out.strategy_mode = raw.strategy_mode;
  }

  if (raw.target_coin_id !== undefined) {
    if (typeof raw.target_coin_id !== "string" || !raw.target_coin_id.trim()) {
      throw httpError(400, "overrides.target_coin_id must be a non-empty string");
    }
    out.target_coin_id = raw.target_coin_id.trim();
  }

  if (raw.allowed_sides !== undefined) {
    const allowed = ["long_only", "long_flat", "long_short"];
    if (!allowed.includes(raw.allowed_sides as string)) {
      throw httpError(
        400,
        `overrides.allowed_sides must be one of ${allowed.join(", ")}`,
      );
    }
    out.allowed_sides = raw.allowed_sides;
  }

  for (const key of ["long_coin_ids", "short_coin_ids"] as const) {
    if (raw[key] === undefined) continue;
    if (
      !Array.isArray(raw[key]) ||
      !(raw[key] as unknown[]).every((id) => typeof id === "string")
    ) {
      throw httpError(400, `overrides.${key} must be an array of strings`);
    }
    out[key] = raw[key];
  }

  return Object.keys(out).length > 0
    ? (out as StrategyRunOverrides)
    : undefined;
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

function optionalPositiveInteger(
  body: Record<string, unknown>,
  key: string,
): number | undefined {
  const value = body[key];
  if (value === undefined || value === null) return undefined;
  const parsed = typeof value === "string" ? Number(value) : value;
  if (!Number.isInteger(parsed) || (parsed as number) < 1) {
    throw httpError(
      400,
      `Request body field '${key}' must be a positive integer`,
    );
  }
  return parsed as number;
}

function parseScreenMarketsInput(
  body: Record<string, unknown>,
): ScreenMarketsInput {
  const factor = body.factor;
  if (
    factor !== undefined &&
    factor !== "momentum" &&
    factor !== "risk_adjusted" &&
    factor !== "low_volatility"
  ) {
    throw httpError(400, "Request body field 'factor' is invalid");
  }

  const query = body.query;
  const asOf = body.asOf ?? body.as_of;
  const gmxOnly = body.gmxOnly ?? body.gmx_only;
  if (query !== undefined && typeof query !== "string") {
    throw httpError(400, "Request body field 'query' must be a string");
  }
  if (asOf !== undefined && typeof asOf !== "string") {
    throw httpError(400, "Request body field 'asOf' must be a string");
  }
  if (gmxOnly !== undefined && typeof gmxOnly !== "boolean") {
    throw httpError(400, "Request body field 'gmxOnly' must be a boolean");
  }
  const limit = optionalPositiveInteger(body, "limit");
  const timeoutSeconds = optionalPositiveInteger(body, "timeoutSeconds");

  return {
    ...(typeof query === "string" ? { query } : {}),
    ...(factor ? { factor } : {}),
    ...(gmxOnly !== undefined ? { gmxOnly } : {}),
    ...(typeof asOf === "string" ? { asOf } : {}),
    ...(limit !== undefined ? { limit } : {}),
    ...(timeoutSeconds !== undefined ? { timeoutSeconds } : {}),
  };
}

function parsePinnedScreenerDefinition(
  value: unknown,
): PinnedScreenerDefinition {
  if (!isRecord(value)) {
    throw httpError(400, "Request body field 'definition' must be an object");
  }
  const input = parseScreenMarketsInput(value);
  if (!input.factor) {
    throw httpError(400, "Screener definition must include 'factor'");
  }
  if (input.limit === undefined) {
    throw httpError(400, "Screener definition must include 'limit'");
  }
  if (input.gmxOnly === undefined) {
    throw httpError(400, "Screener definition must include 'gmx_only'");
  }

  return {
    factor: input.factor,
    limit: input.limit,
    gmx_only: input.gmxOnly,
    ...(input.asOf ? { as_of: input.asOf } : {}),
  };
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

function runStructuredResult(run: RunRow) {
  const metadata = isRecord(run.metadata) ? run.metadata : null;
  return metadata?.structured_result ?? null;
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
  if (
    typeof structuredResult.summary === "string" &&
    structuredResult.summary
  ) {
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

function workflowMetadata(workflowState: WorkflowState | undefined) {
  if (!workflowState) return {};
  return {
    brief: workflowState.brief,
    final: workflowState.final,
    thesis: workflowState.thesis,
    universe: workflowState.universe,
    window: workflowState.window,
    attempts: workflowState.attempts,
  };
}

function runSummaryFields(
  structuredResult: unknown,
  workflowState?: WorkflowState,
) {
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
      ...workflowMetadata(workflowState),
    },
  };
}

async function ensureMandateForRun(runId: string, repositories: Repositories) {
  const existing = await repositories.readMandatesForRun(runId);
  if (existing[0]) return existing[0];

  const run = await repositories.readRun(runId);
  if (!run) throw httpError(404, "Run not found");
  const metadata = isRecord(run.metadata) ? run.metadata : null;
  const final = isRecord(metadata?.final) ? metadata.final : null;
  if (final?.kind !== "winner") return null;

  const winner = final as FinalWinner;
  const state = {
    run_id: runId,
    thesis: winner.thesis,
    universe: winner.universe,
    window: winner.window,
    attempts: winner.attempts_summary,
  } as WorkflowState;
  const mandate = buildMandate(winner, state, {
    mandateId: randomUUID(),
  });
  if (!mandate) return null;

  await repositories.insertMandate(mandate);
  return (await repositories.readMandatesForRun(runId))[0] ?? null;
}

export function buildServer(dependencies: ServerDependencies = {}) {
  const executeWorkflow = dependencies.runWorkflow ?? runWorkflow;
  const workflowLLM = dependencies.llm ?? createOpencodeLLMClient();
  const subscribeAgentEvents =
    dependencies.subscribeAgentEvents ?? defaultSubscribeAgentEvents;
  const executeChatAgent = dependencies.chatAgent ?? chatAgent;
  const executeScreenMarkets =
    dependencies.screenMarkets ?? defaultScreenMarkets;
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
    insertMandate: defaultInsertMandate,
    bindVaultToMandate: defaultBindVaultToMandate,
    readVaultForMandate: defaultReadVaultForMandate,
    resolveMarketsBatch: defaultResolveMarketsBatch,
    listGmxTradeableCoins: defaultListGmxTradeableCoins,
    listPinnedScreeners: defaultListPinnedScreeners,
    upsertPinnedScreener: defaultUpsertPinnedScreener,
    readPinnedScreener: defaultReadPinnedScreener,
    deletePinnedScreener: defaultDeletePinnedScreener,
    markPinnedScreenerRefreshed: defaultMarkPinnedScreenerRefreshed,
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

  async function streamStrategyPipelineRun(
    request: FastifyRequest,
    reply: FastifyReply,
  ) {
    const body = (request.body ?? {}) as Record<string, unknown>;
    const userId = requiredText(body, "user_id");
    const strategyId = requiredText(body, "strategy_id");
    const text = resolveMessageText(body);
    // Deterministic thesis overrides (e.g. the wizard's strategy-type picker
    // selecting a single-asset / pair / long-short shape). Applied after
    // interpret_brief and re-validated, same as a structured rerun.
    const overrides = parseStrategyRunOverrides(body.overrides);
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
      workflowState = await executeWorkflow(
        runId,
        text as string | WizardBrief,
        {
          llm: workflowLLM,
        },
        { overrides },
      );
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
        runSummaryFields(structuredResult, workflowState),
      );
    }

    const run = await repositories.readRun(runId);
    if (!run) throw new Error(`Run missing after execution: ${runId}`);
    const payload = runResponse(run, structuredResult) as Record<
      string,
      unknown
    >;
    finalize(payload);
    if (workflowError) request.log.error({ workflowError, runId });
  }

  app.get("/health", async () => ({ ok: true }));

  // The GMX-tradeable coin set (coin_id + symbol), for the strategy-type
  // picker's asset selector. Read-only; safe without the API key gate below
  // would still apply -- this sits after it like the other routes.
  app.get("/markets/gmx", async () => {
    const coins = await repositories.listGmxTradeableCoins();
    return { coins };
  });

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
  app.get<{ Params: { id: string } }>("/runs/:id/vault", async (request) => {
    const runId = request.params.id;
    const mandate = await ensureMandateForRun(runId, repositories);
    if (!mandate) {
      return {
        deployable: false,
        reason: "No strategy mandate for this run yet",
      };
    }

    const existing = await repositories.readVaultForMandate(mandate.mandateId);
    if (existing) {
      return {
        deployable: false,
        reason: "This strategy is already bound to a vault",
        mandate_id: mandate.mandateId,
        chain_id: existing.chainId,
        vault_address: existing.vaultAddress,
        asset_address: existing.assetAddress,
        display_name: existing.displayName,
        status: existing.status,
      };
    }

    return {
      deployable: true,
      mandate_id: mandate.mandateId,
      status: mandate.status,
    };
  });

  app.post<{ Params: { id: string } }>("/runs/:id/vault", async (request) => {
    const runId = request.params.id;
    const body = (request.body ?? {}) as Record<string, unknown>;
    const chainId = requiredNumber(body, "chain_id");
    const vaultAddress = requiredText(body, "vault_address");
    const assetAddress = requiredText(body, "asset_address");
    const displayName = requiredText(body, "display_name");
    if (displayName.length > 80) {
      throw httpError(400, "Strategy name must be 80 characters or fewer");
    }

    const mandate = await ensureMandateForRun(runId, repositories);
    if (!mandate) throw httpError(404, "No strategy mandate for this run yet");

    const existing = await repositories.readVaultForMandate(mandate.mandateId);
    if (existing)
      throw httpError(409, "This strategy is already bound to a vault");

    await repositories.bindVaultToMandate({
      chainId,
      vaultAddress,
      mandateId: mandate.mandateId,
      assetAddress,
      displayName,
    });

    return {
      mandate_id: mandate.mandateId,
      chain_id: chainId,
      vault_address: vaultAddress,
      asset_address: assetAddress,
      display_name: displayName,
      status: "active",
    };
  });

  app.get<{ Params: { id: string } }>(
    "/runs/:id/vault/allocation",
    async (request) => {
      const runId = request.params.id;
      const mandate = await ensureMandateForRun(runId, repositories);
      if (!mandate)
        throw httpError(404, "No strategy mandate for this run yet");

      const vault = await repositories.readVaultForMandate(mandate.mandateId);
      if (!vault) throw httpError(404, "No vault bound to this strategy yet");

      const spec = mandate.spec as {
        initial_target_allocation?: unknown;
        allowed_sides?: unknown;
        template_id?: unknown;
      };

      const targetAllocation = Array.isArray(spec.initial_target_allocation)
        ? spec.initial_target_allocation
        : [];
      const coinIds = targetAllocation
        .map((item) =>
          isRecord(item) && typeof item.coin_id === "string"
            ? item.coin_id
            : null,
        )
        .filter((coinId): coinId is string => Boolean(coinId));
      const { resolved, failures } =
        await repositories.resolveMarketsBatch(coinIds);
      const unsupportedChainReason =
        vault.chainId === ARBITRUM_SEPOLIA_CHAIN_ID
          ? "GMX v2 execution is not configured for Arbitrum Sepolia"
          : vault.chainId !== ARBITRUM_ONE_CHAIN_ID
            ? `GMX v2 execution is not configured for chain ${vault.chainId}`
            : undefined;
      const missing = [
        ...(unsupportedChainReason ? [unsupportedChainReason] : []),
        ...(failures.size > 0
          ? [
              `GMX market resolution for ${Array.from(failures.keys()).join(
                ", ",
              )}`,
            ]
          : []),
      ];
      const executable = missing.length === 0;

      return {
        executable,
        reason:
          unsupportedChainReason ??
          (executable
            ? "Allocation can be executed as GMX v2 increase orders."
            : "Resolve missing GMX markets before execution."),
        mandate_id: mandate.mandateId,
        chain_id: vault.chainId,
        vault_address: vault.vaultAddress,
        asset_address: vault.assetAddress,
        template_id: spec.template_id ?? null,
        allowed_sides: spec.allowed_sides ?? null,
        target_allocation: targetAllocation.map((item) => {
          if (!isRecord(item) || typeof item.coin_id !== "string") return item;
          const market = resolved.get(item.coin_id);
          return {
            ...item,
            gmx_market: market
              ? {
                  chain: "arbitrum",
                  market_token: market.gmxMarket,
                  index_token: market.indexToken,
                  long_token: market.longToken,
                  short_token: market.shortToken,
                  collateral_token: market.collateralToken,
                  collateral_decimals: market.collateralDecimals,
                }
              : null,
          };
        }),
        missing,
      };
    },
  );

  app.post("/messages", async (request) => {
    const body = (request.body ?? {}) as Record<string, unknown>;
    const userId = requiredText(body, "user_id");
    const strategyId = requiredText(body, "strategy_id");
    const text = resolveMessageText(body);
    // Deterministic thesis overrides (e.g. the wizard's strategy-type picker
    // selecting a single-asset / pair / long-short shape). Applied after
    // interpret_brief and re-validated, same as a structured rerun.
    const overrides = parseStrategyRunOverrides(body.overrides);
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
      workflowState = await executeWorkflow(
        runId,
        text as string | WizardBrief,
        {
          llm: workflowLLM,
        },
        { overrides },
      );
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
        runSummaryFields(structuredResult, workflowState),
      );
    }

    const run = await repositories.readRun(runId);
    if (!run) throw new Error(`Run missing after execution: ${runId}`);
    if (workflowError) throw workflowError;
    return runResponse(run, structuredResult);
  });

  app.post("/internal/tools/run-strategy-pipeline", async (request) => {
    const body = (request.body ?? {}) as Record<string, unknown>;
    if (!isRecord(body)) {
      throw httpError(400, "Request body must be a JSON object");
    }

    const brief = requiredText(body, "brief");
    const chatSessionId = requiredText(body, "chat_session_id");
    if (!executeChatAgent.runStrategyPipeline) {
      throw httpError(500, "Chat agent cannot start strategy pipeline runs");
    }

    const based_on_run_id =
      typeof body.based_on_run_id === "string" && body.based_on_run_id.trim()
        ? body.based_on_run_id.trim()
        : undefined;
    const data_as_of =
      typeof body.data_as_of === "string" && body.data_as_of.trim()
        ? body.data_as_of.trim()
        : undefined;
    const overrides = parseStrategyRunOverrides(body.overrides);

    return executeChatAgent.runStrategyPipeline(
      { brief, based_on_run_id, overrides, data_as_of },
      chatSessionId,
    );
  });

  app.post("/internal/tools/screen-markets", async (request) => {
    const body = (request.body ?? {}) as Record<string, unknown>;
    if (!isRecord(body)) {
      throw httpError(400, "Request body must be a JSON object");
    }

    return executeScreenMarkets(parseScreenMarketsInput(body));
  });

  app.post("/internal/tools/run-research-code", async (request) => {
    const body = (request.body ?? {}) as Record<string, unknown>;
    if (!isRecord(body)) {
      throw httpError(400, "Request body must be a JSON object");
    }
    return defaultRunResearchCode({
      code: requiredText(body, "code"),
      purpose: requiredText(body, "purpose"),
      ...(typeof body.timeout_seconds === "number"
        ? { timeoutSeconds: body.timeout_seconds }
        : {}),
    });
  });

  app.post("/screeners/markets", async (request) => {
    const body = (request.body ?? {}) as Record<string, unknown>;
    if (!isRecord(body)) {
      throw httpError(400, "Request body must be a JSON object");
    }

    return executeScreenMarkets(parseScreenMarketsInput(body));
  });

  app.get<{ Querystring: { user_id?: string } }>(
    "/screeners/pins",
    async (request) => {
      const userId = request.query.user_id?.trim();
      if (!userId) throw httpError(400, "Query must include user_id");
      return repositories.listPinnedScreeners(userId);
    },
  );

  app.post("/screeners/pins", async (request) => {
    const body = (request.body ?? {}) as Record<string, unknown>;
    if (!isRecord(body)) {
      throw httpError(400, "Request body must be a JSON object");
    }
    const userId = requiredText(body, "user_id");
    const title = requiredText(body, "title");
    const definition = parsePinnedScreenerDefinition(body.definition);

    return repositories.upsertPinnedScreener({ userId, title, definition });
  });

  app.delete<{ Params: { id: string }; Querystring: { user_id?: string } }>(
    "/screeners/pins/:id",
    async (request) => {
      const userId = request.query.user_id?.trim();
      if (!userId) throw httpError(400, "Query must include user_id");
      const count = await repositories.deletePinnedScreener(
        userId,
        request.params.id,
      );
      if (count === 0) throw httpError(404, "Pinned screener not found");
      return { ok: true };
    },
  );

  app.post<{ Params: { id: string } }>(
    "/screeners/pins/:id/refresh",
    async (request) => {
      const body = (request.body ?? {}) as Record<string, unknown>;
      if (!isRecord(body)) {
        throw httpError(400, "Request body must be a JSON object");
      }
      const userId = requiredText(body, "user_id");
      const pinned = await repositories.readPinnedScreener(
        userId,
        request.params.id,
      );
      if (!pinned) throw httpError(404, "Pinned screener not found");

      const result = await executeScreenMarkets({
        factor: pinned.definition.factor,
        limit: pinned.definition.limit,
        gmxOnly: pinned.definition.gmx_only,
        ...(pinned.definition.as_of ? { asOf: pinned.definition.as_of } : {}),
      });
      await repositories.markPinnedScreenerRefreshed(userId, pinned.id);
      return result;
    },
  );

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
      ...(response.structured_result
        ? { structured_result: response.structured_result }
        : {}),
    };
  });

  app.post("/chat/messages/stream", async (request, reply) => {
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

    reply.hijack();
    const raw = reply.raw;
    raw.setHeader("Content-Type", "text/event-stream");
    raw.setHeader("Cache-Control", "no-cache, no-transform");
    raw.setHeader("Connection", "keep-alive");
    raw.setHeader("X-Accel-Buffering", "no");
    raw.flushHeaders?.();

    let clientGone = false;
    raw.on("close", () => {
      clientGone = true;
    });

    const send = (payload: unknown) => {
      if (clientGone || raw.writableEnded) return;
      const eventName =
        isRecord(payload) && typeof payload.type === "string"
          ? payload.type
          : "message";
      raw.write(`event: ${eventName}\n`);
      raw.write(`data: ${JSON.stringify(payload)}\n\n`);
    };

    try {
      if (executeChatAgent.runStream) {
        await executeChatAgent.runStream(
          { chatSessionId, message, userId },
          (event) => send(event),
        );
      } else {
        const response = await executeChatAgent.run({
          chatSessionId,
          message,
          userId,
        });
        send({ type: "chat.delta", content: response.content });
        send({ type: "chat.completed", ...response });
      }
    } catch (error) {
      send({ type: "chat.error", message: errorMessage(error) });
    } finally {
      if (!clientGone && !raw.writableEnded) raw.end();
    }
  });

  app.get<{ Params: { id: string } }>("/runs/:id", async (request) => {
    const run = await repositories.readRun(request.params.id);
    if (!run) throw httpError(404, "Run not found");
    const stages = await repositories.listStageRunsByRunId(request.params.id);
    return runResponse(run, runStructuredResult(run), stages);
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

  app.post("/strategy-pipeline/runs/stream", streamStrategyPipelineRun);

  app.post("/messages/stream", streamStrategyPipelineRun);

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
