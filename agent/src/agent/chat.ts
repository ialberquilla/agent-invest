import { randomUUID } from "node:crypto";

import { desc, eq, or, sql } from "drizzle-orm";

import { db as defaultDb } from "../db/client";
import { appendEvent as defaultAppendEvent } from "../db/repositories/agent-events";
import {
  recordToolCall as defaultRecordToolCall,
  recordToolResult as defaultRecordToolResult,
} from "../db/repositories/agent-tool-calls";
import { conversationThreads, runs } from "../db/schema";
import { effectiveToolName, extractToolPart } from "./opencode-events";
import {
  disabledOpencodeBuiltinsTools,
  getOrCreateManagedOpencode,
  parseOpencodeModel,
  RUN_RESEARCH_CODE_TOOL,
  RUN_STRATEGY_PIPELINE_TOOL,
  SCREEN_MARKETS_TOOL,
  resolveOpencodeModel,
  type ManagedOpencode,
} from "./session";
import {
  screenMarkets as defaultScreenMarkets,
  type ScreenerResult,
} from "../tools/screen-markets";
import { createOpencodeLLMClient } from "./workflow/llm";
import { runWorkflowAndPersist } from "./workflow/persist";
import type { StrategyRunOverrides, WizardBrief } from "./workflow/state";

export const CHAT_PROMPT = `You are the Agent Invest chat agent.

Answer general investing and quantitative-finance questions clearly and directly. You can explain concepts, ask brief clarifying questions, and help the user shape an investment-strategy brief.

You have exactly three tools available:
- ${RUN_STRATEGY_PIPELINE_TOOL}({ brief, chat_session_id, based_on_run_id?, overrides?, data_as_of? }): starts an asynchronous strategy pipeline from the user's brief and returns { run_id } immediately. For a follow-up that reshapes a prior run (e.g. "try fewer assets", "lower the max drawdown", "exclude stablecoins"), pass based_on_run_id (the prior run_id) and an overrides object containing ONLY the fields the user asked to change — do not re-derive the whole brief.
- ${SCREEN_MARKETS_TOOL}({ query, factor, limit, gmxOnly }): returns a read-only structured market screener backed by ingested data and GMX market resolution.
- ${RUN_RESEARCH_CODE_TOOL}({ code, purpose }): runs short Python research code against read-only market-data views for open-ended quantitative analysis.

STRICT TOOL ROUTING RULES:
- For any request asking to list, rank, screen, compare, find top/best/worst, or build a watchlist of coins/tickers/assets/markets, you MUST call ${SCREEN_MARKETS_TOOL}. This includes informal phrasing and typos.
- Never answer market screen/ranking/list/watchlist requests with markdown tables, numbered lists, or remembered ticker rankings. The frontend can only render pin/refresh actions when ${SCREEN_MARKETS_TOOL} is called and returns structured data.
- Use ${RUN_STRATEGY_PIPELINE_TOOL} only when the user explicitly asks you to build, test, run, launch, or evaluate an investment strategy.
- When the user asks to iterate on a strategy run you already started in this chat (fewer/more assets, different drawdown/horizon/rebalance, exclude or hand-pick coins, change top_n/top_skip), call ${RUN_STRATEGY_PIPELINE_TOOL} again with based_on_run_id set to that run's run_id and an overrides object holding only the changed fields. Available overrides: asset_count_min, asset_count_max, max_weight_per_asset, max_cash_weight, max_drawdown, horizon_days, rebalance_frequency, top_n, top_skip, exclude_stablecoins, exclude_wrapped, hand_picked_coin_ids, strategy_mode, target_coin_id. To pivot a basket into a single-market trend setup, pass strategy_mode="single_asset" with asset_count_min=1 and asset_count_max=1 (and optionally target_coin_id). Reuse the original brief text.
- Use ${RUN_RESEARCH_CODE_TOOL} for bespoke historical/statistical analysis, event studies, return distributions, regime splits, or bracket-style research that cannot be answered by ${SCREEN_MARKETS_TOOL}. Generated code must use query(sql), include sample sizes, and surface assumptions/uncertainty.
- Do not launch a strategy pipeline for a market screen, watchlist, or discretionary single-position trade idea.
- Do not use ${RUN_RESEARCH_CODE_TOOL} to fetch network data, read secrets, write databases, or give financial advice.
- When recent strategy pipeline run context is provided, use it to answer follow-up questions about those prior runs.
- Never invent unavailable details; progress and new run results are delivered through the run's event streams.

RESEARCH RESULT PRESENTATION:
- After ${RUN_RESEARCH_CODE_TOOL} returns, present the answer in a polished research-note format: one-sentence takeaway first, then a compact table for key numbers, then assumptions/limits.
- Use GitHub-flavored markdown tables for distributions, event counts, regime splits, parameter sweeps, and stop/target sensitivity grids. Keep numeric precision readable: percentages to 1 decimal place unless more precision matters.
- If the research code produced chart artifacts, mention each chart by name and explain what it shows. If a visual would materially clarify the answer, generate one with matplotlib and research_api.save_chart(...).
- Always include sample size, date range when available, and a short uncertainty / not-advice note. Do not bury small-sample warnings.
- Do not paste large raw JSON blobs unless the user asks. Summarize the result and show only the code when it is useful for auditability or when the user asks for it.`;

export const CHAT_ALLOWED_TOOLS = {
  [RUN_STRATEGY_PIPELINE_TOOL]: true,
  [SCREEN_MARKETS_TOOL]: true,
  [RUN_RESEARCH_CODE_TOOL]: true,
} as const;

export type ChatAgentInput = {
  chatSessionId: string;
  userId: string;
  message: string;
};

export type ChatAgentResponse = {
  content: string;
  opencode_session_id: string;
  run_id?: string;
  structured_result?: ScreenerResult;
};

export type ChatStreamEvent =
  | {
      type: "chat.started";
      chat_session_id: string;
      opencode_session_id: string;
    }
  | { type: "chat.delta"; content: string }
  | { type: "tool.updated"; run_id?: string; status?: string }
  | (ChatAgentResponse & { type: "chat.completed" })
  | { type: "chat.error"; message: string };

export type RunStrategyPipelineInput = {
  brief: string;
  // Structured-rerun inputs (PR1). based_on_run_id traces the parent run
  // a rerun was derived from; overrides deterministically reshape the
  // interpreted thesis; data_as_of pins the data snapshot.
  based_on_run_id?: string;
  overrides?: StrategyRunOverrides;
  data_as_of?: string;
};

export type RunStrategyPipelineResult = {
  run_id: string;
};

type Db = typeof defaultDb;

export type ChatAgentDependencies = {
  db?: Db;
  getManagedOpencode?: () => Promise<ManagedOpencode>;
  runWorkflow?: typeof runWorkflowAndPersist;
  screenMarkets?: typeof defaultScreenMarkets;
  onBackgroundError?: (error: unknown, runId: string) => void;
  env?: NodeJS.ProcessEnv;
  appendEvent?: typeof defaultAppendEvent;
  recordToolCall?: typeof defaultRecordToolCall;
  recordToolResult?: typeof defaultRecordToolResult;
};

export function createChatAgent(dependencies: ChatAgentDependencies = {}) {
  const db = dependencies.db ?? defaultDb;
  const executeWorkflow = dependencies.runWorkflow ?? runWorkflowAndPersist;
  const executeScreenMarkets = dependencies.screenMarkets ?? defaultScreenMarkets;
  const getManagedOpencode =
    dependencies.getManagedOpencode ?? (() => getOrCreateManagedOpencode());
  const env = dependencies.env ?? process.env;
  const appendEvent = dependencies.appendEvent ?? defaultAppendEvent;
  const recordToolCall = dependencies.recordToolCall ?? defaultRecordToolCall;
  const recordToolResult =
    dependencies.recordToolResult ?? defaultRecordToolResult;

  async function runStrategyPipeline(
    input: RunStrategyPipelineInput,
    chatSessionId?: string,
  ): Promise<RunStrategyPipelineResult> {
    const brief = input.brief.trim();
    if (!brief) throw new Error("run_strategy_pipeline requires a brief");

    const options = {
      overrides: input.overrides,
      based_on_run_id: input.based_on_run_id,
      data_as_of: input.data_as_of,
    };

    const runId = randomUUID();
    await db.insert(runs).values({
      runId,
      threadId: chatSessionId,
      kind: "strategy_pipeline",
      status: "running",
      metadata: {
        brief,
        based_on_run_id: input.based_on_run_id ?? null,
        overrides: input.overrides ?? null,
        data_as_of: input.data_as_of ?? null,
      },
    });

    void executeWorkflow(
      runId,
      brief as string | WizardBrief,
      {
        db,
        llm: createOpencodeLLMClient({ env, sessionTitle: `workflow ${runId}` }),
      },
      options,
    ).catch((error: unknown) => {
      // The persist helper already records status=failed on exception
      // before re-throwing, so this catch is just a notification hook.
      dependencies.onBackgroundError?.(error, runId);
    });

    return { run_id: runId };
  }

  return {
    allowedTools: CHAT_ALLOWED_TOOLS,
    runStrategyPipeline,
    async run(input: ChatAgentInput): Promise<ChatAgentResponse> {
      return runChat(input);
    },
    async runStream(
      input: ChatAgentInput,
      onEvent: (event: ChatStreamEvent) => void | Promise<void>,
    ): Promise<ChatAgentResponse> {
      try {
        const response = await runChat(input, onEvent);
        await onEvent({ type: "chat.completed", ...response });
        return response;
      } catch (error) {
        await onEvent({
          type: "chat.error",
          message: error instanceof Error ? error.message : String(error),
        });
        throw error;
      }
    },
  };

  async function runChat(
    input: ChatAgentInput,
    onEvent?: (event: ChatStreamEvent) => void | Promise<void>,
  ): Promise<ChatAgentResponse> {
      const message = input.message.trim();
      if (!message) throw new Error("chat message is required");

      const managed = await getManagedOpencode();
      const sessionId = await getOrCreateChatOpencodeSession(
        db,
        managed,
        input.chatSessionId,
        input.userId,
      );

      const runContext = await strategyRunContext(db, input.chatSessionId);
      const abortEvents = new AbortController();
      await onEvent?.({
        type: "chat.started",
        chat_session_id: input.chatSessionId,
        opencode_session_id: sessionId,
      });

      const events = collectChatSessionEvents({
        managed,
        appendEvent,
        recordToolCall,
        recordToolResult,
        inputMessage: message,
        threadId: input.chatSessionId,
        sessionId,
        signal: abortEvents.signal,
        onEvent,
      });

      let response: Awaited<ReturnType<typeof managed.client.session.prompt>>;
      let eventScreenerResult: ScreenerResult | null = null;
      try {
        response = await managed.client.session.prompt({
          path: { id: sessionId },
          body: {
            model: parseOpencodeModel(resolveOpencodeModel(env)),
            system: [
              CHAT_PROMPT,
              `Current chat_session_id: ${input.chatSessionId}`,
              runContext,
            ]
              .filter(Boolean)
              .join("\n\n"),
            tools: { ...disabledOpencodeBuiltinsTools(), ...CHAT_ALLOWED_TOOLS },
            parts: [{ type: "text", text: message }],
          },
          throwOnError: true,
        });
      } finally {
        abortEvents.abort();
        eventScreenerResult = await events;
      }

      for (const part of response.data?.parts ?? []) {
        if (!part || typeof part !== "object") continue;
        await appendEvent({
          threadId: input.chatSessionId,
          eventType: "message.part.updated",
          payload: { event: { type: "message.part.updated", properties: { part } } },
        });
      }

      const runId = extractRunId(response.data);
      const structuredResult = extractScreenerResult(response.data) ?? eventScreenerResult;
      const content = structuredResult
        ? screenerReply(structuredResult)
        : extractChatResponseText(response.data);
      if (content) await onEvent?.({ type: "chat.delta", content });
      if (runId) await onEvent?.({ type: "tool.updated", run_id: runId });
      return {
        content,
        opencode_session_id: sessionId,
        ...(runId ? { run_id: runId } : {}),
        ...(structuredResult ? { structured_result: structuredResult } : {}),
      };
    }
}

export const chatAgent = createChatAgent();

async function strategyRunContext(db: Db, chatSessionId: string) {
  const recentRuns = await db
    .select({
      runId: runs.runId,
      status: runs.status,
      reply: runs.reply,
      error: runs.error,
      metadata: runs.metadata,
      startedAt: runs.startedAt,
    })
    .from(runs)
    .where(
      or(eq(runs.threadId, chatSessionId), eq(runs.strategyId, chatSessionId)),
    )
    .orderBy(desc(runs.startedAt))
    .limit(3);

  if (recentRuns.length === 0) return "";

  return [
    "Recent strategy pipeline runs in this chat. Use this to understand follow-ups about previous strategies; do not invent unavailable results.",
    ...recentRuns.map((run) => {
      const metadata = isRecord(run.metadata) ? run.metadata : {};
      const structured = isRecord(metadata.structured_result)
        ? metadata.structured_result
        : {};
      const title = typeof structured.title === "string" ? structured.title : "";
      const summary =
        typeof structured.summary === "string" ? structured.summary : run.reply;
      const brief = formatBrief(metadata.brief);
      const metrics = formatRunMetrics(structured);
      const benchmark = formatBenchmarkComparison(structured);
      const details = [
        `run_id=${run.runId}`,
        `status=${run.status}`,
        brief ? `brief=${brief}` : "",
        title ? `title=${title}` : "",
        summary ? `summary=${summary}` : "",
        metrics,
        benchmark,
        run.error ? `error=${run.error}` : "",
      ].filter(Boolean);
      return `- ${details.join("; ")}`;
    }),
  ].join("\n");
}

async function getOrCreateChatOpencodeSession(
  db: Db,
  managed: ManagedOpencode,
  chatSessionId: string,
  userId: string,
) {
  const [thread] = await db
    .select({ providerSessionId: conversationThreads.providerSessionId })
    .from(conversationThreads)
    .where(eq(conversationThreads.threadId, chatSessionId));

  const existingSessionId = thread?.providerSessionId?.trim();
  if (existingSessionId) return existingSessionId;

  const session = await managed.client.session.create({
    body: { title: `chat ${chatSessionId}` },
    throwOnError: true,
  });
  const sessionId = session.data.id as string;

  await db
    .insert(conversationThreads)
    .values({
      threadId: chatSessionId,
      userId,
      provider: "opencode",
      providerSessionId: sessionId,
      title: "Agent Invest chat",
    })
    .onConflictDoUpdate({
      target: conversationThreads.threadId,
      set: { providerSessionId: sessionId, updatedAt: sql`NOW()` },
    });

  return sessionId;
}

type CollectChatSessionEventsInput = {
  managed: ManagedOpencode;
  appendEvent: typeof defaultAppendEvent;
  recordToolCall: typeof defaultRecordToolCall;
  recordToolResult: typeof defaultRecordToolResult;
  inputMessage: string;
  threadId: string;
  sessionId: string;
  signal: AbortSignal;
  onEvent?: (event: ChatStreamEvent) => void | Promise<void>;
};

async function collectChatSessionEvents({
  managed,
  appendEvent,
  recordToolCall,
  recordToolResult,
  inputMessage,
  threadId,
  sessionId,
  signal,
  onEvent,
}: CollectChatSessionEventsInput) {
  const seenCallIds = new Set<string>();
  const finishedCallIds = new Set<string>();
  let lastText = "";
  let screenerResult: ScreenerResult | null = null;

  try {
    const response = await managed.client.event.subscribe({ signal });
    for await (const event of response.stream as AsyncIterable<unknown>) {
      const eventSessionId = extractSessionId(event);
      if (eventSessionId && eventSessionId !== sessionId) continue;

      const rawEvent = await appendEvent({
        threadId,
        eventType: extractEventType(event),
        payload: { event },
      });
      const rawEventId = rawEvent?.eventId ?? null;

      const text = extractTextFromEvent(event, inputMessage);
      if (text && text !== lastText) {
        lastText = text;
        await onEvent?.({ type: "chat.delta", content: text });
      }

      const toolPart = extractToolPart(event);
      if (!toolPart) continue;
      if (toolPart.tool === SCREEN_MARKETS_TOOL) {
        const parsed = parseToolOutput(toolPart.state?.output);
        if (isScreenerResult(parsed)) screenerResult = parsed;
      }
      await onEvent?.({
        type: "tool.updated",
        status: toolPart.state?.status,
        run_id: extractRunId({ parts: [toolPart] }) ?? undefined,
      });

      const status = toolPart.state?.status;
      const hasArgs =
        toolPart.state?.input !== undefined &&
        toolPart.state.input !== null &&
        Object.keys(toolPart.state.input as Record<string, unknown>).length > 0;

      if (!seenCallIds.has(toolPart.callID)) {
        seenCallIds.add(toolPart.callID);
        await recordToolCall({
          toolCallId: toolPart.callID,
          toolName: effectiveToolName(toolPart.tool, toolPart.state?.input),
          args: toolPart.state?.input ?? {},
          threadId,
          startedEventId: rawEventId,
        });
      } else if (hasArgs) {
        await recordToolCall({
          toolCallId: toolPart.callID,
          toolName: effectiveToolName(toolPart.tool, toolPart.state?.input),
          args: toolPart.state?.input ?? {},
          threadId,
        });
      }

      if (
        (status === "completed" || status === "error") &&
        !finishedCallIds.has(toolPart.callID)
      ) {
        finishedCallIds.add(toolPart.callID);
        const isError = status === "error";
        const errorMessage = isError
          ? typeof toolPart.state?.error === "string"
            ? toolPart.state.error
            : null
          : null;
        const result = isError
          ? toolPart.state?.error ?? null
          : toolPart.state?.output ?? null;

        await recordToolResult({
          toolCallId: toolPart.callID,
          toolName: effectiveToolName(toolPart.tool, toolPart.state?.input),
          result,
          isError,
          errorMessage,
          finishedEventId: rawEventId,
        });
      }
    }
  } catch (error) {
    if (!signal.aborted) throw error;
  }
  return screenerResult;
}

function extractSessionId(event: unknown): string | null {
  if (!event || typeof event !== "object") return null;
  const queue: Array<Record<string, unknown>> = [
    event as Record<string, unknown>,
  ];
  const seen = new Set<unknown>();
  while (queue.length > 0) {
    const value = queue.shift()!;
    if (seen.has(value)) continue;
    seen.add(value);
    for (const key of ["sessionID", "sessionId", "session_id"]) {
      const candidate = value[key];
      if (typeof candidate === "string") return candidate;
    }
    for (const child of Object.values(value)) {
      if (child && typeof child === "object" && !Array.isArray(child)) {
        queue.push(child as Record<string, unknown>);
      }
    }
  }
  return null;
}

function extractEventType(event: unknown): string {
  if (!event || typeof event !== "object") return "opencode.event";
  const type = (event as Record<string, unknown>).type;
  return typeof type === "string" && type.trim() ? type : "opencode.event";
}

export function extractChatResponseText(result: { parts?: unknown[] }) {
  return (result.parts ?? [])
    .map((part) => {
      if (!part || typeof part !== "object") return "";
      if ((part as { type?: unknown }).type !== "text") return "";
      const text = (part as { text?: unknown }).text;
      return typeof text === "string" ? text : "";
    })
    .filter(Boolean)
    .join("\n");
}

function extractRunId(result: { parts?: unknown[] }) {
  for (const part of result.parts ?? []) {
    if (!part || typeof part !== "object") continue;
    const toolPart = part as {
      type?: unknown;
      tool?: unknown;
      state?: { output?: unknown };
    };
    if (
      toolPart.type !== "tool" ||
      toolPart.tool !== RUN_STRATEGY_PIPELINE_TOOL
    ) {
      continue;
    }

    const output = toolPart.state?.output;
    const parsed = typeof output === "string" ? parseJson(output) : output;
    if (isRecord(parsed) && typeof parsed.run_id === "string") {
      return parsed.run_id;
    }
  }
  return null;
}

function extractScreenerResult(result: { parts?: unknown[] }): ScreenerResult | null {
  for (const part of result.parts ?? []) {
    const nested = findScreenerResult(part);
    if (nested) return nested;
    if (!part || typeof part !== "object") continue;
    const toolPart = part as {
      type?: unknown;
      tool?: unknown;
      state?: { output?: unknown };
    };
    if (toolPart.type !== "tool" || toolPart.tool !== SCREEN_MARKETS_TOOL) {
      continue;
    }

    const parsed = parseToolOutput(toolPart.state?.output);
    if (isScreenerResult(parsed)) return parsed;
  }
  return null;
}

function findScreenerResult(value: unknown): ScreenerResult | null {
  if (isScreenerResult(value)) return value;
  const parsed = typeof value === "string" ? parseJson(value) : null;
  if (isScreenerResult(parsed)) return parsed;
  if (!value || typeof value !== "object") return null;

  if (Array.isArray(value)) {
    for (const entry of value) {
      const found = findScreenerResult(entry);
      if (found) return found;
    }
    return null;
  }

  for (const entry of Object.values(value)) {
    const found = findScreenerResult(entry);
    if (found) return found;
  }
  return null;
}

function parseToolOutput(output: unknown): unknown {
  const parsed = typeof output === "string" ? parseJson(output) : output;
  if (!isRecord(parsed)) return parsed;

  const content = parsed.content;
  if (Array.isArray(content)) {
    for (const entry of content) {
      if (!isRecord(entry) || entry.type !== "text") continue;
      const text = entry.text;
      if (typeof text !== "string") continue;
      const nested = parseJson(text);
      if (nested) return nested;
    }
  }

  return parsed;
}

function isScreenerResult(value: unknown): value is ScreenerResult {
  return (
    isRecord(value) &&
    value.type === "market_screener" &&
    value.version === 1 &&
    typeof value.title === "string" &&
    Array.isArray(value.rows)
  );
}

function screenerReply(result: ScreenerResult) {
  const rows = result.rows
    .slice(0, 5)
    .map((row) => `#${row.rank} ${row.symbol}`)
    .join(", ");
  return [
    result.title,
    result.summary,
    rows ? `Top rows: ${rows}.` : "No GMX-tradeable rows matched the screen.",
    "Use the Long/Short buttons on the card to open an order ticket preview; no transaction is submitted from chat.",
  ].join("\n\n");
}

function parseJson(value: string) {
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return null;
  }
}

export function extractTextFromEvent(event: unknown, inputMessage = "") {
  if (!event || typeof event !== "object") return "";
  const texts: string[] = [];
  const ignoredText = inputMessage.trim();
  const queue: Array<Record<string, unknown>> = [event as Record<string, unknown>];
  const seen = new Set<unknown>();

  while (queue.length > 0) {
    const value = queue.shift()!;
    if (seen.has(value)) continue;
    seen.add(value);

    if (value.type === "text" && typeof value.text === "string") {
      const text = value.text.trim();
      if (text && text !== ignoredText) texts.push(value.text);
    }

    for (const child of Object.values(value)) {
      if (child && typeof child === "object") {
        if (Array.isArray(child)) {
          for (const item of child) {
            if (item && typeof item === "object") {
              queue.push(item as Record<string, unknown>);
            }
          }
        } else {
          queue.push(child as Record<string, unknown>);
        }
      }
    }
  }

  return texts.join("\n").trim();
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function formatBrief(value: unknown) {
  if (typeof value === "string") return value;
  if (isRecord(value)) return JSON.stringify(value);
  return "";
}

function formatRunMetrics(structured: Record<string, unknown>) {
  const backtest = isRecord(structured.backtest) ? structured.backtest : {};
  const kpis = isRecord(structured.kpis) ? structured.kpis : {};
  const parts = [
    typeof structured.template_id === "string"
      ? `template=${structured.template_id}`
      : "",
    typeof backtest.benchmark === "string"
      ? `benchmark=${backtest.benchmark}`
      : "",
    typeof backtest.start_date === "string" &&
    typeof backtest.end_date === "string"
      ? `window=${backtest.start_date}..${backtest.end_date}`
      : "",
    metric("cagr", kpis.cagr),
    metric("sharpe", kpis.sharpe_ratio),
    metric("max_drawdown", kpis.max_drawdown),
    metric("final_equity_multiple", kpis.final_equity_multiple),
  ].filter(Boolean);

  return parts.length > 0 ? `metrics=${parts.join(", ")}` : "";
}

function metric(label: string, value: unknown) {
  return typeof value === "number" && Number.isFinite(value)
    ? `${label}=${value}`
    : "";
}

function formatBenchmarkComparison(structured: Record<string, unknown>) {
  const charts = isRecord(structured.charts) ? structured.charts : {};
  const equityCurve = Array.isArray(charts.equity_curve)
    ? charts.equity_curve.filter(isRecord)
    : [];
  const drawdown = Array.isArray(charts.drawdown)
    ? charts.drawdown.filter(isRecord)
    : [];
  const first = equityCurve.find(
    (point) =>
      isFiniteNumber(point.strategy_equity) &&
      isFiniteNumber(point.benchmark_equity),
  );
  const last = equityCurve
    .slice()
    .reverse()
    .find(
      (point) =>
        isFiniteNumber(point.strategy_equity) &&
        isFiniteNumber(point.benchmark_equity),
    );

  if (!first || !last) return "";

  const firstStrategy = first.strategy_equity as number;
  const lastStrategy = last.strategy_equity as number;
  const firstBenchmark = first.benchmark_equity as number;
  const lastBenchmark = last.benchmark_equity as number;
  if (firstStrategy === 0 || firstBenchmark === 0) return "";

  const strategyReturn = lastStrategy / firstStrategy - 1;
  const benchmarkReturn = lastBenchmark / firstBenchmark - 1;
  const benchmarkDrawdown = minFinite(
    drawdown.map((point) => point.benchmark_drawdown),
  );
  const comparison = [
    `strategy_return=${formatPercent(strategyReturn)}`,
    `benchmark_return=${formatPercent(benchmarkReturn)}`,
    `excess_return=${formatPercent(strategyReturn - benchmarkReturn)}`,
    `strategy_final_equity=${formatNumber(lastStrategy)}`,
    `benchmark_final_equity=${formatNumber(lastBenchmark)}`,
    benchmarkDrawdown === null
      ? ""
      : `benchmark_max_drawdown=${formatPercent(benchmarkDrawdown)}`,
  ].filter(Boolean);

  return comparison.length > 0
    ? `benchmark_comparison=${comparison.join(", ")}`
    : "";
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function minFinite(values: unknown[]) {
  const finite = values.filter(isFiniteNumber);
  return finite.length > 0 ? Math.min(...finite) : null;
}

function formatPercent(value: number) {
  return `${(value * 100).toFixed(2)}%`;
}

function formatNumber(value: number) {
  return Number.isInteger(value) ? String(value) : value.toFixed(2);
}
