import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  getOrCreateManagedOpencode,
  parseOpencodeModel,
  resolveOpencodeModel,
  type ManagedOpencode,
} from "../session";
import {
  createStageRun as defaultCreateStageRun,
  updateStageRun as defaultUpdateStageRun,
} from "../../db/repositories/stage-runs";
import { appendEvent as defaultAppendEvent } from "../../db/repositories/agent-events";
import type { NewStageRun, StageRun } from "../../db/schema";
import { STAGE_EVENT_TYPES } from "../stage-events";

export type StageName = "thesis" | "designer" | "adjudicator" | "reporter";

export type TerminalExpectation = "none" | "optional" | "required";

export type StageDefinition = {
  name: StageName;
  allowedTools: readonly string[];
  terminal: TerminalExpectation;
  promptPath: string;
  model?: string;
  validateOutput?: (output: unknown, result: PromptResult) => void;
};

export type StageRunInput = Record<string, unknown>;
export type StageRunOutput = Record<string, unknown>;

export interface StageRunner<
  Input extends StageRunInput = StageRunInput,
  Output = StageRunOutput,
> {
  run(input: Input, runId: string, round: number): Promise<Output>;
}

type CreateStageRun = typeof defaultCreateStageRun;
type UpdateStageRun = typeof defaultUpdateStageRun;
type AppendEvent = typeof defaultAppendEvent;

export type StageRunnerOptions = {
  env?: NodeJS.ProcessEnv;
  getManagedOpencode?: typeof getOrCreateManagedOpencode;
  createStageRun?: CreateStageRun;
  updateStageRun?: UpdateStageRun;
  appendEvent?: AppendEvent;
  availableTools?: readonly string[];
};

type PromptResult = {
  info?: unknown;
  parts?: unknown[];
};

const DEFAULT_AVAILABLE_TOOLS = [
  "bash",
  "edit",
  "glob",
  "grep",
  "list",
  "patch",
  "read",
  "task",
  "todo",
  "webfetch",
  "write",
  "question",
] as const;

const PARSE_RETRY_SYSTEM_MESSAGE =
  "Your previous response could not be parsed as JSON. Reply with only valid JSON matching the requested structured output. Do not include Markdown fences, prose, or tool calls.";

const stagesDirectory = dirname(fileURLToPath(import.meta.url));

export function buildToolConfig(
  allowedTools: readonly string[],
  availableTools: readonly string[] = DEFAULT_AVAILABLE_TOOLS,
) {
  const allowed = new Set(allowedTools);
  const tools: Record<string, boolean> = {};

  for (const tool of availableTools) {
    tools[tool] = allowed.has(tool);
  }

  for (const tool of allowedTools) {
    tools[tool] = true;
  }

  return tools;
}

export async function loadStagePrompt(promptPath: string) {
  return readFile(resolve(stagesDirectory, "prompts", promptPath), "utf8");
}

export function eventIncludesTerminalTool(event: unknown) {
  return JSON.stringify(event).match(/"(bash|terminal|shell)"/) !== null;
}

export function extractToolName(event: unknown) {
  if (!event || typeof event !== "object") return null;

  const queue = [event as Record<string, unknown>];
  for (const value of queue) {
    const part = value.part;
    if (part && typeof part === "object" && !Array.isArray(part)) {
      const tool = (part as Record<string, unknown>).tool;
      if (typeof tool === "string") return tool;
    }

    for (const key of ["tool", "toolName", "tool_name", "name"]) {
      const candidate = value[key];
      if (typeof candidate === "string") return candidate;
    }

    for (const child of Object.values(value)) {
      if (child && typeof child === "object") {
        queue.push(child as Record<string, unknown>);
      }
    }
  }

  return null;
}

function extractSessionId(event: unknown) {
  if (!event || typeof event !== "object") return null;

  const queue = [event as Record<string, unknown>];
  for (const value of queue) {
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

function extractEventType(event: unknown) {
  if (!event || typeof event !== "object") return "opencode.event";
  const type = (event as Record<string, unknown>).type;
  return typeof type === "string" && type.trim() ? type : "opencode.event";
}

export async function subscribeStageEvents(
  managed: ManagedOpencode,
  options: { signal?: AbortSignal } = {},
) {
  const response = await managed.client.event.subscribe({
    signal: options.signal,
  });

  return response.stream;
}

export function parseStructuredOutput(text: string) {
  const trimmed = text.trim();

  if (!trimmed) throw new Error("Stage returned an empty response");

  try {
    return JSON.parse(trimmed) as unknown;
  } catch (error) {
    const fenced = trimmed.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i);
    if (fenced?.[1]) return JSON.parse(fenced[1]) as unknown;
    throw error;
  }
}

function extractText(result: PromptResult) {
  return (result.parts ?? [])
    .map((part) => {
      if (!part || typeof part !== "object") return "";
      const text = (part as { text?: unknown }).text;
      return typeof text === "string" ? text : "";
    })
    .join("\n");
}

function extractTokenCount(info: unknown, key: "input" | "output") {
  if (!info || typeof info !== "object") return null;
  const usage = (info as { usage?: Record<string, unknown> }).usage;
  const value = usage?.[`${key}Tokens`] ?? usage?.[`${key}_tokens`];

  return typeof value === "number" ? value : null;
}

async function createFreshSession(managed: ManagedOpencode, title: string) {
  const session = await managed.client.session.create({
    body: { title },
    throwOnError: true,
  });

  return session.data.id as string;
}

async function promptSession(
  managed: ManagedOpencode,
  request: {
    sessionId: string;
    text: string;
    system: string;
    model: string;
    tools: Record<string, boolean>;
  },
) {
  const response = await managed.client.session.prompt({
    path: { id: request.sessionId },
    body: {
      model: parseOpencodeModel(request.model),
      tools: request.tools,
      system: request.system,
      parts: [{ type: "text", text: request.text }],
    },
    throwOnError: true,
  });

  return response.data as PromptResult;
}

export function createStageRunner<
  Input extends StageRunInput,
  Output = StageRunOutput,
>(
  definition: StageDefinition,
  options: StageRunnerOptions = {},
): StageRunner<Input, Output> {
  const env = options.env ?? process.env;
  const getManagedOpencode =
    options.getManagedOpencode ?? getOrCreateManagedOpencode;
  const createStageRun = options.createStageRun ?? defaultCreateStageRun;
  const updateStageRun = options.updateStageRun ?? defaultUpdateStageRun;
  const appendEvent =
    options.appendEvent ??
    (options.createStageRun ? noopAppendEvent : defaultAppendEvent);
  const availableTools = options.availableTools ?? DEFAULT_AVAILABLE_TOOLS;

  return {
    async run(input, runId, round) {
      const model = definition.model ?? resolveOpencodeModel(env);
      const stageRunId = randomUUID();
      const startedAt = new Date();
      const tools = buildToolConfig(definition.allowedTools, availableTools);

      await createStageRun({
        stageRunId,
        runId,
        stage: definition.name,
        round,
        status: "pending",
        model,
        input,
        startedAt,
      } satisfies NewStageRun);

      const stagePayload = {
        stage: definition.name,
        round,
        stage_run_id: stageRunId,
      };

      await appendEvent({
        runId,
        eventType: STAGE_EVENT_TYPES.started,
        payload: stagePayload,
      });

      let abortToolEvents: AbortController | undefined;
      let toolEvents: Promise<void> | undefined;
      let lastResult: PromptResult | undefined;

      try {
        const managed = await getManagedOpencode(env);
        const sessionId = await createFreshSession(
          managed,
          `${definition.name} stage ${runId} round ${round}`,
        );
        abortToolEvents = new AbortController();
        toolEvents = collectToolCallEvents(
          managed,
          appendEvent,
          runId,
          sessionId,
          {
            stage: definition.name,
            round,
            stage_run_id: stageRunId,
          },
          abortToolEvents.signal,
        );
        const prompt = await loadStagePrompt(definition.promptPath);

        await updateStageRun(stageRunId, {
          status: "running",
          opencodeSessionId: sessionId,
        });

        for (let attempt = 0; attempt < 2; attempt += 1) {
          const system =
            attempt === 0
              ? prompt
              : `${prompt}\n\n${PARSE_RETRY_SYSTEM_MESSAGE}`;
          lastResult = await promptSession(managed, {
            sessionId,
            text: JSON.stringify(input),
            system,
            model,
            tools,
          });
          await appendPromptPartEvents(
            appendEvent,
            runId,
            stagePayload,
            lastResult,
          );

          try {
            const output = parseStructuredOutput(
              extractText(lastResult),
            ) as Output;
            definition.validateOutput?.(output, lastResult);
            await updateStageRun(stageRunId, {
              status: "succeeded",
              output: output as NewStageRun["output"],
              endedAt: new Date(),
              tokensIn: extractTokenCount(lastResult.info, "input"),
              tokensOut: extractTokenCount(lastResult.info, "output"),
            });
            abortToolEvents.abort();
            await toolEvents;
            await appendEvent({
              runId,
              eventType: STAGE_EVENT_TYPES.completed,
              payload: stagePayload,
            });
            return output;
          } catch (error) {
            if (attempt === 0) continue;
            throw error;
          }
        }

        throw new Error("Stage failed to produce structured output");
      } catch (error) {
        await updateStageRun(stageRunId, {
          status: "failed",
          error: error instanceof Error ? error.message : String(error),
          output: lastResult
            ? {
                raw_response: extractText(lastResult),
                info: lastResult.info ?? null,
              }
            : undefined,
          endedAt: new Date(),
        });
        abortToolEvents?.abort();
        await toolEvents;
        await appendEvent({
          runId,
          eventType: STAGE_EVENT_TYPES.failed,
          payload: {
            ...stagePayload,
            error: error instanceof Error ? error.message : String(error),
          },
        });
        throw error;
      }
    },
  };
}

async function noopAppendEvent() {
  return undefined as unknown as Awaited<ReturnType<AppendEvent>>;
}

async function appendPromptPartEvents(
  appendEvent: AppendEvent,
  runId: string,
  payload: { stage: StageName; round: number; stage_run_id: string },
  result: PromptResult,
) {
  for (const part of result.parts ?? []) {
    if (!part || typeof part !== "object") continue;
    await appendEvent({
      runId,
      eventType: "message.part.updated",
      payload: {
        ...payload,
        event: {
          type: "message.part.updated",
          properties: { part },
        },
      },
    });
  }
}

async function collectToolCallEvents(
  managed: ManagedOpencode,
  appendEvent: AppendEvent,
  runId: string,
  sessionId: string,
  payload: { stage: StageName; round: number; stage_run_id: string },
  signal: AbortSignal,
) {
  try {
    const stream = await subscribeStageEvents(managed, { signal });
    for await (const event of stream as AsyncIterable<unknown>) {
      const eventSessionId = extractSessionId(event);
      if (eventSessionId && eventSessionId !== sessionId) continue;

      await appendEvent({
        runId,
        eventType: extractEventType(event),
        payload: { ...payload, event },
      });

      const toolName = extractToolName(event);
      if (!toolName) continue;

      await appendEvent({
        runId,
        eventType: STAGE_EVENT_TYPES.toolCall,
        payload: { ...payload, tool_name: toolName },
      });
    }
  } catch (error) {
    if (!signal.aborted) throw error;
  }
}
