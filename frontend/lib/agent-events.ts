import type { Run } from "@/lib/types";
import type { SseMessage } from "@/lib/sse";

export type ToolStatus = "pending" | "running" | "completed" | "error";

export type ReasoningPart = {
  kind: "reasoning";
  id: string;
  text: string;
  completed: boolean;
};

export type TextPart = {
  kind: "text";
  id: string;
  text: string;
  completed: boolean;
};

export type ToolPart = {
  kind: "tool";
  id: string;
  name: string;
  command?: string;
  description?: string;
  status: ToolStatus;
  output?: string;
  errorMessage?: string;
};

export type StepBoundaryPart = {
  kind: "step-start" | "step-finish";
  id: string;
};

export type TimelinePart =
  | ReasoningPart
  | TextPart
  | ToolPart
  | StepBoundaryPart;

export type TimelineState = {
  parts: TimelinePart[];
  done: boolean;
  finalRun?: Run;
};

export const initialTimeline: TimelineState = { parts: [], done: false };

type AnyRecord = Record<string, unknown>;

function asRecord(value: unknown): AnyRecord | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return value as AnyRecord;
}

function getString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function safeJsonParse(raw: string): unknown {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export function reduceTimeline(
  state: TimelineState,
  message: SseMessage,
): TimelineState {
  const payload = safeJsonParse(message.data);

  if (message.event === "run.completed") {
    return { ...state, done: true, finalRun: payload as Run };
  }
  if (message.event === "run.finalizing") {
    return upsertPart(state, {
      kind: "tool",
      id: "run-finalizing",
      name: "report",
      description: "Creating structured report and charts",
      status: "running",
    });
  }
  if (message.event === "run.started") {
    return state;
  }

  if (message.event.startsWith("stage.")) {
    return upsertPart(state, partFromStageEvent(message.event, payload));
  }

  const payloadRecord = asRecord(payload);
  if (!payloadRecord) return state;
  const properties = asRecord(payloadRecord.properties) ?? payloadRecord;

  if (message.event === "message.part.updated") {
    const part = asRecord(properties.part);
    if (!part) return state;
    return upsertPart(state, partFromUpdate(part));
  }

  if (message.event === "message.part.delta") {
    const partID = getString(properties.partID);
    const field = getString(properties.field);
    const delta = getString(properties.delta);
    if (!partID || !delta) return state;
    return appendDelta(state, partID, field ?? "text", delta);
  }

  return state;
}

function partFromStageEvent(
  eventName: string,
  payload: unknown,
): TimelinePart | null {
  const event = asRecord(payload);
  const eventId = getString(event?.event_id);
  const eventType = getString(event?.event_type) ?? eventName;
  const stagePayload = asRecord(event?.payload);
  const stage = getString(stagePayload?.stage) ?? "stage";
  const round =
    typeof stagePayload?.round === "number" ? ` r${stagePayload.round}` : "";
  const stageRunId =
    getString(stagePayload?.stage_run_id) ?? eventId ?? eventType;
  const toolName = getString(stagePayload?.tool_name);
  const error = getString(stagePayload?.error);

  if (eventType === "stage.tool_call") {
    return {
      kind: "tool",
      id: eventId ?? `${stageRunId}:${toolName ?? "tool"}`,
      name: toolName ?? "tool",
      description: `${stage}${round}`,
      status: error ? "error" : "running",
      errorMessage: error,
    };
  }

  const status = eventType === "stage.failed" ? "error" : "completed";
  const label = eventType.replace("stage.", "").replace("_", " ");
  return {
    kind: "tool",
    id: eventId ?? `${stageRunId}:${eventType}`,
    name: stage,
    description: `${label}${round}`,
    status,
    errorMessage: error,
  };
}

function partFromUpdate(part: AnyRecord): TimelinePart | null {
  const id = getString(part.id);
  const type = getString(part.type);
  if (!id || !type) return null;

  const time = asRecord(part.time);
  const completed = !!(time && time.end !== undefined);

  if (type === "reasoning") {
    return {
      kind: "reasoning",
      id,
      text: getString(part.text) ?? "",
      completed,
    };
  }
  if (type === "text") {
    return {
      kind: "text",
      id,
      text: getString(part.text) ?? "",
      completed,
    };
  }
  if (type === "tool") {
    const state = asRecord(part.state) ?? {};
    const input = asRecord(state.input) ?? {};
    const metadata = asRecord(state.metadata) ?? {};
    const status = (getString(state.status) as ToolStatus) ?? "pending";
    return {
      kind: "tool",
      id,
      name: getString(part.tool) ?? "tool",
      command: getString(input.command),
      description:
        getString(input.description) ?? getString(metadata.description),
      status,
      output: getString(metadata.output),
      errorMessage: getString(state.error),
    };
  }
  if (type === "step-start" || type === "step-finish") {
    return { kind: type, id };
  }
  return null;
}

function upsertPart(
  state: TimelineState,
  next: TimelinePart | null,
): TimelineState {
  if (!next) return state;
  const index = state.parts.findIndex((part) => part.id === next.id);
  if (index === -1) {
    return { ...state, parts: [...state.parts, next] };
  }
  const previous = state.parts[index];
  const merged = mergePart(previous, next);
  if (merged === previous) return state;
  const copy = state.parts.slice();
  copy[index] = merged;
  return { ...state, parts: copy };
}

function mergePart(previous: TimelinePart, next: TimelinePart): TimelinePart {
  if (previous.kind !== next.kind) return next;
  if (previous.kind === "reasoning" || previous.kind === "text") {
    const incoming = next as ReasoningPart | TextPart;
    const incomingText = incoming.text;
    const previousText = previous.text;
    return {
      ...previous,
      ...incoming,
      text:
        incomingText.length >= previousText.length
          ? incomingText
          : previousText,
    };
  }
  return { ...previous, ...next };
}

function appendDelta(
  state: TimelineState,
  partID: string,
  field: string,
  delta: string,
): TimelineState {
  if (field !== "text") return state;
  const index = state.parts.findIndex((part) => part.id === partID);
  if (index === -1) return state;
  const part = state.parts[index];
  if (part.kind !== "reasoning" && part.kind !== "text") return state;
  const updated: TimelinePart = { ...part, text: part.text + delta };
  const copy = state.parts.slice();
  copy[index] = updated;
  return { ...state, parts: copy };
}
