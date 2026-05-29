"use client";

import { Fragment, useEffect, useMemo, useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import {
  initialTimeline,
  reduceTimeline,
  type TimelinePart,
  type ToolStatus,
} from "@/lib/agent-events";
import type { StageEvent, StageEventType } from "@/lib/types";
import { cn } from "@/lib/utils";

const TOOL_STATUS_LABEL: Record<ToolStatus, string> = {
  pending: "queued",
  running: "running",
  completed: "done",
  error: "error",
};

const TOOL_STATUS_TONE: Record<ToolStatus, string> = {
  pending: "bg-muted text-muted-foreground",
  running: "bg-blue-500/15 text-blue-700 dark:text-blue-300",
  completed: "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300",
  error: "bg-destructive/15 text-destructive",
};

export type LiveActivityProps = {
  runId?: string;
  parts?: TimelinePart[];
  fullWidth?: boolean;
  includeText?: boolean;
};

type ActivityFilter = {
  stage: string;
  round: string;
};

type EventGroup = {
  key: string;
  stage: string;
  round: number;
  stageRunId: string;
  events: StageEvent[];
};

const STAGE_EVENT_LABEL: Record<StageEventType, string> = {
  "stage.started": "started",
  "stage.tool_call": "tool call",
  "stage.completed": "completed",
  "stage.failed": "failed",
};

const STAGE_EVENT_TONE: Record<StageEventType, string> = {
  "stage.started": "bg-blue-500/15 text-blue-700 dark:text-blue-300",
  "stage.tool_call": "bg-violet-500/15 text-violet-700 dark:text-violet-300",
  "stage.completed": "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300",
  "stage.failed": "bg-destructive/15 text-destructive",
};

export function LiveActivity({
  runId,
  parts = [],
  fullWidth = false,
  includeText = true,
}: LiveActivityProps) {
  const [filters, setFilters] = useState<ActivityFilter>({
    stage: "",
    round: "",
  });
  const [events, setEvents] = useState<StageEvent[]>([]);
  const [status, setStatus] = useState<"idle" | "loading" | "error">("idle");

  useEffect(() => {
    if (!runId) return;

    // SSE-only: the backend sends a `snapshot` frame with everything
    // appended so far, then `event` frames as new rows arrive. The old
    // 1Hz polling against /events is gone.
    setStatus("loading");
    const source = new EventSource(
      `/api/runs/${encodeURIComponent(runId)}/events/stream`,
    );

    const isTerminalEvent = (event: StageEvent): boolean => {
      if (
        event.event_type !== "stage.completed" &&
        event.event_type !== "stage.failed"
      ) {
        return false;
      }
      const payload = event.payload;
      return (
        typeof payload === "object" &&
        payload !== null &&
        (payload as { stage?: unknown }).stage === "workflow"
      );
    };

    const onSnapshot = (frame: MessageEvent) => {
      try {
        const payload = JSON.parse(frame.data) as unknown;
        if (!Array.isArray(payload)) throw new Error("Invalid snapshot");
        const filtered = payload.filter(isStageEvent);
        setEvents(filtered);
        setStatus("idle");
        if (filtered.some(isTerminalEvent)) source.close();
      } catch {
        setStatus("error");
      }
    };

    const onEvent = (frame: MessageEvent) => {
      try {
        const payload = JSON.parse(frame.data) as unknown;
        if (!isStageEvent(payload)) return;
        setEvents((prev) =>
          prev.some((e) => e.event_id === payload.event_id)
            ? prev
            : [...prev, payload],
        );
        if (isTerminalEvent(payload)) source.close();
      } catch {
        // Non-stage frames are ignored.
      }
    };

    source.addEventListener("snapshot", onSnapshot);
    source.addEventListener("event", onEvent);
    source.addEventListener("error", () => {
      // EventSource auto-reconnects unless we close it explicitly.
      // We only mark status=error when the connection is in CLOSED state.
      if (source.readyState === EventSource.CLOSED) setStatus("error");
    });

    return () => {
      source.removeEventListener("snapshot", onSnapshot);
      source.removeEventListener("event", onEvent);
      source.close();
    };
  }, [runId]);

  // The SSE feed delivers every event for the run; we apply the
  // stage/round filter client-side now that the backend is push-only.
  const filteredEvents = useMemo(() => {
    if (!filters.stage && !filters.round) return events;
    return events.filter((event) => {
      const payload =
        typeof event.payload === "object" && event.payload !== null
          ? (event.payload as { stage?: unknown; round?: unknown })
          : undefined;
      if (filters.stage && payload?.stage !== filters.stage) return false;
      if (filters.round && String(payload?.round ?? "") !== filters.round) {
        return false;
      }
      return true;
    });
  }, [events, filters.stage, filters.round]);

  const groups = useMemo(
    () => groupEvents(filteredEvents),
    [filteredEvents],
  );
  const rawParts = useMemo(
    () => rawTimelineParts(filteredEvents),
    [filteredEvents],
  );

  const visibleParts = [...parts, ...rawParts].filter((part) =>
    isVisible(part, includeText),
  );

  if (visibleParts.length > 0) {
    return (
      <div className="flex justify-start">
        <Card
          size="sm"
          className={cn(
            "border-dashed bg-muted/30 py-3",
            fullWidth ? "w-full" : "max-w-[90%] sm:max-w-[80%]",
          )}
        >
          <CardContent className="space-y-3">
            <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              agent activity
            </p>
            <div className="space-y-2">
              {visibleParts.map((part) => (
                <PartView key={part.id} part={part} />
              ))}
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  if (runId) {
    if (status !== "loading" && groups.length === 0) return null;

    return (
      <div className="flex justify-start">
        <Card
          size="sm"
          className={cn(
            "border-dashed bg-muted/30 py-3",
            fullWidth ? "w-full" : "max-w-[90%] sm:max-w-[80%]",
          )}
        >
          <CardContent className="space-y-3">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
              <div>
                <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                  stage activity
                </p>
                {status === "error" ? (
                  <p className="mt-1 text-xs text-destructive">
                    Unable to load activity events.
                  </p>
                ) : null}
              </div>
              <ActivityFilters
                events={events}
                filters={filters}
                onChange={setFilters}
              />
            </div>
            <div className="space-y-4">
              {groups.map((group) => (
                <EventGroupView key={group.key} group={group} />
              ))}
              {status === "loading" && groups.length === 0 ? (
                <p className="text-sm italic text-muted-foreground">
                  loading activity...
                </p>
              ) : null}
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  return null;
}

function ActivityFilters({
  events,
  filters,
  onChange,
}: {
  events: StageEvent[];
  filters: ActivityFilter;
  onChange: (filters: ActivityFilter) => void;
}) {
  const stages = uniqueSorted(
    events.map((event) => stringPayload(event, "stage")),
  );
  const rounds = uniqueSorted(
    events
      .map((event) => numberPayload(event, "round"))
      .filter((round): round is number => round !== null)
      .map(String),
  );

  return (
    <div className="flex flex-wrap gap-2 text-xs">
      <label className="flex items-center gap-1 text-muted-foreground">
        Stage
        <select
          className="h-8 rounded-md border bg-background px-2 text-foreground"
          value={filters.stage}
          onChange={(event) =>
            onChange({ ...filters, stage: event.target.value })
          }
        >
          <option value="">All</option>
          {stages.map((stage) => (
            <option key={stage} value={stage}>
              {stage}
            </option>
          ))}
        </select>
      </label>
      <label className="flex items-center gap-1 text-muted-foreground">
        Round
        <select
          className="h-8 rounded-md border bg-background px-2 text-foreground"
          value={filters.round}
          onChange={(event) =>
            onChange({ ...filters, round: event.target.value })
          }
        >
          <option value="">All</option>
          {rounds.map((round) => (
            <option key={round} value={round}>
              {round}
            </option>
          ))}
        </select>
      </label>
    </div>
  );
}

function EventGroupView({ group }: { group: EventGroup }) {
  const startedAt = group.events.find(
    (event) => event.event_type === "stage.started",
  )?.created_at;

  return (
    <section className="rounded-lg border bg-background/60 p-3">
      <div className="mb-3 flex flex-wrap items-center gap-2">
        <Badge variant="outline" className="capitalize">
          {group.stage}
        </Badge>
        <Badge variant="secondary">round {group.round}</Badge>
        <span className="font-mono text-[10px] text-muted-foreground">
          {group.stageRunId}
        </span>
      </div>
      <ol className="relative space-y-3 border-l pl-4">
        {group.events.map((event) => (
          <StageEventView
            key={event.event_id}
            event={event}
            durationMs={
              startedAt ? elapsedMs(startedAt, event.created_at) : null
            }
          />
        ))}
      </ol>
    </section>
  );
}

function StageEventView({
  event,
  durationMs,
}: {
  event: StageEvent;
  durationMs: number | null;
}) {
  const toolName = stringPayload(event, "tool_name");
  const error = stringPayload(event, "error");

  return (
    <li className="relative">
      <span className="absolute -left-[21px] top-1.5 h-2.5 w-2.5 rounded-full border border-background bg-muted-foreground" />
      <div className="flex flex-wrap items-center gap-2">
        <Badge
          className={cn("text-[10px]", STAGE_EVENT_TONE[event.event_type])}
        >
          {STAGE_EVENT_LABEL[event.event_type]}
        </Badge>
        {toolName ? (
          <Badge variant="outline" className="font-mono text-[10px] uppercase">
            {toolName}
          </Badge>
        ) : null}
        {durationMs !== null ? (
          <span className="text-[11px] text-muted-foreground">
            +{formatDuration(durationMs)}
          </span>
        ) : null}
        <time
          className="text-[11px] text-muted-foreground"
          dateTime={event.created_at}
        >
          {formatTime(event.created_at)}
        </time>
      </div>
      {error ? <p className="mt-1 text-xs text-destructive">{error}</p> : null}
      <StagePayloadDetails payload={event.payload} skip={error ? ["error"] : []} />
    </li>
  );
}

// Renders the non-metadata fields of a stage event's payload as a
// compact key/value grid, so the user can see what each workflow step
// actually produced (universe size, window length, candidate counts,
// decide action, etc.) without opening devtools.
const PAYLOAD_SKIP_KEYS = new Set([
  "stage",
  "round",
  "stage_run_id",
  "transition_n",
  "tool_name",
]);

function StagePayloadDetails({
  payload,
  skip = [],
}: {
  payload: StageEvent["payload"];
  skip?: string[];
}) {
  const skipSet = useMemo(
    () => new Set([...PAYLOAD_SKIP_KEYS, ...skip]),
    [skip],
  );
  const entries = useMemo(() => {
    if (typeof payload !== "object" || payload === null) return [];
    return Object.entries(payload).filter(
      ([key, value]) => !skipSet.has(key) && value !== undefined,
    );
  }, [payload, skipSet]);

  if (entries.length === 0) return null;

  return (
    <dl className="mt-1.5 grid grid-cols-[max-content_minmax(0,1fr)] gap-x-3 gap-y-0.5 text-[11px]">
      {entries.map(([key, value]) => (
        <Fragment key={key}>
          <dt className="self-start whitespace-nowrap font-mono text-muted-foreground">
            {key}
          </dt>
          <dd className="whitespace-pre-wrap break-words font-mono leading-relaxed text-foreground/80">
            {formatPayloadValue(value)}
          </dd>
        </Fragment>
      ))}
    </dl>
  );
}

function formatPayloadValue(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (Array.isArray(value)) {
    return value
      .map((item) =>
        typeof item === "string" ||
        typeof item === "number" ||
        typeof item === "boolean"
          ? String(item)
          : JSON.stringify(item),
      )
      .join(", ");
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function isVisible(part: TimelinePart, includeText: boolean): boolean {
  if (part.kind === "step-start" || part.kind === "step-finish") return false;
  if (part.kind === "text" && !includeText) return false;
  if (part.kind === "reasoning" || part.kind === "text") {
    return part.text.trim().length > 0;
  }
  return true;
}

function PartView({ part }: { part: TimelinePart }) {
  if (part.kind === "reasoning") {
    return (
      <div className="rounded-md border-l-2 border-amber-400/50 bg-background/50 px-3 py-2">
        <p className="text-[10px] font-semibold uppercase tracking-wider text-amber-700 dark:text-amber-400">
          reasoning {part.completed ? null : <span aria-hidden>…</span>}
        </p>
        <pre className="mt-1 whitespace-pre-wrap break-words font-sans text-xs italic leading-5 text-muted-foreground">
          {part.text}
        </pre>
      </div>
    );
  }

  if (part.kind === "tool") {
    const statusLabel = TOOL_STATUS_LABEL[part.status] ?? part.status;
    const tone =
      TOOL_STATUS_TONE[part.status] ?? "bg-muted text-muted-foreground";
    return (
      <div className="rounded-md border border-border bg-background/60 px-3 py-2">
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline" className="font-mono text-[10px] uppercase">
            {part.name}
          </Badge>
          <Badge className={cn("text-[10px]", tone)}>{statusLabel}</Badge>
          {part.description ? (
            <span className="truncate text-xs text-muted-foreground">
              {part.description}
            </span>
          ) : null}
        </div>
        {part.command ? (
          <pre className="mt-2 max-h-32 overflow-auto rounded bg-muted/60 px-2 py-1 font-mono text-[11px] leading-5">
            {part.command}
          </pre>
        ) : null}
        {part.output ? (
          <pre className="mt-2 max-h-40 overflow-auto rounded bg-muted/40 px-2 py-1 font-mono text-[11px] leading-5 text-muted-foreground">
            {truncate(part.output, 1200)}
          </pre>
        ) : null}
        {part.errorMessage ? (
          <p className="mt-2 text-xs text-destructive">{part.errorMessage}</p>
        ) : null}
      </div>
    );
  }

  if (part.kind === "text") {
    return (
      <div className="rounded-md border-l-2 border-primary/40 bg-background/80 px-3 py-2">
        <p className="text-[10px] font-semibold uppercase tracking-wider text-primary/80">
          drafting reply {part.completed ? null : <span aria-hidden>…</span>}
        </p>
        <pre className="mt-1 whitespace-pre-wrap break-words font-sans text-sm leading-6">
          {part.text}
        </pre>
      </div>
    );
  }

  return null;
}

function truncate(text: string, max: number): string {
  if (text.length <= max) return text;
  return `…${text.slice(-max)}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isStageEventType(value: unknown): value is StageEventType {
  return typeof value === "string" && value.length > 0;
}

function isStageEvent(value: unknown): value is StageEvent {
  return (
    isRecord(value) &&
    typeof value.event_id === "string" &&
    isStageEventType(value.event_type) &&
    isRecord(value.payload) &&
    typeof value.created_at === "string"
  );
}

function stringPayload(event: StageEvent, key: keyof StageEvent["payload"]) {
  const value = event.payload[key];
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function numberPayload(event: StageEvent, key: keyof StageEvent["payload"]) {
  const value = event.payload[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function groupEvents(events: StageEvent[]): EventGroup[] {
  const groups = new Map<string, EventGroup>();

  for (const event of events) {
    if (!event.event_type.startsWith("stage.")) continue;
    const stage = stringPayload(event, "stage");
    const round = numberPayload(event, "round");
    const stageRunId = stringPayload(event, "stage_run_id");
    if (!stage || round === null || !stageRunId) continue;

    const key = `${stageRunId}:${stage}:${round}`;
    const group = groups.get(key) ?? {
      key,
      stage,
      round,
      stageRunId,
      events: [],
    };
    group.events.push(event);
    groups.set(key, group);
  }

  return [...groups.values()]
    .map((group) => ({
      ...group,
      events: [...group.events].sort((left, right) =>
        left.created_at.localeCompare(right.created_at),
      ),
    }))
    .sort((left, right) => {
      if (left.round !== right.round) return left.round - right.round;
      const stage = left.stage.localeCompare(right.stage);
      if (stage !== 0) return stage;
      return (
        left.events[0]?.created_at.localeCompare(
          right.events[0]?.created_at ?? "",
        ) ?? 0
      );
    });
}

function rawTimelineParts(events: StageEvent[]): TimelinePart[] {
  let timeline = initialTimeline;

  for (const event of events) {
    if (event.event_type.startsWith("stage.")) continue;
    const payload = isRecord(event.payload) ? event.payload.event : null;
    timeline = reduceTimeline(timeline, {
      event: event.event_type,
      data: JSON.stringify(payload ?? event.payload),
    });
  }

  return timeline.parts;
}

function uniqueSorted(values: Array<string | null>) {
  return [
    ...new Set(values.filter((value): value is string => Boolean(value))),
  ].sort();
}

function elapsedMs(start: string, end: string) {
  const startMs = Date.parse(start);
  const endMs = Date.parse(end);
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) return null;
  return Math.max(0, endMs - startMs);
}

function formatDuration(ms: number) {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(ms < 10000 ? 1 : 0)}s`;
}

function formatTime(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}
