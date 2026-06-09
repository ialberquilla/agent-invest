import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { and, asc, eq, like, sql } from "drizzle-orm";

import { db as defaultDb } from "../client";
import { agentEvents, type AgentEvent, type NewAgentEvent } from "../schema";

type Db = typeof defaultDb;

export type AppendEventInput = Omit<NewAgentEvent, "createdAt" | "eventId"> & {
  eventId?: string;
};

export type ListStageEventsFilters = {
  stage?: string;
  round?: number;
};

// In-process pub/sub so SSE consumers (the frontend's LiveActivity)
// see events the instant they're written without polling the DB.
// Single-process only -- swap for pg LISTEN/NOTIFY if we ever scale
// the API server horizontally.
const eventEmitter = new EventEmitter();
eventEmitter.setMaxListeners(0); // unbounded; one listener per active SSE client

export async function appendEvent(
  input: AppendEventInput,
  db: Db = defaultDb,
): Promise<AgentEvent> {
  const [event] = await db
    .insert(agentEvents)
    .values({ ...input, eventId: input.eventId ?? randomUUID() })
    .returning();

  if (!event) throw new Error("Failed to append agent event");
  eventEmitter.emit("event", event);
  return event;
}

// Subscribe to newly-appended events for a single run_id. Returns
// an unsubscribe function. Caller is responsible for invoking it
// when the SSE client disconnects.
export function subscribeAgentEvents(
  runId: string,
  onEvent: (event: AgentEvent) => void,
): () => void {
  const handler = (event: AgentEvent) => {
    if (event.runId === runId) onEvent(event);
  };
  eventEmitter.on("event", handler);
  return () => eventEmitter.off("event", handler);
}

export async function listStageEventsByRunId(
  runId: string,
  filters: ListStageEventsFilters = {},
  db: Db = defaultDb,
): Promise<AgentEvent[]> {
  const conditions = [eq(agentEvents.runId, runId)];

  if (filters.stage !== undefined) {
    conditions.push(like(agentEvents.eventType, "stage.%"));
    conditions.push(sql`${agentEvents.payload}->>'stage' = ${filters.stage}`);
  }

  if (filters.round !== undefined) {
    conditions.push(like(agentEvents.eventType, "stage.%"));
    conditions.push(
      sql`${agentEvents.payload}->>'round' = ${String(filters.round)}`,
    );
  }

  return db
    .select()
    .from(agentEvents)
    .where(and(...conditions))
    .orderBy(asc(agentEvents.createdAt), asc(agentEvents.eventId));
}
