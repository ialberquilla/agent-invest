import { randomUUID } from "node:crypto";
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

export async function appendEvent(
  input: AppendEventInput,
  db: Db = defaultDb,
): Promise<AgentEvent> {
  const [event] = await db
    .insert(agentEvents)
    .values({ ...input, eventId: input.eventId ?? randomUUID() })
    .returning();

  if (!event) throw new Error("Failed to append agent event");
  return event;
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
