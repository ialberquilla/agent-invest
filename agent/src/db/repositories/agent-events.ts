import { randomUUID } from "node:crypto";

import { db as defaultDb } from "../client";
import { agentEvents, type AgentEvent, type NewAgentEvent } from "../schema";

type Db = typeof defaultDb;

export type AppendEventInput = Omit<NewAgentEvent, "createdAt" | "eventId"> & {
  eventId?: string;
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
