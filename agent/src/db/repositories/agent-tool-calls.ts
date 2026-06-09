import { and, eq, sql } from "drizzle-orm";

import { db as defaultDb } from "../client";
import { agentToolCalls } from "../schema";

type Db = typeof defaultDb;

export type RecordToolCallInput = {
  toolCallId: string;
  toolName: string;
  args: unknown;
  runId?: string | null;
  threadId?: string | null;
  stageRunId?: string | null;
  startedEventId?: string | null;
  startedAt?: Date;
};

export type RecordToolResultInput = {
  toolCallId: string;
  result: unknown;
  isError?: boolean;
  errorMessage?: string | null;
  finishedEventId?: string | null;
  finishedAt?: Date;
  toolName?: string;
};

export async function recordToolCall(
  input: RecordToolCallInput,
  db: Db = defaultDb,
): Promise<void> {
  await db
    .insert(agentToolCalls)
    .values({
      toolCallId: input.toolCallId,
      toolName: input.toolName,
      args: input.args as object,
      runId: input.runId ?? null,
      threadId: input.threadId ?? null,
      stageRunId: input.stageRunId ?? null,
      startedEventId: input.startedEventId ?? null,
      startedAt: input.startedAt ?? new Date(),
    })
    .onConflictDoUpdate({
      target: agentToolCalls.toolCallId,
      set: {
        toolName: input.toolName,
        args: input.args as object,
        ...(input.startedEventId
          ? { startedEventId: input.startedEventId }
          : {}),
      },
    });
}

export async function recordToolResult(
  input: RecordToolResultInput,
  db: Db = defaultDb,
): Promise<void> {
  const finishedAt = input.finishedAt ?? new Date();
  await db
    .update(agentToolCalls)
    .set({
      result: input.result as object,
      isError: input.isError ?? false,
      errorMessage: input.errorMessage ?? null,
      finishedAt,
      finishedEventId: input.finishedEventId ?? null,
      ...(input.toolName ? { toolName: input.toolName } : {}),
      durationMs: sql`GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (${finishedAt.toISOString()}::timestamptz - "started_at")) * 1000))::integer`,
    })
    .where(eq(agentToolCalls.toolCallId, input.toolCallId));
}

export async function listByRunId(runId: string, db: Db = defaultDb) {
  return db
    .select()
    .from(agentToolCalls)
    .where(eq(agentToolCalls.runId, runId))
    .orderBy(agentToolCalls.startedAt);
}

export async function listByThreadId(
  threadId: string,
  db: Db = defaultDb,
) {
  return db
    .select()
    .from(agentToolCalls)
    .where(eq(agentToolCalls.threadId, threadId))
    .orderBy(agentToolCalls.startedAt);
}

export async function countByRunId(runId: string, db: Db = defaultDb) {
  const [row] = await db
    .select({ count: sql<number>`count(*)::int` })
    .from(agentToolCalls)
    .where(and(eq(agentToolCalls.runId, runId)));
  return row?.count ?? 0;
}
