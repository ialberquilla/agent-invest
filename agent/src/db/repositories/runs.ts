import { eq, sql } from "drizzle-orm";

import { db as defaultDb } from "../client";
import { runs } from "../schema";

type Db = typeof defaultDb;

export type RunRow = {
  runId: string;
  status: string;
  startedAt: Date | string;
  endedAt: Date | string | null;
  exitCode: number | null;
  reply: string | null;
  error: string | null;
};

type RunSummaryFields = {
  winnerTemplateId?: string | null;
  winnersByDimension?: unknown;
  roundHistory?: unknown;
  refinementReasons?: unknown;
  metadata?: unknown;
};

const runColumns = {
  endedAt: runs.endedAt,
  error: runs.error,
  exitCode: runs.exitCode,
  reply: runs.reply,
  runId: runs.runId,
  startedAt: runs.startedAt,
  status: runs.status,
};

export async function createRun(
  runId: string,
  strategyId: string,
  db: Db = defaultDb,
) {
  await db.insert(runs).values({ runId, status: "running", strategyId });
}

export async function readRun(
  runId: string,
  db: Db = defaultDb,
): Promise<RunRow | null> {
  const [run] = await db
    .select(runColumns)
    .from(runs)
    .where(eq(runs.runId, runId));

  return run ?? null;
}

export async function markRunCompleted(
  runId: string,
  reply: string,
  summaryFieldsOrDb: RunSummaryFields | Db = {},
  db: Db = defaultDb,
) {
  const summaryFields = isDb(summaryFieldsOrDb) ? {} : summaryFieldsOrDb;
  const client = isDb(summaryFieldsOrDb) ? summaryFieldsOrDb : db;
  await client
    .update(runs)
    .set({
      endedAt: sql`NOW()`,
      error: null,
      exitCode: 0,
      reply,
      status: "completed",
      ...summaryFields,
    })
    .where(eq(runs.runId, runId));
}

function isDb(value: RunSummaryFields | Db): value is Db {
  return typeof (value as { update?: unknown }).update === "function";
}

export async function markRunFailed(
  runId: string,
  error: string,
  db: Db = defaultDb,
) {
  await db
    .update(runs)
    .set({
      endedAt: sql`NOW()`,
      error,
      exitCode: 1,
      reply: null,
      status: "failed",
    })
    .where(eq(runs.runId, runId));
}
