import { asc, eq } from "drizzle-orm";

import { db as defaultDb } from "../client";
import { stageRuns, type NewStageRun, type StageRun } from "../schema";

type Db = typeof defaultDb;

export async function createStageRun(
  input: NewStageRun,
  db: Db = defaultDb,
): Promise<StageRun> {
  const [stageRun] = await db.insert(stageRuns).values(input).returning();

  if (!stageRun) throw new Error("Failed to create stage run");
  return stageRun;
}

export async function updateStageRun(
  stageRunId: string,
  updates: Partial<Omit<NewStageRun, "stageRunId" | "runId">>,
  db: Db = defaultDb,
): Promise<StageRun | null> {
  const [stageRun] = await db
    .update(stageRuns)
    .set(updates)
    .where(eq(stageRuns.stageRunId, stageRunId))
    .returning();

  return stageRun ?? null;
}

export async function getStageRun(
  stageRunId: string,
  db: Db = defaultDb,
): Promise<StageRun | null> {
  const [stageRun] = await db
    .select()
    .from(stageRuns)
    .where(eq(stageRuns.stageRunId, stageRunId));

  return stageRun ?? null;
}

export async function listStageRunsByRunId(
  runId: string,
  db: Db = defaultDb,
): Promise<StageRun[]> {
  return db
    .select()
    .from(stageRuns)
    .where(eq(stageRuns.runId, runId))
    .orderBy(
      asc(stageRuns.stage),
      asc(stageRuns.round),
      asc(stageRuns.startedAt),
    );
}
