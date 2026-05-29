import { and, desc, eq } from "drizzle-orm";

import { db as defaultDb } from "../client";
import {
  stageEvalRuns,
  type NewStageEvalRun,
  type StageEvalRun,
} from "../schema";

type Db = typeof defaultDb;

export type ListEvalRunsOptions = {
  stage?: string;
  fixtureId?: string;
  limit?: number;
};

export async function createEvalRun(
  input: NewStageEvalRun,
  db: Db = defaultDb,
): Promise<StageEvalRun> {
  const [evalRun] = await db.insert(stageEvalRuns).values(input).returning();

  if (!evalRun) throw new Error("Failed to create stage eval run");
  return evalRun;
}

export async function listEvalRuns(
  options: ListEvalRunsOptions = {},
  db: Db = defaultDb,
): Promise<StageEvalRun[]> {
  const filters = [
    options.stage ? eq(stageEvalRuns.stage, options.stage) : undefined,
    options.fixtureId
      ? eq(stageEvalRuns.fixtureId, options.fixtureId)
      : undefined,
  ].filter((filter) => filter !== undefined);

  return db
    .select()
    .from(stageEvalRuns)
    .where(filters.length > 0 ? and(...filters) : undefined)
    .orderBy(desc(stageEvalRuns.createdAt))
    .limit(options.limit ?? 50);
}

export async function getEvalRun(
  evalRunId: string,
  db: Db = defaultDb,
): Promise<StageEvalRun | null> {
  const [evalRun] = await db
    .select()
    .from(stageEvalRuns)
    .where(eq(stageEvalRuns.evalRunId, evalRunId))
    .limit(1);

  return evalRun ?? null;
}
