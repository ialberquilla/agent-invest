import { desc, eq } from "drizzle-orm";

import type { StrategyMandate } from "../../agent/workflow/mandate.ts";
import { db as defaultDb } from "../client";
import {
  type NewStrategyMandateRow,
  type StrategyMandateRow,
  strategyMandates,
} from "../schema";

type Db = Omit<typeof defaultDb, "$client">;

// Persist a freshly built mandate as a `pending` row. Idempotent on mandate_id
// so a retried persist never double-writes.
export async function insertMandate(
  mandate: StrategyMandate,
  db: Db = defaultDb,
): Promise<StrategyMandateRow | null> {
  const row: NewStrategyMandateRow = {
    mandateId: mandate.mandate_id,
    runId: mandate.run_id,
    version: mandate.version,
    status: mandate.status,
    templateId: mandate.template_id,
    spec: mandate as never,
  };

  const [inserted] = await db
    .insert(strategyMandates)
    .values(row)
    .onConflictDoNothing()
    .returning();

  return inserted ?? null;
}

export async function readMandate(
  mandateId: string,
  db: Db = defaultDb,
): Promise<StrategyMandateRow | null> {
  const [mandate] = await db
    .select()
    .from(strategyMandates)
    .where(eq(strategyMandates.mandateId, mandateId));

  return mandate ?? null;
}

export async function readMandatesForRun(
  runId: string,
  db: Db = defaultDb,
): Promise<StrategyMandateRow[]> {
  return db
    .select()
    .from(strategyMandates)
    .where(eq(strategyMandates.runId, runId))
    .orderBy(desc(strategyMandates.createdAt));
}

export async function readMandatesByStatus(
  status: StrategyMandate["status"],
  db: Db = defaultDb,
): Promise<StrategyMandateRow[]> {
  return db
    .select()
    .from(strategyMandates)
    .where(eq(strategyMandates.status, status))
    .orderBy(desc(strategyMandates.createdAt));
}

export async function updateMandateStatus(
  mandateId: string,
  status: StrategyMandate["status"],
  db: Db = defaultDb,
): Promise<number> {
  const result = await db
    .update(strategyMandates)
    .set({ status, updatedAt: new Date() })
    .where(eq(strategyMandates.mandateId, mandateId));

  return result.rowCount ?? 0;
}
