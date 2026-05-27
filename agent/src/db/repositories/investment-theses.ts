import { eq } from "drizzle-orm";

import { db as defaultDb } from "../client";
import { investmentTheses, type InvestmentThesis } from "../schema";

type Db = typeof defaultDb;

export type CreateInvestmentThesisInput = {
  runId: string;
  objective: string;
  payload: Record<string, unknown>;
};

export async function createInvestmentThesis(
  input: CreateInvestmentThesisInput,
  db: Db = defaultDb,
): Promise<InvestmentThesis> {
  const [created] = await db
    .insert(investmentTheses)
    .values({
      objective: input.objective,
      payload: input.payload,
      runId: input.runId,
    })
    .onConflictDoNothing({ target: investmentTheses.runId })
    .returning();

  if (created) return created;

  const [existing] = await db
    .select()
    .from(investmentTheses)
    .where(eq(investmentTheses.runId, input.runId));

  if (!existing) throw new Error("Failed to create or read investment thesis");
  return existing;
}
