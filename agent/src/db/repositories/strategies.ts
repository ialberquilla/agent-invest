import { and, eq, sql } from "drizzle-orm";

import { db as defaultDb } from "../client";
import { strategies } from "../schema";
import { ensureUser } from "./users";

type Db = Omit<typeof defaultDb, "$client">;

export type StrategyOwnership = {
  userId: string;
};

export type StrategySession = {
  opencodeSessionId: string | null;
  title: string;
};

export async function ensureStrategy(
  userId: string,
  strategyId: string,
  db: Db = defaultDb,
) {
  await ensureUser(userId, db);
  await db
    .insert(strategies)
    .values({
      opencodeSessionId: "",
      strategyId,
      title: "",
      userId,
    })
    .onConflictDoNothing();

  return readStrategyOwnership(strategyId, db);
}

export async function readStrategyOwnership(
  strategyId: string,
  db: Db = defaultDb,
): Promise<StrategyOwnership | null> {
  const [strategy] = await db
    .select({ userId: strategies.userId })
    .from(strategies)
    .where(eq(strategies.strategyId, strategyId));

  return strategy ?? null;
}

export async function touchStrategy(strategyId: string, db: Db = defaultDb) {
  await db
    .update(strategies)
    .set({ lastUsedAt: sql`NOW()` })
    .where(eq(strategies.strategyId, strategyId));
}

export async function updateStrategyTitleIfBlank(
  strategyId: string,
  title: string,
  db: Db = defaultDb,
) {
  const normalizedTitle = title.trim();
  if (!normalizedTitle) return;

  await db
    .update(strategies)
    .set({ title: normalizedTitle })
    .where(
      sql`${strategies.strategyId} = ${strategyId} AND btrim(${strategies.title}) = ''`,
    );
}

export async function readStrategySession(
  strategyId: string,
  options: { lockForUpdate?: boolean } = {},
  db: Db = defaultDb,
): Promise<StrategySession | null> {
  const query = db
    .select({
      opencodeSessionId: strategies.opencodeSessionId,
      title: strategies.title,
    })
    .from(strategies)
    .where(eq(strategies.strategyId, strategyId));

  const [strategy] = options.lockForUpdate
    ? await query.for("update")
    : await query;

  return strategy ?? null;
}

export async function updateStrategySession(
  strategyId: string,
  opencodeSessionId: string,
  db: Db = defaultDb,
) {
  const result = await db
    .update(strategies)
    .set({ opencodeSessionId })
    .where(eq(strategies.strategyId, strategyId));

  return result.rowCount ?? 0;
}

export async function claimStrategies(
  targetUserId: string,
  anonymousUserId: string,
  db: Db = defaultDb,
) {
  await ensureUser(targetUserId, db);
  const result = await db
    .update(strategies)
    .set({ userId: targetUserId })
    .where(
      and(
        eq(strategies.userId, anonymousUserId),
        sql`${strategies.strategyId} IS NOT NULL`,
      ),
    );

  return result.rowCount ?? 0;
}
