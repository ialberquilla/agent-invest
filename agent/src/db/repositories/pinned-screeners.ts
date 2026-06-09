import { and, desc, eq, sql } from "drizzle-orm";

import { db as defaultDb } from "../client";
import { pinnedScreeners } from "../schema";
import { ensureUser } from "./users";

type Db = Omit<typeof defaultDb, "$client">;

export type PinnedScreenerDefinition = {
  factor: "momentum" | "risk_adjusted" | "low_volatility";
  limit: number;
  gmx_only: boolean;
  as_of?: string;
};

export type PinnedScreener = {
  id: string;
  user_id: string;
  title: string;
  definition: PinnedScreenerDefinition;
  created_at: string;
  updated_at: string;
  last_refreshed_at: string | null;
};

export function screenerId(definition: PinnedScreenerDefinition) {
  return [
    definition.factor,
    definition.limit,
    definition.gmx_only ? "gmx" : "all",
    definition.as_of ?? "latest",
  ].join(":");
}

export async function listPinnedScreeners(userId: string, db: Db = defaultDb) {
  const rows = await db
    .select()
    .from(pinnedScreeners)
    .where(eq(pinnedScreeners.userId, userId))
    .orderBy(desc(pinnedScreeners.updatedAt));

  return rows.map(toPinnedScreener);
}

export async function upsertPinnedScreener(
  input: {
    userId: string;
    title: string;
    definition: PinnedScreenerDefinition;
  },
  db: Db = defaultDb,
) {
  await ensureUser(input.userId, db);
  const id = screenerId(input.definition);
  const [row] = await db
    .insert(pinnedScreeners)
    .values({
      screenerId: id,
      userId: input.userId,
      title: input.title,
      definition: input.definition,
    })
    .onConflictDoUpdate({
      target: [pinnedScreeners.userId, pinnedScreeners.screenerId],
      set: {
        title: input.title,
        definition: input.definition,
        updatedAt: sql`NOW()`,
      },
    })
    .returning();

  return toPinnedScreener(row);
}

export async function readPinnedScreener(
  userId: string,
  screenerId: string,
  db: Db = defaultDb,
) {
  const [row] = await db
    .select()
    .from(pinnedScreeners)
    .where(
      and(
        eq(pinnedScreeners.userId, userId),
        eq(pinnedScreeners.screenerId, screenerId),
      ),
    );

  return row ? toPinnedScreener(row) : null;
}

export async function deletePinnedScreener(
  userId: string,
  screenerId: string,
  db: Db = defaultDb,
) {
  const result = await db
    .delete(pinnedScreeners)
    .where(
      and(
        eq(pinnedScreeners.userId, userId),
        eq(pinnedScreeners.screenerId, screenerId),
      ),
    );

  return result.rowCount ?? 0;
}

export async function markPinnedScreenerRefreshed(
  userId: string,
  screenerId: string,
  db: Db = defaultDb,
) {
  await db
    .update(pinnedScreeners)
    .set({ lastRefreshedAt: sql`NOW()`, updatedAt: sql`NOW()` })
    .where(
      and(
        eq(pinnedScreeners.userId, userId),
        eq(pinnedScreeners.screenerId, screenerId),
      ),
    );
}

function toPinnedScreener(row: typeof pinnedScreeners.$inferSelect): PinnedScreener {
  return {
    id: row.screenerId,
    user_id: row.userId,
    title: row.title,
    definition: row.definition as PinnedScreenerDefinition,
    created_at: row.createdAt.toISOString(),
    updated_at: row.updatedAt.toISOString(),
    last_refreshed_at: row.lastRefreshedAt?.toISOString() ?? null,
  };
}
