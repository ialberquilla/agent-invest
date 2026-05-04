import { db as defaultDb } from "../client";
import { users } from "../schema";

type Db = Omit<typeof defaultDb, "$client">;

export async function ensureUser(userId: string, db: Db = defaultDb) {
  await db.insert(users).values({ userId }).onConflictDoNothing();
}
