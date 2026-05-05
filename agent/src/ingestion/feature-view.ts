import { sql } from "drizzle-orm";

import { db } from "../db/client";

type FeatureViewRefreshDatabase = Pick<typeof db, "execute">;

export async function refreshAssetUniverseFeatures(
  database: FeatureViewRefreshDatabase = db,
): Promise<void> {
  await database.execute(
    sql`REFRESH MATERIALIZED VIEW CONCURRENTLY "agent_asset_universe_features"`,
  );
}
