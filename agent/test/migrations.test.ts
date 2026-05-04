import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

test("local migration smoke test applies pending Drizzle migrations", async (t) => {
  if (!process.env.DATABASE_URL || !process.env.RUN_MIGRATION_SMOKE_TEST) {
    t.skip(
      "Set DATABASE_URL and RUN_MIGRATION_SMOKE_TEST=1 to run the local Postgres migration smoke test.",
    );
    return;
  }

  const { pgPool } = await import("../src/db/client");

  try {
    await execFileAsync("pnpm", ["db:migrate"], {
      cwd: fileURLToPath(new URL("..", import.meta.url)),
    });

    const result = await pgPool.query<{ count: string }>(
      "SELECT COUNT(*)::text AS count FROM drizzle.__drizzle_migrations",
    );

    assert.ok(Number(result.rows[0]?.count ?? 0) > 0);
  } finally {
    await pgPool.end();
  }
});
