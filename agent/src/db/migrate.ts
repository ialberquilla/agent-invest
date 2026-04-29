import "../env.js";
import { access, readFile, readdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import type { PoolClient } from "pg";

import { describePostgresTarget, pg } from "./client.js";

type Migration = {
  name: string;
  path: string;
};

const MIGRATIONS_TABLE = "schema_migrations";

async function resolveMigrationsDirectory(): Promise<string> {
  const currentDirectory = dirname(fileURLToPath(import.meta.url));
  const candidates = [
    resolve(currentDirectory, "migrations"),
    resolve(currentDirectory, "../../src/db/migrations"),
  ];

  for (const candidate of candidates) {
    try {
      await access(candidate);
      return candidate;
    } catch {
      continue;
    }
  }

  throw new Error("Could not locate the SQL migrations directory.");
}

async function listPendingMigrations(): Promise<Migration[]> {
  const migrationsDirectory = await resolveMigrationsDirectory();
  const entries = await readdir(migrationsDirectory, { withFileTypes: true });

  return entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".up.sql"))
    .map((entry) => ({
      name: entry.name,
      path: resolve(migrationsDirectory, entry.name),
    }))
    .sort((left, right) => left.name.localeCompare(right.name));
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function ensureMigrationsTable(client: PoolClient): Promise<void> {
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (
      name TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

async function getAppliedMigrations(client: PoolClient): Promise<Set<string>> {
  const result = await client.query<{ name: string }>(
    `SELECT name FROM ${MIGRATIONS_TABLE}`,
  );

  return new Set(result.rows.map((row) => row.name));
}

async function applyMigration(
  client: PoolClient,
  migration: Migration,
): Promise<void> {
  const sql = await readFile(migration.path, "utf8");

  await client.query("BEGIN");

  try {
    await client.query(sql);
    await client.query(`INSERT INTO ${MIGRATIONS_TABLE} (name) VALUES ($1)`, [
      migration.name,
    ]);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw new Error(
      `Failed to apply migration ${migration.name}: ${getErrorMessage(error)}`,
    );
  }
}

export async function runMigrations(): Promise<void> {
  let client: PoolClient;

  try {
    client = await pg.connect();
    await client.query("SELECT 1");
  } catch (error) {
    throw new Error(
      `Failed to connect to Postgres at ${describePostgresTarget()}: ${getErrorMessage(error)}`,
    );
  }

  try {
    await ensureMigrationsTable(client);

    const migrations = await listPendingMigrations();
    const appliedMigrations = await getAppliedMigrations(client);
    let appliedCount = 0;

    for (const migration of migrations) {
      if (appliedMigrations.has(migration.name)) {
        continue;
      }

      await applyMigration(client, migration);
      appliedCount += 1;
      console.info(`Applied migration ${migration.name}`);
    }

    if (appliedCount === 0) {
      console.info("Database schema is already up to date.");
      return;
    }

    console.info(`Applied ${appliedCount} migration(s).`);
  } finally {
    client.release();
  }
}

async function main(): Promise<void> {
  try {
    await runMigrations();
  } finally {
    await pg.end();
  }
}

void main().catch((error) => {
  console.error(getErrorMessage(error));
  process.exitCode = 1;
});
