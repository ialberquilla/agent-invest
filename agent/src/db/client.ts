import { Pool, type PoolConfig } from "pg";

function getPoolConfigFromEnv(): PoolConfig {
  if (process.env.DATABASE_URL) {
    return { connectionString: process.env.DATABASE_URL };
  }

  const port = process.env.PGPORT
    ? Number.parseInt(process.env.PGPORT, 10)
    : undefined;

  if (process.env.PGPORT && Number.isNaN(port)) {
    throw new Error(`Invalid PGPORT value: ${process.env.PGPORT}`);
  }

  return {
    host: process.env.PGHOST,
    port,
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
  };
}

export function describePostgresTarget(): string {
  if (process.env.DATABASE_URL) {
    try {
      const url = new URL(process.env.DATABASE_URL);
      const database = url.pathname.replace(/^\//, "") || "postgres";
      const port = url.port || "5432";

      return `${url.hostname || "localhost"}:${port}/${database}`;
    } catch {
      return "DATABASE_URL";
    }
  }

  const host = process.env.PGHOST || "localhost";
  const port = process.env.PGPORT || "5432";
  const database = process.env.PGDATABASE || process.env.PGUSER || "postgres";

  return `${host}:${port}/${database}`;
}

export const pg = new Pool(getPoolConfigFromEnv());
