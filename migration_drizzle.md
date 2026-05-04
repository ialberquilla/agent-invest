# Drizzle Migration Plan

## Goal

Replace the current hand-written PostgreSQL access and migration system with Drizzle ORM and drizzle-kit migrations, while preparing the data model for conversations, backtests, artifacts, and historical market data.

This migration can assume the current local Docker database data is disposable. The preferred path is a clean schema reset instead of preserving the existing `schema_migrations` history.

## Original State

The original database system was intentionally small:

- `agent/src/db/client.ts` creates a raw `pg.Pool`.
- Drizzle owns migrations through `agent/drizzle.config.ts` and generated SQL in `agent/migrations`.
- The retired hand-written runner at `agent/src/db/migrate.ts` and old `agent/src/db/migrations/*.sql` files have been removed.
- `agent/src/api/server.ts` embeds raw SQL for users, strategies, and runs.
- `agent/src/agent/session.ts` embeds raw SQL and manual transactions for OpenCode session creation.
- `docker-compose.yml` runs PostgreSQL 16 using the named volume `postgres_data`.

Original tables before the Drizzle baseline:

- `users`
- `strategies`
- `runs`
- `schema_migrations`

## Recommended Stack

Use:

- `drizzle-orm`
- `drizzle-kit`
- PostgreSQL 16
- Existing `pg` driver/pool
- TypeScript schema definitions
- Generated SQL migrations

Do not move large generated files directly into PostgreSQL. Use PostgreSQL for structured/queryable state and object/file storage for large artifacts.

## Storage Boundaries

### PostgreSQL

Store structured, queryable, transactional data in PostgreSQL:

- users
- strategies
- conversation threads
- conversation messages
- agent/tool events
- runs
- backtest requests
- backtest result summaries/KPIs
- artifact metadata
- asset metadata
- small/medium historical price data

### JSONB

Use `jsonb` for structured payloads that are useful to query but too flexible for stable columns:

- tool call payloads
- model/provider metadata
- allocation snapshots
- cost assumptions
- raw report summaries
- agent event payloads
- source ingestion metadata

### File/Object Storage

Store large files outside the database:

- PNG charts
- CSV exports
- large reports
- raw source datasets
- generated artifacts
- large logs

PostgreSQL should store only metadata and storage references for these files.

## Proposed Schema

### users

Stores app users.

Columns:

- `user_id text primary key`
- `created_at timestamptz not null default now()`

Keep `text` unless all callers are guaranteed to provide UUIDs. If all IDs are guaranteed UUIDs, switch to `uuid` during this reset.

### strategies

Stores user-owned investment strategies.

Columns:

- `strategy_id text primary key`
- `user_id text not null references users(user_id) on delete cascade`
- `opencode_session_id text null`
- `title text not null default ''`
- `created_at timestamptz not null default now()`
- `last_used_at timestamptz not null default now()`

Indexes:

- `(user_id, last_used_at desc)`
- `(opencode_session_id)`

Change from current behavior:

- Use `null` for missing `opencode_session_id` instead of an empty string.

### conversation_threads

Stores logical conversations between a user and the agent.

Columns:

- `thread_id text primary key`
- `user_id text not null references users(user_id) on delete cascade`
- `strategy_id text null references strategies(strategy_id) on delete cascade`
- `provider text not null default 'opencode'`
- `provider_session_id text null`
- `title text not null default ''`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Indexes:

- `(user_id, updated_at desc)`
- `(strategy_id, updated_at desc)`
- `(provider, provider_session_id)`

### conversation_messages

Stores user, assistant, system, and tool messages.

Columns:

- `message_id text primary key`
- `thread_id text not null references conversation_threads(thread_id) on delete cascade`
- `role text not null`
- `content text not null default ''`
- `model text null`
- `provider_message_id text null`
- `status text not null default 'completed'`
- `token_input integer null`
- `token_output integer null`
- `metadata jsonb not null default '{}'::jsonb`
- `created_at timestamptz not null default now()`

Indexes:

- `(thread_id, created_at)`
- `(role)`
- `(created_at desc)`

Consider a check constraint for `role` values:

- `system`
- `user`
- `assistant`
- `tool`

### agent_events

Stores tool calls, provider events, streaming milestones, and audit events.

Columns:

- `event_id text primary key`
- `thread_id text null references conversation_threads(thread_id) on delete cascade`
- `message_id text null references conversation_messages(message_id) on delete set null`
- `run_id text null`
- `event_type text not null`
- `payload jsonb not null default '{}'::jsonb`
- `created_at timestamptz not null default now()`

Indexes:

- `(thread_id, created_at)`
- `(message_id, created_at)`
- `(run_id, created_at)`
- `(event_type, created_at desc)`

### runs

Stores executable work such as agent turns, backtests, ingestion jobs, or scripts.

Columns:

- `run_id text primary key`
- `strategy_id text null references strategies(strategy_id) on delete cascade`
- `thread_id text null references conversation_threads(thread_id) on delete set null`
- `kind text not null default 'agent_turn'`
- `status text not null`
- `started_at timestamptz not null default now()`
- `ended_at timestamptz null`
- `exit_code integer null`
- `reply text null`
- `error text null`
- `metadata jsonb not null default '{}'::jsonb`

Indexes:

- `(strategy_id, started_at desc)`
- `(thread_id, started_at desc)`
- `(status)`
- `(kind, started_at desc)`

Consider a check constraint for stable `status` values:

- `queued`
- `running`
- `completed`
- `failed`
- `cancelled`

### backtest_requests

Stores the exact inputs used for a backtest.

Columns:

- `backtest_id text primary key`
- `run_id text not null references runs(run_id) on delete cascade`
- `strategy_id text null references strategies(strategy_id) on delete cascade`
- `allocation jsonb not null`
- `rebalance text not null default 'none'`
- `costs jsonb not null default '{}'::jsonb`
- `initial_capital_usd numeric null`
- `start_date date null`
- `end_date date null`
- `created_at timestamptz not null default now()`

Indexes:

- `(run_id)`
- `(strategy_id, created_at desc)`
- `(start_date, end_date)`

### backtest_results

Stores normalized KPIs and optional structured report data.

Columns:

- `backtest_id text primary key references backtest_requests(backtest_id) on delete cascade`
- `run_id text not null references runs(run_id) on delete cascade`
- `cagr numeric null`
- `sharpe_ratio numeric null`
- `sortino_ratio numeric null`
- `max_drawdown numeric null`
- `calmar_ratio numeric null`
- `monthly_hit_rate numeric null`
- `final_equity_usd numeric null`
- `total_trading_cost_usd numeric null`
- `total_num_swaps integer null`
- `report jsonb not null default '{}'::jsonb`
- `created_at timestamptz not null default now()`

Indexes:

- `(run_id)`
- `(created_at desc)`
- `(sharpe_ratio desc)`
- `(cagr desc)`
- `(max_drawdown asc)`

### artifacts

Stores metadata for generated files.

Columns:

- `artifact_id text primary key`
- `run_id text null references runs(run_id) on delete cascade`
- `backtest_id text null references backtest_requests(backtest_id) on delete cascade`
- `kind text not null`
- `storage_key text not null`
- `content_type text not null`
- `size_bytes bigint null`
- `checksum text null`
- `metadata jsonb not null default '{}'::jsonb`
- `created_at timestamptz not null default now()`

Indexes:

- `(run_id, created_at)`
- `(backtest_id, created_at)`
- `(kind)`
- unique `(storage_key)`

### assets

Stores asset metadata.

Columns:

- `asset_id text primary key`
- `source text not null`
- `source_asset_id text not null`
- `symbol text not null`
- `name text not null`
- `market_cap_rank integer null`
- `metadata jsonb not null default '{}'::jsonb`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

Indexes:

- unique `(source, source_asset_id)`
- `(symbol)`
- `(market_cap_rank)`

### asset_prices

Stores small/medium historical price data.

Columns:

- `asset_id text not null references assets(asset_id) on delete cascade`
- `timestamp timestamptz not null`
- `source text not null`
- `open numeric null`
- `high numeric null`
- `low numeric null`
- `close numeric not null`
- `volume numeric null`
- `market_cap numeric null`
- `metadata jsonb not null default '{}'::jsonb`
- primary key `(asset_id, timestamp, source)`

Indexes:

- `(asset_id, timestamp)`
- `(timestamp)`
- `(source, timestamp)`

If historical data becomes large, consider TimescaleDB or Parquet/DuckDB instead of keeping all analytical data in ordinary PostgreSQL tables.

## New File Structure

Recommended structure:

```text
agent/
  drizzle.config.ts
  src/
    db/
      client.ts
      schema.ts
      repositories/
        users.ts
        strategies.ts
        conversations.ts
        runs.ts
        backtests.ts
        artifacts.ts
        market-data.ts
    agent/
      session.ts
      prompt.ts
    api/
      server.ts
  drizzle/
    migrations/
```

The exact folder names can be adjusted, but the important part is to avoid scattering Drizzle queries directly through route handlers.

## Package Changes

Add runtime dependency:

```bash
pnpm --filter @agent-invest/agent add drizzle-orm
```

Add dev dependency:

```bash
pnpm --filter @agent-invest/agent add -D drizzle-kit
```

Keep:

```text
pg
@types/pg
```

Update `agent/package.json` scripts conceptually:

```json
{
  "db:generate": "drizzle-kit generate",
  "db:migrate": "drizzle-kit migrate",
  "db:studio": "drizzle-kit studio"
}
```

If the application needs migrations to run from compiled production code, use Drizzle's migrator from a small TypeScript/Node entrypoint instead of relying only on the CLI.

## Drizzle Config

Add `agent/drizzle.config.ts`.

Expected responsibilities:

- point to `src/db/schema.ts`
- output migrations to `migrations`
- use PostgreSQL dialect
- read database credentials from `DATABASE_URL`

The config should use the existing environment loading convention if needed.

## Code Migration Tasks

### Database Client

Replace the raw `pg` export with a Drizzle database export while preserving access to the underlying pool if needed.

Needs:

- create `Pool` from current env handling
- create `drizzle(pool)`
- export `db`
- optionally export `pgPool` for shutdown and low-level access
- keep `describePostgresTarget()` if it remains useful for diagnostics

### Repositories

Create repository functions for database operations currently embedded in API/session code.

Suggested repository responsibilities:

- `users.ensureUser(userId)`
- `strategies.ensureStrategyForUser(userId, strategyId)`
- `strategies.touchStrategy(strategyId)`
- `strategies.updateTitleIfEmpty(strategyId, title)`
- `strategies.getSession(strategyId, options)`
- `strategies.setOpencodeSession(strategyId, sessionId)`
- `runs.createRun(...)`
- `runs.readRun(runId)`
- `runs.markCompleted(...)`
- `runs.markFailed(...)`
- `conversations.createThread(...)`
- `conversations.appendMessage(...)`
- `agentEvents.appendEvent(...)`
- `backtests.createRequest(...)`
- `backtests.storeResult(...)`
- `artifacts.createArtifact(...)`

### API Server

Refactor `agent/src/api/server.ts` so route handlers call repositories/services rather than raw SQL.

Current raw SQL functions to replace:

- `ensureStrategyExists`
- `maybeUpdateStrategyTitle`
- `readRun`
- direct `UPDATE strategies SET last_used_at = NOW()` calls
- direct `INSERT INTO runs` calls
- direct terminal run updates

### Session Manager

Refactor `agent/src/agent/session.ts` transaction handling.

Current behavior to preserve:

- read strategy title and session ID
- if session ID exists, reuse it
- otherwise create OpenCode session
- persist the new session ID
- protect against concurrent creation

Use a Drizzle transaction.

Keep Postgres-specific locking where useful. Drizzle can run raw SQL inside a transaction for row locks or advisory locks.

Recommended behavior:

- use nullable `opencode_session_id`
- use transaction for get-or-create
- use `FOR UPDATE` or an advisory lock to avoid duplicate session creation

## Migration Strategy

Because current Docker data can be deleted, use a clean baseline.

Steps:

1. Add Drizzle dependencies and config.
2. Create `src/db/schema.ts` with the target schema.
3. Generate a new baseline migration using drizzle-kit.
4. Stop Docker Compose and delete the current named database volume.
5. Start Postgres fresh.
6. Run Drizzle migrations.
7. Refactor application DB calls to repositories.
8. Update tests.
9. Remove the old migration runner and old SQL migration folder.

Docker reset command:

```bash
docker compose down -v
docker compose up -d postgres
pnpm db:migrate
```

Use `docker compose down -v` carefully because it deletes the named PostgreSQL volume.

## Retired Legacy Migration Files

After the Drizzle migration was completed:

- `agent/src/db/migrate.ts` was removed.
- `agent/src/db/migrations/*.sql` was removed from the active migration path.
- `dist/migrate.js` is no longer built or referenced.
- `agent/package.json` uses `drizzle-kit migrate` for `db:migrate`.
- The root `package.json` delegates `db:migrate` to the agent package.

## Test Changes

Current tests use SQL-string doubles. These will become brittle after Drizzle.

Recommended test strategy:

- Unit test repository functions where logic is non-trivial.
- API tests should mock repositories/services, not exact SQL strings.
- Add at least one integration migration test against PostgreSQL if CI supports it.
- Keep session concurrency tests, but move locking assertions to behavior instead of specific SQL strings.

Specific tests likely needing updates:

- `agent/test/server.test.ts`
- `agent/test/session.test.ts`

## Historical Data Strategy

Start with PostgreSQL for asset metadata and daily/hourly price rows if dataset size is moderate.

Move to another storage/query layer only when needed:

- TimescaleDB if time-series volume grows but staying in Postgres is desirable.
- DuckDB/Parquet for local research/backtesting analytics.
- ClickHouse only if query volume and data scale justify operating another database.

Do not prematurely add a second database before the access pattern and data volume are clear.

## Artifact Strategy

Keep the current local storage approach initially, but formalize artifact metadata in PostgreSQL.

Recommended abstraction:

- local filesystem in development
- S3/R2/GCS-compatible object storage in production later

Store only `storage_key`, metadata, checksum, size, kind, and relationships in PostgreSQL.

## Risks

- Test doubles based on exact SQL strings will need significant changes.
- Concurrency behavior in session creation must be preserved.
- Large historical price data can outgrow ordinary relational access patterns.
- Storing too much raw LLM/tool output in PostgreSQL can create table bloat.
- Resetting Docker volumes deletes local data permanently.

## Recommended Implementation Order

1. Add Drizzle dependencies/config/schema.
2. Generate and verify baseline migration.
3. Reset Docker DB and apply migration.
4. Add repositories for existing tables only.
5. Refactor current API/session code to Drizzle repositories.
6. Update tests until current behavior is preserved.
7. Add conversations/messages/events schema usage.
8. Add backtest request/result/artifact persistence.
9. Add historical data persistence only after deciding expected scale.
10. Remove old migration system.

## Final Recommendation

Use Drizzle + PostgreSQL as the core persistence framework.

Keep PostgreSQL responsible for transactional and queryable application data. Use JSONB for flexible structured payloads. Store large files and generated artifacts outside PostgreSQL with metadata rows pointing to their storage locations. Consider TimescaleDB or DuckDB/Parquet later only if historical market data grows beyond what ordinary PostgreSQL handles comfortably.
