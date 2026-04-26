# agent-invest

Phase 1 storage is local filesystem only. S3 and AWS credentials are no longer required.

## Storage Root

Set `STORAGE_ROOT` to the directory that should hold agent memory files and parquet datasets.

- Local default: `<repo>/.data/storage`
- Docker default: `/app/.data/storage`

The storage layout is:

```text
STORAGE_ROOT/
  users/<user_id>/profile.md
  users/<user_id>/strategies/<strategy_id>/instructions.md
  users/<user_id>/strategies/<strategy_id>/memory.md
  users/<user_id>/strategies/<strategy_id>/artifacts/<run_id>/output.json
  datasets/<name>.parquet
```

## Local Development

1. Install JavaScript dependencies with `pnpm install`.
2. Install Python dependencies with `uv sync --project agent/scripts`.
3. Run TypeScript tests with `pnpm test`.
4. Run Python tests with `uv run --project agent/scripts pytest`.

## Docker

Run `docker compose up --build`.

The compose stack binds `./.data` into the container so `STORAGE_ROOT=/app/.data/storage` survives restarts.
