CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  strategy_id TEXT NOT NULL REFERENCES strategies (strategy_id) ON DELETE CASCADE,
  status TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  ended_at TIMESTAMPTZ,
  exit_code INTEGER
);

CREATE INDEX IF NOT EXISTS runs_strategy_id_started_at_idx
  ON runs (strategy_id, started_at DESC);

CREATE INDEX IF NOT EXISTS runs_status_idx
  ON runs (status);
