CREATE TABLE IF NOT EXISTS events (
  run_id TEXT NOT NULL REFERENCES runs (run_id) ON DELETE CASCADE,
  seq BIGINT NOT NULL,
  type TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (run_id, seq)
);

CREATE INDEX IF NOT EXISTS events_run_id_created_at_idx
  ON events (run_id, created_at);

CREATE INDEX IF NOT EXISTS events_type_idx
  ON events (type);
