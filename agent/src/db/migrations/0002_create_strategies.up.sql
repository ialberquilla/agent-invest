CREATE TABLE IF NOT EXISTS strategies (
  strategy_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
  opencode_session_id TEXT NOT NULL,
  title TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS strategies_user_id_last_used_at_idx
  ON strategies (user_id, last_used_at DESC);

CREATE INDEX IF NOT EXISTS strategies_opencode_session_id_idx
  ON strategies (opencode_session_id);
