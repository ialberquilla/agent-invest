export type ArtifactRef = {
  kind: string;
  path: string;
};

export type Run = {
  run_id: string;
  status: string;
  started_at: string;
  ended_at: string | null;
  exit_code: number | null;
  reply: string | null;
  error: string | null;
  artifacts?: ArtifactRef[];
};

export type MessageRequest = {
  user_id: string;
  strategy_id: string;
  text: string;
};

export type StrategyCreateResponse = {
  strategy_id: string;
};
