export type ArtifactRef = {
  kind: string;
  path: string;
};

export type StrategyResult = {
  title: string;
  summary: string;
  reasoning: string;
  allocation: StrategyAllocationItem[];
  kpis: StrategyKpis;
  assumptions: string[];
  risks: string[];
  constraint_violations?: string[];
  next_steps: string[];
  backtest: StrategyBacktestSummary;
  charts: StrategyCharts;
};

export type ScreenerResult = {
  type: "market_screener";
  version: 1;
  title: string;
  summary: string;
  definition: {
    factor: "momentum" | "risk_adjusted" | "low_volatility";
    limit: number;
    gmx_only: boolean;
    as_of?: string;
  };
  rows: ScreenerRow[];
  notes: string[];
};

export type ScreenerRow = {
  rank: number;
  source_rank: number;
  coin_id: string;
  symbol: string;
  market_name: string | null;
  is_gmx_tradeable: boolean;
  factor_values: Record<string, number | null>;
  metrics: ScreenerMetric[];
  actions: {
    long: { enabled: boolean; label: string };
    short: { enabled: boolean; label: string };
  };
  gmx_market: null | {
    chain: "arbitrum";
    market_token: string;
    index_token: string;
    long_token: string;
    short_token: string;
    collateral_token: string;
    collateral_decimals: number;
  };
};

export type ScreenerMetric = {
  id: string;
  label: string;
  value: number | null;
  format: "percent" | "number";
};

export type StructuredChatResult = StrategyResult | ScreenerResult;

export type StrategyAllocationItem = {
  asset: string;
  symbol?: string | null;
  coin_id?: string | null;
  weight: number;
  rationale: string;
};

export type StrategyKpis = {
  cagr?: number | null;
  sharpe_ratio?: number | null;
  sortino_ratio?: number | null;
  max_drawdown?: number | null;
  calmar_ratio?: number | null;
  monthly_hit_rate?: number | null;
  final_equity_usd?: number | null;
  final_equity_multiple?: number | null;
  total_trading_cost_usd?: number | null;
  total_num_swaps?: number | null;
};

export type StrategyBacktestSummary = {
  start_date?: string | null;
  end_date?: string | null;
  rebalance?: "none" | "daily" | "weekly" | "monthly" | null;
  initial_capital_usd?: number | null;
  capital_mode?: "usd" | "normalized" | null;
  benchmark?: "bitcoin" | null;
};

export type StrategyCharts = {
  equity_curve?: StrategyEquityPoint[] | null;
  drawdown?: StrategyDrawdownPoint[] | null;
  allocation?: StrategyChartAllocationItem[] | null;
  target_allocation?: StrategyChartAllocationItem[] | null;
  final_allocation?: StrategyChartAllocationItem[] | null;
};

export type StrategyEquityPoint = {
  date: string;
  strategy_equity: number;
  benchmark_equity?: number | null;
};

export type StrategyDrawdownPoint = {
  date: string;
  strategy_drawdown: number;
  benchmark_drawdown?: number | null;
};

export type StrategyChartAllocationItem = {
  asset: string;
  weight: number;
};

export type Run = {
  run_id: string;
  status: string;
  started_at: string;
  ended_at: string | null;
  exit_code: number | null;
  reply: string | null;
  error: string | null;
  stages?: StageRunSummary[];
  artifacts?: ArtifactRef[];
  structured_result?: StrategyResult | null;
};

export type StageRunSummary = {
  stage_run_id: string;
  stage: string;
  round: number;
  status: string;
  started_at: string;
  ended_at: string | null;
  model: string;
  tokens: {
    input: number | null;
    output: number | null;
  };
};

export type StageRunDelta = {
  run_id: string;
  stage_run_id: string;
  stage: string;
  round: number;
  status: string;
};

export type StageEventType =
  | "stage.started"
  | "stage.tool_call"
  | "stage.completed"
  | "stage.failed"
  | string;

export type StageEvent = {
  event_id: string;
  event_type: StageEventType;
  payload: {
    stage?: unknown;
    round?: unknown;
    stage_run_id?: unknown;
    tool_name?: unknown;
    error?: unknown;
    event?: unknown;
  };
  created_at: string;
};

export type StageRunDetail = StageRunSummary & {
  run_id: string;
  opencode_session_id: string | null;
  input: unknown;
  output: unknown;
  error: string | null;
};

export type MessageRequest = {
  user_id: string;
  strategy_id: string;
  text?: string;
  wizard_params?: unknown;
};

export type StrategyCreateResponse = {
  strategy_id: string;
};
