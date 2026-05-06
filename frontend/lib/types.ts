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
  artifacts?: ArtifactRef[];
  structured_result?: StrategyResult | null;
};

export type MessageRequest = {
  user_id: string;
  strategy_id: string;
  text: string;
};

export type StrategyCreateResponse = {
  strategy_id: string;
};
