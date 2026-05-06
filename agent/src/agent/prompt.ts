import { getObject, storageLayout } from "../storage/local";

type PromptSectionReader = (key: string) => Promise<string | null>;

export type AgentScriptDefinition = {
  name: string;
  summary: string;
  signature: string;
  example: string;
  note?: string;
};

export type BuildSystemPromptOptions = {
  userId: string;
  strategyId: string;
  readSection?: PromptSectionReader;
};

export const RESPONSE_POLICY = [
  "- Do not call the `question` tool or otherwise ask the user clarifying questions. The runtime is non-interactive and cannot answer them — the prompt loop will hang.",
  "- When the request is ambiguous, pick reasonable defaults, state your assumptions in the final reply, and proceed.",
  "- Always finish each turn with a brief `text` reply (the user-visible answer). Tool calls and reasoning alone are not a complete turn.",
  "- Treat portfolio outputs as educational research and scenario analysis, not personalized financial advice. Do not claim to know the user's full financial situation, do not guarantee returns, and include appropriate risk/assumption language in the final reply.",
  "- You may still provide concrete model portfolios, token weights, backtest-based comparisons, and refinement suggestions when framed as research examples for the user's stated constraints. Do not refuse solely because the topic is investing or crypto allocation.",
  "- Whenever the user asks you to design, evaluate, compare, recommend, refine, or simulate a portfolio allocation or strategy, you MUST call `run_backtest` at least once to ground your answer. Treat this as mandatory; never report performance metrics or propose an allocation without running a backtest first.",
  "- For any portfolio strategy output, including strategy creation, evaluation, comparison, recommendation, refinement, or simulation, you MUST call `finalize_strategy_result` after the final `run_backtest`. This tool is the structured response contract; do not put JSON in the final text reply.",
  "- `finalize_strategy_result` takes only human-authored fields plus `backtest_label`; it reads KPIs and chart data from the selected backtest artifacts. Do not invent KPIs or chart values.",
  "- Use decimal numbers for all allocation weights, for example 0.6 for 60%. Do not use strings like `60%`.",
  "- Put the user-facing explanation in `reasoning`: explain why the allocation or comparison follows from the user's constraints and the backtest results.",
  "- Use a phased portfolio workflow: infer the user's primary objective, translate it into factor screens, shortlist with `rank_universe`, build a small number of simple candidate allocations, backtest them, validate against BTC and constraints, then finalize the best valid or closest-valid candidate.",
  "- Use `rank_universe` to shortlist candidate coins when the user asks for a portfolio, allocation, recommendation, comparison, or refinement and has not specified exact coins. Convert preferences into filters: top-N universe -> max market-cap rank; all-available universe -> no market-cap rank cap unless the user asks for liquidity/large-cap safety; minimum market cap -> min market cap; exclusions -> stablecoin/wrapped filters; risk posture -> risk profile; drawdown tolerance -> volatility/drawdown/trend constraints.",
  "- Select factors by objective: return/max-upside should start from return and momentum factors; lower-volatility should start from volatility, drawdown, and Sharpe/Sortino factors; balanced growth should blend risk-adjusted return, positive trend, and drawdown control.",
  "- Treat the user's time horizon as the primary backtest-history requirement. Do not include assets with less history than the main backtest window unless the user explicitly allows a shorter test; mention attractive excluded assets as watchlist/excluded candidates when relevant.",
  "- After backtesting, validate the chosen candidate against the objective: return/max-upside should beat BTC on CAGR or final multiple; lower-volatility should improve volatility or drawdown versus BTC; balanced growth should improve risk-adjusted return without unacceptable drawdown.",
  "- If the selected candidate misses the objective or a user constraint, include `constraint_violations` in `finalize_strategy_result` and state that it is the closest tested candidate, not a fully compliant result.",
  "- Do not overfit by brute-force optimizing historical KPIs. Use factor-informed candidates and simple allocations; avoid tiny arbitrary weight changes solely to improve a historical metric.",
  "- If the user specifies a rebalance cadence, pass that exact cadence to `run_backtest`; only choose among supported cadences `none`, `weekly`, or `monthly` when the user says the agent may decide. Never use unsupported cadences such as quarterly.",
  "- The allocation passed to `finalize_strategy_result` must use the same coin_ids and weights as the final `run_backtest` allocation. Do not summarize, rename, or swap assets after backtesting.",
].join("\n");

export const MEMORY_DISCIPLINE_GUIDANCE = [
  "- Use opencode's built-in shell and file-edit tools for memory I/O. Read or update `users/<user_id>/profile.md` and `users/<user_id>/strategies/<strategy_id>/memory.md` under `STORAGE_ROOT` with `cat`, `tee`, or direct file edits instead of calling a dedicated memory script.",
  "- Ask before changing `profile.md`. Strategy `memory.md` is working memory, so keep it concise and update it freely when it helps future turns.",
].join("\n");

export const AGENT_SCRIPT_REGISTRY: readonly AgentScriptDefinition[] = [
  {
    name: "list_universe",
    summary:
      "List the top-N coins by market cap from the database-backed universe dataset.",
    signature: "--top-n <count> [--as-of YYYY-MM-DD]",
    example:
      "cd agent/scripts && uv run --project . python -m agent_invest_scripts.list_universe --top-n 50 --as-of 2026-04-25",
  },
  {
    name: "rank_universe",
    summary:
      "Rank candidate coins from the daily-refreshed DB feature universe for portfolio construction.",
    signature:
      "--top-n <count> [--max-market-cap-rank <rank>] [--min-market-cap <usd>] [--min-data-days-365d <days>] [--min-history-days <days>] [--first-price-before <YYYY-MM-DD>] [--positive-trend-only] [--min-return-180d <value>] [--min-return-365d <value>] [--max-volatility-180d <value>] [--max-drawdown-180d <negative_value>] [--exclude-stablecoins] [--exclude-wrapped] [--risk-profile preserve|balanced|aggressive|max_upside] [--objective return|low_volatility|balanced|max_upside] [--sort market_cap_rank|momentum_180d|sharpe_180d|low_volatility]",
    example:
      "cd agent/scripts && uv run --project . python -m agent_invest_scripts.rank_universe --top-n 20 --max-market-cap-rank 100 --min-data-days-365d 180 --positive-trend-only --sort sharpe_180d",
    note: "Use this before run_backtest when selecting candidate coins. Map user preferences into filters and risk-profile ranking. Conservative preferences should favor low volatility and higher-quality trend data; aggressive/max-upside preferences may include smaller market-cap ranks and higher momentum, while still respecting explicit market-cap, exclusion, drawdown, cash, asset-count, and concentration constraints. It is a screening tool, not a substitute for backtesting.",
  },
  {
    name: "run_backtest",
    summary:
      "Score a portfolio allocation against historical prices. Returns standardized KPIs (cagr, sharpe_ratio, sortino_ratio, max_drawdown, calmar_ratio, monthly_hit_rate, final_equity_usd, total_trading_cost_usd, total_num_swaps) and writes equity_curve.png + drawdown.png + report.json under <STORAGE_ROOT>/artifacts/run_backtest/<label>/.",
    signature:
      "--allocation '<json>' [--rebalance none|daily|weekly|monthly] [--costs '<json>'] [--initial-capital-usd <usd>] [--normalized-capital] [--label <name>]",
    example:
      'cd agent/scripts && uv run --project . python -m agent_invest_scripts.run_backtest --allocation \'{"type":"static","weights":{"bitcoin":0.6,"ethereum":0.4},"start":"2024-01-01","end":"2024-12-31"}\' --rebalance monthly --label sixty_forty',
    note: 'Allocation forms: {"type":"static","weights":{coin_id:weight,...},"start":"YYYY-MM-DD","end":"YYYY-MM-DD"} for buy-and-hold or periodic-rebalance to constant weights, OR {"type":"weights","rows":[{"date":"YYYY-MM-DD","coin_id":...,"weight":...},...]} for time-varying allocations. Weights must sum to <= 1.0; the remainder is treated as cash. Use --rebalance none for true buy-and-hold (single rebalance at start). Use --normalized-capital when the user did not provide starting capital and asked to reason in percentages/normalized units. Use list_universe first to discover valid coin_ids.',
  },
  {
    name: "finalize_strategy_result",
    summary:
      "Create the canonical structured strategy result JSON from human-authored fields and a selected run_backtest label. This is mandatory after the final backtest for portfolio strategy outputs.",
    signature: "--payload '<json>'",
    example:
      'cd agent/scripts && uv run --project . python -m agent_invest_scripts.finalize_strategy_result --payload \'{"title":"BTC/ETH 60/40","summary":"Monthly rebalanced BTC/ETH portfolio.","reasoning":"The backtest shows improved risk-adjusted returns versus the benchmark.","allocation":[{"asset":"Bitcoin","symbol":"BTC","coin_id":"bitcoin","weight":0.6,"rationale":"Core benchmark exposure."},{"asset":"Ethereum","symbol":"ETH","coin_id":"ethereum","weight":0.4,"rationale":"Diversifies crypto beta."}],"assumptions":["Historical prices are representative."],"risks":["Crypto drawdowns can be severe."],"next_steps":["Monitor drawdown and rebalance monthly."],"backtest_label":"btc_eth_60_40"}\'',
    note: "The payload must include non-empty title, summary, reasoning, allocation, assumptions, risks, next_steps, and backtest_label. Include constraint_violations when the selected strategy misses a user constraint such as maximum drawdown. The tool writes artifacts/strategy_result/<backtest_label>/strategy_result.json and returns its path.",
  },
  {
    name: "list_runs",
    summary: "List prior runs for a strategy with one-line summaries.",
    signature: "--strategy-id <strategy_id> [--limit <count>]",
    example:
      "cd agent/scripts && uv run --project . python -m agent_invest_scripts.list_runs --strategy-id 11111111-1111-1111-1111-111111111111 --limit 10",
  },
] as const;

async function defaultReadSection(key: string): Promise<string | null> {
  return getObject(key);
}

function renderSection(title: string, body: string): string {
  return `# ${title}\n${body}`;
}

function renderScriptDefinition(script: AgentScriptDefinition): string {
  const lines = [
    `- \`${script.name}\``,
    `  Purpose: ${script.summary}`,
    `  Signature: \`${script.signature}\``,
    `  Example: \`${script.example}\``,
  ];

  if (script.note) {
    lines.push(`  Note: ${script.note}`);
  }

  return lines.join("\n");
}

export function buildToolManifestSection(): string {
  const scriptEntries = AGENT_SCRIPT_REGISTRY.map(renderScriptDefinition).join(
    "\n\n",
  );

  return renderSection(
    "Tool Manifest",
    [
      "All agent-facing Python scripts live under `agent/scripts/agent_invest_scripts/`.",
      "Always invoke them from `agent/scripts` with `uv run --project . python -m agent_invest_scripts.<script> ...`.",
      "Each script enforces its own per-call timeout and exits non-zero when it times out.",
      "Each script prints structured JSON to stdout, logs to stderr only, and exits non-zero on error.",
      "If a script times out, stop and surface that failure instead of retrying the same command.",
      "",
      "## Scripts",
      scriptEntries,
      "",
      "## Memory discipline",
      MEMORY_DISCIPLINE_GUIDANCE,
    ].join("\n"),
  );
}

export async function buildSystemPrompt({
  userId,
  strategyId,
  readSection = defaultReadSection,
}: BuildSystemPromptOptions): Promise<string> {
  const [profile, instructions, memory] = await Promise.all([
    readSection(storageLayout.userProfileKey(userId)),
    readSection(storageLayout.strategyInstructionsKey(userId, strategyId)),
    readSection(storageLayout.strategyMemoryKey(userId, strategyId)),
  ]);

  return [
    renderSection("Response Policy", RESPONSE_POLICY),
    renderSection("User Profile", profile ?? ""),
    renderSection("Strategy Instructions", instructions ?? ""),
    renderSection("Strategy Memory", memory ?? ""),
    buildToolManifestSection(),
  ].join("\n\n");
}
