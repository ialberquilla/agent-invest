export type AllocationWizardState = {
  universe: "top10" | "top25" | "top50" | "all";
  exclusions: string[];
  minimumMarketCap: "none" | "100m" | "500m" | "1b" | "10b";
  concentrationLimit: "20" | "30" | "agent";
  maxDrawdown: "10" | "20" | "35" | "50" | "moreThan50";
  riskPreference: "preserve" | "balanced" | "aggressive" | "maxUpside";
  horizon: "3m" | "6m" | "1y" | "3yPlus";
  rebalance: "none" | "monthly" | "weekly" | "agent";
  initialCapitalUsd: string;
  cashAllocation: "none" | "10" | "25" | "agent";
  targetAssets: "3-5" | "5-10" | "10-20" | "agent";
};

const labels = {
  universe: {
    top10: "top 10 cryptoassets by market cap",
    top25: "top 25 cryptoassets by market cap",
    top50: "top 50 cryptoassets by market cap",
    all: "all available assets",
  },
  exclusions: {
    stablecoins: "stablecoins",
    wrapped: "wrapped assets",
  },
  minimumMarketCap: {
    none: "no minimum",
    "100m": "$100M+",
    "500m": "$500M+",
    "1b": "$1B+",
    "10b": "$10B+",
  },
  concentrationLimit: {
    "20": "max 20% per token",
    "30": "max 30% per token",
    agent: "let agent decide",
  },
  maxDrawdown: {
    "10": "10%",
    "20": "20%",
    "35": "35%",
    "50": "50%",
    moreThan50: "more than 50%",
  },
  riskPreference: {
    preserve: "preserve capital",
    balanced: "balanced growth",
    aggressive: "aggressive growth",
    maxUpside: "maximum upside",
  },
  horizon: {
    "3m": "3 months",
    "6m": "6 months",
    "1y": "1 year",
    "3yPlus": "3+ years",
  },
  rebalance: {
    none: "none / buy-and-hold",
    monthly: "monthly",
    weekly: "weekly",
    agent: "let agent decide",
  },
  cashAllocation: {
    none: "no cash",
    "10": "up to 10%",
    "25": "up to 25%",
    agent: "let agent decide",
  },
  targetAssets: {
    "3-5": "3-5",
    "5-10": "5-10",
    "10-20": "10-20",
    agent: "let agent decide",
  },
} as const;

function formatExclusions(exclusions: string[]) {
  if (exclusions.length === 0) {
    return "none";
  }

  return exclusions
    .map(
      (exclusion) =>
        labels.exclusions[exclusion as keyof typeof labels.exclusions] ??
        exclusion,
    )
    .join(", ");
}

function formatInitialCapital(value: string) {
  const trimmed = value.trim();

  if (!trimmed) {
    return "not provided; reason in percentages only";
  }

  const amount = Number(trimmed);

  if (!Number.isFinite(amount) || amount <= 0) {
    return trimmed;
  }

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(amount);
}

export function buildWizardPrompt(state: AllocationWizardState) {
  return `Create an educational investment research brief from the following user constraints. This is scenario analysis, not personalized financial advice.

User brief:
- Universe: ${labels.universe[state.universe]}
- Exclusions: ${formatExclusions(state.exclusions)}
- Minimum market cap: ${labels.minimumMarketCap[state.minimumMarketCap]}
- Max token weight: ${labels.concentrationLimit[state.concentrationLimit]}
- Maximum financially acceptable drawdown: ${labels.maxDrawdown[state.maxDrawdown]}
- Risk preference: ${labels.riskPreference[state.riskPreference]}
- Time horizon: ${labels.horizon[state.horizon]}
- Rebalance cadence: ${labels.rebalance[state.rebalance]}
- Cash allowed: ${labels.cashAllocation[state.cashAllocation]}
- Initial capital: ${formatInitialCapital(state.initialCapitalUsd)}
- Target number of assets: ${labels.targetAssets[state.targetAssets]}

Please:
1. Work in phases: interpret the primary objective, screen the feature table, build a small set of simple candidate allocations, backtest them, then validate the selected candidate against BTC and the user constraints.
2. Use rank_universe before selecting assets, and translate the user brief into tool filters: universe size, exclusions, minimum market cap, risk preference, drawdown tolerance, target asset count, cash, and concentration limit. If universe is all available assets, do not add a market-cap rank cap unless needed for a stated liquidity/risk constraint.
3. Select assets from the feature table based on the user's objective, not from generic default portfolios. Return/max-upside objectives should start from return and momentum factors; lower-volatility objectives should start from volatility, drawdown, and Sharpe/Sortino factors; balanced objectives should blend risk-adjusted return and drawdown control.
4. Treat the selected time horizon as the primary backtest-history requirement. Assets with less history than the primary backtest window should not be included in the main allocation unless the user explicitly allows a shorter test, but high-ranking excluded assets should be mentioned as watchlist or excluded candidates when relevant.
5. Propose a research-model allocation with token weights that respects the max token weight, cash allowance, and target asset count.
6. Use the exact rebalance cadence from the user brief unless it says "let agent decide". Only use supported cadences: none, weekly, or monthly.
7. Backtest the allocation and validate that the result actually fits the objective: return/max-upside should beat BTC on CAGR or final multiple; lower-volatility should improve volatility or drawdown versus BTC; balanced should improve risk-adjusted return without unacceptable drawdown.
8. If the selected strategy misses the objective or a user constraint, include constraint_violations and explain that it is the closest tested candidate, not a fully compliant result.
9. Explain why each selected asset fits the user's stated factors, including risk posture, market-cap/liquidity filter if used, trend/momentum, volatility/drawdown, history eligibility, and diversification role.
10. Report CAGR, Sharpe, Sortino, max drawdown, Calmar, monthly hit rate, final equity, and number of swaps when available.
11. State assumptions and limitations, including excluded high-ranking assets with insufficient history or data gaps.
12. Include concrete next steps for refining the strategy.
13. Do not overfit by brute-force optimizing historical KPIs; use factor-informed candidates and simple allocations, then validate them.
14. Keep the final selected assets, allocation chart, and backtested allocation identical.
15. Include a concise note that this is not financial advice and that the user should validate suitability independently.`;
}
