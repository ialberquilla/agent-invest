export type AllocationWizardState = {
  universe: "top10" | "top25" | "top50" | "majors";
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
    majors: "major cryptoassets only",
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
1. Use the available universe and backtesting tools when useful.
2. Propose a research-model allocation with token weights.
3. Explain the rationale.
4. Backtest the allocation if possible.
5. Report CAGR, Sharpe, Sortino, max drawdown, Calmar, monthly hit rate, final equity, and number of swaps when available.
6. State assumptions and limitations.
7. Include concrete next steps for refining the strategy.
8. Respect the lower practical risk tolerance implied by the financial drawdown limit.
9. Infer the practical optimization objective from the user's constraints rather than treating any single metric as the only goal.
10. Include a concise note that this is not financial advice and that the user should validate suitability independently.`;
}
