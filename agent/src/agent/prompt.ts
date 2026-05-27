export type AllocationWizardParams = {
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

const allocationWizardLabels = {
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

function formatWizardExclusions(exclusions: string[]) {
  if (exclusions.length === 0) return "none";

  return exclusions
    .map(
      (exclusion) =>
        allocationWizardLabels.exclusions[
          exclusion as keyof typeof allocationWizardLabels.exclusions
        ] ?? exclusion,
    )
    .join(", ");
}

function formatWizardInitialCapital(value: string) {
  const trimmed = value.trim();
  if (!trimmed) return "not provided; reason in percentages only";

  const amount = Number(trimmed);
  if (!Number.isFinite(amount) || amount <= 0) return trimmed;

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(amount);
}

export function buildAllocationWizardPrompt(params: AllocationWizardParams) {
  return `Create an educational investment research brief from the following user constraints. This is scenario analysis, not personalized financial advice.

User brief:
- Universe: ${allocationWizardLabels.universe[params.universe]}
- Exclusions: ${formatWizardExclusions(params.exclusions)}
- Minimum market cap: ${allocationWizardLabels.minimumMarketCap[params.minimumMarketCap]}
- Max token weight: ${allocationWizardLabels.concentrationLimit[params.concentrationLimit]}
- Maximum financially acceptable drawdown: ${allocationWizardLabels.maxDrawdown[params.maxDrawdown]}
- Risk preference: ${allocationWizardLabels.riskPreference[params.riskPreference]}
- Time horizon: ${allocationWizardLabels.horizon[params.horizon]}
- Rebalance cadence: ${allocationWizardLabels.rebalance[params.rebalance]}
- Cash allowed: ${allocationWizardLabels.cashAllocation[params.cashAllocation]}
- Initial capital: ${formatWizardInitialCapital(params.initialCapitalUsd)}
- Target number of assets: ${allocationWizardLabels.targetAssets[params.targetAssets]}`;
}
