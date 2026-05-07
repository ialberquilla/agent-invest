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
