// Wizard strategy-type picker. "basket" runs the full allocation wizard
// unchanged; the other types collect a couple of fields and submit a
// deterministic StrategyRunOverrides object that reshapes the interpreted
// thesis (the same overrides plumbing the rerun buttons use). Keeping the
// override math here makes it unit-testable independent of the React UI.

export type WizardStrategyType =
  | "basket"
  | "single_asset"
  | "pair_trade"
  | "long_short";

export type StrategyTypeOption = {
  value: WizardStrategyType;
  label: string;
  description: string;
};

export const STRATEGY_TYPE_OPTIONS: StrategyTypeOption[] = [
  {
    value: "basket",
    label: "Diversified basket",
    description: "A multi-asset long allocation. Full guided setup.",
  },
  {
    value: "single_asset",
    label: "Single-asset trend",
    description: "One market, long when trending up, otherwise cash.",
  },
  {
    value: "pair_trade",
    label: "Pair trade",
    description: "Long one market, short another (relative value).",
  },
  {
    value: "long_short",
    label: "Long/short momentum",
    description: "Long the strongest, short the weakest (market-neutral).",
  },
];

export type NonBasketFields = {
  targetCoin: string;
  longCoin: string;
  shortCoin: string;
  poolSize: number;
};

export const DEFAULT_NON_BASKET_FIELDS: NonBasketFields = {
  targetCoin: "",
  longCoin: "",
  shortCoin: "",
  poolSize: 8,
};

// A coin id as the pipeline expects it: lower-case, hyphenated (e.g. "bitcoin",
// "avalanche-2"). We normalise light user input but don't guess.
export function normalizeCoinId(raw: string): string {
  return raw.trim().toLowerCase().replace(/\s+/g, "-");
}

export type ValidationResult = { ok: true } | { ok: false; message: string };

export function validateNonBasket(
  type: WizardStrategyType,
  fields: NonBasketFields,
): ValidationResult {
  if (type === "single_asset") {
    if (!normalizeCoinId(fields.targetCoin)) {
      return { ok: false, message: "Enter the coin id to trade (e.g. bitcoin)." };
    }
  }
  if (type === "pair_trade") {
    const long = normalizeCoinId(fields.longCoin);
    const short = normalizeCoinId(fields.shortCoin);
    if (!long || !short) {
      return { ok: false, message: "Enter both the long and the short coin id." };
    }
    if (long === short) {
      return { ok: false, message: "The long and short coins must differ." };
    }
  }
  if (type === "long_short") {
    if (!Number.isFinite(fields.poolSize) || fields.poolSize < 4) {
      return { ok: false, message: "The ranking pool needs at least 4 assets." };
    }
  }
  return { ok: true };
}

// Deterministic overrides for a non-basket type. Returns undefined for
// "basket" (the full wizard owns that path). Assumes validateNonBasket passed.
export function buildStrategyOverrides(
  type: WizardStrategyType,
  fields: NonBasketFields,
): Record<string, unknown> | undefined {
  switch (type) {
    case "basket":
      return undefined;
    case "single_asset":
      return {
        strategy_mode: "single_asset",
        asset_count_min: 1,
        asset_count_max: 1,
        target_coin_id: normalizeCoinId(fields.targetCoin),
      };
    case "pair_trade":
      return {
        strategy_mode: "pair_trade",
        allowed_sides: "long_short",
        asset_count_min: 2,
        asset_count_max: 2,
        long_coin_ids: [normalizeCoinId(fields.longCoin)],
        short_coin_ids: [normalizeCoinId(fields.shortCoin)],
      };
    case "long_short": {
      const pool = Math.max(4, Math.round(fields.poolSize));
      return {
        strategy_mode: "long_short_portfolio",
        allowed_sides: "long_short",
        asset_count_min: pool,
        asset_count_max: pool,
      };
    }
  }
}
