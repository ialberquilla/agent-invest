import { describe, expect, it } from "vitest";

import {
  buildStrategyOverrides,
  DEFAULT_NON_BASKET_FIELDS,
  normalizeCoinId,
  validateNonBasket,
  type NonBasketFields,
} from "./strategy-types";

function fields(patch: Partial<NonBasketFields> = {}): NonBasketFields {
  return { ...DEFAULT_NON_BASKET_FIELDS, ...patch };
}

describe("normalizeCoinId", () => {
  it("lower-cases and hyphenates", () => {
    expect(normalizeCoinId("  Avalanche 2 ")).toBe("avalanche-2");
    expect(normalizeCoinId("Bitcoin")).toBe("bitcoin");
  });
});

describe("validateNonBasket", () => {
  it("requires a coin for single_asset", () => {
    expect(validateNonBasket("single_asset", fields()).ok).toBe(false);
    expect(validateNonBasket("single_asset", fields({ targetCoin: "btc" })).ok).toBe(true);
  });

  it("requires two distinct coins for a pair", () => {
    expect(validateNonBasket("pair_trade", fields({ longCoin: "eth" })).ok).toBe(false);
    expect(
      validateNonBasket("pair_trade", fields({ longCoin: "btc", shortCoin: "btc" })).ok,
    ).toBe(false);
    expect(
      validateNonBasket("pair_trade", fields({ longCoin: "eth", shortCoin: "btc" })).ok,
    ).toBe(true);
  });

  it("requires a pool of at least 4 for long/short", () => {
    expect(validateNonBasket("long_short", fields({ poolSize: 3 })).ok).toBe(false);
    expect(validateNonBasket("long_short", fields({ poolSize: 8 })).ok).toBe(true);
  });
});

describe("buildStrategyOverrides", () => {
  it("returns undefined for a basket (the full wizard owns it)", () => {
    expect(buildStrategyOverrides("basket", fields())).toBeUndefined();
  });

  it("collapses a single-asset run to one position with explicit shape + defaults", () => {
    expect(buildStrategyOverrides("single_asset", fields({ targetCoin: "Bitcoin" }))).toEqual({
      strategy_mode: "single_asset",
      allowed_sides: "long_flat",
      execution_mode: "wallet_direct",
      asset_count_min: 1,
      asset_count_max: 1,
      target_coin_id: "bitcoin",
      horizon_days: 365,
      max_drawdown: 0.5,
      signal_speed: "balanced",
    });
  });

  it("maps single-asset assumption choices to overrides", () => {
    expect(
      buildStrategyOverrides(
        "single_asset",
        fields({
          targetCoin: "ETH",
          horizon: "2y",
          maxDrawdownPct: 20,
          signalSpeed: "fast",
        }),
      ),
    ).toEqual({
      strategy_mode: "single_asset",
      allowed_sides: "long_flat",
      execution_mode: "wallet_direct",
      asset_count_min: 1,
      asset_count_max: 1,
      target_coin_id: "eth",
      horizon_days: 730,
      max_drawdown: 0.2,
      signal_speed: "fast",
    });
  });

  it("builds named legs for a pair trade", () => {
    expect(
      buildStrategyOverrides("pair_trade", fields({ longCoin: "ETH", shortCoin: "BTC" })),
    ).toEqual({
      strategy_mode: "pair_trade",
      allowed_sides: "long_short",
      asset_count_min: 2,
      asset_count_max: 2,
      long_coin_ids: ["eth"],
      short_coin_ids: ["btc"],
    });
  });

  it("sets a market-neutral long/short pool, floored at 4", () => {
    expect(buildStrategyOverrides("long_short", fields({ poolSize: 2 }))).toEqual({
      strategy_mode: "long_short_portfolio",
      allowed_sides: "long_short",
      asset_count_min: 4,
      asset_count_max: 4,
    });
  });
});
