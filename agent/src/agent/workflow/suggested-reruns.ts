// Deterministic follow-up suggestions for a finished run. Each entry is a
// labelled, feasibility-checked StrategyRunOverrides delta the frontend can
// render as a one-click rerun (PR3). Suggestions are derived purely from
// the resolved (post-override) thesis, so they are reproducible and never
// LLM-generated.
//
// Feasibility is guaranteed by construction: every candidate is run through
// applyOverrides against the source thesis and dropped if it would produce
// an infeasible thesis. This also encodes the weight-cap coupling -- a
// "fewer assets" suggestion pairs the smaller count with enough per-asset
// weight room to cover the non-cash portion, or it is filtered out.

import { applyOverrides } from "./overrides.ts";
import type { StrategyRunOverrides, Thesis } from "./state.ts";

export type SuggestedRerun = {
  label: string;
  rationale: string;
  overrides: StrategyRunOverrides;
};

export function buildSuggestedReruns(thesis: Thesis): SuggestedRerun[] {
  const c = thesis?.constraints;
  const hints = thesis?.universe_hints;
  // Total by design: this runs at persist time on the structured result,
  // so a missing/partial thesis must yield no suggestions rather than
  // throwing and failing the whole run response.
  if (!c || !hints) return [];
  const candidates: SuggestedRerun[] = [];

  // Fewer assets: concentrate into a smaller basket. Pair the lower count
  // with enough per-asset weight room that the non-cash portion can still
  // be filled (max_weight_per_asset * asset_count_min >= 1 - max_cash).
  {
    const newMin = Math.max(2, Math.min(3, c.asset_count_min));
    const newMax = Math.max(newMin + 1, Math.min(c.asset_count_max, newMin + 2));
    if (newMax < c.asset_count_max) {
      const needed = (1 - c.max_cash_weight) / newMin;
      const cap = clamp01(
        Math.max(c.max_weight_per_asset, ceilTo(needed, 0.05)),
      );
      candidates.push({
        label: "Fewer assets",
        rationale: `Concentrate into ${newMin}-${newMax} assets instead of ${c.asset_count_min}-${c.asset_count_max}.`,
        overrides: {
          asset_count_min: newMin,
          asset_count_max: newMax,
          max_weight_per_asset: cap,
        },
      });
    }
  }

  // More assets: broaden the basket when the eligible universe has room.
  {
    const newMax = Math.min(c.asset_count_max + 5, hints.top_n);
    if (newMax > c.asset_count_max) {
      candidates.push({
        label: "More assets",
        rationale: `Diversify across up to ${newMax} assets.`,
        overrides: { asset_count_max: newMax },
      });
    }
  }

  // Lower drawdown tolerance: ask for a more defensive risk profile.
  if (c.max_drawdown > 0.15) {
    const target = round2(Math.max(0.1, c.max_drawdown - 0.1));
    candidates.push({
      label: "Lower max drawdown",
      rationale: `Tighten the drawdown limit to ${(target * 100).toFixed(0)}%.`,
      overrides: { max_drawdown: target },
    });
  }

  // Exclude stablecoins when they are currently eligible.
  if (!hints.exclude_stablecoins) {
    candidates.push({
      label: "Exclude stablecoins",
      rationale: "Drop stablecoins from the eligible universe.",
      overrides: { exclude_stablecoins: true },
    });
  }

  // Change rebalance cadence to the next less-frequent option (lower cost,
  // less churn). Only offered when a less-frequent option exists.
  {
    const target = lessFrequent(thesis.rebalance_frequency);
    if (target) {
      candidates.push({
        label: `Rebalance ${target}`,
        rationale: `Trade less often by switching the cadence to ${target}.`,
        overrides: { rebalance_frequency: target },
      });
    }
  }

  // Keep only suggestions that remain feasible against this thesis.
  return candidates.filter((s) => isFeasible(thesis, s.overrides));
}

function isFeasible(thesis: Thesis, overrides: StrategyRunOverrides): boolean {
  try {
    applyOverrides(thesis, overrides);
    return true;
  } catch {
    return false;
  }
}

function lessFrequent(
  frequency: Thesis["rebalance_frequency"],
): Thesis["rebalance_frequency"] | null {
  switch (frequency) {
    case "daily":
      return "weekly";
    case "weekly":
      return "monthly";
    case "monthly":
      return "quarterly";
    case "quarterly":
      return null;
  }
}

function clamp01(value: number): number {
  return Math.min(1, Math.max(0, value));
}

function ceilTo(value: number, step: number): number {
  return Math.ceil(value / step) * step;
}

function round2(value: number): number {
  return Math.round(value * 100) / 100;
}
