// Phase 3 of plans/integrate_contracts.md: the live allocator entrypoint the
// keeper uses. Maps a persisted StrategyMandate to the compute_live_allocation
// request and runs it. The Python side reuses the backtest's own per-candidate
// path, so the live target can never drift from the backtested strategy (parity
// is proven in scripts/tests/test_live_allocation.py).

import {
  computeLiveAllocation,
  type LiveAllocationRequest,
  type LiveAllocationResponse,
} from "../workflow/cli.ts";
import type { StrategyMandate } from "../workflow/mandate.ts";
import { scriptObjectiveFromWorkflow } from "../workflow/steps/run_and_validate.ts";

export function mandateToLiveAllocationRequest(
  mandate: StrategyMandate,
  options: { asOf?: string } = {},
): LiveAllocationRequest {
  const config: Record<string, unknown> = { weighting: mandate.weighting };
  if (mandate.rebalance_trigger !== undefined) {
    config.rebalance_trigger = mandate.rebalance_trigger;
  }
  if (mandate.core_weight !== undefined) {
    config.core_weight = mandate.core_weight;
  }
  if (mandate.sleeve_cap !== undefined) {
    config.sleeve_cap = mandate.sleeve_cap;
  }

  return {
    template_id: mandate.template_id,
    select_top: mandate.select_top,
    config,
    coin_ids: mandate.coin_ids,
    objective: scriptObjectiveFromWorkflow(mandate.objective),
    horizon_days: mandate.horizon_days,
    as_of: options.asOf,
  };
}

// Compute the strategy's current target weights for `mandate` as of `asOf`
// (default: latest available data). Returns the latest completed rebalance's
// weights with per-leg side and net/gross/cash summaries.
export async function computeMandateAllocation(
  mandate: StrategyMandate,
  options: {
    asOf?: string;
    timeoutSeconds?: number;
  } = {},
): Promise<LiveAllocationResponse> {
  return computeLiveAllocation(
    mandateToLiveAllocationRequest(mandate, { asOf: options.asOf }),
    { timeoutSeconds: options.timeoutSeconds },
  );
}
