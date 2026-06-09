"use client";

import { useCallback, useEffect, useState, type ReactNode } from "react";
import Link from "next/link";
import { Area, AreaChart, Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from "recharts";
import { ArrowLeft, ExternalLink, RefreshCw } from "lucide-react";
import { useWallets } from "@privy-io/react-auth";

import { Badge } from "@/components/ui/badge";
import { Button, buttonVariants } from "@/components/ui/button";
import { ChartContainer, ChartTooltipContent } from "@/components/ui/chart";
import {
  closeVaultOpenPositionsOnChain,
  closeVaultPositionsAndWithdrawIdleOnChain,
  executeVaultAllocationOnChain,
  fundVaultGasOnChain,
  readAllocationReadiness,
  readVaultBalances,
  withdrawVaultIdleOnChain,
  withdrawVaultNativeOnChain,
  type VaultAllocationReadiness,
} from "@/lib/deploy-vault";
import {
  readGmxAccountActivity,
  type GmxAccountActivity,
  type GmxOpenPosition,
  type GmxPendingOrder,
} from "@/lib/gmx-positions";
import type { DeployedStrategy } from "@/lib/local-store";
import type { Run, StrategyResult } from "@/lib/types";
import { cn } from "@/lib/utils";

type Balances = { idle: string; gas: string };

type BacktestLoadState =
  | { status: "idle" | "loading" }
  | { status: "success"; run: Run; result: StrategyResult }
  | { status: "missing"; message: string }
  | { status: "error"; message: string };

type AllocationDatum = {
  label: string;
  weight: number;
  notionalUsd?: number;
};

const EMPTY_ACTIVITY: GmxAccountActivity = {
  openPositions: [],
  pendingOrders: [],
};

const usdFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 2,
});

const tokenFormatter = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 6,
});

const CHART_COLORS = [
  "var(--chart-1)",
  "var(--chart-2)",
  "var(--chart-3)",
  "var(--chart-4)",
  "var(--chart-5)",
];

export function VaultManagerPane({
  vault,
  onBack,
}: {
  vault: DeployedStrategy;
  onBack: () => void;
}) {
  const { wallets } = useWallets();
  const [balances, setBalances] = useState<Balances | null>(null);
  const [allocation, setAllocation] = useState<VaultAllocationReadiness | null>(
    null,
  );
  const [backtest, setBacktest] = useState<BacktestLoadState>({
    status: "idle",
  });
  const [activity, setActivity] = useState<GmxAccountActivity>(EMPTY_ACTIVITY);
  const [isLoadingActivity, setIsLoadingActivity] = useState(false);
  const [gasAmount, setGasAmount] = useState("0.002");
  const [closeNotional, setCloseNotional] = useState("");
  const [maxLegsInput, setMaxLegsInput] = useState("2");
  const [busy, setBusy] = useState<string | null>(null);
  const [closingPositionId, setClosingPositionId] = useState<string | null>(
    null,
  );
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const maxLegs =
    maxLegsInput.trim().length > 0 ? Number(maxLegsInput) : undefined;

  const refreshBalances = useCallback(async () => {
    try {
      setBalances(await readVaultBalances(vault.vault_address));
    } catch {
      // leave previous balances; surfaced lazily on the next action
    }
  }, [vault.vault_address]);

  const refreshActivity = useCallback(async () => {
    setIsLoadingActivity(true);
    try {
      const next = await readGmxAccountActivity([
        { type: "vault", address: vault.vault_address, label: vault.label },
      ]);
      setActivity(next);
    } catch {
      // Balances/actions are still usable if the read-only GMX status call fails.
    } finally {
      setIsLoadingActivity(false);
    }
  }, [vault.label, vault.vault_address]);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const [nextBalances, nextActivity] = await Promise.all([
          readVaultBalances(vault.vault_address),
          readGmxAccountActivity([
            { type: "vault", address: vault.vault_address, label: vault.label },
          ]),
        ]);
        if (!cancelled) {
          setBalances(nextBalances);
          setActivity(nextActivity);
        }
      } catch {
        // ignore; balances stay null and actions surface their own errors
      }
      if (vault.run_id) {
        try {
          const next = await readAllocationReadiness(vault.run_id);
          if (!cancelled) setAllocation(next);
        } catch {
          if (!cancelled) setAllocation(null);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [vault.label, vault.vault_address, vault.run_id]);

  useEffect(() => {
    if (!vault.run_id) {
      return;
    }

    const abortController = new AbortController();

    void (async () => {
      try {
        setBacktest({ status: "loading" });
        const response = await fetch(
          `/api/runs/${encodeURIComponent(vault.run_id ?? "")}`,
          { cache: "no-store", signal: abortController.signal },
        );
        const payload = (await response.json().catch(() => null)) as unknown;

        if (!response.ok) {
          throw new Error("Unable to load the originating backtest run.");
        }

        if (!isRunWithStrategyResult(payload)) {
          setBacktest({
            status: "missing",
            message: "The originating run does not include a backtest report.",
          });
          return;
        }

        setBacktest({
          status: "success",
          run: payload,
          result: payload.structured_result,
        });
      } catch (err) {
        if ((err as Error).name === "AbortError") return;
        setBacktest({
          status: "error",
          message: err instanceof Error ? err.message : "Unable to load backtest.",
        });
      }
    })();

    return () => abortController.abort();
  }, [vault.run_id]);

  async function provider() {
    const wallet = wallets[0];
    if (!wallet) throw new Error("Connect a wallet first");
    return wallet.getEthereumProvider();
  }

  async function run(label: string, fn: () => Promise<void>) {
    setBusy(label);
    setError(null);
    setNotice(null);
    try {
      await fn();
      await refreshBalances();
      await refreshActivity();
      window.dispatchEvent(new Event("agent-invest:deployed-strategies"));
    } catch (err) {
      setError(err instanceof Error ? err.message : `${label} failed`);
    } finally {
      setBusy(null);
    }
  }

  const idleNum = Number(balances?.idle ?? "0");
  const gasNum = Number(balances?.gas ?? "0");
  const canExecute =
    Boolean(allocation?.executable) && idleNum > 0 && gasNum > 0;
  const totalOpenSize = activity.openPositions.reduce(
    (sum, row) => sum + row.sizeUsd,
    0,
  );
  const totalPendingSize = activity.pendingOrders.reduce(
    (sum, row) => sum + row.sizeUsd,
    0,
  );
  const executionStatus =
    activity.pendingOrders.length > 0
      ? "Orders pending"
      : activity.openPositions.length > 0
        ? "Live"
        : "No live GMX position";
  const currentAllocation = currentAllocationFromPositions(
    activity.openPositions,
  );
  const targetAllocation = targetAllocationFromReadiness(
    allocation?.target_allocation ?? [],
  );
  const backtestView: BacktestLoadState = vault.run_id
    ? backtest
    : {
        status: "missing",
        message: "This vault is not linked to an originating backtest run.",
      };
  const backtestPnl =
    backtestView.status === "success"
      ? summarizeBacktestPnl(backtestView.result)
      : null;

  return (
    <div className="min-h-0 flex-1 overflow-y-auto bg-background">
      <div className="mx-auto w-full max-w-6xl space-y-5 px-4 py-5 sm:px-6 lg:px-8">
        <div className="flex flex-col gap-4 rounded-2xl border bg-card p-4 shadow-xs sm:p-5 lg:flex-row lg:items-start lg:justify-between">
          <div className="min-w-0 space-y-3">
            <button
              type="button"
              onClick={onBack}
              className="inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground"
            >
              <ArrowLeft className="size-4" /> Back
            </button>
            <div>
              <div className="flex flex-wrap items-center gap-2">
                <p className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
                  Strategy vault
                </p>
                <Badge
                  variant={
                    activity.openPositions.length > 0 ? "default" : "outline"
                  }
                >
                  {executionStatus}
                </Badge>
              </div>
              <h1 className="mt-1 font-heading text-2xl font-semibold tracking-tight">
                {vault.label}
              </h1>
              <p className="mt-1 break-all font-mono text-xs text-muted-foreground">
                {vault.vault_address}
              </p>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            {vault.run_id ? (
              <Link
                href={`/runs/${encodeURIComponent(vault.run_id)}`}
                className={buttonVariants({ variant: "outline", size: "sm" })}
              >
                Open backtest
                <ExternalLink className="size-3.5" />
              </Link>
            ) : null}
            <Button
              variant="outline"
              size="sm"
              onClick={() => void refreshActivity()}
              disabled={isLoadingActivity}
            >
              <RefreshCw
                className={cn("size-3.5", isLoadingActivity && "animate-spin")}
              />
              Refresh live data
            </Button>
          </div>
        </div>

        <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <MetricCard
            label="Idle collateral"
            value={balances ? `${balances.idle} USDC` : "Loading..."}
            helper="Free USDC in the vault."
          />
          <MetricCard
            label="Gas tank"
            value={balances ? `${balances.gas} ETH` : "Loading..."}
            helper="ETH reserved for GMX execution fees."
          />
          <MetricCard
            label="Live exposure"
            value={usdFormatter.format(totalOpenSize)}
            helper={`${activity.openPositions.length} open position${activity.openPositions.length === 1 ? "" : "s"}`}
          />
          <MetricCard
            label="Backtest PnL"
            value={backtestPnl?.label ?? backtestStatusLabel(backtestView)}
            helper={backtestPnl?.helper ?? "From the originating strategy run."}
            tone={backtestPnl?.tone}
          />
        </section>

        <div className="grid gap-5 xl:grid-cols-[minmax(0,1.05fr)_minmax(0,0.95fr)]">
          <section className="space-y-4 rounded-2xl border bg-card p-4 shadow-xs sm:p-5">
            <SectionHeader
              title="Current allocation"
              description="Live GMX position mix by open notional, not collateral balance."
            />
            <AllocationDonutChart
              items={currentAllocation}
              empty="No open GMX positions yet. Execute the strategy to populate this chart."
              valueLabel="Open notional"
            />
          </section>

          <section className="space-y-4 rounded-2xl border bg-card p-4 shadow-xs sm:p-5">
            <SectionHeader
              title="Target allocation"
              description="The mandate allocation that execution will try to open."
            />
            <AllocationDonutChart
              items={targetAllocation}
              empty={
                allocation
                  ? "No target allocation is available for this vault."
                  : "Loading target allocation..."
              }
            />
          </section>
        </div>

        <section className="rounded-2xl border bg-card p-4 shadow-xs sm:p-5">
          <BacktestSummary state={backtestView} />
        </section>

        <section className="space-y-4 rounded-2xl border bg-card p-4 shadow-xs sm:p-5">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
            <SectionHeader
              title="Live GMX positions"
              description={
                activity.pendingOrders.length > 0
                  ? "GMX has accepted orders that have not executed yet."
                  : activity.openPositions.length > 0
                    ? "Open and pending positions for this strategy vault."
                    : "No pending orders or open positions found for this vault."
              }
            />
            <div className="grid grid-cols-2 gap-2 text-sm sm:min-w-72">
              <div className="rounded-xl border bg-background/60 p-3">
                <p className="text-xs text-muted-foreground">Open notional</p>
                <p className="font-mono font-semibold tabular-nums">
                  {usdFormatter.format(totalOpenSize)}
                </p>
              </div>
              <div className="rounded-xl border bg-background/60 p-3">
                <p className="text-xs text-muted-foreground">Pending notional</p>
                <p className="font-mono font-semibold tabular-nums">
                  {usdFormatter.format(totalPendingSize)}
                </p>
              </div>
            </div>
          </div>
          <PositionsTable
            rows={activity.openPositions}
            busy={busy !== null}
            closingPositionId={closingPositionId}
            onClosePosition={(position) => {
              if (gasNum <= 0) {
                setError("Fund the gas tank before closing a GMX position.");
                return;
              }
              setClosingPositionId(position.id);
              void run("Close position", async () =>
                setNotice(
                  `GMX close order transaction confirmed: ${shortHash(
                    await closeVaultOpenPositionsOnChain(
                      await provider(),
                      vault.vault_address,
                      vault.asset_address,
                      [position],
                      { payFromTank: true },
                    ),
                  )}`,
                ),
              ).finally(() => setClosingPositionId(null));
            }}
          />
          <OrdersTable rows={activity.pendingOrders} />
        </section>

        <section className="space-y-4 rounded-2xl border bg-card p-4 shadow-xs sm:p-5">
          <SectionHeader
            title="Rebalance history"
            description="Keeper-driven rebalances will appear here once automated monitoring is connected."
          />
          <RebalanceTable rows={[]} />
        </section>

        <div className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
          <section className="space-y-3 rounded-2xl border bg-card p-4 shadow-xs sm:p-5">
            <SectionHeader
              title="1. Prepare gas"
              description="Pre-fund ETH to pay GMX execution fees, then sweep leftovers later."
            />
            <div className="flex flex-col gap-2 sm:flex-row">
              <input
                className="min-w-0 flex-1 rounded-md border bg-background px-3 py-2 text-sm"
                inputMode="decimal"
                placeholder="ETH amount"
                value={gasAmount}
                onChange={(e) => setGasAmount(e.target.value)}
              />
              <Button
                variant="secondary"
                disabled={busy !== null || gasAmount.trim().length === 0}
                onClick={() =>
                  run(
                    "Fund gas",
                    async () =>
                      void (await fundVaultGasOnChain(
                        await provider(),
                        vault.vault_address,
                        gasAmount,
                      )),
                  )
                }
              >
                {busy === "Fund gas" ? "Funding..." : "Fund gas"}
              </Button>
              <Button
                variant="ghost"
                disabled={busy !== null}
                onClick={() =>
                  run(
                    "Sweep gas",
                    async () =>
                      void (await withdrawVaultNativeOnChain(
                        await provider(),
                        vault.vault_address,
                      )),
                  )
                }
              >
                {busy === "Sweep gas" ? "Sweeping..." : "Sweep"}
              </Button>
            </div>
            <p className="text-xs text-muted-foreground">
              Rule of thumb: about 0.0002 ETH per GMX leg.
            </p>
          </section>

          <section className="space-y-3 rounded-2xl border bg-card p-4 shadow-xs sm:p-5">
            <SectionHeader
              title="2. Execute or close"
              description="Open the target allocation, or close a notional amount back to USDC."
            />
            {vault.run_id ? (
              allocation ? (
                <>
                  <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
                    <label className="text-xs font-medium text-muted-foreground">
                      Max legs
                    </label>
                    <input
                      className="w-full rounded-md border bg-background px-2 py-1 text-sm sm:w-24"
                      inputMode="numeric"
                      placeholder="Max legs"
                      value={maxLegsInput}
                      onChange={(e) => setMaxLegsInput(e.target.value)}
                    />
                    <span className="text-xs text-muted-foreground">
                      Caps and renormalizes legs so each clears GMX&apos;s $1 min.
                      Blank = full.
                    </span>
                  </div>
                  <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
                    <Button
                      disabled={busy !== null || !canExecute}
                      onClick={() =>
                        run("Execute", async () =>
                          setNotice(
                            `GMX order transaction confirmed: ${shortHash(
                              await executeVaultAllocationOnChain(
                                await provider(),
                                allocation,
                                balances?.idle ?? "0",
                                { payFromTank: true, maxLegs },
                              ),
                            )}`,
                          ),
                        )
                      }
                    >
                      {busy === "Execute" ? "Executing..." : "Execute target"}
                    </Button>
                    <input
                      className="min-w-0 flex-1 rounded-md border bg-background px-3 py-2 text-sm"
                      inputMode="decimal"
                      placeholder="Close notional USDC"
                      value={closeNotional}
                      onChange={(e) => setCloseNotional(e.target.value)}
                    />
                    <Button
                      variant="destructive"
                      disabled={
                        busy !== null ||
                        closeNotional.trim().length === 0 ||
                        !allocation.executable
                      }
                      onClick={() =>
                        run(
                          "Close",
                          async () =>
                            void (await closeVaultPositionsAndWithdrawIdleOnChain(
                              await provider(),
                              allocation,
                              closeNotional,
                              { payFromTank: true, maxLegs },
                            )),
                        )
                      }
                    >
                      {busy === "Close" ? "Closing..." : "Close + withdraw"}
                    </Button>
                  </div>
                  {idleNum > 0 && gasNum <= 0 ? (
                    <p className="text-xs text-amber-500">
                      Fund the gas tank before executing.
                    </p>
                  ) : null}
                  {!allocation.executable ? (
                    <p className="text-xs text-amber-500">
                      {allocation.missing?.join(", ") ||
                        allocation.reason ||
                        "Allocation is not executable"}
                    </p>
                  ) : null}
                </>
              ) : (
                <p className="text-sm text-muted-foreground">
                  Loading allocation...
                </p>
              )
            ) : (
              <p className="text-sm text-muted-foreground">
                This vault was deployed before run tracking, so execute and
                close are not available here.
              </p>
            )}
          </section>
        </div>

        <section className="space-y-3 rounded-2xl border bg-card p-4 shadow-xs sm:p-5">
          <SectionHeader
            title="3. Recover idle USDC"
            description="Withdraw free collateral back to your wallet. Close open positions first to free collateral."
          />
          <Button
            variant="secondary"
            disabled={busy !== null || idleNum <= 0}
            onClick={() =>
              run(
                "Withdraw",
                async () =>
                  void (await withdrawVaultIdleOnChain(
                    await provider(),
                    vault.vault_address,
                  )),
              )
            }
          >
            {busy === "Withdraw" ? "Withdrawing..." : "Withdraw all USDC"}
          </Button>
        </section>

        {error ? (
          <div className="rounded-xl border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
            {error}
          </div>
        ) : null}
        {notice ? (
          <div className="rounded-xl border bg-card p-3 text-sm text-muted-foreground">
            {notice}
          </div>
        ) : null}
      </div>
    </div>
  );
}

function SectionHeader({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <div className="space-y-1">
      <h2 className="font-heading text-base font-semibold tracking-tight">
        {title}
      </h2>
      <p className="text-sm text-muted-foreground">{description}</p>
    </div>
  );
}

function MetricCard({
  label,
  value,
  helper,
  tone = "neutral",
}: {
  label: string;
  value: string;
  helper: string;
  tone?: "positive" | "negative" | "neutral";
}) {
  return (
    <div className="rounded-2xl border bg-card p-4 shadow-xs">
      <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
        {label}
      </p>
      <p
        className={cn(
          "mt-2 font-heading text-xl font-bold tracking-tight tabular-nums",
          tone === "positive" && "text-emerald-600 dark:text-emerald-300",
          tone === "negative" && "text-red-600 dark:text-red-300",
        )}
      >
        {value}
      </p>
      <p className="mt-1 text-xs text-muted-foreground">{helper}</p>
    </div>
  );
}

function AllocationDonutChart({
  items,
  empty,
  valueLabel = "Weight",
}: {
  items: AllocationDatum[];
  empty: string;
  valueLabel?: string;
}) {
  if (items.length === 0) {
    return <ChartEmpty message={empty} />;
  }

  return (
    <div className="grid min-w-0 gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(14rem,0.8fr)] lg:items-center">
      <ChartContainer className="h-60 w-full min-w-0 overflow-hidden sm:h-72">
        <ResponsiveContainer width="99%" height="100%">
          <PieChart>
            <Pie
              data={items}
              dataKey="weight"
              nameKey="label"
              innerRadius="56%"
              outerRadius="84%"
              paddingAngle={2}
            >
              {items.map((item, index) => (
                <Cell
                  key={item.label}
                  fill={CHART_COLORS[index % CHART_COLORS.length]}
                />
              ))}
            </Pie>
            <Tooltip
              content={<ChartTooltipContent valueFormatter={formatPercent} />}
            />
          </PieChart>
        </ResponsiveContainer>
      </ChartContainer>
      <div className="min-w-0 space-y-2">
        {items.map((item, index) => (
          <div
            key={item.label}
            className="grid min-w-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-3 rounded-xl bg-muted/35 px-3 py-2 text-sm"
          >
            <div className="flex min-w-0 items-center gap-2">
              <span
                className="size-2.5 shrink-0 rounded-full"
                style={{ backgroundColor: CHART_COLORS[index % CHART_COLORS.length] }}
              />
              <span className="min-w-0 truncate font-medium">{item.label}</span>
            </div>
            <div className="text-right">
              <p className="font-mono font-semibold tabular-nums">
                {formatPercent(item.weight)}
              </p>
              {item.notionalUsd !== undefined ? (
                <p className="text-[11px] text-muted-foreground">
                  {valueLabel}: {usdFormatter.format(item.notionalUsd)}
                </p>
              ) : null}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function BacktestSummary({ state }: { state: BacktestLoadState }) {
  if (state.status === "idle" || state.status === "loading") {
    return (
      <SectionHeader
        title="Originating backtest"
        description="Loading the backtest that produced this deployed strategy..."
      />
    );
  }

  if (state.status === "missing" || state.status === "error") {
    return (
      <SectionHeader title="Originating backtest" description={state.message} />
    );
  }

  if (state.status !== "success") return null;

  const { result, run } = state;
  const pnl = summarizeBacktestPnl(result);
  const backtest = result.backtest ?? {};

  return (
    <div className="grid gap-5 xl:grid-cols-[minmax(0,0.95fr)_minmax(0,1.05fr)] xl:items-start">
      <div className="space-y-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <SectionHeader
            title="Originating backtest"
            description={result.title || "The research run that produced this vault mandate."}
          />
          <Link
            href={`/runs/${encodeURIComponent(run.run_id)}`}
            className={buttonVariants({ variant: "outline", size: "sm" })}
          >
            Open run
            <ExternalLink className="size-3.5" />
          </Link>
        </div>
        <div className="grid gap-3 sm:grid-cols-2">
          <MetricCard
            label="PnL"
            value={pnl.label}
            helper={pnl.helper}
            tone={pnl.tone}
          />
          <MetricCard
            label="Sharpe"
            value={formatNumber(result.kpis?.sharpe_ratio)}
            helper={`Max drawdown ${formatPercent(result.kpis?.max_drawdown)}`}
          />
          <MetricCard
            label="CAGR"
            value={formatPercent(result.kpis?.cagr)}
            helper={`Final equity ${formatCurrency(result.kpis?.final_equity_usd)}`}
          />
          <MetricCard
            label="Window"
            value={`${formatShortDate(backtest.start_date)} to ${formatShortDate(backtest.end_date)}`}
            helper={`Rebalance: ${backtest.rebalance ?? "not provided"}`}
          />
        </div>
      </div>
      <div className="space-y-3">
        <h3 className="font-heading text-sm font-semibold tracking-tight">
          Backtest equity curve
        </h3>
        <BacktestEquityChart result={result} />
      </div>
    </div>
  );
}

function BacktestEquityChart({ result }: { result: StrategyResult }) {
  const data = (result.charts?.equity_curve ?? [])
    .map((point) => ({
      date: point.date,
      strategy: finiteNumber(point.strategy_equity),
      benchmark: finiteNumber(point.benchmark_equity),
    }))
    .filter((point) => point.strategy !== null || point.benchmark !== null);

  if (data.length === 0) {
    return <ChartEmpty message="No equity curve was stored for this run." />;
  }

  return (
    <ChartContainer className="h-72 w-full min-w-0 overflow-hidden">
      <ResponsiveContainer width="99%" height="100%">
        <AreaChart data={data} margin={{ left: 4, right: 4, top: 8, bottom: 4 }}>
          <Tooltip
            cursor={{ stroke: "var(--chart-grid)", strokeDasharray: "3 3" }}
            content={
              <ChartTooltipContent
                labelFormatter={formatShortDate}
                valueFormatter={formatCurrency}
              />
            }
          />
          <Area
            type="monotone"
            dataKey="strategy"
            name="Strategy"
            stroke="var(--chart-1)"
            fill="var(--chart-1)"
            strokeWidth={2.5}
            fillOpacity={0.16}
            connectNulls
          />
          <Area
            type="monotone"
            dataKey="benchmark"
            name="Benchmark"
            stroke="var(--chart-4)"
            fill="var(--chart-4)"
            strokeWidth={2}
            fillOpacity={0.08}
            connectNulls
          />
        </AreaChart>
      </ResponsiveContainer>
    </ChartContainer>
  );
}

function ChartEmpty({ message }: { message: string }) {
  return (
    <div className="flex min-h-56 items-center justify-center rounded-xl border border-dashed bg-muted/20 p-6 text-center text-sm text-muted-foreground">
      {message}
    </div>
  );
}

function PositionsTable({
  rows,
  busy,
  closingPositionId,
  onClosePosition,
}: {
  rows: GmxOpenPosition[];
  busy: boolean;
  closingPositionId: string | null;
  onClosePosition: (position: GmxOpenPosition) => void;
}) {
  return (
    <ActivityTable title="Open positions" empty="No open positions" hasActions>
      {rows.map((row) => (
        <tr key={row.id} className="border-b last:border-0">
          <td className="px-3 py-2 font-medium">{row.marketName}</td>
          <td className="px-3 py-2">
            <SideBadge side={row.side} />
          </td>
          <td className="px-3 py-2 text-right font-mono tabular-nums">
            {usdFormatter.format(row.sizeUsd)}
          </td>
          <td className="px-3 py-2 text-right font-mono tabular-nums">
            {tokenFormatter.format(row.collateralAmount)} {row.collateralSymbol}
          </td>
          <td className="px-3 py-2 text-right">
            <Button
              size="sm"
              variant="secondary"
              disabled={busy}
              onClick={() => onClosePosition(row)}
            >
              {closingPositionId === row.id ? "Closing…" : "Close"}
            </Button>
          </td>
        </tr>
      ))}
    </ActivityTable>
  );
}

function OrdersTable({ rows }: { rows: GmxPendingOrder[] }) {
  return (
    <ActivityTable title="Pending orders" empty="No pending orders">
      {rows.map((row) => (
        <tr key={row.id} className="border-b last:border-0">
          <td className="px-3 py-2 font-medium">{row.marketName}</td>
          <td className="px-3 py-2">
            <SideBadge side={row.side} />
          </td>
          <td className="px-3 py-2 text-sm text-muted-foreground">
            {row.orderType}
          </td>
          <td className="px-3 py-2 text-right font-mono tabular-nums">
            {usdFormatter.format(row.sizeUsd)}
          </td>
          <td className="px-3 py-2 text-right font-mono tabular-nums">
            {tokenFormatter.format(row.collateralAmount)} {row.collateralSymbol}
          </td>
        </tr>
      ))}
    </ActivityTable>
  );
}

function RebalanceTable({ rows }: { rows: never[] }) {
  return (
    <div className="overflow-x-auto rounded-xl border bg-background/70">
      <table className="w-full min-w-[640px] text-left text-sm">
        <thead className="border-b bg-muted/50 text-xs uppercase tracking-wide text-muted-foreground">
          <tr>
            <th className="px-3 py-2 font-medium">Time</th>
            <th className="px-3 py-2 font-medium">Trigger</th>
            <th className="px-3 py-2 text-right font-medium">Before</th>
            <th className="px-3 py-2 text-right font-medium">After</th>
            <th className="px-3 py-2 text-right font-medium">Status</th>
          </tr>
        </thead>
        <tbody>
          {rows.length > 0 ? null : (
            <tr>
              <td
                className="px-3 py-6 text-center text-xs text-muted-foreground"
                colSpan={5}
              >
                No rebalances yet. The keeper is not connected in this build.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function ActivityTable({
  title,
  empty,
  children,
  hasActions = false,
}: {
  title: string;
  empty: string;
  children: ReactNode;
  hasActions?: boolean;
}) {
  const hasRows = Array.isArray(children)
    ? children.length > 0
    : Boolean(children);
  return (
    <section className="space-y-2">
      <h2 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
        {title}
      </h2>
      <div className="overflow-x-auto rounded-xl border bg-background/70">
        <table className="w-full min-w-[560px] text-left text-sm">
          <thead className="border-b bg-muted/50 text-xs uppercase tracking-wide text-muted-foreground">
            <tr>
              <th className="px-3 py-2 font-medium">Market</th>
              <th className="px-3 py-2 font-medium">Side</th>
              {title === "Pending orders" ? (
                <th className="px-3 py-2 font-medium">Type</th>
              ) : null}
              <th className="px-3 py-2 text-right font-medium">Size</th>
              <th className="px-3 py-2 text-right font-medium">Collateral</th>
              {hasActions ? (
                <th className="px-3 py-2 text-right font-medium">Action</th>
              ) : null}
            </tr>
          </thead>
          <tbody>
            {hasRows ? (
              children
            ) : (
              <tr>
                <td
                  className="px-3 py-4 text-center text-xs text-muted-foreground"
                  colSpan={
                    (title === "Pending orders" ? 5 : 4) + (hasActions ? 1 : 0)
                  }
                >
                  {empty}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function SideBadge({ side }: { side: "Long" | "Short" }) {
  return (
    <Badge
      variant="outline"
      className={cn(
        side === "Long"
          ? "border-emerald-500/30 text-emerald-700 dark:text-emerald-300"
          : "border-red-500/30 text-red-700 dark:text-red-300",
      )}
    >
      {side}
    </Badge>
  );
}

function currentAllocationFromPositions(
  positions: GmxOpenPosition[],
): AllocationDatum[] {
  const totals = new Map<string, number>();
  for (const position of positions) {
    if (!Number.isFinite(position.sizeUsd) || position.sizeUsd <= 0) continue;
    const key = `${position.marketName} ${position.side}`;
    totals.set(key, (totals.get(key) ?? 0) + position.sizeUsd);
  }

  const total = Array.from(totals.values()).reduce((sum, value) => sum + value, 0);
  if (total <= 0) return [];

  return Array.from(totals.entries())
    .map(([label, notionalUsd]) => ({
      label,
      notionalUsd,
      weight: notionalUsd / total,
    }))
    .sort((a, b) => b.weight - a.weight);
}

function targetAllocationFromReadiness(
  target: VaultAllocationReadiness["target_allocation"],
): AllocationDatum[] {
  return target
    .map((item) => ({
      label: formatCoinId(item.coin_id ?? "asset"),
      weight: finiteNumber(item.weight) ?? 0,
    }))
    .filter((item) => item.weight > 0)
    .sort((a, b) => b.weight - a.weight);
}

function summarizeBacktestPnl(result: StrategyResult): {
  label: string;
  helper: string;
  tone: "positive" | "negative" | "neutral";
} {
  const initial = finiteNumber(result.backtest?.initial_capital_usd);
  const final = finiteNumber(result.kpis?.final_equity_usd);
  const multiple = finiteNumber(result.kpis?.final_equity_multiple);

  if (initial !== null && final !== null && initial !== 0) {
    const pnl = final - initial;
    const pnlPct = pnl / initial;
    return {
      label: formatSignedCurrency(pnl),
      helper: `${formatSignedPercent(pnlPct)} over the selected backtest`,
      tone: pnl > 0 ? "positive" : pnl < 0 ? "negative" : "neutral",
    };
  }

  if (multiple !== null) {
    const pnlPct = multiple - 1;
    return {
      label: `${formatNumber(multiple)}x`,
      helper: `${formatSignedPercent(pnlPct)} over the selected backtest`,
      tone: pnlPct > 0 ? "positive" : pnlPct < 0 ? "negative" : "neutral",
    };
  }

  return {
    label: "Not available",
    helper: "The originating run did not store final equity.",
    tone: "neutral",
  };
}

function backtestStatusLabel(state: BacktestLoadState) {
  switch (state.status) {
    case "success":
      return summarizeBacktestPnl(state.result).label;
    case "missing":
      return "Not linked";
    case "error":
      return "Unavailable";
    default:
      return "Loading...";
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isRunWithStrategyResult(
  value: unknown,
): value is Run & { structured_result: StrategyResult } {
  if (!isRecord(value) || !isRecord(value.structured_result)) return false;
  const result = value.structured_result;
  return (
    typeof value.run_id === "string" &&
    typeof value.status === "string" &&
    typeof result.title === "string" &&
    isRecord(result.kpis) &&
    (isRecord(result.backtest) || result.backtest === null) &&
    isRecord(result.charts)
  );
}

function finiteNumber(value: number | null | undefined) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function formatPercent(value: number | null | undefined) {
  const numeric = finiteNumber(value);
  if (numeric === null) return "Not provided";
  return new Intl.NumberFormat("en-US", {
    style: "percent",
    maximumFractionDigits: 1,
  }).format(numeric);
}

function formatCurrency(value: number | null | undefined) {
  const numeric = finiteNumber(value);
  if (numeric === null) return "Not provided";
  return usdFormatter.format(numeric);
}

function formatSignedCurrency(value: number) {
  const formatted = usdFormatter.format(Math.abs(value));
  if (value > 0) return `+${formatted}`;
  if (value < 0) return `-${formatted}`;
  return formatted;
}

function formatSignedPercent(value: number) {
  const formatted = formatPercent(Math.abs(value));
  if (value > 0) return `+${formatted}`;
  if (value < 0) return `-${formatted}`;
  return formatted;
}

function formatNumber(value: number | null | undefined) {
  const numeric = finiteNumber(value);
  if (numeric === null) return "Not provided";
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 2,
  }).format(numeric);
}

function formatShortDate(value: string | null | undefined) {
  if (!value) return "Not provided";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(date);
}

function formatCoinId(value: string) {
  return value
    .split(/[-_]/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function shortHash(hash: string) {
  return `${hash.slice(0, 10)}...${hash.slice(-6)}`;
}
