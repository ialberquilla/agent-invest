"use client";

import { useCallback, useEffect, useState, type ReactNode } from "react";
import { ArrowLeft, RefreshCw } from "lucide-react";
import { useWallets } from "@privy-io/react-auth";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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
import { cn } from "@/lib/utils";

type Balances = { idle: string; gas: string };

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

  return (
    <div className="min-h-0 flex-1 overflow-y-auto bg-background">
      <div className="mx-auto max-w-2xl space-y-4 p-6">
        <button
          type="button"
          onClick={onBack}
          className="inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="size-4" /> Back
        </button>

        <div>
          <p className="text-[11px] uppercase tracking-wide text-muted-foreground">
            Vault
          </p>
          <h1 className="font-heading text-xl font-semibold">{vault.label}</h1>
          <p className="break-all font-mono text-xs text-muted-foreground">
            {vault.vault_address}
          </p>
        </div>

        <div className="grid grid-cols-2 gap-3">
          <div className="rounded-lg border bg-card p-3">
            <p className="text-xs text-muted-foreground">Idle collateral</p>
            <p className="text-lg font-semibold">
              {balances ? `${balances.idle} USDC` : "…"}
            </p>
          </div>
          <div className="rounded-lg border bg-card p-3">
            <p className="text-xs text-muted-foreground">Gas tank</p>
            <p className="text-lg font-semibold">
              {balances ? `${balances.gas} ETH` : "…"}
            </p>
          </div>
        </div>

        <div className="space-y-3 rounded-lg border bg-card p-3">
          <div className="flex items-start justify-between gap-3">
            <div>
              <p className="text-sm font-medium">Live GMX status</p>
              <p className="text-xs text-muted-foreground">
                {activity.pendingOrders.length > 0
                  ? "GMX has accepted orders that have not executed yet."
                  : activity.openPositions.length > 0
                    ? "GMX positions are open for this strategy vault."
                    : "No pending orders or open positions found for this vault."}
              </p>
            </div>
            <div className="flex items-center gap-2">
              <Badge
                variant={
                  activity.openPositions.length > 0 ? "default" : "outline"
                }
              >
                {executionStatus}
              </Badge>
              <Button
                variant="outline"
                size="sm"
                onClick={() => void refreshActivity()}
                disabled={isLoadingActivity}
              >
                <RefreshCw
                  className={cn(
                    "size-3.5",
                    isLoadingActivity && "animate-spin",
                  )}
                />
                Refresh
              </Button>
            </div>
          </div>
          <div className="grid grid-cols-2 gap-3 text-sm">
            <div className="rounded-md border bg-background/60 p-2">
              <p className="text-xs text-muted-foreground">Open notional</p>
              <p className="font-mono font-semibold tabular-nums">
                {usdFormatter.format(totalOpenSize)}
              </p>
            </div>
            <div className="rounded-md border bg-background/60 p-2">
              <p className="text-xs text-muted-foreground">Pending notional</p>
              <p className="font-mono font-semibold tabular-nums">
                {usdFormatter.format(totalPendingSize)}
              </p>
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
        </div>

        {/* Gas tank */}
        <div className="space-y-2 rounded-lg border bg-card p-3">
          <p className="text-sm font-medium">Gas tank</p>
          <div className="flex gap-2">
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
              {busy === "Fund gas" ? "Funding…" : "Fund gas"}
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
              {busy === "Sweep gas" ? "Sweeping…" : "Sweep"}
            </Button>
          </div>
          <p className="text-xs text-muted-foreground">
            Pre-fund ETH to pay GMX execution fees (≈0.0002 ETH per leg). Sweep
            reclaims leftover + refunds.
          </p>
        </div>

        {/* Trade */}
        <div className="space-y-2 rounded-lg border bg-card p-3">
          <p className="text-sm font-medium">Strategy</p>
          {vault.run_id ? (
            allocation ? (
              <>
                <p className="text-xs text-muted-foreground">
                  Target:{" "}
                  {allocation.target_allocation
                    .map(
                      (i) =>
                        `${i.coin_id ?? "asset"} ${Math.round((i.weight ?? 0) * 100)}%`,
                    )
                    .join(", ")}
                </p>
                <div className="flex items-center gap-2">
                  <input
                    className="w-24 rounded-md border bg-background px-2 py-1 text-sm"
                    inputMode="numeric"
                    placeholder="Max legs"
                    value={maxLegsInput}
                    onChange={(e) => setMaxLegsInput(e.target.value)}
                  />
                  <span className="text-[11px] text-muted-foreground">
                    Caps + renormalizes legs so each clears GMX&apos;s $1 min.
                    Blank = full.
                  </span>
                </div>
                <div className="flex flex-wrap items-center gap-2 pt-1">
                  <Button
                    size="sm"
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
                    {busy === "Execute" ? "Executing…" : "Execute strategy"}
                  </Button>
                  {idleNum > 0 && gasNum <= 0 ? (
                    <span className="text-[11px] text-amber-500">
                      Fund the gas tank first ↑
                    </span>
                  ) : null}
                  {allocation && !allocation.executable ? (
                    <span className="text-[11px] text-amber-500">
                      {allocation.missing?.join(", ") ||
                        allocation.reason ||
                        "Allocation is not executable"}
                    </span>
                  ) : null}
                  <input
                    className="min-w-0 flex-1 rounded-md border bg-background px-2 py-1 text-sm"
                    inputMode="decimal"
                    placeholder="Close notional USDC"
                    value={closeNotional}
                    onChange={(e) => setCloseNotional(e.target.value)}
                  />
                  <Button
                    size="sm"
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
                    {busy === "Close" ? "Closing…" : "Close + withdraw"}
                  </Button>
                </div>
              </>
            ) : (
              <p className="text-xs text-muted-foreground">
                Loading allocation…
              </p>
            )
          ) : (
            <p className="text-xs text-muted-foreground">
              This vault was deployed before run tracking, so execute/close
              aren&apos;t available here. Open the originating run to trade, or
              use Withdraw below to recover collateral.
            </p>
          )}
        </div>

        {/* Recover */}
        <div className="space-y-2 rounded-lg border bg-card p-3">
          <p className="text-sm font-medium">Recover</p>
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
            {busy === "Withdraw" ? "Withdrawing…" : "Withdraw all USDC"}
          </Button>
          <p className="text-xs text-muted-foreground">
            Sends idle collateral back to your wallet (owner-only). Close open
            positions first to free collateral.
          </p>
        </div>

        {error ? <p className="text-sm text-destructive">{error}</p> : null}
        {notice ? (
          <p className="text-sm text-muted-foreground">{notice}</p>
        ) : null}
      </div>
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

function shortHash(hash: string) {
  return `${hash.slice(0, 10)}...${hash.slice(-6)}`;
}
