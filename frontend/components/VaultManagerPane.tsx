"use client";

import { useCallback, useEffect, useState } from "react";
import { ArrowLeft } from "lucide-react";
import { useWallets } from "@privy-io/react-auth";

import { Button } from "@/components/ui/button";
import {
  closeVaultPositionsAndWithdrawIdleOnChain,
  executeVaultAllocationOnChain,
  fundVaultGasOnChain,
  readAllocationReadiness,
  readVaultBalances,
  withdrawVaultIdleOnChain,
  withdrawVaultNativeOnChain,
  type VaultAllocationReadiness,
} from "@/lib/deploy-vault";
import type { DeployedStrategy } from "@/lib/local-store";

type Balances = { idle: string; gas: string };

export function VaultManagerPane({
  vault,
  onBack,
}: {
  vault: DeployedStrategy;
  onBack: () => void;
}) {
  const { wallets } = useWallets();
  const [balances, setBalances] = useState<Balances | null>(null);
  const [allocation, setAllocation] = useState<VaultAllocationReadiness | null>(null);
  const [gasAmount, setGasAmount] = useState("0.002");
  const [closeNotional, setCloseNotional] = useState("");
  const [maxLegsInput, setMaxLegsInput] = useState("2");
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const maxLegs = maxLegsInput.trim().length > 0 ? Number(maxLegsInput) : undefined;

  const refreshBalances = useCallback(async () => {
    try {
      setBalances(await readVaultBalances(vault.vault_address));
    } catch {
      // leave previous balances; surfaced lazily on the next action
    }
  }, [vault.vault_address]);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const next = await readVaultBalances(vault.vault_address);
        if (!cancelled) setBalances(next);
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
  }, [vault.vault_address, vault.run_id]);

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
    } catch (err) {
      setError(err instanceof Error ? err.message : `${label} failed`);
    } finally {
      setBusy(null);
    }
  }

  const idleNum = Number(balances?.idle ?? "0");
  const gasNum = Number(balances?.gas ?? "0");
  const canExecute = Boolean(allocation) && idleNum > 0 && gasNum > 0;

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
          <p className="text-[11px] uppercase tracking-wide text-muted-foreground">Vault</p>
          <h1 className="font-heading text-xl font-semibold">{vault.label}</h1>
          <p className="break-all font-mono text-xs text-muted-foreground">{vault.vault_address}</p>
        </div>

        <div className="grid grid-cols-2 gap-3">
          <div className="rounded-lg border bg-card p-3">
            <p className="text-xs text-muted-foreground">Idle collateral</p>
            <p className="text-lg font-semibold">{balances ? `${balances.idle} USDC` : "…"}</p>
          </div>
          <div className="rounded-lg border bg-card p-3">
            <p className="text-xs text-muted-foreground">Gas tank</p>
            <p className="text-lg font-semibold">{balances ? `${balances.gas} ETH` : "…"}</p>
          </div>
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
                run("Fund gas", async () =>
                  void (await fundVaultGasOnChain(await provider(), vault.vault_address, gasAmount)),
                )
              }
            >
              {busy === "Fund gas" ? "Funding…" : "Fund gas"}
            </Button>
            <Button
              variant="ghost"
              disabled={busy !== null}
              onClick={() =>
                run("Sweep gas", async () =>
                  void (await withdrawVaultNativeOnChain(await provider(), vault.vault_address)),
                )
              }
            >
              {busy === "Sweep gas" ? "Sweeping…" : "Sweep"}
            </Button>
          </div>
          <p className="text-xs text-muted-foreground">
            Pre-fund ETH to pay GMX execution fees (≈0.0002 ETH per leg). Sweep reclaims leftover + refunds.
          </p>
        </div>

        {/* Trade */}
        <div className="space-y-2 rounded-lg border bg-card p-3">
          <p className="text-sm font-medium">Strategy</p>
          {vault.run_id ? (
            allocation ? (
              <>
                <p className="text-xs text-muted-foreground">
                  Target: {allocation.target_allocation
                    .map((i) => `${i.coin_id ?? "asset"} ${Math.round((i.weight ?? 0) * 100)}%`)
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
                    Caps + renormalizes legs so each clears GMX&apos;s $1 min. Blank = full.
                  </span>
                </div>
                <div className="flex flex-wrap items-center gap-2 pt-1">
                  <Button
                    size="sm"
                    disabled={busy !== null || !canExecute}
                    onClick={() =>
                      run("Execute", async () =>
                        void (await executeVaultAllocationOnChain(
                          await provider(),
                          allocation,
                          balances?.idle ?? "0",
                          { payFromTank: true, maxLegs },
                        )),
                      )
                    }
                  >
                    {busy === "Execute" ? "Executing…" : "Execute strategy"}
                  </Button>
                  {idleNum > 0 && gasNum <= 0 ? (
                    <span className="text-[11px] text-amber-500">Fund the gas tank first ↑</span>
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
                    disabled={busy !== null || closeNotional.trim().length === 0}
                    onClick={() =>
                      run("Close", async () =>
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
              <p className="text-xs text-muted-foreground">Loading allocation…</p>
            )
          ) : (
            <p className="text-xs text-muted-foreground">
              This vault was deployed before run tracking, so execute/close aren&apos;t available here.
              Open the originating run to trade, or use Withdraw below to recover collateral.
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
              run("Withdraw", async () =>
                void (await withdrawVaultIdleOnChain(await provider(), vault.vault_address)),
              )
            }
          >
            {busy === "Withdraw" ? "Withdrawing…" : "Withdraw all USDC"}
          </Button>
          <p className="text-xs text-muted-foreground">
            Sends idle collateral back to your wallet (owner-only). Close open positions first to free
            collateral.
          </p>
        </div>

        {error ? <p className="text-sm text-destructive">{error}</p> : null}
        {notice ? <p className="text-sm text-muted-foreground">{notice}</p> : null}
      </div>
    </div>
  );
}
