"use client";

import { useEffect, useState } from "react";
import { usePrivy, useWallets } from "@privy-io/react-auth";

import { Button } from "@/components/ui/button";
import {
  assertVaultDeployable,
  closeVaultPositionsAndWithdrawIdleOnChain,
  deployVaultOnChain,
  executeVaultAllocationOnChain,
  fundVaultGasOnChain,
  fundVaultOnChain,
  getExistingVaultBinding,
  readAllocationReadiness,
  readVaultBalances,
  readVaultNativeBalance,
  saveVaultBinding,
  withdrawVaultNativeOnChain,
  type VaultAllocationReadiness,
  type VaultBindingResponse,
} from "@/lib/deploy-vault";
import { upsertDeployedStrategy } from "@/lib/local-store";

type DeployState =
  | { status: "idle" }
  | { status: "deploying" }
  | {
      status: "deployed";
      binding: VaultBindingResponse;
      idleBalance?: string;
      fundError?: string;
      gasBalance?: string;
      gasError?: string;
      allocation?: VaultAllocationReadiness;
      allocationError?: string;
    }
  | { status: "funding"; binding: VaultBindingResponse; amount: string }
  | { status: "error"; message: string };

export function DeployVaultButton({ runId }: { runId: string }) {
  const { authenticated, login, getAccessToken } = usePrivy();
  const { wallets } = useWallets();
  const [state, setState] = useState<DeployState>({ status: "idle" });

  // If this run already has a vault bound (e.g. after a reload), restore the manage panel instead
  // of showing "Deploy on-chain" again — which would deploy a duplicate vault.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const binding = await getExistingVaultBinding(runId);
        if (cancelled || !binding) return;
        upsertDeployedStrategy({
          mandate_id: binding.mandate_id,
          chain_id: binding.chain_id,
          vault_address: binding.vault_address,
          asset_address: binding.asset_address,
          status: binding.status,
          run_id: runId,
        });
        window.dispatchEvent(new Event("agent-invest:deployed-strategies"));
        const balances = await readVaultBalances(binding.vault_address).catch(() => undefined);
        if (cancelled) return;
        setState((current) =>
          current.status === "idle"
            ? {
                status: "deployed",
                binding,
                idleBalance: balances?.idle,
                gasBalance: balances?.gas,
              }
            : current,
        );
      } catch {
        // best-effort restore; ignore failures and keep the deploy button
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [runId]);

  async function handleDeploy() {
    if (!authenticated) {
      login();
      return;
    }

    const wallet = wallets[0];
    if (!wallet) {
      setState({ status: "error", message: "Connect a wallet to deploy" });
      return;
    }

    setState({ status: "deploying" });
    try {
      const accessToken = await getAccessToken();
      await assertVaultDeployable(runId, accessToken);
      const provider = await wallet.getEthereumProvider();
      const deployment = await deployVaultOnChain(provider);
      // 2. Persist the vault<->mandate binding + activate the mandate.
      const binding = await saveVaultBinding(
        runId,
        deployment,
        accessToken,
      );
      upsertDeployedStrategy({
        mandate_id: binding.mandate_id,
        chain_id: binding.chain_id,
        vault_address: binding.vault_address,
        asset_address: binding.asset_address,
        status: binding.status,
        run_id: runId,
      });
      window.dispatchEvent(new Event("agent-invest:deployed-strategies"));
      setState({ status: "deployed", binding });
    } catch (error) {
      setState({
        status: "error",
        message: error instanceof Error ? error.message : "Deploy failed",
      });
    }
  }

  async function handleFund(binding: VaultBindingResponse, amount: string, ethGas?: string) {
    const wallet = wallets[0];
    if (!wallet) {
      setState({ status: "error", message: "Connect a wallet to fund" });
      return;
    }

    setState({ status: "funding", binding, amount });
    try {
      const provider = await wallet.getEthereumProvider();
      const idleBalance = await fundVaultOnChain(provider, binding.vault_address, amount, ethGas);
      const gasBalance = await readVaultNativeBalance(provider, binding.vault_address).catch(
        () => undefined,
      );
      setState({ status: "deployed", binding, idleBalance, gasBalance });
    } catch (error) {
      setState({
        status: "deployed",
        binding,
        idleBalance: undefined,
        fundError: error instanceof Error ? error.message : "Funding failed",
      });
    }
  }

  async function handleFundGas(binding: VaultBindingResponse, ethAmount: string) {
    const wallet = wallets[0];
    if (!wallet) {
      setState({ status: "error", message: "Connect a wallet to fund gas" });
      return;
    }
    try {
      const provider = await wallet.getEthereumProvider();
      const gasBalance = await fundVaultGasOnChain(provider, binding.vault_address, ethAmount);
      setState((current) =>
        current.status === "deployed" ? { ...current, gasBalance, gasError: undefined } : current,
      );
    } catch (error) {
      setState((current) =>
        current.status === "deployed"
          ? { ...current, gasError: error instanceof Error ? error.message : "Gas funding failed" }
          : current,
      );
    }
  }

  async function handleSweepGas(binding: VaultBindingResponse) {
    const wallet = wallets[0];
    if (!wallet) {
      setState({ status: "error", message: "Connect a wallet to sweep gas" });
      return;
    }
    try {
      const provider = await wallet.getEthereumProvider();
      const gasBalance = await withdrawVaultNativeOnChain(provider, binding.vault_address);
      setState((current) =>
        current.status === "deployed" ? { ...current, gasBalance, gasError: undefined } : current,
      );
    } catch (error) {
      setState((current) =>
        current.status === "deployed"
          ? { ...current, gasError: error instanceof Error ? error.message : "Gas sweep failed" }
          : current,
      );
    }
  }

  async function handleAllocate(binding: VaultBindingResponse, idleBalance?: string) {
    try {
      const allocation = await readAllocationReadiness(runId);
      setState({ status: "deployed", binding, idleBalance, allocation });
    } catch (error) {
      setState({
        status: "deployed",
        binding,
        idleBalance,
        allocationError:
          error instanceof Error ? error.message : "Allocation check failed",
      });
    }
  }

  async function handleExecute(
    allocation: VaultAllocationReadiness,
    idleBalance: string,
    maxLegs?: number,
  ) {
    const wallet = wallets[0];
    if (!wallet) {
      setState({ status: "error", message: "Connect a wallet to execute" });
      return;
    }
    try {
      const provider = await wallet.getEthereumProvider();
      const gas = await readVaultNativeBalance(provider, allocation.vault_address);
      if (Number(gas) <= 0) {
        setState((current) =>
          current.status === "deployed"
            ? {
                ...current,
                allocationError:
                  "Fund the gas tank first — GMX execution fees are paid from the vault's ETH. Use “Fund gas” (or add ETH when funding USDC).",
              }
            : current,
        );
        return;
      }
      await executeVaultAllocationOnChain(provider, allocation, idleBalance, {
        payFromTank: true,
        maxLegs,
      });
      setState((current) =>
        current.status === "deployed"
          ? { ...current, allocation: { ...allocation, reason: "GMX increase orders submitted." } }
          : current,
      );
    } catch (error) {
      setState((current) =>
        current.status === "deployed"
          ? { ...current, allocationError: error instanceof Error ? error.message : "Execution failed" }
          : current,
      );
    }
  }

  async function handleClose(
    allocation: VaultAllocationReadiness,
    notionalUsd: string,
    maxLegs?: number,
  ) {
    const wallet = wallets[0];
    if (!wallet) {
      setState({ status: "error", message: "Connect a wallet to close" });
      return;
    }
    try {
      const provider = await wallet.getEthereumProvider();
      await closeVaultPositionsAndWithdrawIdleOnChain(provider, allocation, notionalUsd, {
        payFromTank: true,
        maxLegs,
      });
      setState((current) =>
        current.status === "deployed"
          ? { ...current, allocation: { ...allocation, reason: "Close orders submitted and idle collateral withdrawn." } }
          : current,
      );
    } catch (error) {
      setState((current) =>
        current.status === "deployed"
          ? { ...current, allocationError: error instanceof Error ? error.message : "Close failed" }
          : current,
      );
    }
  }

  if (state.status === "deployed" || state.status === "funding") {
    const binding = state.binding;
    const isFunding = state.status === "funding";
    return (
      <FundVaultPanel
        binding={binding}
        idleBalance={state.status === "deployed" ? state.idleBalance : undefined}
        fundError={state.status === "deployed" ? state.fundError : undefined}
        gasBalance={state.status === "deployed" ? state.gasBalance : undefined}
        gasError={state.status === "deployed" ? state.gasError : undefined}
        allocation={state.status === "deployed" ? state.allocation : undefined}
        allocationError={
          state.status === "deployed" ? state.allocationError : undefined
        }
        funding={isFunding}
        onFund={handleFund}
        onFundGas={handleFundGas}
        onSweepGas={handleSweepGas}
        onAllocate={handleAllocate}
        onExecute={handleExecute}
        onClose={handleClose}
      />
    );
  }

  return (
    <div className="flex flex-col gap-2">
      <Button onClick={handleDeploy} disabled={state.status === "deploying"}>
        {state.status === "deploying"
          ? "Deploying…"
          : authenticated
            ? "Deploy on-chain"
            : "Log in to deploy"}
      </Button>
      {state.status === "error" ? (
        <p className="text-sm text-destructive">{state.message}</p>
      ) : null}
    </div>
  );
}

function FundVaultPanel({
  binding,
  idleBalance,
  fundError,
  gasBalance,
  gasError,
  allocation,
  allocationError,
  funding,
  onFund,
  onFundGas,
  onSweepGas,
  onAllocate,
  onExecute,
  onClose,
}: {
  binding: VaultBindingResponse;
  idleBalance?: string;
  fundError?: string;
  gasBalance?: string;
  gasError?: string;
  allocation?: VaultAllocationReadiness;
  allocationError?: string;
  funding: boolean;
  onFund: (binding: VaultBindingResponse, amount: string, ethGas?: string) => void;
  onFundGas: (binding: VaultBindingResponse, ethAmount: string) => void;
  onSweepGas: (binding: VaultBindingResponse) => void;
  onAllocate: (binding: VaultBindingResponse, idleBalance?: string) => void;
  onExecute: (allocation: VaultAllocationReadiness, idleBalance: string, maxLegs?: number) => void;
  onClose: (allocation: VaultAllocationReadiness, notionalUsd: string, maxLegs?: number) => void;
}) {
  const [amount, setAmount] = useState("");
  const [fundGasAmount, setFundGasAmount] = useState("");
  const [gasAmount, setGasAmount] = useState("");
  const [closeNotional, setCloseNotional] = useState("");
  const [maxLegsInput, setMaxLegsInput] = useState("");
  const maxLegs = maxLegsInput.trim().length > 0 ? Number(maxLegsInput) : undefined;

  return (
    <div className="min-w-72 space-y-2 rounded-lg border bg-background p-3 text-sm">
      <p className="text-muted-foreground">
        Deployed at <span className="font-mono">{binding.vault_address}</span>
      </p>
      <div className="flex gap-2">
        <input
          className="min-w-0 flex-1 rounded-md border bg-background px-3 py-2"
          inputMode="decimal"
          placeholder="USDC amount"
          value={amount}
          onChange={(event) => setAmount(event.target.value)}
          disabled={funding}
        />
        <input
          className="w-24 rounded-md border bg-background px-2 py-2"
          inputMode="decimal"
          placeholder="+ETH gas"
          value={fundGasAmount}
          onChange={(event) => setFundGasAmount(event.target.value)}
          disabled={funding}
          title="ETH for the gas tank, funded in the same deposit tx"
        />
        <Button
          onClick={() => onFund(binding, amount, fundGasAmount)}
          disabled={funding || amount.trim().length === 0}
        >
          {funding ? "Funding…" : "Fund vault"}
        </Button>
      </div>
      <p className="text-xs text-muted-foreground">
        {idleBalance
          ? `Idle collateral: ${idleBalance} USDC`
          : "Deposits USDC + tops up the ETH gas tank in one tx (after approve)."}
      </p>
      {fundError ? <p className="text-xs text-destructive">{fundError}</p> : null}
      <div className="flex gap-2 border-t pt-2">
        <input
          className="min-w-0 flex-1 rounded-md border bg-background px-3 py-2"
          inputMode="decimal"
          placeholder="ETH gas amount"
          value={gasAmount}
          onChange={(event) => setGasAmount(event.target.value)}
        />
        <Button
          variant="secondary"
          onClick={() => onFundGas(binding, gasAmount)}
          disabled={gasAmount.trim().length === 0}
        >
          Fund gas
        </Button>
      </div>
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span>
          {gasBalance ? `Gas tank: ${gasBalance} ETH` : "Pre-fund ETH to pay GMX execution fees."}
        </span>
        <Button size="sm" variant="ghost" onClick={() => onSweepGas(binding)}>
          Sweep gas
        </Button>
      </div>
      {gasError ? <p className="text-xs text-destructive">{gasError}</p> : null}
      <Button
        variant="secondary"
        onClick={() => onAllocate(binding, idleBalance)}
        disabled={!idleBalance}
      >
        Check allocation
      </Button>
      {allocation ? (
        <div className="space-y-1 text-xs text-muted-foreground">
          <p>{allocation.reason ?? "Allocation is ready."}</p>
          {allocation.target_allocation.length > 0 ? (
            <p>
              Target: {allocation.target_allocation
                .map((item) => `${item.coin_id ?? "asset"} ${Math.round((item.weight ?? 0) * 100)}%`)
                .join(", ")}
            </p>
          ) : null}
          <div className="flex items-center gap-2 pt-2">
            <input
              className="w-28 rounded-md border bg-background px-2 py-1"
              inputMode="numeric"
              placeholder="Max legs (test)"
              value={maxLegsInput}
              onChange={(event) => setMaxLegsInput(event.target.value)}
            />
            <span className="text-[11px] text-muted-foreground">
              Caps + renormalizes legs so each clears GMX&apos;s $1 min. Blank = full allocation.
            </span>
          </div>
          <div className="flex flex-wrap gap-2 pt-1">
            <Button
              size="sm"
              onClick={() => (idleBalance ? onExecute(allocation, idleBalance, maxLegs) : undefined)}
              disabled={!idleBalance}
            >
              Execute strategy
            </Button>
            <input
              className="min-w-0 flex-1 rounded-md border bg-background px-2 py-1"
              inputMode="decimal"
              placeholder="Close notional USDC"
              value={closeNotional}
              onChange={(event) => setCloseNotional(event.target.value)}
            />
            <Button
              size="sm"
              variant="destructive"
              onClick={() => onClose(allocation, closeNotional, maxLegs)}
              disabled={closeNotional.trim().length === 0}
            >
              Close + withdraw
            </Button>
          </div>
          {allocation.missing?.length ? (
            <p>Missing config: {allocation.missing.join(", ")}</p>
          ) : null}
        </div>
      ) : null}
      {allocationError ? (
        <p className="text-xs text-destructive">{allocationError}</p>
      ) : null}
    </div>
  );
}
