"use client";

import { useState } from "react";
import { usePrivy, useWallets } from "@privy-io/react-auth";

import { Button } from "@/components/ui/button";
import {
  assertVaultDeployable,
  closeVaultPositionsAndWithdrawIdleOnChain,
  deployVaultOnChain,
  executeVaultAllocationOnChain,
  fundVaultOnChain,
  readAllocationReadiness,
  saveVaultBinding,
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
      allocation?: VaultAllocationReadiness;
      allocationError?: string;
    }
  | { status: "funding"; binding: VaultBindingResponse; amount: string }
  | { status: "error"; message: string };

export function DeployVaultButton({ runId }: { runId: string }) {
  const { authenticated, login, getAccessToken } = usePrivy();
  const { wallets } = useWallets();
  const [state, setState] = useState<DeployState>({ status: "idle" });

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

  async function handleFund(binding: VaultBindingResponse, amount: string) {
    const wallet = wallets[0];
    if (!wallet) {
      setState({ status: "error", message: "Connect a wallet to fund" });
      return;
    }

    setState({ status: "funding", binding, amount });
    try {
      const provider = await wallet.getEthereumProvider();
      const idleBalance = await fundVaultOnChain(
        provider,
        binding.vault_address,
        amount,
      );
      setState({ status: "deployed", binding, idleBalance });
    } catch (error) {
      setState({
        status: "deployed",
        binding,
        idleBalance: undefined,
        fundError: error instanceof Error ? error.message : "Funding failed",
      });
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

  async function handleExecute(allocation: VaultAllocationReadiness, idleBalance: string) {
    const wallet = wallets[0];
    if (!wallet) {
      setState({ status: "error", message: "Connect a wallet to execute" });
      return;
    }
    try {
      const provider = await wallet.getEthereumProvider();
      await executeVaultAllocationOnChain(provider, allocation, idleBalance);
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

  async function handleClose(allocation: VaultAllocationReadiness, notionalUsd: string) {
    const wallet = wallets[0];
    if (!wallet) {
      setState({ status: "error", message: "Connect a wallet to close" });
      return;
    }
    try {
      const provider = await wallet.getEthereumProvider();
      await closeVaultPositionsAndWithdrawIdleOnChain(provider, allocation, notionalUsd);
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
        allocation={state.status === "deployed" ? state.allocation : undefined}
        allocationError={
          state.status === "deployed" ? state.allocationError : undefined
        }
        funding={isFunding}
        onFund={handleFund}
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
  allocation,
  allocationError,
  funding,
  onFund,
  onAllocate,
  onExecute,
  onClose,
}: {
  binding: VaultBindingResponse;
  idleBalance?: string;
  fundError?: string;
  allocation?: VaultAllocationReadiness;
  allocationError?: string;
  funding: boolean;
  onFund: (binding: VaultBindingResponse, amount: string) => void;
  onAllocate: (binding: VaultBindingResponse, idleBalance?: string) => void;
  onExecute: (allocation: VaultAllocationReadiness, idleBalance: string) => void;
  onClose: (allocation: VaultAllocationReadiness, notionalUsd: string) => void;
}) {
  const [amount, setAmount] = useState("");
  const [closeNotional, setCloseNotional] = useState("");

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
        <Button
          onClick={() => onFund(binding, amount)}
          disabled={funding || amount.trim().length === 0}
        >
          {funding ? "Funding…" : "Fund vault"}
        </Button>
      </div>
      <p className="text-xs text-muted-foreground">
        {idleBalance
          ? `Idle collateral: ${idleBalance} USDC`
          : "Approve USDC, then deposit collateral into the vault."}
      </p>
      {fundError ? <p className="text-xs text-destructive">{fundError}</p> : null}
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
          <div className="flex flex-wrap gap-2 pt-2">
            <Button
              size="sm"
              onClick={() => idleBalance ? onExecute(allocation, idleBalance) : undefined}
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
              onClick={() => onClose(allocation, closeNotional)}
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
