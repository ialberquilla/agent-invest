"use client";

import { useEffect, useState } from "react";
import { usePrivy, useWallets } from "@privy-io/react-auth";
import { Check, LoaderCircle, X } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
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
  type VaultDeployProgress,
} from "@/lib/deploy-vault";
import { upsertDeployedStrategy } from "@/lib/local-store";
import { cn } from "@/lib/utils";

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

type DeployStepId =
  | "name"
  | "deploy"
  | "fund"
  | "review"
  | "execute";

type DeployModalState = {
  open: boolean;
  status:
    | "naming"
    | "deploying"
    | "ready"
    | "funding"
    | "checking"
    | "executing"
    | "complete"
    | "error";
  activeStep: DeployStepId;
  completedSteps: DeployStepId[];
  detail?: string;
  error?: string;
  txHash?: string;
  vaultAddress?: string;
};

const DEPLOY_NAME_MAX_LENGTH = 80;

const DEPLOY_STEPS: Array<{
  id: DeployStepId;
  title: string;
  description: string;
}> = [
  {
    id: "name",
    title: "Name the strategy",
    description: "This is what you will see later under Deployed strategies.",
  },
  {
    id: "deploy",
    title: "Deploy vault",
    description: "We create the vault on-chain and save it to this strategy.",
  },
  {
    id: "fund",
    title: "Fund vault",
    description: "Deposit USDC and optionally top up ETH for GMX execution fees.",
  },
  {
    id: "review",
    title: "Review allocation",
    description: "Load the target allocation and cap test execution legs if needed.",
  },
  {
    id: "execute",
    title: "Execute strategy",
    description: "Submit the GMX orders from the funded vault.",
  },
];

const INITIAL_DEPLOY_MODAL_STATE: DeployModalState = {
  open: false,
  status: "naming",
  activeStep: "name",
  completedSteps: [],
};

function normalizeDeployName(value: string) {
  return value.trim().replace(/\s+/g, " ");
}

function mergeCompleted(
  current: DeployStepId[],
  ...steps: DeployStepId[]
) {
  return Array.from(new Set([...current, ...steps]));
}

function shortenHash(value: string) {
  return `${value.slice(0, 10)}...${value.slice(-6)}`;
}

function deployErrorDetail(message: string) {
  if (message === "Run not found") {
    return "This strategy report belongs to a run the backend can no longer find. Re-run the strategy, then deploy from the fresh result.";
  }

  if (message === "No strategy mandate for this run yet") {
    return "This run does not have a finalized deployable mandate yet. Wait for the strategy result to finish, then try again.";
  }

  return `Deployment stopped before all steps completed: ${message}`;
}

export function DeployVaultButton({ runId }: { runId: string }) {
  const { authenticated, login, getAccessToken } = usePrivy();
  const { wallets } = useWallets();
  const [state, setState] = useState<DeployState>({ status: "idle" });
  const [deployName, setDeployName] = useState("");
  const [deployModal, setDeployModal] = useState<DeployModalState>(
    INITIAL_DEPLOY_MODAL_STATE,
  );
  const [showAdvancedManage, setShowAdvancedManage] = useState(false);

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
          label: binding.display_name,
        });
        window.dispatchEvent(new Event("agent-invest:deployed-strategies"));
        setDeployName((current) => current || binding.display_name);
        const balances = await readVaultBalances(binding.vault_address).catch(
          () => undefined,
        );
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

  function handleDeployClick() {
    if (!authenticated) {
      login();
      return;
    }

    setDeployModal({
      ...INITIAL_DEPLOY_MODAL_STATE,
      open: true,
      detail: "Give this on-chain strategy a name before deploying.",
    });
  }

  function closeDeployModal() {
    setDeployModal((current) =>
      ["deploying", "funding", "checking", "executing"].includes(
        current.status,
      )
        ? current
        : { ...current, open: false },
    );
  }

  function trackVaultDeployProgress(progress: VaultDeployProgress) {
    setDeployModal((current) => {
      if (progress.phase === "checking_wallet") {
        return {
          ...current,
          activeStep: "deploy",
          detail: "Checking the connected wallet account.",
        };
      }

      if (progress.phase === "switching_chain") {
        return {
          ...current,
          activeStep: "deploy",
          detail: `Switching your wallet to chain ${progress.chainId}.`,
        };
      }

      if (progress.phase === "requesting_signature") {
        return {
          ...current,
          activeStep: "deploy",
          detail: "Confirm the vault deployment in your wallet.",
        };
      }

      if (progress.phase === "transaction_submitted") {
        return {
          ...current,
          activeStep: "deploy",
          detail: "Transaction submitted. Waiting for chain confirmation.",
          txHash: progress.hash,
        };
      }

      return {
        ...current,
        activeStep: "deploy",
        detail: "Vault created. Saving it to your account.",
        vaultAddress: progress.vaultAddress,
      };
    });
  }

  async function handleStartDeploy() {
    const displayName = normalizeDeployName(deployName);
    if (!displayName) {
      setDeployModal((current) => ({
        ...current,
        open: true,
        status: "error",
        activeStep: "name",
        detail: "Add a name so this strategy is easy to find later.",
        error: "Strategy name is required.",
      }));
      return;
    }

    if (displayName.length > DEPLOY_NAME_MAX_LENGTH) {
      setDeployModal((current) => ({
        ...current,
        open: true,
        status: "error",
        activeStep: "name",
        detail: "Use a shorter name before deploying.",
        error: `Strategy name must be ${DEPLOY_NAME_MAX_LENGTH} characters or fewer.`,
      }));
      return;
    }

    const wallet = wallets[0];
    if (!wallet) {
      setDeployModal((current) => ({
        ...current,
        open: true,
        status: "error",
        activeStep: "deploy",
        completedSteps: ["name"],
        detail: "Connect a wallet before deploying this vault.",
        error: "Connect a wallet to deploy.",
      }));
      setState({ status: "error", message: "Connect a wallet to deploy" });
      return;
    }

    setState({ status: "deploying" });
    setDeployModal({
      open: true,
      status: "deploying",
      activeStep: "deploy",
      completedSteps: ["name"],
      detail: "Checking this run is ready to deploy on-chain.",
    });
    try {
      const accessToken = await getAccessToken();
      await assertVaultDeployable(runId, accessToken);
      setDeployModal((current) => ({
        ...current,
        activeStep: "deploy",
        detail: "Opening your wallet and preparing the deployment transaction.",
      }));
      const provider = await wallet.getEthereumProvider();
      const deployment = await deployVaultOnChain(provider, {
        onProgress: trackVaultDeployProgress,
      });
      // 2. Persist the vault<->mandate binding + activate the mandate.
      const binding = await saveVaultBinding(
        runId,
        deployment,
        accessToken,
        displayName,
      );
      upsertDeployedStrategy({
        mandate_id: binding.mandate_id,
        chain_id: binding.chain_id,
        vault_address: binding.vault_address,
        asset_address: binding.asset_address,
        status: binding.status,
        label: binding.display_name,
        run_id: runId,
      });
      window.dispatchEvent(new Event("agent-invest:deployed-strategies"));
      setDeployModal((current) => ({
        ...current,
        status: "ready",
        activeStep: "fund",
        completedSteps: mergeCompleted(current.completedSteps, "name", "deploy"),
        detail: "Vault deployed. Deposit USDC next, and add ETH for the GMX gas tank if you plan to execute now.",
        vaultAddress: binding.vault_address,
      }));
      setState({ status: "deployed", binding });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Deploy failed";
      setDeployModal((current) => ({
        ...current,
        open: true,
        status: "error",
        detail: deployErrorDetail(message),
        error: message,
      }));
      setState({
        status: "error",
        message,
      });
    }
  }

  async function handleFund(
    binding: VaultBindingResponse,
    amount: string,
    ethGas?: string,
  ) {
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
        ethGas,
      );
      const gasBalance = await readVaultNativeBalance(
        provider,
        binding.vault_address,
      ).catch(() => undefined);
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

  async function handleLaunchFund(
    binding: VaultBindingResponse,
    amount: string,
    ethGas?: string,
  ) {
    const wallet = wallets[0];
    if (!wallet) {
      setDeployModal((current) => ({
        ...current,
        status: "error",
        activeStep: "fund",
        detail: "Connect a wallet before funding this vault.",
        error: "Connect a wallet to fund.",
      }));
      return;
    }

    setState({ status: "funding", binding, amount });
    setDeployModal((current) => ({
      ...current,
      status: "funding",
      activeStep: "fund",
      detail: "Approve the USDC deposit, then wait for the funding transaction to confirm.",
      error: undefined,
    }));

    try {
      const provider = await wallet.getEthereumProvider();
      const idleBalance = await fundVaultOnChain(
        provider,
        binding.vault_address,
        amount,
        ethGas,
      );
      const gasBalance = await readVaultNativeBalance(
        provider,
        binding.vault_address,
      ).catch(() => undefined);
      setState({ status: "deployed", binding, idleBalance, gasBalance });
      setDeployModal((current) => ({
        ...current,
        status: "ready",
        activeStep: "review",
        completedSteps: mergeCompleted(current.completedSteps, "fund"),
        detail: `Vault funded with ${idleBalance} USDC idle collateral. Review the allocation next.`,
      }));
    } catch (error) {
      const message = error instanceof Error ? error.message : "Funding failed";
      setState({ status: "deployed", binding, fundError: message });
      setDeployModal((current) => ({
        ...current,
        status: "error",
        activeStep: "fund",
        detail: "Funding did not complete. You can retry with the same or a different amount.",
        error: message,
      }));
    }
  }

  async function handleLaunchAllocate(
    binding: VaultBindingResponse,
    idleBalance?: string,
  ) {
    setDeployModal((current) => ({
      ...current,
      status: "checking",
      activeStep: "review",
      detail: "Loading the live allocation plan for this vault.",
      error: undefined,
    }));

    try {
      const allocation = await readAllocationReadiness(runId);
      setState({ status: "deployed", binding, idleBalance, allocation });
      setDeployModal((current) => ({
        ...current,
        status: allocation.executable ? "ready" : "error",
        activeStep: allocation.executable ? "execute" : "review",
        completedSteps: allocation.executable
          ? mergeCompleted(current.completedSteps, "review")
          : current.completedSteps,
        detail:
          allocation.reason ??
          (allocation.executable
            ? "Allocation is ready to execute."
            : "Allocation is not executable yet."),
        error: allocation.executable
          ? undefined
          : allocation.missing?.join(", ") || allocation.reason,
      }));
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Allocation check failed";
      setState({ status: "deployed", binding, idleBalance, allocationError: message });
      setDeployModal((current) => ({
        ...current,
        status: "error",
        activeStep: "review",
        detail: "Allocation check failed. Retry after the backend is healthy.",
        error: message,
      }));
    }
  }

  async function handleLaunchExecute(
    allocation: VaultAllocationReadiness,
    idleBalance: string,
    maxLegs?: number,
  ) {
    const wallet = wallets[0];
    if (!wallet) {
      setDeployModal((current) => ({
        ...current,
        status: "error",
        activeStep: "execute",
        detail: "Connect a wallet before executing this strategy.",
        error: "Connect a wallet to execute.",
      }));
      return;
    }

    if (!allocation.executable) {
      setDeployModal((current) => ({
        ...current,
        status: "error",
        activeStep: "execute",
        detail: "The allocation is not executable yet.",
        error: `Missing: ${allocation.missing?.join(", ") || allocation.reason || "check allocation again"}`,
      }));
      return;
    }

    setDeployModal((current) => ({
      ...current,
      status: "executing",
      activeStep: "execute",
      detail: "Submitting the GMX orders from the vault. Keep your wallet open for confirmations.",
      error: undefined,
    }));

    try {
      const provider = await wallet.getEthereumProvider();
      const gas = await readVaultNativeBalance(provider, allocation.vault_address);
      if (Number(gas) <= 0) {
        throw new Error(
          "Fund the gas tank first. GMX execution fees are paid from the vault's ETH.",
        );
      }

      await executeVaultAllocationOnChain(provider, allocation, idleBalance, {
        payFromTank: true,
        maxLegs,
      });
      setState((current) =>
        current.status === "deployed"
          ? {
              ...current,
              allocation: {
                ...allocation,
                reason: "GMX increase orders submitted.",
              },
              allocationError: undefined,
            }
          : current,
      );
      setDeployModal((current) => ({
        ...current,
        status: "complete",
        activeStep: "execute",
        completedSteps: DEPLOY_STEPS.map((step) => step.id),
        detail: "Strategy orders submitted. The vault is now launched.",
      }));
    } catch (error) {
      const message = error instanceof Error ? error.message : "Execution failed";
      setState((current) =>
        current.status === "deployed"
          ? { ...current, allocationError: message }
          : current,
      );
      setDeployModal((current) => ({
        ...current,
        status: "error",
        activeStep: "execute",
        detail: "Execution did not complete. You can adjust max legs or retry.",
        error: message,
      }));
    }
  }

  async function handleFundGas(
    binding: VaultBindingResponse,
    ethAmount: string,
  ) {
    const wallet = wallets[0];
    if (!wallet) {
      setState({ status: "error", message: "Connect a wallet to fund gas" });
      return;
    }
    try {
      const provider = await wallet.getEthereumProvider();
      const gasBalance = await fundVaultGasOnChain(
        provider,
        binding.vault_address,
        ethAmount,
      );
      setState((current) =>
        current.status === "deployed"
          ? { ...current, gasBalance, gasError: undefined }
          : current,
      );
    } catch (error) {
      setState((current) =>
        current.status === "deployed"
          ? {
              ...current,
              gasError:
                error instanceof Error ? error.message : "Gas funding failed",
            }
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
      const gasBalance = await withdrawVaultNativeOnChain(
        provider,
        binding.vault_address,
      );
      setState((current) =>
        current.status === "deployed"
          ? { ...current, gasBalance, gasError: undefined }
          : current,
      );
    } catch (error) {
      setState((current) =>
        current.status === "deployed"
          ? {
              ...current,
              gasError:
                error instanceof Error ? error.message : "Gas sweep failed",
            }
          : current,
      );
    }
  }

  async function handleAllocate(
    binding: VaultBindingResponse,
    idleBalance?: string,
  ) {
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
    if (!allocation.executable) {
      setState((current) =>
        current.status === "deployed"
          ? {
              ...current,
              allocationError: `Allocation is not executable: ${
                allocation.missing?.join(", ") ||
                allocation.reason ||
                "check allocation again"
              }`,
            }
          : current,
      );
      return;
    }
    try {
      const provider = await wallet.getEthereumProvider();
      const gas = await readVaultNativeBalance(
        provider,
        allocation.vault_address,
      );
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
          ? {
              ...current,
              allocation: {
                ...allocation,
                reason: "GMX increase orders submitted.",
              },
            }
          : current,
      );
    } catch (error) {
      setState((current) =>
        current.status === "deployed"
          ? {
              ...current,
              allocationError:
                error instanceof Error ? error.message : "Execution failed",
            }
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
    if (!allocation.executable) {
      setState((current) =>
        current.status === "deployed"
          ? {
              ...current,
              allocationError: `Allocation is not executable: ${
                allocation.missing?.join(", ") ||
                allocation.reason ||
                "check allocation again"
              }`,
            }
          : current,
      );
      return;
    }
    try {
      const provider = await wallet.getEthereumProvider();
      await closeVaultPositionsAndWithdrawIdleOnChain(
        provider,
        allocation,
        notionalUsd,
        {
          payFromTank: true,
          maxLegs,
        },
      );
      setState((current) =>
        current.status === "deployed"
          ? {
              ...current,
              allocation: {
                ...allocation,
                reason: "Close orders submitted and idle collateral withdrawn.",
              },
            }
          : current,
      );
    } catch (error) {
      setState((current) =>
        current.status === "deployed"
          ? {
              ...current,
              allocationError:
                error instanceof Error ? error.message : "Close failed",
            }
          : current,
      );
    }
  }

  function openLaunchFlow(binding: VaultBindingResponse) {
    const current = state.status === "deployed" ? state : null;
    const hasFunding = Boolean(current?.idleBalance);
    const hasAllocation = Boolean(current?.allocation);
    setDeployName((name) => name || binding.display_name);
    setDeployModal({
      open: true,
      status: "ready",
      activeStep: hasAllocation ? "execute" : hasFunding ? "review" : "fund",
      completedSteps: [
        "name",
        "deploy",
        ...(hasFunding ? (["fund"] as DeployStepId[]) : []),
        ...(hasAllocation ? (["review"] as DeployStepId[]) : []),
      ],
      detail: hasAllocation
        ? "Allocation is loaded. Execute when you are ready."
        : hasFunding
          ? "Vault is funded. Review the allocation next."
          : "Vault is deployed. Fund it to continue the launch.",
      vaultAddress: binding.vault_address,
    });
  }

  if (state.status === "deployed" || state.status === "funding") {
    const binding = state.binding;
    const isFunding = state.status === "funding";
    return (
      <>
        <VaultLaunchSummary
          binding={binding}
          idleBalance={state.status === "deployed" ? state.idleBalance : undefined}
          gasBalance={state.status === "deployed" ? state.gasBalance : undefined}
          allocation={state.status === "deployed" ? state.allocation : undefined}
          onContinue={() => openLaunchFlow(binding)}
          onToggleAdvanced={() => setShowAdvancedManage((current) => !current)}
          advancedOpen={showAdvancedManage}
        />
        {showAdvancedManage ? (
          <FundVaultPanel
          binding={binding}
          idleBalance={
            state.status === "deployed" ? state.idleBalance : undefined
          }
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
        ) : null}
        <DeployFlowModal
          state={deployModal}
          name={deployName}
          binding={binding}
          idleBalance={state.status === "deployed" ? state.idleBalance : undefined}
          fundError={state.status === "deployed" ? state.fundError : undefined}
          gasBalance={state.status === "deployed" ? state.gasBalance : undefined}
          allocation={state.status === "deployed" ? state.allocation : undefined}
          allocationError={
            state.status === "deployed" ? state.allocationError : undefined
          }
          canStart={normalizeDeployName(deployName).length > 0}
          onNameChange={setDeployName}
          onStart={handleStartDeploy}
          onFund={handleLaunchFund}
          onAllocate={handleLaunchAllocate}
          onExecute={handleLaunchExecute}
          onClose={closeDeployModal}
        />
      </>
    );
  }

  return (
    <>
      <div className="flex flex-col gap-2">
        <Button
          onClick={handleDeployClick}
          disabled={state.status === "deploying"}
        >
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
      <DeployFlowModal
        state={deployModal}
        name={deployName}
        binding={undefined}
        idleBalance={undefined}
        fundError={undefined}
        gasBalance={undefined}
        allocation={undefined}
        allocationError={undefined}
        canStart={normalizeDeployName(deployName).length > 0}
        onNameChange={setDeployName}
        onStart={handleStartDeploy}
        onFund={handleLaunchFund}
        onAllocate={handleLaunchAllocate}
        onExecute={handleLaunchExecute}
        onClose={closeDeployModal}
      />
    </>
  );
}

function VaultLaunchSummary({
  binding,
  idleBalance,
  gasBalance,
  allocation,
  onContinue,
  onToggleAdvanced,
  advancedOpen,
}: {
  binding: VaultBindingResponse;
  idleBalance?: string;
  gasBalance?: string;
  allocation?: VaultAllocationReadiness;
  onContinue: () => void;
  onToggleAdvanced: () => void;
  advancedOpen: boolean;
}) {
  return (
    <div className="min-w-72 space-y-3 rounded-xl border bg-background p-3 text-sm">
      <div>
        <p className="font-medium">{binding.display_name}</p>
        <p className="mt-1 truncate font-mono text-xs text-muted-foreground">
          {binding.vault_address}
        </p>
      </div>
      <div className="grid gap-2 text-xs text-muted-foreground sm:grid-cols-3">
        <span className="rounded-lg bg-muted/60 px-2 py-1.5">
          USDC: {idleBalance ?? "not funded"}
        </span>
        <span className="rounded-lg bg-muted/60 px-2 py-1.5">
          Gas: {gasBalance ?? "empty"}
        </span>
        <span className="rounded-lg bg-muted/60 px-2 py-1.5">
          {allocation ? "allocation loaded" : "not executed"}
        </span>
      </div>
      <div className="flex flex-wrap gap-2">
        <Button onClick={onContinue}>
          {allocation ? "Continue execution" : idleBalance ? "Review allocation" : "Continue launch"}
        </Button>
        <Button variant="secondary" onClick={onToggleAdvanced}>
          {advancedOpen ? "Hide advanced" : "Advanced controls"}
        </Button>
      </div>
    </div>
  );
}

function DeployFlowModal({
  state,
  name,
  binding,
  idleBalance,
  fundError,
  gasBalance,
  allocation,
  allocationError,
  canStart,
  onNameChange,
  onStart,
  onFund,
  onAllocate,
  onExecute,
  onClose,
}: {
  state: DeployModalState;
  name: string;
  binding?: VaultBindingResponse;
  idleBalance?: string;
  fundError?: string;
  gasBalance?: string;
  allocation?: VaultAllocationReadiness;
  allocationError?: string;
  canStart: boolean;
  onNameChange: (value: string) => void;
  onStart: () => void;
  onFund: (
    binding: VaultBindingResponse,
    amount: string,
    ethGas?: string,
  ) => void;
  onAllocate: (binding: VaultBindingResponse, idleBalance?: string) => void;
  onExecute: (
    allocation: VaultAllocationReadiness,
    idleBalance: string,
    maxLegs?: number,
  ) => void;
  onClose: () => void;
}) {
  const [fundAmount, setFundAmount] = useState("");
  const [fundGasAmount, setFundGasAmount] = useState("");
  const [maxLegsInput, setMaxLegsInput] = useState("");
  if (!state.open) return null;

  const completedCount =
    state.status === "complete"
      ? DEPLOY_STEPS.length
      : state.completedSteps.length;
  const remaining = Math.max(DEPLOY_STEPS.length - completedCount, 0);
  const activeIndex = Math.max(
    DEPLOY_STEPS.findIndex((step) => step.id === state.activeStep),
    0,
  );
  const progress = Math.round((completedCount / DEPLOY_STEPS.length) * 100);
  const isWorking = ["deploying", "funding", "checking", "executing"].includes(
    state.status,
  );
  const canEditName = !binding && !isWorking && state.status !== "complete";
  const maxLegs =
    maxLegsInput.trim().length > 0 ? Number(maxLegsInput) : undefined;
  const canFund = Boolean(binding) && fundAmount.trim().length > 0;
  const canReview = Boolean(binding && idleBalance);
  const canExecute = Boolean(allocation?.executable && idleBalance);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-background/70 p-4 backdrop-blur-sm">
      <section
        role="dialog"
        aria-modal="true"
        aria-labelledby="deploy-vault-title"
        className="w-full max-w-xl overflow-hidden rounded-3xl border bg-card text-card-foreground shadow-2xl"
      >
        <div className="relative border-b bg-gradient-to-br from-muted/70 via-card to-card p-5 sm:p-6">
          <button
            type="button"
            onClick={onClose}
            disabled={state.status === "deploying"}
            aria-label="Close deploy dialog"
            className="absolute right-4 top-4 rounded-full p-1.5 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground disabled:pointer-events-none disabled:opacity-40"
          >
            <X className="size-4" />
          </button>
          <div className="space-y-3 pr-8">
            <div className="flex flex-wrap items-center gap-2 text-xs font-medium uppercase tracking-[0.18em] text-muted-foreground">
              <span>Step {Math.min(activeIndex + 1, DEPLOY_STEPS.length)} of {DEPLOY_STEPS.length}</span>
              <span className="h-1 w-1 rounded-full bg-muted-foreground/50" />
              <span>{remaining} left</span>
            </div>
            <div>
              <h2 id="deploy-vault-title" className="font-heading text-2xl font-semibold tracking-tight">
                Launch strategy on-chain
              </h2>
              <p className="mt-1 text-sm text-muted-foreground">
                One flow for deploy, deposit, allocation review, caps, and execution.
              </p>
            </div>
            <div className="h-2 overflow-hidden rounded-full bg-muted">
              <div
                className={cn(
                  "h-full rounded-full transition-all duration-500",
                  state.status === "error"
                    ? "bg-destructive"
                    : "bg-primary",
                )}
                style={{ width: `${progress}%` }}
              />
            </div>
          </div>
        </div>

        <div className="space-y-5 p-5 sm:p-6">
          <div className="rounded-2xl border bg-background p-4">
            <label htmlFor="deploy-strategy-name" className="text-sm font-medium">
              Strategy name
            </label>
            <Input
              id="deploy-strategy-name"
              className="mt-2 h-10"
              placeholder="Example: BTC momentum vault"
              value={name}
              onChange={(event) => onNameChange(event.target.value)}
              maxLength={DEPLOY_NAME_MAX_LENGTH}
              disabled={!canEditName}
            />
            <div className="mt-2 flex items-center justify-between gap-3 text-xs text-muted-foreground">
              <span>This label is saved with the vault and shown in the sidebar.</span>
              <span>{normalizeDeployName(name).length}/{DEPLOY_NAME_MAX_LENGTH}</span>
            </div>
          </div>

          <ol className="space-y-2">
            {DEPLOY_STEPS.map((step, index) => {
              const isComplete =
                state.status === "complete" || state.completedSteps.includes(step.id);
              const isActive = !isComplete && state.activeStep === step.id;
              const isError = state.status === "error" && state.activeStep === step.id;

              return (
                <li
                  key={step.id}
                  className={cn(
                    "flex gap-3 rounded-2xl border p-3 transition-colors",
                    isComplete
                      ? "border-primary/20 bg-primary/5"
                      : isActive
                        ? "border-ring/50 bg-muted/60"
                        : "bg-background",
                    isError && "border-destructive/40 bg-destructive/5",
                  )}
                >
                  <span
                    className={cn(
                      "mt-0.5 flex size-7 shrink-0 items-center justify-center rounded-full border text-xs font-semibold",
                      isComplete
                        ? "border-primary bg-primary text-primary-foreground"
                        : isError
                          ? "border-destructive text-destructive"
                          : isActive
                            ? "border-ring text-foreground"
                            : "border-border text-muted-foreground",
                    )}
                  >
                    {isComplete ? (
                      <Check className="size-3.5" />
                    ) : isActive && isWorking ? (
                      <LoaderCircle className="size-3.5 animate-spin" />
                    ) : isError ? (
                      <X className="size-3.5" />
                    ) : (
                      index + 1
                    )}
                  </span>
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center justify-between gap-3">
                      <p className="text-sm font-medium">{step.title}</p>
                      <span className="text-[11px] uppercase tracking-wide text-muted-foreground">
                        {isComplete
                          ? "Done"
                          : isError
                            ? "Needs attention"
                            : isActive
                              ? "Now"
                              : "Waiting"}
                      </span>
                    </div>
                    <p className="mt-0.5 text-xs text-muted-foreground">
                      {step.description}
                    </p>
                  </div>
                </li>
              );
            })}
          </ol>

          {binding ? (
            <div className="space-y-4 rounded-2xl border bg-background p-4">
              <div>
                <p className="text-sm font-medium">Vault</p>
                <p className="mt-1 break-all font-mono text-xs text-muted-foreground">
                  {binding.vault_address}
                </p>
              </div>

              {state.activeStep === "fund" ? (
                <div className="space-y-3">
                  <div className="grid gap-2 sm:grid-cols-[1fr_8rem]">
                    <Input
                      inputMode="decimal"
                      placeholder="USDC amount"
                      value={fundAmount}
                      onChange={(event) => setFundAmount(event.target.value)}
                      disabled={state.status === "funding"}
                    />
                    <Input
                      inputMode="decimal"
                      placeholder="+ETH gas"
                      value={fundGasAmount}
                      onChange={(event) => setFundGasAmount(event.target.value)}
                      disabled={state.status === "funding"}
                    />
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Deposit USDC collateral. Add ETH here to fund the vault gas tank in the same deposit transaction.
                  </p>
                  {idleBalance ? (
                    <p className="text-xs text-muted-foreground">
                      Idle collateral: {idleBalance} USDC
                    </p>
                  ) : null}
                  {gasBalance ? (
                    <p className="text-xs text-muted-foreground">
                      Gas tank: {gasBalance} ETH
                    </p>
                  ) : null}
                  {fundError ? <p className="text-xs text-destructive">{fundError}</p> : null}
                  <Button
                    onClick={() => binding ? onFund(binding, fundAmount, fundGasAmount) : undefined}
                    disabled={!canFund || state.status === "funding"}
                  >
                    {state.status === "funding" ? "Funding..." : "Fund and continue"}
                  </Button>
                </div>
              ) : null}

              {state.activeStep === "review" || state.activeStep === "execute" ? (
                <div className="space-y-3 border-t pt-4">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <p className="text-sm font-medium">Allocation</p>
                      <p className="text-xs text-muted-foreground">
                        {allocation?.reason ?? "Load the allocation before execution."}
                      </p>
                    </div>
                    <Button
                      variant="secondary"
                      onClick={() => binding ? onAllocate(binding, idleBalance) : undefined}
                      disabled={!canReview || state.status === "checking"}
                    >
                      {state.status === "checking" ? "Checking..." : allocation ? "Refresh" : "Check allocation"}
                    </Button>
                  </div>

                  {allocation?.target_allocation.length ? (
                    <p className="text-xs text-muted-foreground">
                      Target: {allocation.target_allocation
                        .map(
                          (item) =>
                            `${item.coin_id ?? "asset"} ${Math.round((item.weight ?? 0) * 100)}%`,
                        )
                        .join(", ")}
                    </p>
                  ) : null}
                  {allocation?.missing?.length ? (
                    <p className="text-xs text-destructive">
                      Missing config: {allocation.missing.join(", ")}
                    </p>
                  ) : null}
                  {allocationError ? (
                    <p className="text-xs text-destructive">{allocationError}</p>
                  ) : null}

                  {allocation ? (
                    <div className="space-y-3 border-t pt-4">
                      <div className="grid gap-2 sm:grid-cols-[8rem_1fr]">
                        <Input
                          inputMode="numeric"
                          placeholder="Max legs"
                          value={maxLegsInput}
                          onChange={(event) => setMaxLegsInput(event.target.value)}
                        />
                        <p className="text-xs text-muted-foreground">
                          Optional test cap. Caps and renormalizes legs so each order clears GMX&apos;s $1 minimum. Blank executes the full allocation.
                        </p>
                      </div>
                      <Button
                        onClick={() =>
                          allocation && idleBalance
                            ? onExecute(allocation, idleBalance, maxLegs)
                            : undefined
                        }
                        disabled={!canExecute || state.status === "executing"}
                      >
                        {state.status === "executing" ? "Executing..." : "Execute strategy"}
                      </Button>
                    </div>
                  ) : null}
                </div>
              ) : null}
            </div>
          ) : null}

          {state.detail || state.txHash || state.vaultAddress || state.error ? (
            <div className="rounded-2xl border bg-muted/40 p-4 text-sm">
              {state.detail ? <p>{state.detail}</p> : null}
              {state.txHash ? (
                <p className="mt-2 text-xs text-muted-foreground">
                  Transaction: <span className="font-mono">{shortenHash(state.txHash)}</span>
                </p>
              ) : null}
              {state.vaultAddress ? (
                <p className="mt-2 text-xs text-muted-foreground">
                  Vault: <span className="font-mono">{state.vaultAddress}</span>
                </p>
              ) : null}
              {state.error ? (
                <p className="mt-2 text-sm text-destructive">{state.error}</p>
              ) : null}
            </div>
          ) : null}

          <div className="flex flex-col-reverse gap-2 sm:flex-row sm:justify-end">
            {!isWorking ? (
              <Button type="button" variant="secondary" onClick={onClose}>
                {state.status === "complete" ? "Close" : "Cancel"}
              </Button>
            ) : null}
            {!binding ? (
              <Button
                type="button"
                onClick={onStart}
                disabled={isWorking || !canStart}
              >
                {isWorking ? (
                  <>
                    <LoaderCircle className="size-4 animate-spin" />
                    Working...
                  </>
                ) : state.status === "error" ? (
                  "Try again"
                ) : (
                  "Start deploy"
                )}
              </Button>
            ) : null}
          </div>
        </div>
      </section>
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
  onFund: (
    binding: VaultBindingResponse,
    amount: string,
    ethGas?: string,
  ) => void;
  onFundGas: (binding: VaultBindingResponse, ethAmount: string) => void;
  onSweepGas: (binding: VaultBindingResponse) => void;
  onAllocate: (binding: VaultBindingResponse, idleBalance?: string) => void;
  onExecute: (
    allocation: VaultAllocationReadiness,
    idleBalance: string,
    maxLegs?: number,
  ) => void;
  onClose: (
    allocation: VaultAllocationReadiness,
    notionalUsd: string,
    maxLegs?: number,
  ) => void;
}) {
  const [amount, setAmount] = useState("");
  const [fundGasAmount, setFundGasAmount] = useState("");
  const [gasAmount, setGasAmount] = useState("");
  const [closeNotional, setCloseNotional] = useState("");
  const [maxLegsInput, setMaxLegsInput] = useState("");
  const maxLegs =
    maxLegsInput.trim().length > 0 ? Number(maxLegsInput) : undefined;
  const canExecuteAllocation =
    Boolean(idleBalance) && allocation?.executable === true;
  const canCloseAllocation = allocation?.executable === true;

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
      {fundError ? (
        <p className="text-xs text-destructive">{fundError}</p>
      ) : null}
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
          {gasBalance
            ? `Gas tank: ${gasBalance} ETH`
            : "Pre-fund ETH to pay GMX execution fees."}
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
              Target:{" "}
              {allocation.target_allocation
                .map(
                  (item) =>
                    `${item.coin_id ?? "asset"} ${Math.round((item.weight ?? 0) * 100)}%`,
                )
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
              Caps + renormalizes legs so each clears GMX&apos;s $1 min. Blank =
              full allocation.
            </span>
          </div>
          <div className="flex flex-wrap gap-2 pt-1">
            <Button
              size="sm"
              onClick={() =>
                idleBalance
                  ? onExecute(allocation, idleBalance, maxLegs)
                  : undefined
              }
              disabled={!canExecuteAllocation}
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
              disabled={
                closeNotional.trim().length === 0 || !canCloseAllocation
              }
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
