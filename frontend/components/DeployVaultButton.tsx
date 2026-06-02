"use client";

import { useState } from "react";
import { usePrivy, useWallets } from "@privy-io/react-auth";

import { Button } from "@/components/ui/button";
import {
  deployVaultOnChain,
  saveVaultBinding,
  type VaultBindingResponse,
} from "@/lib/deploy-vault";

type DeployState =
  | { status: "idle" }
  | { status: "deploying" }
  | { status: "deployed"; binding: VaultBindingResponse }
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
      const provider = await wallet.getEthereumProvider();
      const deployment = await deployVaultOnChain(provider);
      // 2. Persist the vault<->mandate binding + activate the mandate.
      const binding = await saveVaultBinding(
        runId,
        deployment,
        await getAccessToken(),
      );
      setState({ status: "deployed", binding });
    } catch (error) {
      setState({
        status: "error",
        message: error instanceof Error ? error.message : "Deploy failed",
      });
    }
  }

  if (state.status === "deployed") {
    return (
      <p className="text-sm text-muted-foreground">
        Deployed at{" "}
        <span className="font-mono">{state.binding.vault_address}</span> ·
        strategy active
      </p>
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
