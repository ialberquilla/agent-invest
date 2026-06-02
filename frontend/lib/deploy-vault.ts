// Client-side vault deployment seam.
//
// The on-chain broadcast (deploy StrategyVault from a connected wallet) is wired
// later via Privy — that step is the ONLY piece left. Everything downstream of
// it (persisting the vault<->mandate binding) is implemented in `saveVaultBinding`
// and exercised end-to-end as soon as `deployVaultOnChain` returns a real address.

// Arbitrum One + native USDC. Move to env/config when more chains are supported.
export const STRATEGY_VAULT_CHAIN_ID = 42161;
export const STRATEGY_VAULT_ASSET = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831";

export type VaultDeployment = {
  chainId: number;
  vaultAddress: string;
  assetAddress: string;
};

export type VaultBindingResponse = {
  mandate_id: string;
  chain_id: number;
  vault_address: string;
  asset_address: string;
  status: string;
};

// TODO(privy): connect wallet, broadcast `DeployStrategyVault.s.sol`
// (StrategyVault(asset, owner, ...)), wait for the receipt, and return the
// deployed address. Until then this throws so the button surfaces a clear state.
export async function deployVaultOnChain(): Promise<VaultDeployment> {
  throw new Error("On-chain deploy is not wired yet (Privy wallet coming soon)");
}

// Persist the deployed vault and promote the run's mandate to `active`.
export async function saveVaultBinding(
  runId: string,
  deployment: VaultDeployment,
): Promise<VaultBindingResponse> {
  const response = await fetch(
    `/api/runs/${encodeURIComponent(runId)}/vault`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        chain_id: deployment.chainId,
        vault_address: deployment.vaultAddress,
        asset_address: deployment.assetAddress,
      }),
    },
  );

  const payload = (await response.json().catch(() => null)) as
    | (VaultBindingResponse & { message?: string })
    | null;

  if (!response.ok) {
    throw new Error(payload?.message ?? "Failed to save vault binding");
  }

  return payload as VaultBindingResponse;
}
