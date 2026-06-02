import {
  createPublicClient,
  createWalletClient,
  custom,
  http,
  type Hex,
} from "viem";
import { arbitrum } from "viem/chains";

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

type EthereumProvider = Parameters<typeof custom>[0];

const STRATEGY_VAULT_ABI = [
  {
    type: "constructor",
    inputs: [
      { name: "asset_", type: "address" },
      { name: "owner_", type: "address" },
    ],
  },
] as const;

function strategyVaultBytecode() {
  const bytecode = process.env.NEXT_PUBLIC_STRATEGY_VAULT_BYTECODE;
  if (!bytecode?.startsWith("0x")) {
    throw new Error("NEXT_PUBLIC_STRATEGY_VAULT_BYTECODE is not configured");
  }
  return bytecode as Hex;
}

export async function deployVaultOnChain(
  provider: EthereumProvider,
): Promise<VaultDeployment> {
  const walletClient = createWalletClient({
    chain: arbitrum,
    transport: custom(provider),
  });
  const publicClient = createPublicClient({ chain: arbitrum, transport: http() });
  const [account] = await walletClient.getAddresses();
  if (!account) throw new Error("Connect a wallet before deploying");

  const currentChainId = await walletClient.getChainId();
  if (currentChainId !== STRATEGY_VAULT_CHAIN_ID) {
    await walletClient.switchChain({ id: STRATEGY_VAULT_CHAIN_ID });
  }

  const hash = await walletClient.deployContract({
    abi: STRATEGY_VAULT_ABI,
    account,
    args: [STRATEGY_VAULT_ASSET, account],
    bytecode: strategyVaultBytecode(),
    chain: arbitrum,
  });
  const receipt = await publicClient.waitForTransactionReceipt({ hash });
  if (!receipt.contractAddress) {
    throw new Error("Vault deployment receipt did not include a contract address");
  }

  return {
    chainId: STRATEGY_VAULT_CHAIN_ID,
    vaultAddress: receipt.contractAddress,
    assetAddress: STRATEGY_VAULT_ASSET,
  };
}

// Persist the deployed vault and promote the run's mandate to `active`.
export async function saveVaultBinding(
  runId: string,
  deployment: VaultDeployment,
  accessToken?: string | null,
): Promise<VaultBindingResponse> {
  const response = await fetch(
    `/api/runs/${encodeURIComponent(runId)}/vault`,
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(accessToken ? { authorization: `Bearer ${accessToken}` } : {}),
      },
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
