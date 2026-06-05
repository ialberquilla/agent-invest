import {
  createPublicClient,
  createWalletClient,
  custom,
  erc20Abi,
  formatUnits,
  http,
  parseEventLogs,
  parseUnits,
  type Hex,
} from "viem";
import { arbitrum, arbitrumSepolia } from "viem/chains";

import strategyVaultArtifact from "../../contracts/out/StrategyVault.sol/StrategyVault.json";
import vaultFactoryArtifact from "../../contracts/out/VaultFactory.sol/VaultFactory.json";

const SUPPORTED_CHAINS = [arbitrum, arbitrumSepolia] as const;

// Defaults to Arbitrum Sepolia + Circle testnet USDC so the frontend can exercise
// an end-to-end deployment without risking mainnet funds.
export const STRATEGY_VAULT_CHAIN_ID = Number(
  process.env.NEXT_PUBLIC_STRATEGY_VAULT_CHAIN_ID ?? arbitrumSepolia.id,
);
export const STRATEGY_VAULT_ASSET =
  process.env.NEXT_PUBLIC_STRATEGY_VAULT_ASSET ??
  "0x75faf114eafb1BDbe2F0316DF893fd58CE46AA4d";
export const STRATEGY_VAULT_FACTORY_ADDRESS =
  process.env.NEXT_PUBLIC_STRATEGY_VAULT_FACTORY_ADDRESS ??
  (STRATEGY_VAULT_CHAIN_ID === arbitrumSepolia.id
    ? "0xDc5c257107C1C3533C7a097a94DfeF19dcC9e599"
    : undefined);

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

export type VaultDeployabilityResponse =
  | {
      deployable: true;
      mandate_id: string;
      status: string;
    }
  | ({
      deployable: false;
      reason: string;
    } & Partial<VaultBindingResponse>);

export type VaultAllocationReadiness = {
  executable: boolean;
  reason?: string;
  mandate_id: string;
  chain_id: number;
  vault_address: string;
  asset_address: string;
  template_id: string | null;
  allowed_sides: string | null;
  target_allocation: Array<{
    coin_id?: string;
    weight?: number;
    gmx_market?: null | {
      market_token: string;
      collateral_token: string;
      collateral_decimals: number;
    };
  }>;
  missing?: string[];
};

type EthereumProvider = Parameters<typeof custom>[0];
type VaultCreatedLog = { args: { vault: `0x${string}` } };

const ASSET_DECIMALS = 6;

const STRATEGY_VAULT_BYTECODE = strategyVaultArtifact.bytecode.object as Hex;
const STRATEGY_VAULT_ABI = [] as const;
const VAULT_FACTORY_ABI = [
  {
    type: "constructor",
    inputs: [
      { name: "implementation", type: "address" },
      { name: "beaconOwner", type: "address" },
    ],
  },
  {
    type: "function",
    name: "createVault",
    inputs: [
      { name: "asset", type: "address" },
      { name: "owner", type: "address" },
    ],
    outputs: [{ name: "vault", type: "address" }],
    stateMutability: "nonpayable",
  },
  {
    type: "event",
    name: "VaultCreated",
    inputs: [
      { name: "vault", type: "address", indexed: true },
      { name: "owner", type: "address", indexed: true },
      { name: "asset", type: "address", indexed: true },
    ],
  },
] as const;
const STRATEGY_VAULT_USER_ABI = [
  {
    type: "function",
    name: "deposit",
    inputs: [{ name: "amount", type: "uint256" }],
    outputs: [],
    stateMutability: "nonpayable",
  },
  {
    type: "function",
    name: "idleBalance",
    inputs: [],
    outputs: [{ name: "", type: "uint256" }],
    stateMutability: "view",
  },
  {
    type: "function",
    name: "setGmxRouting",
    inputs: [
      { name: "exchangeRouter", type: "address" },
      { name: "router", type: "address" },
      { name: "orderVault", type: "address" },
    ],
    outputs: [],
    stateMutability: "nonpayable",
  },
  {
    type: "function",
    name: "withdraw",
    inputs: [
      { name: "amount", type: "uint256" },
      { name: "to", type: "address" },
    ],
    outputs: [],
    stateMutability: "nonpayable",
  },
  {
    type: "function",
    name: "createGmxMarketIncreaseOrders",
    inputs: [
      {
        name: "orders",
        type: "tuple[]",
        components: [
          { name: "exchangeRouter", type: "address" },
          { name: "router", type: "address" },
          { name: "orderVault", type: "address" },
          { name: "market", type: "address" },
          { name: "collateralToken", type: "address" },
          { name: "isLong", type: "bool" },
          { name: "shouldUnwrapNativeToken", type: "bool" },
          { name: "sizeDeltaUsd", type: "uint256" },
          { name: "collateralAmount", type: "uint256" },
          { name: "acceptablePrice", type: "uint256" },
          { name: "executionFee", type: "uint256" },
          { name: "referralCode", type: "bytes32" },
        ],
      },
    ],
    outputs: [{ name: "orderKeys", type: "bytes32[]" }],
    stateMutability: "payable",
  },
  {
    type: "function",
    name: "createGmxMarketDecreaseOrders",
    inputs: [
      {
        name: "orders",
        type: "tuple[]",
        components: [
          { name: "exchangeRouter", type: "address" },
          { name: "orderVault", type: "address" },
          { name: "market", type: "address" },
          { name: "collateralToken", type: "address" },
          { name: "isLong", type: "bool" },
          { name: "shouldUnwrapNativeToken", type: "bool" },
          { name: "sizeDeltaUsd", type: "uint256" },
          { name: "collateralWithdrawalAmount", type: "uint256" },
          { name: "acceptablePrice", type: "uint256" },
          { name: "executionFee", type: "uint256" },
          { name: "minOutputAmount", type: "uint256" },
          { name: "decreasePositionSwapType", type: "uint8" },
          { name: "referralCode", type: "bytes32" },
        ],
      },
    ],
    outputs: [{ name: "orderKeys", type: "bytes32[]" }],
    stateMutability: "payable",
  },
] as const;
const VAULT_FACTORY_BYTECODE = vaultFactoryArtifact.bytecode.object as Hex;

function strategyVaultChain() {
  const chain = SUPPORTED_CHAINS.find(
    (supportedChain) => supportedChain.id === STRATEGY_VAULT_CHAIN_ID,
  );
  if (!chain) {
    throw new Error(
      `Unsupported vault chain ${STRATEGY_VAULT_CHAIN_ID}; use Arbitrum One or Arbitrum Sepolia`,
    );
  }
  return chain;
}

export async function deployVaultOnChain(
  provider: EthereumProvider,
): Promise<VaultDeployment> {
  const chain = strategyVaultChain();
  const walletClient = createWalletClient({
    chain,
    transport: custom(provider),
  });
  const publicClient = createPublicClient({ chain, transport: http() });
  const [account] = await walletClient.getAddresses();
  if (!account) throw new Error("Connect a wallet before deploying");

  const currentChainId = await walletClient.getChainId();
  if (currentChainId !== STRATEGY_VAULT_CHAIN_ID) {
    await walletClient.switchChain({ id: STRATEGY_VAULT_CHAIN_ID });
  }

  if (STRATEGY_VAULT_FACTORY_ADDRESS) {
    const createVaultHash = await walletClient.writeContract({
      address: STRATEGY_VAULT_FACTORY_ADDRESS as `0x${string}`,
      abi: VAULT_FACTORY_ABI,
      functionName: "createVault",
      args: [STRATEGY_VAULT_ASSET, account],
      account,
      chain,
    });
    const createVaultReceipt = await publicClient.waitForTransactionReceipt({
      hash: createVaultHash,
    });
    const vaultAddress = vaultAddressFromReceipt(createVaultReceipt.logs);

    return {
      chainId: STRATEGY_VAULT_CHAIN_ID,
      vaultAddress,
      assetAddress: STRATEGY_VAULT_ASSET,
    };
  }

  const implementationHash = await walletClient.deployContract({
    abi: STRATEGY_VAULT_ABI,
    account,
    bytecode: STRATEGY_VAULT_BYTECODE,
    chain,
  });
  const implementationReceipt = await publicClient.waitForTransactionReceipt({
    hash: implementationHash,
  });
  if (!implementationReceipt.contractAddress) {
    throw new Error(
      "StrategyVault implementation deployment receipt did not include a contract address",
    );
  }

  const factoryHash = await walletClient.deployContract({
    abi: VAULT_FACTORY_ABI,
    account,
    args: [implementationReceipt.contractAddress, account],
    bytecode: VAULT_FACTORY_BYTECODE,
    chain,
  });
  const factoryReceipt = await publicClient.waitForTransactionReceipt({
    hash: factoryHash,
  });
  if (!factoryReceipt.contractAddress) {
    throw new Error("VaultFactory deployment receipt did not include a contract address");
  }

  const createVaultHash = await walletClient.writeContract({
    address: factoryReceipt.contractAddress,
    abi: VAULT_FACTORY_ABI,
    functionName: "createVault",
    args: [STRATEGY_VAULT_ASSET, account],
    account,
    chain,
  });
  const createVaultReceipt = await publicClient.waitForTransactionReceipt({
    hash: createVaultHash,
  });
  const vaultAddress = vaultAddressFromReceipt(createVaultReceipt.logs);

  return {
    chainId: STRATEGY_VAULT_CHAIN_ID,
    vaultAddress,
    assetAddress: STRATEGY_VAULT_ASSET,
  };
}

function vaultAddressFromReceipt(logs: Parameters<typeof parseEventLogs>[0]["logs"]) {
  const [vaultCreated] = parseEventLogs({
    abi: VAULT_FACTORY_ABI,
    eventName: "VaultCreated",
    logs,
  }) as unknown as VaultCreatedLog[];
  const vaultAddress = vaultCreated?.args.vault;
  if (!vaultAddress) {
    throw new Error("Vault creation receipt did not include a VaultCreated event");
  }
  return vaultAddress;
}

// Persist the deployed vault and promote the run's mandate to `active`.
export async function assertVaultDeployable(
  runId: string,
  accessToken?: string | null,
): Promise<VaultDeployabilityResponse> {
  const response = await fetch(`/api/runs/${encodeURIComponent(runId)}/vault`, {
    headers: {
      ...(accessToken ? { authorization: `Bearer ${accessToken}` } : {}),
    },
  });

  const payload = (await response.json().catch(() => null)) as
    | (VaultDeployabilityResponse & { message?: string })
    | null;

  if (!response.ok) {
    throw new Error(payload?.message ?? "Failed to check vault deployability");
  }
  if (!payload?.deployable) {
    throw new Error(payload?.reason ?? "Vault is not ready to deploy");
  }

  return payload;
}

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

export async function fundVaultOnChain(
  provider: EthereumProvider,
  vaultAddress: string,
  amount: string,
) {
  const chain = strategyVaultChain();
  const walletClient = createWalletClient({
    chain,
    transport: custom(provider),
  });
  const publicClient = createPublicClient({ chain, transport: http() });
  const [account] = await walletClient.getAddresses();
  if (!account) throw new Error("Connect a wallet before funding");

  const currentChainId = await walletClient.getChainId();
  if (currentChainId !== STRATEGY_VAULT_CHAIN_ID) {
    await walletClient.switchChain({ id: STRATEGY_VAULT_CHAIN_ID });
  }

  const amountUnits = parseUnits(amount, ASSET_DECIMALS);
  if (amountUnits <= BigInt(0)) {
    throw new Error("Enter an amount greater than zero");
  }

  const approveHash = await walletClient.writeContract({
    address: STRATEGY_VAULT_ASSET as `0x${string}`,
    abi: erc20Abi,
    functionName: "approve",
    args: [vaultAddress as `0x${string}`, amountUnits],
    account,
    chain,
  });
  await publicClient.waitForTransactionReceipt({ hash: approveHash });

  const depositHash = await walletClient.writeContract({
    address: vaultAddress as `0x${string}`,
    abi: STRATEGY_VAULT_USER_ABI,
    functionName: "deposit",
    args: [amountUnits],
    account,
    chain,
  });
  await publicClient.waitForTransactionReceipt({ hash: depositHash });

  return readVaultIdleBalance(provider, vaultAddress);
}

export async function readVaultIdleBalance(
  provider: EthereumProvider,
  vaultAddress: string,
) {
  const chain = strategyVaultChain();
  const publicClient = createPublicClient({ chain, transport: custom(provider) });
  const balance = await publicClient.readContract({
    address: vaultAddress as `0x${string}`,
    abi: STRATEGY_VAULT_USER_ABI,
    functionName: "idleBalance",
  });
  return formatUnits(balance, ASSET_DECIMALS);
}

export async function readAllocationReadiness(
  runId: string,
): Promise<VaultAllocationReadiness> {
  const response = await fetch(
    `/api/runs/${encodeURIComponent(runId)}/vault/allocation`,
  );
  const payload = (await response.json().catch(() => null)) as
    | (VaultAllocationReadiness & { message?: string })
    | null;

  if (!response.ok) {
    throw new Error(payload?.message ?? "Failed to check allocation readiness");
  }

  return payload as VaultAllocationReadiness;
}

const ZERO_REFERRAL = "0x0000000000000000000000000000000000000000000000000000000000000000" as const;
const USD_DECIMALS = 30;

function requiredEnv(name: string, value: string | undefined) {
  if (!value) throw new Error(`Set ${name} before submitting GMX orders`);
  return value;
}

function executionFee() {
  return BigInt(process.env.NEXT_PUBLIC_GMX_EXECUTION_FEE_WEI ?? "0");
}

function acceptablePrice() {
  return BigInt(process.env.NEXT_PUBLIC_GMX_ACCEPTABLE_PRICE ?? "0");
}

function orderInputs(allocation: VaultAllocationReadiness, collateralUnits: bigint) {
  const fee = executionFee();
  const price = acceptablePrice();
  if (fee <= BigInt(0)) throw new Error("Set NEXT_PUBLIC_GMX_EXECUTION_FEE_WEI greater than zero");
  if (price <= BigInt(0)) throw new Error("Set NEXT_PUBLIC_GMX_ACCEPTABLE_PRICE greater than zero");

  return allocation.target_allocation
    .filter((item) => (item.weight ?? 0) > 0 && item.coin_id)
    .map((item) => {
      const weightBps = BigInt(Math.round((item.weight ?? 0) * 10_000));
      const collateralAmount = (collateralUnits * weightBps) / BigInt(10_000);
      const sizeDeltaUsd = parseUnits(
        ((Number(collateralAmount) / 10 ** ASSET_DECIMALS).toFixed(6)),
        USD_DECIMALS,
      );
      const market = item.gmx_market?.market_token;
      if (!market) throw new Error(`Backend did not resolve a GMX market for ${item.coin_id}`);
      return { market, collateralAmount, sizeDeltaUsd, fee, price };
    });
}

export async function executeVaultAllocationOnChain(
  provider: EthereumProvider,
  allocation: VaultAllocationReadiness,
  idleBalance: string,
) {
  const chain = strategyVaultChain();
  const walletClient = createWalletClient({ chain, transport: custom(provider) });
  const publicClient = createPublicClient({ chain, transport: http() });
  const [account] = await walletClient.getAddresses();
  if (!account) throw new Error("Connect a wallet before execution");

  const exchangeRouter = requiredEnv("NEXT_PUBLIC_GMX_EXCHANGE_ROUTER", process.env.NEXT_PUBLIC_GMX_EXCHANGE_ROUTER);
  const router = requiredEnv("NEXT_PUBLIC_GMX_ROUTER", process.env.NEXT_PUBLIC_GMX_ROUTER);
  const orderVault = requiredEnv("NEXT_PUBLIC_GMX_ORDER_VAULT", process.env.NEXT_PUBLIC_GMX_ORDER_VAULT);
  const collateralUnits = parseUnits(idleBalance, ASSET_DECIMALS);
  const inputs = orderInputs(allocation, collateralUnits);
  if (inputs.length === 0) throw new Error("No positive allocation legs to execute");

  const routingHash = await walletClient.writeContract({
    address: allocation.vault_address as `0x${string}`,
    abi: STRATEGY_VAULT_USER_ABI,
    functionName: "setGmxRouting",
    args: [exchangeRouter as `0x${string}`, router as `0x${string}`, orderVault as `0x${string}`],
    account,
    chain,
  });
  await publicClient.waitForTransactionReceipt({ hash: routingHash });

  const orders = inputs.map((input) => ({
    exchangeRouter: exchangeRouter as `0x${string}`,
    router: router as `0x${string}`,
    orderVault: orderVault as `0x${string}`,
    market: input.market as `0x${string}`,
    collateralToken: allocation.asset_address as `0x${string}`,
    isLong: true,
    shouldUnwrapNativeToken: false,
    sizeDeltaUsd: input.sizeDeltaUsd,
    collateralAmount: input.collateralAmount,
    acceptablePrice: input.price,
    executionFee: input.fee,
    referralCode: ZERO_REFERRAL,
  }));
  const value = inputs.reduce((sum, input) => sum + input.fee, BigInt(0));
  const hash = await walletClient.writeContract({
    address: allocation.vault_address as `0x${string}`,
    abi: STRATEGY_VAULT_USER_ABI,
    functionName: "createGmxMarketIncreaseOrders",
    args: [orders],
    value,
    account,
    chain,
  });
  await publicClient.waitForTransactionReceipt({ hash });
  return hash;
}

export async function closeVaultPositionsAndWithdrawIdleOnChain(
  provider: EthereumProvider,
  allocation: VaultAllocationReadiness,
  notionalUsd: string,
) {
  const chain = strategyVaultChain();
  const walletClient = createWalletClient({ chain, transport: custom(provider) });
  const publicClient = createPublicClient({ chain, transport: http() });
  const [account] = await walletClient.getAddresses();
  if (!account) throw new Error("Connect a wallet before closing");

  const exchangeRouter = requiredEnv("NEXT_PUBLIC_GMX_EXCHANGE_ROUTER", process.env.NEXT_PUBLIC_GMX_EXCHANGE_ROUTER);
  const orderVault = requiredEnv("NEXT_PUBLIC_GMX_ORDER_VAULT", process.env.NEXT_PUBLIC_GMX_ORDER_VAULT);
  const inputs = orderInputs(allocation, parseUnits(notionalUsd, ASSET_DECIMALS));
  if (inputs.length > 0) {
    const orders = inputs.map((input) => ({
      exchangeRouter: exchangeRouter as `0x${string}`,
      orderVault: orderVault as `0x${string}`,
      market: input.market as `0x${string}`,
      collateralToken: allocation.asset_address as `0x${string}`,
      isLong: true,
      shouldUnwrapNativeToken: false,
      sizeDeltaUsd: input.sizeDeltaUsd,
      collateralWithdrawalAmount: BigInt(0),
      acceptablePrice: input.price,
      executionFee: input.fee,
      minOutputAmount: BigInt(0),
      decreasePositionSwapType: 0,
      referralCode: ZERO_REFERRAL,
    }));
    const value = inputs.reduce((sum, input) => sum + input.fee, BigInt(0));
    const closeHash = await walletClient.writeContract({
      address: allocation.vault_address as `0x${string}`,
      abi: STRATEGY_VAULT_USER_ABI,
      functionName: "createGmxMarketDecreaseOrders",
      args: [orders],
      value,
      account,
      chain,
    });
    await publicClient.waitForTransactionReceipt({ hash: closeHash });
  }

  const idle = await publicClient.readContract({
    address: allocation.vault_address as `0x${string}`,
    abi: STRATEGY_VAULT_USER_ABI,
    functionName: "idleBalance",
  });
  if (idle > BigInt(0)) {
    const withdrawHash = await walletClient.writeContract({
      address: allocation.vault_address as `0x${string}`,
      abi: STRATEGY_VAULT_USER_ABI,
      functionName: "withdraw",
      args: [idle, account],
      account,
      chain,
    });
    await publicClient.waitForTransactionReceipt({ hash: withdrawHash });
  }
}
