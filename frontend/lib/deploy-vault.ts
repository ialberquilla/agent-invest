import {
  createPublicClient,
  createWalletClient,
  custom,
  erc20Abi,
  formatEther,
  formatUnits,
  http,
  parseEther,
  parseEventLogs,
  parseUnits,
} from "viem";
import { arbitrum, arbitrumSepolia } from "viem/chains";
import {
  STRATEGY_VAULT_ADDRESSES,
  STRATEGY_VAULT_CHAIN_ID,
} from "@/lib/contract-addresses";
import {
  estimateExecutionFee,
  getAcceptablePrice,
  getAcceptablePriceForDecrease,
  getMarketIndexToken,
} from "@/lib/gmx-prices";
import type { GmxOpenPosition } from "@/lib/gmx-positions";

const SUPPORTED_CHAINS = [arbitrum, arbitrumSepolia] as const;

export { STRATEGY_VAULT_CHAIN_ID };

export const STRATEGY_VAULT_ASSET = STRATEGY_VAULT_ADDRESSES.asset;
export const STRATEGY_VAULT_FACTORY_ADDRESS = STRATEGY_VAULT_ADDRESSES.factory;

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
  display_name: string;
  status: string;
};

export type VaultDeployProgress =
  | { phase: "checking_wallet" }
  | { phase: "switching_chain"; chainId: number }
  | { phase: "requesting_signature" }
  | { phase: "transaction_submitted"; hash: `0x${string}` }
  | { phase: "transaction_confirmed"; vaultAddress: string };

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

const VAULT_FACTORY_ABI = [
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
    stateMutability: "payable",
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
    name: "gmxRouting",
    inputs: [],
    outputs: [
      { name: "exchangeRouter", type: "address" },
      { name: "router", type: "address" },
      { name: "orderVault", type: "address" },
    ],
    stateMutability: "view",
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
    name: "nativeBalance",
    inputs: [],
    outputs: [{ name: "", type: "uint256" }],
    stateMutability: "view",
  },
  {
    type: "function",
    name: "depositNative",
    inputs: [],
    outputs: [],
    stateMutability: "payable",
  },
  {
    type: "function",
    name: "withdrawNative",
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
  options: { onProgress?: (progress: VaultDeployProgress) => void } = {},
): Promise<VaultDeployment> {
  const chain = strategyVaultChain();
  const walletClient = createWalletClient({
    chain,
    transport: custom(provider),
  });
  const publicClient = createPublicClient({ chain, transport: http() });
  options.onProgress?.({ phase: "checking_wallet" });
  const [account] = await walletClient.getAddresses();
  if (!account) throw new Error("Connect a wallet before deploying");

  const currentChainId = await walletClient.getChainId();
  if (currentChainId !== STRATEGY_VAULT_CHAIN_ID) {
    options.onProgress?.({
      phase: "switching_chain",
      chainId: STRATEGY_VAULT_CHAIN_ID,
    });
    await walletClient.switchChain({ id: STRATEGY_VAULT_CHAIN_ID });
  }

  options.onProgress?.({ phase: "requesting_signature" });
  const createVaultHash = await walletClient.writeContract({
    address: STRATEGY_VAULT_FACTORY_ADDRESS as `0x${string}`,
    abi: VAULT_FACTORY_ABI,
    functionName: "createVault",
    args: [STRATEGY_VAULT_ASSET as `0x${string}`, account],
    account,
    chain,
  });
  options.onProgress?.({
    phase: "transaction_submitted",
    hash: createVaultHash,
  });
  const createVaultReceipt = await publicClient.waitForTransactionReceipt({
    hash: createVaultHash,
  });
  const vaultAddress = vaultAddressFromReceipt(createVaultReceipt.logs);
  options.onProgress?.({ phase: "transaction_confirmed", vaultAddress });

  return {
    chainId: STRATEGY_VAULT_CHAIN_ID,
    vaultAddress,
    assetAddress: STRATEGY_VAULT_ASSET,
  };
}

function vaultAddressFromReceipt(
  logs: Parameters<typeof parseEventLogs>[0]["logs"],
) {
  const [vaultCreated] = parseEventLogs({
    abi: VAULT_FACTORY_ABI,
    eventName: "VaultCreated",
    logs,
  }) as unknown as VaultCreatedLog[];
  const vaultAddress = vaultCreated?.args.vault;
  if (!vaultAddress) {
    throw new Error(
      "Vault creation receipt did not include a VaultCreated event",
    );
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

// Non-throwing read of an already-deployed vault for this run, so the UI can resume the manage
// flow on reload instead of offering a duplicate deploy. Returns null if nothing is bound yet.
export async function getExistingVaultBinding(
  runId: string,
): Promise<VaultBindingResponse | null> {
  const response = await fetch(`/api/runs/${encodeURIComponent(runId)}/vault`);
  const payload = (await response.json().catch(() => null)) as
    | (VaultDeployabilityResponse & Partial<VaultBindingResponse>)
    | null;
  if (!response.ok || !payload || payload.deployable !== false) return null;
  if (!payload.vault_address || !payload.mandate_id) return null;
  return {
    mandate_id: payload.mandate_id,
    chain_id: payload.chain_id ?? STRATEGY_VAULT_CHAIN_ID,
    vault_address: payload.vault_address,
    asset_address: payload.asset_address ?? STRATEGY_VAULT_ASSET,
    display_name:
      typeof payload.display_name === "string" && payload.display_name.trim()
        ? payload.display_name.trim()
        : `Vault ${payload.vault_address.slice(0, 6)}...${payload.vault_address.slice(-4)}`,
    status: payload.status ?? "active",
  };
}

export async function saveVaultBinding(
  runId: string,
  deployment: VaultDeployment,
  accessToken?: string | null,
  displayName = "Unnamed strategy",
): Promise<VaultBindingResponse> {
  const response = await fetch(`/api/runs/${encodeURIComponent(runId)}/vault`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(accessToken ? { authorization: `Bearer ${accessToken}` } : {}),
    },
    body: JSON.stringify({
      chain_id: deployment.chainId,
      vault_address: deployment.vaultAddress,
      asset_address: deployment.assetAddress,
      display_name: displayName,
    }),
  });

  const payload = (await response.json().catch(() => null)) as
    | (VaultBindingResponse & { message?: string })
    | null;

  if (!response.ok) {
    throw new Error(payload?.message ?? "Failed to save vault binding");
  }

  const binding = payload as VaultBindingResponse;
  return {
    ...binding,
    display_name:
      typeof binding.display_name === "string" && binding.display_name.trim()
        ? binding.display_name.trim()
        : displayName,
  };
}

// Deposit USDC collateral and, optionally, top up the native gas tank in the SAME deposit tx
// (`deposit` is payable). Still needs a separate ERC20 approve tx first.
export async function fundVaultOnChain(
  provider: EthereumProvider,
  vaultAddress: string,
  amount: string,
  ethGas?: string,
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
  const gasValue =
    ethGas && ethGas.trim().length > 0 ? parseEther(ethGas) : BigInt(0);

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
    value: gasValue,
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
  const publicClient = createPublicClient({
    chain,
    transport: custom(provider),
  });
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

const ZERO_REFERRAL =
  "0x0000000000000000000000000000000000000000000000000000000000000000" as const;
const USD_DECIMALS = 30;
const DEFAULT_SLIPPAGE_BPS = 100; // 1%

type OrderLeg = {
  market: `0x${string}`;
  collateralAmount: bigint;
  sizeDeltaUsd: bigint;
  fee: bigint;
  price: bigint;
};

async function readGmxRouting(
  publicClient: ReturnType<typeof createPublicClient>,
  vaultAddress: string,
) {
  const [exchangeRouter, router, orderVault] = await publicClient.readContract({
    address: vaultAddress as `0x${string}`,
    abi: STRATEGY_VAULT_USER_ABI,
    functionName: "gmxRouting",
  });
  return { exchangeRouter, router, orderVault };
}

// Build one GMX order leg per positive-weight allocation item, resolving a LIVE acceptable price
// (per market) and a single estimated execution fee. All legs are long in the MVP.
//
// `maxLegs` (TEST ONLY): keep just the top-N legs by weight and renormalize their weights so the
// full collateral is deployed across them. Use this to keep small test sizes above GMX's
// $1 MIN_COLLATERAL_USD per position (e.g. 3 USDC across 2 legs = $1.50 each). Unset = full allocation.
async function buildOrderLegs(
  publicClient: ReturnType<typeof createPublicClient>,
  allocation: VaultAllocationReadiness,
  collateralUnits: bigint,
  slippageBps: number,
  maxLegs?: number,
): Promise<OrderLeg[]> {
  const fee = await estimateExecutionFee(publicClient);

  const positive = allocation.target_allocation.filter(
    (item) => (item.weight ?? 0) > 0 && item.coin_id,
  );
  let kept = positive;
  let renormalize = false;
  if (maxLegs != null && maxLegs > 0 && maxLegs < positive.length) {
    kept = [...positive]
      .sort((a, b) => (b.weight ?? 0) - (a.weight ?? 0))
      .slice(0, maxLegs);
    renormalize = true;
  }
  const keptWeightSum = kept.reduce((sum, item) => sum + (item.weight ?? 0), 0);
  if (kept.length === 0 || keptWeightSum <= 0) return [];

  const legs: OrderLeg[] = [];
  for (const item of kept) {
    const market = item.gmx_market?.market_token;
    if (!market)
      throw new Error(
        `Backend did not resolve a GMX market for ${item.coin_id}`,
      );

    const weight = renormalize
      ? (item.weight ?? 0) / keptWeightSum
      : (item.weight ?? 0);
    const weightBps = BigInt(Math.round(weight * 10_000));
    const collateralAmount = (collateralUnits * weightBps) / BigInt(10_000);
    const sizeDeltaUsd = parseUnits(
      (Number(collateralAmount) / 10 ** ASSET_DECIMALS).toFixed(6),
      USD_DECIMALS,
    );

    const indexToken = await getMarketIndexToken(market);
    const price = await getAcceptablePrice(indexToken, true, slippageBps);
    legs.push({
      market: market as `0x${string}`,
      collateralAmount,
      sizeDeltaUsd,
      fee,
      price,
    });
  }
  return legs;
}

// Fund the vault's native ETH gas tank (pays GMX execution fees). Returns the new native balance.
export async function fundVaultGasOnChain(
  provider: EthereumProvider,
  vaultAddress: string,
  ethAmount: string,
) {
  const chain = strategyVaultChain();
  const walletClient = createWalletClient({
    chain,
    transport: custom(provider),
  });
  const publicClient = createPublicClient({ chain, transport: http() });
  const [account] = await walletClient.getAddresses();
  if (!account) throw new Error("Connect a wallet before funding gas");

  const currentChainId = await walletClient.getChainId();
  if (currentChainId !== STRATEGY_VAULT_CHAIN_ID) {
    await walletClient.switchChain({ id: STRATEGY_VAULT_CHAIN_ID });
  }

  const value = parseEther(ethAmount);
  if (value <= BigInt(0))
    throw new Error("Enter an ETH amount greater than zero");

  const hash = await walletClient.writeContract({
    address: vaultAddress as `0x${string}`,
    abi: STRATEGY_VAULT_USER_ABI,
    functionName: "depositNative",
    value,
    account,
    chain,
  });
  await publicClient.waitForTransactionReceipt({ hash });
  return readVaultNativeBalance(provider, vaultAddress);
}

// Read idle USDC + native gas balances over a public RPC (no wallet needed) — used to hydrate the
// manage panel when resuming an already-deployed vault.
export async function readVaultBalances(vaultAddress: string) {
  const chain = strategyVaultChain();
  const publicClient = createPublicClient({ chain, transport: http() });
  const [idle, gas] = await Promise.all([
    publicClient.readContract({
      address: vaultAddress as `0x${string}`,
      abi: STRATEGY_VAULT_USER_ABI,
      functionName: "idleBalance",
    }),
    publicClient.readContract({
      address: vaultAddress as `0x${string}`,
      abi: STRATEGY_VAULT_USER_ABI,
      functionName: "nativeBalance",
    }),
  ]);
  return { idle: formatUnits(idle, ASSET_DECIMALS), gas: formatEther(gas) };
}

export async function readVaultNativeBalance(
  provider: EthereumProvider,
  vaultAddress: string,
) {
  const chain = strategyVaultChain();
  const publicClient = createPublicClient({
    chain,
    transport: custom(provider),
  });
  const balance = await publicClient.readContract({
    address: vaultAddress as `0x${string}`,
    abi: STRATEGY_VAULT_USER_ABI,
    functionName: "nativeBalance",
  });
  return formatEther(balance);
}

// Withdraw all idle USDC collateral back to the owner (recovery / finish).
export async function withdrawVaultIdleOnChain(
  provider: EthereumProvider,
  vaultAddress: string,
) {
  const chain = strategyVaultChain();
  const walletClient = createWalletClient({
    chain,
    transport: custom(provider),
  });
  const publicClient = createPublicClient({ chain, transport: http() });
  const [account] = await walletClient.getAddresses();
  if (!account) throw new Error("Connect a wallet before withdrawing");

  const currentChainId = await walletClient.getChainId();
  if (currentChainId !== STRATEGY_VAULT_CHAIN_ID) {
    await walletClient.switchChain({ id: STRATEGY_VAULT_CHAIN_ID });
  }

  const idle = await publicClient.readContract({
    address: vaultAddress as `0x${string}`,
    abi: STRATEGY_VAULT_USER_ABI,
    functionName: "idleBalance",
  });
  if (idle <= BigInt(0)) return formatUnits(BigInt(0), ASSET_DECIMALS);

  const hash = await walletClient.writeContract({
    address: vaultAddress as `0x${string}`,
    abi: STRATEGY_VAULT_USER_ABI,
    functionName: "withdraw",
    args: [idle, account],
    account,
    chain,
  });
  await publicClient.waitForTransactionReceipt({ hash });
  return readVaultIdleBalance(provider, vaultAddress);
}

// Sweep native ETH (GMX fee refunds + leftover gas tank) back to the owner.
export async function withdrawVaultNativeOnChain(
  provider: EthereumProvider,
  vaultAddress: string,
) {
  const chain = strategyVaultChain();
  const walletClient = createWalletClient({
    chain,
    transport: custom(provider),
  });
  const publicClient = createPublicClient({ chain, transport: http() });
  const [account] = await walletClient.getAddresses();
  if (!account) throw new Error("Connect a wallet before sweeping gas");

  const balance = await publicClient.readContract({
    address: vaultAddress as `0x${string}`,
    abi: STRATEGY_VAULT_USER_ABI,
    functionName: "nativeBalance",
  });
  if (balance <= BigInt(0)) return formatEther(BigInt(0));

  const hash = await walletClient.writeContract({
    address: vaultAddress as `0x${string}`,
    abi: STRATEGY_VAULT_USER_ABI,
    functionName: "withdrawNative",
    args: [balance, account],
    account,
    chain,
  });
  await publicClient.waitForTransactionReceipt({ hash });
  return readVaultNativeBalance(provider, vaultAddress);
}

export async function executeVaultAllocationOnChain(
  provider: EthereumProvider,
  allocation: VaultAllocationReadiness,
  idleBalance: string,
  options?: { payFromTank?: boolean; slippageBps?: number; maxLegs?: number },
) {
  const chain = strategyVaultChain();
  const walletClient = createWalletClient({
    chain,
    transport: custom(provider),
  });
  const publicClient = createPublicClient({ chain, transport: http() });
  const [account] = await walletClient.getAddresses();
  if (!account) throw new Error("Connect a wallet before execution");

  const { exchangeRouter, router, orderVault } = await readGmxRouting(
    publicClient,
    allocation.vault_address,
  );
  const collateralUnits = parseUnits(idleBalance, ASSET_DECIMALS);
  const legs = await buildOrderLegs(
    publicClient,
    allocation,
    collateralUnits,
    options?.slippageBps ?? DEFAULT_SLIPPAGE_BPS,
    options?.maxLegs,
  );
  if (legs.length === 0)
    throw new Error("No positive allocation legs to execute");

  const orders = legs.map((leg) => ({
    exchangeRouter,
    router,
    orderVault,
    market: leg.market,
    collateralToken: allocation.asset_address as `0x${string}`,
    isLong: true,
    shouldUnwrapNativeToken: false,
    sizeDeltaUsd: leg.sizeDeltaUsd,
    collateralAmount: leg.collateralAmount,
    acceptablePrice: leg.price,
    executionFee: leg.fee,
    referralCode: ZERO_REFERRAL,
  }));
  // payFromTank: rely on the vault's pre-funded gas tank (keeper-style, value 0). Otherwise attach
  // the total execution fee inline (owner-style). Either satisfies the on-chain gas check.
  const totalFee = legs.reduce((sum, leg) => sum + leg.fee, BigInt(0));
  const value = options?.payFromTank ? BigInt(0) : totalFee;
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
  options?: { payFromTank?: boolean; slippageBps?: number; maxLegs?: number },
) {
  const chain = strategyVaultChain();
  const walletClient = createWalletClient({
    chain,
    transport: custom(provider),
  });
  const publicClient = createPublicClient({ chain, transport: http() });
  const [account] = await walletClient.getAddresses();
  if (!account) throw new Error("Connect a wallet before closing");

  const { exchangeRouter, orderVault } = await readGmxRouting(
    publicClient,
    allocation.vault_address,
  );
  const legs = await buildOrderLegs(
    publicClient,
    allocation,
    parseUnits(notionalUsd, ASSET_DECIMALS),
    options?.slippageBps ?? DEFAULT_SLIPPAGE_BPS,
    options?.maxLegs,
  );
  if (legs.length > 0) {
    const orders = legs.map((leg) => ({
      exchangeRouter,
      orderVault,
      market: leg.market,
      collateralToken: allocation.asset_address as `0x${string}`,
      isLong: true,
      shouldUnwrapNativeToken: false,
      sizeDeltaUsd: leg.sizeDeltaUsd,
      collateralWithdrawalAmount: BigInt(0),
      acceptablePrice: leg.price,
      executionFee: leg.fee,
      minOutputAmount: BigInt(0),
      decreasePositionSwapType: 0,
      referralCode: ZERO_REFERRAL,
    }));
    const totalFee = legs.reduce((sum, leg) => sum + leg.fee, BigInt(0));
    const value = options?.payFromTank ? BigInt(0) : totalFee;
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

export async function closeVaultOpenPositionsOnChain(
  provider: EthereumProvider,
  vaultAddress: string,
  assetAddress: string,
  positions: GmxOpenPosition[],
  options?: { payFromTank?: boolean; slippageBps?: number },
) {
  const chain = strategyVaultChain();
  const walletClient = createWalletClient({
    chain,
    transport: custom(provider),
  });
  const publicClient = createPublicClient({ chain, transport: http() });
  const [account] = await walletClient.getAddresses();
  if (!account) throw new Error("Connect a wallet before closing positions");

  const currentChainId = await walletClient.getChainId();
  if (currentChainId !== STRATEGY_VAULT_CHAIN_ID) {
    await walletClient.switchChain({ id: STRATEGY_VAULT_CHAIN_ID });
  }

  const closeable = positions.filter(
    (position) =>
      BigInt(position.sizeUsdRaw) > BigInt(0) &&
      BigInt(position.collateralAmountRaw) > BigInt(0),
  );
  if (closeable.length === 0) throw new Error("No open positions to close");

  const { exchangeRouter, orderVault } = await readGmxRouting(
    publicClient,
    vaultAddress,
  );
  const fee = await estimateExecutionFee(publicClient);
  const slippageBps = options?.slippageBps ?? DEFAULT_SLIPPAGE_BPS;

  const orders = await Promise.all(
    closeable.map(async (position) => {
      const market = position.marketAddress as `0x${string}`;
      const indexToken = await getMarketIndexToken(market);
      const isLong = position.side === "Long";
      return {
        exchangeRouter,
        orderVault,
        market,
        collateralToken: assetAddress as `0x${string}`,
        isLong,
        shouldUnwrapNativeToken: false,
        sizeDeltaUsd: BigInt(position.sizeUsdRaw),
        collateralWithdrawalAmount: BigInt(position.collateralAmountRaw),
        acceptablePrice: await getAcceptablePriceForDecrease(
          indexToken,
          isLong,
          slippageBps,
        ),
        executionFee: fee,
        minOutputAmount: BigInt(0),
        decreasePositionSwapType: 0,
        referralCode: ZERO_REFERRAL,
      };
    }),
  );

  const totalFee = orders.reduce(
    (sum, order) => sum + order.executionFee,
    BigInt(0),
  );
  const value = options?.payFromTank ? BigInt(0) : totalFee;
  const hash = await walletClient.writeContract({
    address: vaultAddress as `0x${string}`,
    abi: STRATEGY_VAULT_USER_ABI,
    functionName: "createGmxMarketDecreaseOrders",
    args: [orders],
    value,
    account,
    chain,
  });
  await publicClient.waitForTransactionReceipt({ hash });
  return hash;
}
