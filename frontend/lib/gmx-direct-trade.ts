import {
  createPublicClient,
  createWalletClient,
  custom,
  encodeFunctionData,
  erc20Abi,
  formatEther,
  formatUnits,
  parseUnits,
} from "viem";
import { arbitrum } from "viem/chains";

import {
  GMX_ADDRESSES,
  STRATEGY_VAULT_CHAIN_ID,
} from "@/lib/contract-addresses";
import {
  estimateExecutionFee,
  getAcceptablePrice,
  getAcceptablePriceForDecrease,
  getMarketIndexToken,
} from "@/lib/gmx-prices";
import type { GmxOpenPosition } from "@/lib/gmx-positions";
import type { ScreenerRow } from "@/lib/types";

type EthereumProvider = Parameters<typeof custom>[0];

const USD_DECIMALS = 30;
const ZERO_REFERRAL =
  "0x0000000000000000000000000000000000000000000000000000000000000000" as const;
const GAS_BUFFER_BPS = 12_000n;
const DIRECT_TRADE_RECEIPT_TIMEOUT_MS = 300_000;

const GMX_EXCHANGE_ROUTER_ABI = [
  {
    type: "function",
    name: "multicall",
    inputs: [{ name: "data", type: "bytes[]" }],
    outputs: [{ name: "results", type: "bytes[]" }],
    stateMutability: "payable",
  },
  {
    type: "function",
    name: "sendWnt",
    inputs: [
      { name: "receiver", type: "address" },
      { name: "amount", type: "uint256" },
    ],
    outputs: [],
    stateMutability: "payable",
  },
  {
    type: "function",
    name: "sendTokens",
    inputs: [
      { name: "token", type: "address" },
      { name: "receiver", type: "address" },
      { name: "amount", type: "uint256" },
    ],
    outputs: [],
    stateMutability: "payable",
  },
  {
    type: "function",
    name: "createOrder",
    inputs: [
      {
        name: "params",
        type: "tuple",
        components: [
          {
            name: "addresses",
            type: "tuple",
            components: [
              { name: "receiver", type: "address" },
              { name: "cancellationReceiver", type: "address" },
              { name: "callbackContract", type: "address" },
              { name: "uiFeeReceiver", type: "address" },
              { name: "market", type: "address" },
              { name: "initialCollateralToken", type: "address" },
              { name: "swapPath", type: "address[]" },
            ],
          },
          {
            name: "numbers",
            type: "tuple",
            components: [
              { name: "sizeDeltaUsd", type: "uint256" },
              { name: "initialCollateralDeltaAmount", type: "uint256" },
              { name: "triggerPrice", type: "uint256" },
              { name: "acceptablePrice", type: "uint256" },
              { name: "executionFee", type: "uint256" },
              { name: "callbackGasLimit", type: "uint256" },
              { name: "minOutputAmount", type: "uint256" },
              { name: "validFromTime", type: "uint256" },
            ],
          },
          { name: "orderType", type: "uint8" },
          { name: "decreasePositionSwapType", type: "uint8" },
          { name: "isLong", type: "bool" },
          { name: "shouldUnwrapNativeToken", type: "bool" },
          { name: "autoCancel", type: "bool" },
          { name: "referralCode", type: "bytes32" },
          { name: "dataList", type: "bytes32[]" },
        ],
      },
    ],
    outputs: [{ name: "orderKey", type: "bytes32" }],
    stateMutability: "payable",
  },
] as const;

export type DirectGmxTradeInput = {
  row: ScreenerRow;
  side: "Long" | "Short";
  collateralUsd: string;
  leverage: string;
  slippageBps: number;
};

export type DirectGmxTradeProgress =
  | { phase: "checking_wallet" }
  | { phase: "checking_balances" }
  | { phase: "requesting_approval" }
  | { phase: "approval_submitted"; hash: `0x${string}` }
  | { phase: "approval_confirmed"; hash: `0x${string}` }
  | { phase: "requesting_order" }
  | { phase: "order_submitted"; hash: `0x${string}` }
  | { phase: "order_confirmed"; hash: `0x${string}` };

export type DirectGmxCloseProgress =
  | { phase: "checking_wallet" }
  | { phase: "checking_balances" }
  | { phase: "requesting_order" }
  | { phase: "order_submitted"; hash: `0x${string}` }
  | { phase: "order_confirmed"; hash: `0x${string}` };

export async function createDirectGmxMarketIncreaseOrder(
  provider: EthereumProvider,
  input: DirectGmxTradeInput,
  options: { onProgress?: (progress: DirectGmxTradeProgress) => void } = {},
) {
  const market = input.row.gmx_market;
  if (!market) throw new Error("This row does not have a GMX market");

  const collateralAmount = parseUnits(
    input.collateralUsd,
    market.collateral_decimals,
  );
  if (collateralAmount <= BigInt(0))
    throw new Error("Enter collateral greater than zero");

  const leverage = Number(input.leverage);
  if (!Number.isFinite(leverage) || leverage < 1)
    throw new Error("Enter leverage of at least 1x");
  const sizeDeltaUsd = parseUnits(
    (Number(input.collateralUsd) * leverage).toFixed(6),
    USD_DECIMALS,
  );

  const walletClient = createWalletClient({
    chain: arbitrum,
    transport: custom(provider),
  });
  const publicClient = createPublicClient({
    chain: arbitrum,
    transport: custom(provider),
  });
  options.onProgress?.({ phase: "checking_wallet" });
  const [account] = await walletClient.getAddresses();
  if (!account) throw new Error("Connect a wallet before trading");

  const chainId = await walletClient.getChainId();
  if (chainId !== STRATEGY_VAULT_CHAIN_ID)
    await walletClient.switchChain({ id: STRATEGY_VAULT_CHAIN_ID });

  const acceptablePrice = await getAcceptablePrice(
    market.index_token,
    input.side === "Long",
    input.slippageBps,
  );
  const quotedExecutionFee = await estimateExecutionFee(publicClient);

  options.onProgress?.({ phase: "checking_balances" });
  const [collateralBalance, allowance, nativeBalance] = await Promise.all([
    publicClient.readContract({
      address: market.collateral_token as `0x${string}`,
      abi: erc20Abi,
      functionName: "balanceOf",
      args: [account],
    }),
    publicClient.readContract({
      address: market.collateral_token as `0x${string}`,
      abi: erc20Abi,
      functionName: "allowance",
      args: [account, GMX_ADDRESSES.router],
    }),
    publicClient.getBalance({ address: account }),
  ]);
  if (collateralBalance < collateralAmount) {
    throw new Error(
      `Insufficient USDC balance. Need ${formatUnits(
        collateralAmount,
        market.collateral_decimals,
      )}, wallet has ${formatUnits(collateralBalance, market.collateral_decimals)}.`,
    );
  }
  if (nativeBalance < quotedExecutionFee) {
    throw new Error(
      `Insufficient Arbitrum ETH for the GMX execution fee. Need at least ${formatEther(
        quotedExecutionFee,
      )} ETH before transaction gas.`,
    );
  }

  let approveHash: `0x${string}` | undefined;
  if (allowance < collateralAmount) {
    options.onProgress?.({ phase: "requesting_approval" });
    approveHash = await walletClient.writeContract({
      address: market.collateral_token as `0x${string}`,
      abi: erc20Abi,
      functionName: "approve",
      args: [GMX_ADDRESSES.router, collateralAmount],
      account,
      chain: arbitrum,
    });
    options.onProgress?.({ phase: "approval_submitted", hash: approveHash });
    await publicClient.waitForTransactionReceipt({
      hash: approveHash,
      timeout: DIRECT_TRADE_RECEIPT_TIMEOUT_MS,
    });
    options.onProgress?.({ phase: "approval_confirmed", hash: approveHash });
  }
  const calls = buildMarketIncreaseCalls({
    account,
    market: market.market_token as `0x${string}`,
    collateralToken: market.collateral_token as `0x${string}`,
    collateralAmount,
    sizeDeltaUsd,
    acceptablePrice,
    executionFee: quotedExecutionFee,
    isLong: input.side === "Long",
  });

  const [orderGas, orderGasPrice, latestNativeBalance] = await Promise.all([
    publicClient.estimateContractGas({
      address: GMX_ADDRESSES.exchangeRouter,
      abi: GMX_EXCHANGE_ROUTER_ABI,
      functionName: "multicall",
      args: [calls],
      value: quotedExecutionFee,
      account,
    }),
    publicClient.getGasPrice(),
    publicClient.getBalance({ address: account }),
  ]);
  const bufferedOrderGas = (orderGas * GAS_BUFFER_BPS) / 10_000n;
  const requiredNative = quotedExecutionFee + bufferedOrderGas * orderGasPrice;
  if (latestNativeBalance < requiredNative) {
    throw new Error(
      `Insufficient Arbitrum ETH. Need about ${formatEther(
        requiredNative,
      )} ETH for the GMX execution fee plus transaction gas, wallet has ${formatEther(
        latestNativeBalance,
      )} ETH.`,
    );
  }

  options.onProgress?.({ phase: "requesting_order" });
  const orderHash = await walletClient.writeContract({
    address: GMX_ADDRESSES.exchangeRouter,
    abi: GMX_EXCHANGE_ROUTER_ABI,
    functionName: "multicall",
    args: [calls],
    value: quotedExecutionFee,
    gas: bufferedOrderGas,
    account,
    chain: arbitrum,
  });
  options.onProgress?.({ phase: "order_submitted", hash: orderHash });
  await publicClient.waitForTransactionReceipt({
    hash: orderHash,
    timeout: DIRECT_TRADE_RECEIPT_TIMEOUT_MS,
  });
  options.onProgress?.({ phase: "order_confirmed", hash: orderHash });
  return { approveHash, orderHash, executionFee: quotedExecutionFee };
}

export async function createDirectGmxMarketDecreaseOrder(
  provider: EthereumProvider,
  position: GmxOpenPosition,
  options: { onProgress?: (progress: DirectGmxCloseProgress) => void } = {},
) {
  const sizeDeltaUsd = BigInt(position.sizeUsdRaw);
  const collateralWithdrawalAmount = BigInt(position.collateralAmountRaw);
  if (sizeDeltaUsd <= BigInt(0)) throw new Error("Position size is zero");
  if (collateralWithdrawalAmount <= BigInt(0)) {
    throw new Error("Position collateral is zero");
  }

  const walletClient = createWalletClient({
    chain: arbitrum,
    transport: custom(provider),
  });
  const publicClient = createPublicClient({
    chain: arbitrum,
    transport: custom(provider),
  });
  options.onProgress?.({ phase: "checking_wallet" });
  const [account] = await walletClient.getAddresses();
  if (!account) throw new Error("Connect a wallet before closing");
  if (account.toLowerCase() !== position.accountAddress.toLowerCase()) {
    throw new Error("Connected wallet does not own this GMX position");
  }

  const chainId = await walletClient.getChainId();
  if (chainId !== STRATEGY_VAULT_CHAIN_ID) {
    await walletClient.switchChain({ id: STRATEGY_VAULT_CHAIN_ID });
  }

  const quotedExecutionFee = await estimateExecutionFee(publicClient);
  const indexToken = await getMarketIndexToken(position.marketAddress);
  const isLong = position.side === "Long";
  const acceptablePrice = await getAcceptablePriceForDecrease(
    indexToken,
    isLong,
    100,
  );
  const calls = buildMarketDecreaseCalls({
    account,
    market: position.marketAddress as `0x${string}`,
    collateralToken: position.collateralTokenAddress as `0x${string}`,
    sizeDeltaUsd,
    collateralWithdrawalAmount,
    acceptablePrice,
    executionFee: quotedExecutionFee,
    isLong,
  });

  options.onProgress?.({ phase: "checking_balances" });
  const [orderGas, orderGasPrice, nativeBalance] = await Promise.all([
    publicClient.estimateContractGas({
      address: GMX_ADDRESSES.exchangeRouter,
      abi: GMX_EXCHANGE_ROUTER_ABI,
      functionName: "multicall",
      args: [calls],
      value: quotedExecutionFee,
      account,
    }),
    publicClient.getGasPrice(),
    publicClient.getBalance({ address: account }),
  ]);
  const bufferedOrderGas = (orderGas * GAS_BUFFER_BPS) / 10_000n;
  const requiredNative = quotedExecutionFee + bufferedOrderGas * orderGasPrice;
  if (nativeBalance < requiredNative) {
    throw new Error(
      `Insufficient Arbitrum ETH. Need about ${formatEther(
        requiredNative,
      )} ETH for the GMX execution fee plus transaction gas, wallet has ${formatEther(
        nativeBalance,
      )} ETH.`,
    );
  }

  options.onProgress?.({ phase: "requesting_order" });
  const orderHash = await walletClient.writeContract({
    address: GMX_ADDRESSES.exchangeRouter,
    abi: GMX_EXCHANGE_ROUTER_ABI,
    functionName: "multicall",
    args: [calls],
    value: quotedExecutionFee,
    gas: bufferedOrderGas,
    account,
    chain: arbitrum,
  });
  options.onProgress?.({ phase: "order_submitted", hash: orderHash });
  await publicClient.waitForTransactionReceipt({
    hash: orderHash,
    timeout: DIRECT_TRADE_RECEIPT_TIMEOUT_MS,
  });
  options.onProgress?.({ phase: "order_confirmed", hash: orderHash });
  return { orderHash, executionFee: quotedExecutionFee };
}

function buildMarketIncreaseCalls({
  account,
  market,
  collateralToken,
  collateralAmount,
  sizeDeltaUsd,
  acceptablePrice,
  executionFee,
  isLong,
}: {
  account: `0x${string}`;
  market: `0x${string}`;
  collateralToken: `0x${string}`;
  collateralAmount: bigint;
  sizeDeltaUsd: bigint;
  acceptablePrice: bigint;
  executionFee: bigint;
  isLong: boolean;
}) {
  return [
    encodeFunctionData({
      abi: GMX_EXCHANGE_ROUTER_ABI,
      functionName: "sendWnt",
      args: [GMX_ADDRESSES.orderVault, executionFee],
    }),
    encodeFunctionData({
      abi: GMX_EXCHANGE_ROUTER_ABI,
      functionName: "sendTokens",
      args: [collateralToken, GMX_ADDRESSES.orderVault, collateralAmount],
    }),
    encodeFunctionData({
      abi: GMX_EXCHANGE_ROUTER_ABI,
      functionName: "createOrder",
      args: [
        {
          addresses: {
            receiver: account,
            cancellationReceiver: account,
            callbackContract: "0x0000000000000000000000000000000000000000",
            uiFeeReceiver: "0x0000000000000000000000000000000000000000",
            market,
            initialCollateralToken: collateralToken,
            swapPath: [],
          },
          numbers: {
            sizeDeltaUsd,
            initialCollateralDeltaAmount: collateralAmount,
            triggerPrice: BigInt(0),
            acceptablePrice,
            executionFee,
            callbackGasLimit: BigInt(0),
            minOutputAmount: BigInt(0),
            validFromTime: BigInt(0),
          },
          orderType: 2,
          decreasePositionSwapType: 0,
          isLong,
          shouldUnwrapNativeToken: false,
          autoCancel: false,
          referralCode: ZERO_REFERRAL,
          dataList: [],
        },
      ],
    }),
  ];
}

function buildMarketDecreaseCalls({
  account,
  market,
  collateralToken,
  sizeDeltaUsd,
  collateralWithdrawalAmount,
  acceptablePrice,
  executionFee,
  isLong,
}: {
  account: `0x${string}`;
  market: `0x${string}`;
  collateralToken: `0x${string}`;
  sizeDeltaUsd: bigint;
  collateralWithdrawalAmount: bigint;
  acceptablePrice: bigint;
  executionFee: bigint;
  isLong: boolean;
}) {
  return [
    encodeFunctionData({
      abi: GMX_EXCHANGE_ROUTER_ABI,
      functionName: "sendWnt",
      args: [GMX_ADDRESSES.orderVault, executionFee],
    }),
    encodeFunctionData({
      abi: GMX_EXCHANGE_ROUTER_ABI,
      functionName: "createOrder",
      args: [
        {
          addresses: {
            receiver: account,
            cancellationReceiver: account,
            callbackContract: "0x0000000000000000000000000000000000000000",
            uiFeeReceiver: "0x0000000000000000000000000000000000000000",
            market,
            initialCollateralToken: collateralToken,
            swapPath: [],
          },
          numbers: {
            sizeDeltaUsd,
            initialCollateralDeltaAmount: collateralWithdrawalAmount,
            triggerPrice: BigInt(0),
            acceptablePrice,
            executionFee,
            callbackGasLimit: BigInt(0),
            minOutputAmount: BigInt(0),
            validFromTime: BigInt(0),
          },
          orderType: 4,
          decreasePositionSwapType: 0,
          isLong,
          shouldUnwrapNativeToken: false,
          autoCancel: false,
          referralCode: ZERO_REFERRAL,
          dataList: [],
        },
      ],
    }),
  ];
}
