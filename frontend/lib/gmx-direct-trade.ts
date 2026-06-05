import {
  createPublicClient,
  createWalletClient,
  custom,
  encodeFunctionData,
  erc20Abi,
  http,
  parseUnits,
} from "viem";
import { arbitrum } from "viem/chains";

import { GMX_ADDRESSES, STRATEGY_VAULT_CHAIN_ID } from "@/lib/contract-addresses";
import type { ScreenerRow } from "@/lib/types";

type EthereumProvider = Parameters<typeof custom>[0];

const USD_DECIMALS = 30;
const ZERO_REFERRAL = "0x0000000000000000000000000000000000000000000000000000000000000000" as const;
const MIN_EXECUTION_FEE_WEI = 50_000_000_000_000n;
const EXECUTION_FEE_BUFFER_BPS = 20_000n;

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

export async function createDirectGmxMarketIncreaseOrder(
  provider: EthereumProvider,
  input: DirectGmxTradeInput,
) {
  const market = input.row.gmx_market;
  if (!market) throw new Error("This row does not have a GMX market");

  const collateralAmount = parseUnits(input.collateralUsd, market.collateral_decimals);
  if (collateralAmount <= BigInt(0)) throw new Error("Enter collateral greater than zero");

  const leverage = Number(input.leverage);
  if (!Number.isFinite(leverage) || leverage < 1) throw new Error("Enter leverage of at least 1x");
  const sizeDeltaUsd = parseUnits((Number(input.collateralUsd) * leverage).toFixed(6), USD_DECIMALS);

  const walletClient = createWalletClient({ chain: arbitrum, transport: custom(provider) });
  const publicClient = createPublicClient({ chain: arbitrum, transport: http() });
  const [account] = await walletClient.getAddresses();
  if (!account) throw new Error("Connect a wallet before trading");

  const chainId = await walletClient.getChainId();
  if (chainId !== STRATEGY_VAULT_CHAIN_ID) await walletClient.switchChain({ id: STRATEGY_VAULT_CHAIN_ID });

  const acceptablePrice = await fetchAcceptablePrice({
    indexToken: market.index_token,
    isLong: input.side === "Long",
    slippageBps: input.slippageBps,
  });

  const approveHash = await walletClient.writeContract({
    address: market.collateral_token as `0x${string}`,
    abi: erc20Abi,
    functionName: "approve",
    args: [GMX_ADDRESSES.router, collateralAmount],
    account,
    chain: arbitrum,
  });
  await publicClient.waitForTransactionReceipt({ hash: approveHash });

  const quotedExecutionFee = await estimateExecutionFee({
    publicClient,
    account,
    market: market.market_token as `0x${string}`,
    collateralToken: market.collateral_token as `0x${string}`,
    collateralAmount,
    sizeDeltaUsd,
    acceptablePrice,
    isLong: input.side === "Long",
  });
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

  const orderHash = await walletClient.writeContract({
    address: GMX_ADDRESSES.exchangeRouter,
    abi: GMX_EXCHANGE_ROUTER_ABI,
    functionName: "multicall",
    args: [calls],
    value: quotedExecutionFee,
    account,
    chain: arbitrum,
  });
  await publicClient.waitForTransactionReceipt({ hash: orderHash });
  return { approveHash, orderHash, executionFee: quotedExecutionFee };
}

async function fetchAcceptablePrice({
  indexToken,
  isLong,
  slippageBps,
}: {
  indexToken: string;
  isLong: boolean;
  slippageBps: number;
}) {
  if (!Number.isFinite(slippageBps) || slippageBps < 0 || slippageBps > 5_000) {
    throw new Error("Enter slippage between 0% and 50%");
  }

  const response = await fetch("https://arbitrum-api.gmxinfra.io/prices/tickers", {
    cache: "no-store",
  });
  if (!response.ok) throw new Error("Unable to fetch GMX prices");

  const tickers = (await response.json()) as Array<{
    tokenAddress?: string;
    minPrice?: string;
    maxPrice?: string;
  }>;
  const ticker = tickers.find(
    (item) => item.tokenAddress?.toLowerCase() === indexToken.toLowerCase(),
  );
  if (!ticker?.minPrice || !ticker.maxPrice) {
    throw new Error("GMX price unavailable for this market");
  }

  const basePrice = BigInt(isLong ? ticker.maxPrice : ticker.minPrice);
  const bps = BigInt(Math.round(slippageBps));
  return isLong
    ? (basePrice * (10_000n + bps)) / 10_000n
    : (basePrice * (10_000n - bps)) / 10_000n;
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

async function estimateExecutionFee({
  publicClient,
  account,
  market,
  collateralToken,
  collateralAmount,
  sizeDeltaUsd,
  acceptablePrice,
  isLong,
}: {
  publicClient: ReturnType<typeof createPublicClient>;
  account: `0x${string}`;
  market: `0x${string}`;
  collateralToken: `0x${string}`;
  collateralAmount: bigint;
  sizeDeltaUsd: bigint;
  acceptablePrice: bigint;
  isLong: boolean;
}) {
  const seedExecutionFee = MIN_EXECUTION_FEE_WEI;
  const seedCalls = buildMarketIncreaseCalls({
    account,
    market,
    collateralToken,
    collateralAmount,
    sizeDeltaUsd,
    acceptablePrice,
    executionFee: seedExecutionFee,
    isLong,
  });

  const [gas, gasPrice] = await Promise.all([
    publicClient.estimateContractGas({
      address: GMX_ADDRESSES.exchangeRouter,
      abi: GMX_EXCHANGE_ROUTER_ABI,
      functionName: "multicall",
      args: [seedCalls],
      value: seedExecutionFee,
      account,
    }),
    publicClient.getGasPrice(),
  ]);
  const buffered = (gas * gasPrice * EXECUTION_FEE_BUFFER_BPS) / 10_000n;
  return buffered > MIN_EXECUTION_FEE_WEI ? buffered : MIN_EXECUTION_FEE_WEI;
}
