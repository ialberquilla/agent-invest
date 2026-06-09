import {
  createPublicClient,
  erc20Abi,
  formatUnits,
  http,
  isAddress,
  type Address,
} from "viem";
import { arbitrum } from "viem/chains";

import { GMX_ADDRESSES } from "@/lib/contract-addresses";

const GMX_API = "https://arbitrum-api.gmxinfra.io";
const GMX_READER_ADDRESS = "0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789";
const USD_DECIMALS = 30;
const DEFAULT_LIMIT = BigInt(50);

const EXCHANGE_ROUTER_DATA_STORE_ABI = [
  {
    type: "function",
    name: "dataStore",
    inputs: [],
    outputs: [{ name: "", type: "address" }],
    stateMutability: "view",
  },
] as const;

const GMX_READER_ABI = [
  {
    type: "function",
    name: "getAccountPositions",
    inputs: [
      { name: "dataStore", type: "address" },
      { name: "account", type: "address" },
      { name: "start", type: "uint256" },
      { name: "end", type: "uint256" },
    ],
    outputs: [
      {
        name: "",
        type: "tuple[]",
        components: [
          {
            name: "addresses",
            type: "tuple",
            components: [
              { name: "account", type: "address" },
              { name: "market", type: "address" },
              { name: "collateralToken", type: "address" },
            ],
          },
          {
            name: "numbers",
            type: "tuple",
            components: [
              { name: "sizeInUsd", type: "uint256" },
              { name: "sizeInTokens", type: "uint256" },
              { name: "collateralAmount", type: "uint256" },
              { name: "pendingImpactAmount", type: "int256" },
              { name: "borrowingFactor", type: "uint256" },
              { name: "fundingFeeAmountPerSize", type: "uint256" },
              { name: "longTokenClaimableFundingAmountPerSize", type: "uint256" },
              { name: "shortTokenClaimableFundingAmountPerSize", type: "uint256" },
              { name: "increasedAtTime", type: "uint256" },
              { name: "decreasedAtTime", type: "uint256" },
            ],
          },
          {
            name: "flags",
            type: "tuple",
            components: [{ name: "isLong", type: "bool" }],
          },
        ],
      },
    ],
    stateMutability: "view",
  },
  {
    type: "function",
    name: "getAccountOrders",
    inputs: [
      { name: "dataStore", type: "address" },
      { name: "account", type: "address" },
      { name: "start", type: "uint256" },
      { name: "end", type: "uint256" },
    ],
    outputs: [
      {
        name: "",
        type: "tuple[]",
        components: [
          { name: "orderKey", type: "bytes32" },
          {
            name: "order",
            type: "tuple",
            components: [
              {
                name: "addresses",
                type: "tuple",
                components: [
                  { name: "account", type: "address" },
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
                  { name: "orderType", type: "uint8" },
                  { name: "decreasePositionSwapType", type: "uint8" },
                  { name: "sizeDeltaUsd", type: "uint256" },
                  { name: "initialCollateralDeltaAmount", type: "uint256" },
                  { name: "triggerPrice", type: "uint256" },
                  { name: "acceptablePrice", type: "uint256" },
                  { name: "executionFee", type: "uint256" },
                  { name: "callbackGasLimit", type: "uint256" },
                  { name: "minOutputAmount", type: "uint256" },
                  { name: "updatedAtTime", type: "uint256" },
                  { name: "validFromTime", type: "uint256" },
                  { name: "srcChainId", type: "uint256" },
                ],
              },
              {
                name: "flags",
                type: "tuple",
                components: [
                  { name: "isLong", type: "bool" },
                  { name: "shouldUnwrapNativeToken", type: "bool" },
                  { name: "isFrozen", type: "bool" },
                  { name: "autoCancel", type: "bool" },
                ],
              },
              { name: "dataList", type: "bytes32[]" },
            ],
          },
        ],
      },
    ],
    stateMutability: "view",
  },
] as const;

export type GmxActivityAccount = {
  type: "wallet" | "vault";
  address: string;
  label?: string;
};

export type GmxOpenPosition = {
  id: string;
  accountType: GmxActivityAccount["type"];
  accountAddress: string;
  accountLabel?: string;
  marketAddress: string;
  marketName: string;
  side: "Long" | "Short";
  sizeUsd: number;
  collateralAmount: number;
  collateralSymbol: string;
  updatedAt?: string;
};

export type GmxPendingOrder = {
  id: string;
  orderKey: string;
  accountType: GmxActivityAccount["type"];
  accountAddress: string;
  accountLabel?: string;
  marketAddress: string;
  marketName: string;
  side: "Long" | "Short";
  orderType: string;
  sizeUsd: number;
  collateralAmount: number;
  collateralSymbol: string;
  updatedAt?: string;
};

export type GmxAccountActivity = {
  openPositions: GmxOpenPosition[];
  pendingOrders: GmxPendingOrder[];
};

type RawPosition = {
  addresses: {
    account: Address;
    market: Address;
    collateralToken: Address;
  };
  numbers: {
    sizeInUsd: bigint;
    collateralAmount: bigint;
    increasedAtTime: bigint;
    decreasedAtTime: bigint;
  };
  flags: { isLong: boolean };
};

type RawOrder = {
  orderKey: string;
  order: {
    addresses: {
      account: Address;
      market: Address;
      initialCollateralToken: Address;
    };
    numbers: {
      orderType: number;
      sizeDeltaUsd: bigint;
      initialCollateralDeltaAmount: bigint;
      updatedAtTime: bigint;
    };
    flags: { isLong: boolean };
  };
};

type MarketMeta = { name: string; marketToken: string };
type TokenMeta = { symbol: string; decimals: number };

const publicClient = createPublicClient({ chain: arbitrum, transport: http() });
let dataStorePromise: Promise<Address> | null = null;
let marketNamePromise: Promise<Map<string, string>> | null = null;
const tokenMetaCache = new Map<string, TokenMeta>();

export async function readGmxAccountActivity(
  accounts: GmxActivityAccount[],
): Promise<GmxAccountActivity> {
  const validAccounts = accounts.filter((account) => isAddress(account.address));
  if (validAccounts.length === 0) {
    return { openPositions: [], pendingOrders: [] };
  }

  const [dataStore, marketNames] = await Promise.all([
    getDataStore(),
    getMarketNames(),
  ]);

  const accountResults = await Promise.all(
    validAccounts.map(async (account) => {
      const address = account.address as Address;
      const [positions, orders] = await Promise.all([
        publicClient.readContract({
          address: GMX_READER_ADDRESS,
          abi: GMX_READER_ABI,
          functionName: "getAccountPositions",
          args: [dataStore, address, BigInt(0), DEFAULT_LIMIT],
        }) as unknown as Promise<readonly RawPosition[]>,
        publicClient.readContract({
          address: GMX_READER_ADDRESS,
          abi: GMX_READER_ABI,
          functionName: "getAccountOrders",
          args: [dataStore, address, BigInt(0), DEFAULT_LIMIT],
        }) as unknown as Promise<readonly RawOrder[]>,
      ]);
      return { account, positions, orders };
    }),
  );

  const tokens = new Set<string>();
  for (const result of accountResults) {
    for (const position of result.positions) {
      tokens.add(position.addresses.collateralToken.toLowerCase());
    }
    for (const order of result.orders) {
      tokens.add(order.order.addresses.initialCollateralToken.toLowerCase());
    }
  }
  const tokenMetas = await resolveTokenMetas(tokens);

  return {
    openPositions: accountResults.flatMap(({ account, positions }) =>
      positions
        .filter((position) => position.numbers.sizeInUsd > BigInt(0))
        .map((position) => {
          const collateralToken = position.addresses.collateralToken.toLowerCase();
          const token = tokenMetas.get(collateralToken) ?? fallbackTokenMeta(collateralToken);
          const updatedAt = timestampFromSeconds(
            position.numbers.decreasedAtTime > BigInt(0)
              ? position.numbers.decreasedAtTime
              : position.numbers.increasedAtTime,
          );

          return {
            id: `${account.address}:${position.addresses.market}:${position.addresses.collateralToken}:${position.flags.isLong}`,
            accountType: account.type,
            accountAddress: account.address,
            accountLabel: account.label,
            marketAddress: position.addresses.market,
            marketName: marketNameFor(marketNames, position.addresses.market),
            side: position.flags.isLong ? "Long" : "Short",
            sizeUsd: usdNumber(position.numbers.sizeInUsd),
            collateralAmount: tokenNumber(position.numbers.collateralAmount, token.decimals),
            collateralSymbol: token.symbol,
            ...(updatedAt ? { updatedAt } : {}),
          };
        }),
    ),
    pendingOrders: accountResults.flatMap(({ account, orders }) =>
      orders.map((entry) => {
        const order = entry.order;
        const collateralToken = order.addresses.initialCollateralToken.toLowerCase();
        const token = tokenMetas.get(collateralToken) ?? fallbackTokenMeta(collateralToken);
        const updatedAt = timestampFromSeconds(order.numbers.updatedAtTime);

        return {
          id: entry.orderKey,
          orderKey: entry.orderKey,
          accountType: account.type,
          accountAddress: account.address,
          accountLabel: account.label,
          marketAddress: order.addresses.market,
          marketName: marketNameFor(marketNames, order.addresses.market),
          side: order.flags.isLong ? "Long" : "Short",
          orderType: orderTypeName(order.numbers.orderType),
          sizeUsd: usdNumber(order.numbers.sizeDeltaUsd),
          collateralAmount: tokenNumber(
            order.numbers.initialCollateralDeltaAmount,
            token.decimals,
          ),
          collateralSymbol: token.symbol,
          ...(updatedAt ? { updatedAt } : {}),
        };
      }),
    ),
  };
}

function getDataStore() {
  dataStorePromise ??= publicClient.readContract({
    address: GMX_ADDRESSES.exchangeRouter,
    abi: EXCHANGE_ROUTER_DATA_STORE_ABI,
    functionName: "dataStore",
  });
  return dataStorePromise;
}

async function getMarketNames() {
  marketNamePromise ??= fetch(`${GMX_API}/markets`, { cache: "no-store" })
    .then(async (response) => {
      if (!response.ok) throw new Error("Unable to fetch GMX markets");
      return (await response.json()) as { markets?: MarketMeta[] };
    })
    .then((payload) => {
      const names = new Map<string, string>();
      for (const market of payload.markets ?? []) {
        if (market.marketToken && market.name) {
          names.set(market.marketToken.toLowerCase(), market.name);
        }
      }
      return names;
    });
  return marketNamePromise;
}

async function resolveTokenMetas(tokens: Set<string>) {
  await Promise.all(
    Array.from(tokens).map(async (tokenAddress) => {
      if (tokenMetaCache.has(tokenAddress)) return;
      if (!isAddress(tokenAddress)) {
        tokenMetaCache.set(tokenAddress, fallbackTokenMeta(tokenAddress));
        return;
      }

      try {
        const [symbol, decimals] = await Promise.all([
          publicClient.readContract({
            address: tokenAddress as Address,
            abi: erc20Abi,
            functionName: "symbol",
          }),
          publicClient.readContract({
            address: tokenAddress as Address,
            abi: erc20Abi,
            functionName: "decimals",
          }),
        ]);
        tokenMetaCache.set(tokenAddress, { symbol, decimals });
      } catch {
        tokenMetaCache.set(tokenAddress, fallbackTokenMeta(tokenAddress));
      }
    }),
  );
  return tokenMetaCache;
}

function fallbackTokenMeta(address: string): TokenMeta {
  if (address.toLowerCase() === GMX_ADDRESSES.orderVault.toLowerCase()) {
    return { symbol: "WNT", decimals: 18 };
  }
  return { symbol: shortAddress(address), decimals: 18 };
}

function marketNameFor(markets: Map<string, string>, address: string) {
  return markets.get(address.toLowerCase()) ?? shortAddress(address);
}

function usdNumber(value: bigint) {
  return Number(formatUnits(value, USD_DECIMALS));
}

function tokenNumber(value: bigint, decimals: number) {
  return Number(formatUnits(value, decimals));
}

function timestampFromSeconds(value: bigint) {
  if (value <= BigInt(0)) return undefined;
  return new Date(Number(value) * 1000).toISOString();
}

function shortAddress(address: string) {
  if (address.length <= 12) return address;
  return `${address.slice(0, 6)}...${address.slice(-4)}`;
}

function orderTypeName(value: number) {
  switch (value) {
    case 0:
      return "Market swap";
    case 1:
      return "Limit swap";
    case 2:
      return "Market increase";
    case 3:
      return "Limit increase";
    case 4:
      return "Market decrease";
    case 5:
      return "Limit decrease";
    case 6:
      return "Stop loss decrease";
    case 7:
      return "Liquidation";
    case 8:
      return "Stop increase";
    default:
      return `Order ${value}`;
  }
}
