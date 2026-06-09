"use client";

import { PrivyProvider } from "@privy-io/react-auth";
import { arbitrum, arbitrumSepolia } from "viem/chains";

import { STRATEGY_VAULT_CHAIN_ID } from "@/lib/deploy-vault";

const WALLET_CHAINS = [arbitrum, arbitrumSepolia];
const DEFAULT_CHAIN =
  WALLET_CHAINS.find((chain) => chain.id === STRATEGY_VAULT_CHAIN_ID) ?? arbitrum;

export function Providers({ children }: { children: React.ReactNode }) {
  const appId = process.env.NEXT_PUBLIC_PRIVY_APP_ID;

  if (!appId) return <>{children}</>;

  return (
    <PrivyProvider
      appId={appId}
      config={{
        loginMethods: ["email", "wallet", "google"],
        defaultChain: DEFAULT_CHAIN,
        supportedChains: WALLET_CHAINS,
        embeddedWallets: {
          ethereum: { createOnLogin: "users-without-wallets" },
        },
      }}
    >
      {children}
    </PrivyProvider>
  );
}
