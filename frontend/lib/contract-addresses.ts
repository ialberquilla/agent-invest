import { arbitrum } from "viem/chains";

export const STRATEGY_VAULT_CHAIN_ID = arbitrum.id;

export const STRATEGY_VAULT_ADDRESSES = {
  // Arbitrum One. `implementation` reflects the latest beacon impl (keeper + mandate, 2026-06-10).
  // Factory/beacon/vault addresses are stable across beacon upgrades.
  implementation: "0xF33050467dDC712a35022297d1e31A7B8d7ad07A",
  factory: "0xd335d60DF2B199Cc3E7438A79a2725F64bD29F3b",
  beacon: "0x637C3338D7FdE7092Aba28a6F98dc598D143CD78",
  initialVault: "0x42E69E9b8e196c182c01C17219004CF7B10F2954",
  asset: "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
} as const;

export const GMX_ADDRESSES = {
  exchangeRouter: "0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41",
  router: "0x7452c558d45f8afC8c83dAe62C3f8A5BE19c71f6",
  orderVault: "0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5",
} as const;
