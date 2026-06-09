import { arbitrum } from "viem/chains";

export const STRATEGY_VAULT_CHAIN_ID = arbitrum.id;

export const STRATEGY_VAULT_ADDRESSES = {
  implementation: "0xF262e703644151cD30cc2cDaAA9e3cc12449619b",
  factory: "0xA0884B15535A747739B7C4CD68808215053B0828",
  initialVault: "0x54e96718166Ec862Bb62Bc8A0d49F8B890f8CA00",
  asset: "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
} as const;

export const GMX_ADDRESSES = {
  exchangeRouter: "0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41",
  router: "0x7452c558d45f8afC8c83dAe62C3f8A5BE19c71f6",
  orderVault: "0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5",
} as const;
