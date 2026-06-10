# Contract Deployments

## Arbitrum One

- Chain ID: `42161`
- Deployment script: `script/DeployStrategyVault.s.sol:DeployStrategyVault`
- Broadcast file: `broadcast/DeployStrategyVault.s.sol/42161/run-latest.json`

### Contracts (current — keeper + mandate, deployed 2026-06-10)

| Contract | Address |
| --- | --- |
| `StrategyVault` implementation (current, keeper + mandate) | `0xF33050467dDC712a35022297d1e31A7B8d7ad07A` |
| `VaultFactory` | `0xd335d60DF2B199Cc3E7438A79a2725F64bD29F3b` |
| `UpgradeableBeacon` | `0x637C3338D7FdE7092Aba28a6F98dc598D143CD78` |
| Initial vault | `0x42E69E9b8e196c182c01C17219004CF7B10F2954` |

### Beacon upgrades

| Date | New implementation | Tx | Notes |
| --- | --- | --- | --- |
| 2026-06-10 | `0xF33050467dDC712a35022297d1e31A7B8d7ad07A` | `0xf5ec63c74df6dd113d3f1c9a99d7a55a693c6fc0562b47188ee882bcb16cb261` | keeper role (`setKeeper`/`keeper`) + mandate (`setMandate`/`mandate`); decrease collateral-withdrawal is owner-only. Storage append-only (ERC-7201); existing vaults default to keeper=0 / unbounded mandate until owner opts in. |
| 2026-06-09 | `0xf04B711585360FD6b61e588369c651d1CF15dD2e` | `0x54b42d69bd49e32bbb28a7251a26a69015795346f269c2048d12efbe5700a9c7` | payable `deposit` (USDC + gas-tank top-up in one tx) |

Original impl at this factory: `0x5379ED99B967B1371FB01533E6d64c1AaD0425F3`.

### Transactions

| Step | Transaction Hash | Block | Gas Used | Paid |
| --- | --- | --- | --- | --- |
| Deploy `StrategyVault` implementation | `0xdadb5c297c1f3a9ea695b642a506f37dabbf724046cc551ee53c1067a4bdf224` | `471625292` | `2,400,760` | `0.00004802000152 ETH` |
| Deploy `VaultFactory` (+ beacon) | `0x39d61da84d900b15645a7bee4d1a95c4395033eade5a2dc52254c5dfecf039b4` | `471625295` | `857,709` | `0.000017166187926 ETH` |
| Create initial vault | `0x18a44614adcc879a1a614083502affab184daf6203cbe45c675d6939ecf4dcce` | `471625304` | `348,488` | `0.00000702551808 ETH` |

- Total gas used: `3,606,957`
- Total paid: `0.000072211707526 ETH`
- Average gas price: `0.020058666 gwei`

### Superseded (Model A — exact `msg.value`, no native sweep; do not use)

| Contract | Address |
| --- | --- |
| `StrategyVault` implementation | `0xF262e703644151cD30cc2cDaAA9e3cc12449619b` |
| `VaultFactory` | `0xA0884B15535A747739B7C4CD68808215053B0828` |
| Initial vault | `0x54e96718166Ec862Bb62Bc8A0d49F8B890f8CA00` |

### Deployment Inputs

| Input | Value |
| --- | --- |
| `STRATEGY_VAULT_ASSET` | `0xaf88d065e77c8cC2239327C5EDb3A432268e5831` |
| `GMX_EXCHANGE_ROUTER` | `0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41` |
| `GMX_ROUTER` | `0x7452c558d45f8afC8c83dAe62C3f8A5BE19c71f6` |
| `GMX_ORDER_VAULT` | `0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5` |

### Notes

- `STRATEGY_VAULT_ASSET` is native Circle USDC on Arbitrum One.
- `BEACON_OWNER` controls upgrades for all vault proxies created by this factory.
- The initial vault is owned by the `STRATEGY_VAULT_OWNER` used at deployment time.
