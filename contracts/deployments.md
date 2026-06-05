# Contract Deployments

## Arbitrum One

- Chain ID: `42161`
- Deployment script: `script/DeployStrategyVault.s.sol:DeployStrategyVault`
- Broadcast file: `broadcast/DeployStrategyVault.s.sol/42161/run-latest.json`

### Contracts

| Contract | Address |
| --- | --- |
| `StrategyVault` implementation | `0xF262e703644151cD30cc2cDaAA9e3cc12449619b` |
| `VaultFactory` | `0xA0884B15535A747739B7C4CD68808215053B0828` |
| Initial vault | `0x54e96718166Ec862Bb62Bc8A0d49F8B890f8CA00` |

### Transactions

| Step | Transaction Hash | Block | Gas Used | Paid |
| --- | --- | --- | --- | --- |
| Deploy `StrategyVault` implementation | `0x2587a8472367227020be97b5ed38fe4229527de6f059d3f928f2d19898c1fcaf` | `470370988` | `6,285,454` | `0.000137538304428 ETH` |
| Deploy `VaultFactory` | `0x20dfb829f0651421b6e680e0c954dcc0513fb4cbdae11ba4a04f81ec2900f6be` | `470370991` | `2,694,835` | `0.00005862883026 ETH` |
| Create initial vault | `0xc1f3b5ea0f1557f80185f7bddd7251ac49b648dc85a886851b20aad48a0a648c` | `470370993` | `485,243` | `0.000010387111658 ETH` |

- Total gas used: `9,465,532`
- Total paid: `0.000206554246346 ETH`
- Average gas price: `0.021681333 gwei`

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
