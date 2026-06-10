# StrategyVault — Security Invariants

`StrategyVault` is a **single-depositor** (no shares, not ERC-4626) upgradeable collateral
vault that executes owner- or keeper-approved GMX v2 perp orders. Strategy intelligence stays
off-chain; the vault only **custodies** funds and **constrains** execution.

A smart contract only earns its place over an EOA / smart wallet when it enforces something a
key cannot. This vault makes three guarantees its owner cannot violate even by mistake, and a
delegated keeper cannot violate even if its key is compromised.

## Roles

| Role | Who | Can do | Cannot do |
| --- | --- | --- | --- |
| **Owner** | the sole depositor/beneficiary (`Ownable2Step`) | everything below + deposit/withdraw, set routing, set keeper, set mandate, pause | — |
| **Keeper** | an off-chain agent bot, optional, owner-settable | open / adjust / close / cancel GMX orders **within the mandate** | move funds, change config, raise the mandate |
| **Anyone** | — | `depositNative` (top up the gas tank only) | everything else |

## Invariants

### I1 — Custody/execution separation: the keeper can execute but can never exfiltrate
Every fund-exit and configuration path is `onlyOwner`: `withdraw`, `withdrawNative`,
`setGmxRouting`, `setKeeper`, `setMandate`, `pause`/`unpause`. The keeper is gated only on the
GMX order paths (`onlyOwnerOrKeeper`). Therefore a delegated (or compromised) keeper can move
positions but cannot remove a single token of collateral or native ETH, and cannot widen its
own authority.
*Tested:* `test_KeeperCannotExfiltrateOrReconfigure`, `test_KeeperCan*`, `test_StrangerCannotCreateOrder`.

### I2 — Funds can only ever flow back to the vault
GMX order `receiver` and `cancellationReceiver` are hard-pinned to `address(this)` in
`GmxOrderBuilder` — they are never taken from caller input. Collateral, payouts, and execution-fee
refunds always return to the vault, from which only the owner can withdraw. This holds
regardless of who submitted the order (owner or keeper).
*Tested:* `test_KeeperCanCreateIncreaseOrder` (asserts `receiver == vault`), `testFork_CanSubmit*`.

### I3 — Routing is trusted-only
Every order's `exchangeRouter` / `router` / `orderVault` / `collateralToken` must equal the
owner-configured routing (`_requireTrusted*`). An attacker cannot redirect the multicall to a
malicious router/order-vault to drain approvals. `cancelGmxOrder` likewise rejects any
exchangeRouter other than the configured one.
*Tested:* `test_GmxOrderRevertsOnUntrustedRouting`, `test_CancelGmxOrderRevertsOnUntrustedRouting`.

### I4 — Commitment device: increases are bounded by the mandate
`_requireWithinMandate` constrains **every** increase order (owner and keeper alike) by two
opt-in bounds:
- **Market allowlist** — if non-empty, `order.market` must be in the set. Empty set = any market
  (e.g. a momentum strategy that trades the whole screened universe).
- **Leverage ceiling** — if `maxLeverageBps != 0`, the order must satisfy
  `sizeDeltaUsd * 10_000 <= maxLeverageBps * collateralUsd30`, and must add collateral
  (`collateralAmount > 0`) so size can never be added against zero margin (the "zero-collateral
  leverage bypass"). Collateral is valued 1:1 in USD (the asset is a USD stablecoin), so no price
  oracle is required.

*Scope:* this bounds each order's **own** leverage, not cumulative position leverage across
multiple increases (which would require a GMX Reader call — intentionally out of scope).
*Tested:* `test_IncreaseRevertsWhenMarketNotAllowed`, `test_IncreaseSucceedsWhenMarketAllowed`,
`test_IncreaseRevertsWhenLeverageTooHigh`, `test_IncreaseSucceedsAtLeverageCeiling`,
`test_IncreaseRevertsOnZeroCollateralWhenLeverageBounded`, `test_KeeperIncreaseRespectsMandate`.

### I5 — De-risking is always permitted; re-levering is not (the asymmetry)
Size-reducing/closing decreases and cancels are **never** mandate-constrained — the agent must
always be able to reduce or exit a position, even in a market later removed from the allowlist or
one now over the leverage ceiling. **But not every decrease is a de-risk:** a GMX decrease with
`sizeDeltaUsd == 0, collateralWithdrawalAmount > 0` withdraws margin without reducing size, which
*raises* leverage. Such bare collateral withdrawals are therefore restricted to the **owner**
(`_requireDecreaseAllowed`); a keeper that could do this would defeat I4 by opening at-cap and then
stripping margin. The keeper keeps full ability to reduce/close size (collateral returns to the
vault on close). "Can always save you, never overextend you" — enforced, not assumed.
*Tested:* `test_DecreaseIgnoresMandate`, `test_KeeperCanCreateDecreaseOrder` (pure size reduction),
`test_KeeperCannotWithdrawCollateralViaDecrease`, `test_OwnerCanWithdrawCollateralViaDecrease`,
`test_KeeperCanCancelOrder`.

### I6 — Upgrade-safe storage
State lives in a single ERC-7201 namespaced struct at a fixed slot. The keeper/mandate fields
were **appended** to the end of `StrategyVaultStorage`; existing beacon vaults preserve their
storage across the upgrade. Reordering these fields would corrupt every deployed vault and is
forbidden (see the `@dev` note on the struct).
*Tested:* `test_BeaconUpgradeSwapsLogicAndPreservesStorage`.

## Known non-goals / accepted risks

- **No cumulative-leverage tracking** (see I4 scope). Per-order bounding only.
- **`VaultFactory.createVault` is permissionless** — anyone can deploy a vault for any owner.
  No funds are at risk (each vault is initialized with its real owner); only matters to an
  off-chain indexer, which must filter by real `owner()` rather than trust `VaultCreated` events.
- **Keeper-set fill prices are unbounded.** `acceptablePrice` (increase/decrease) and
  `minOutputAmount`/`decreasePositionSwapType` (decrease) are passed through without an on-chain
  reference band. A compromised keeper can submit loose fills; this is an accepted residual of the
  keeper trust surface (a price band would require the price oracle this design deliberately avoids,
  and GMX v2 fills against oracle prices rather than AMM spot). Mitigation if needed: owner
  pre-commits price bounds, or rotate the keeper (`setKeeper(0)` reverts to owner-only).
- **Pooled / multi-depositor / on-chain NAV is explicitly out of scope.** Single-depositor is a
  deliberate choice to avoid the valuation/dilution/oracle-arbitrage surface.
