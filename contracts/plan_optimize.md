# Contracts Production Optimization Plan

## Scope

This plan covers the Solidity package in `contracts/`:

- `src/StrategyVault.sol` - upgradeable single-user collateral vault that executes bounded GMX v2 orders.
- `src/VaultFactory.sol` - factory that deploys beacon-proxy vaults and owns the shared beacon lifecycle through a configured beacon owner.
- `src/interfaces/IGmxV2ExchangeRouter.sol` - minimal GMX v2 exchange-router interface used by the vault.
- `script/DeployStrategyVault.s.sol` - development/testnet deployment script.
- `test/StrategyVault.t.sol` and `test/fork/GmxOrderFork.t.sol` - unit, fuzz-style, upgrade, and Arbitrum fork coverage.
- `foundry.toml` and `package.json` - Foundry config and package scripts.

Inputs used for this review:

- `solskill` production Solidity standards.
- `improve-codebase-architecture` architecture vocabulary: Module, Interface, Implementation, Depth, Seam, Adapter, Leverage, Locality.
- Local test run: `forge test` passes 60 tests.

This is not a replacement for a professional audit. Before mainnet deployment, budget for at least one independent audit plus a remediation round.

## Current State

The contracts are a good MVP foundation. They already include several production-minded choices:

- Strict pragma on production contracts.
- Custom errors instead of string `require`.
- `Ownable2StepUpgradeable`, `PausableUpgradeable`, and reentrancy protection.
- Beacon-proxy deployment with implementation initializers disabled.
- ERC-7201-style namespaced storage for the vault.
- Keeper separation from owner powers.
- GMX routing pinning for increase and decrease orders.
- Forced safe receivers for GMX proceeds, cancellation refunds, callbacks, and UI fees.
- Market allowlist, max per-order notional, per-order leverage guard, and keeper rebalance interval.
- Unit tests for custody, authorization, upgrades, mandate checks, GMX order creation, pause behavior, and factory behavior.
- Fork tests proving real GMX market-increase order submission on Arbitrum.

The production gap is not that the contracts are careless. The gap is that the current vault is still an MVP risk shell around a powerful trading Adapter. The keeper cannot withdraw funds, but it can still submit economically bad trades within the current mandate. The upgrade/admin process is not yet production-operationalized. CI and security tooling are not yet present. The largest Module, `StrategyVault`, mixes custody, mandate policy, GMX Adapter logic, routing config, emergency controls, and upgrade storage in one Implementation.

## Production Readiness Goals

1. Make a compromised keeper unable to cause unacceptable economic harm, not merely unable to withdraw collateral.
2. Make upgrade authority explicit, multisig/timelock-controlled, testable, and documented.
3. Make GMX integration failures local to a small Adapter seam with malicious-router tests.
4. Make core safety claims executable as fuzz and invariant tests.
5. Make deployment repeatable, verifiable, and safe by default.
6. Make the public contract Interface match actual behavior and product docs.
7. Keep the architecture small, but deepen the Modules where they increase Locality and reduce audit surface.

## Priority 0: Mainnet Blockers

These items should be complete before any mainnet deployment that controls real user collateral.

### 1. Add Price And Slippage Safety For Keeper Orders

Status: Completed

Files:

- `src/StrategyVault.sol`
- `src/interfaces/`
- `test/StrategyVault.t.sol`
- `test/fork/GmxOrderFork.t.sol`

Problem:

`acceptablePrice` is fully caller-supplied for both market increases and decreases. A compromised or buggy keeper cannot redirect collateral, but it can submit a conforming GMX order with a catastrophic acceptable price. That is an economic-loss path, not a withdrawal path, and it is the most important missing production guard.

Plan:

1. Add a mandate-level slippage control, likely `maxSlippageBps`, with sane min/max validation.
2. Add a price validation path for keeper orders.
3. Prefer one of these designs.

Design A: GMX Reader/Oracle validation.

- Add GMX Reader/DataStore/Oracle interfaces.
- Read current market price or validated GMX price context.
- Validate `acceptablePrice` against `maxSlippageBps` for long/short increase/decrease directions.
- Keep owner orders either exempt or separately checked based on product risk policy.

Design B: Owner-signed EIP-712 trade intent.

- Owner signs a bounded intent containing market, side, size, collateral, max slippage, expiry, nonce, and keeper.
- Keeper submits the signed intent with the GMX order.
- Contract verifies signature, expiry, nonce, and exact bounds before forwarding to GMX.
- This is a strong Seam if on-chain oracle reads are too expensive or too brittle.

Recommendation:

Start with Design B if the product already has a user confirmation/deploy flow. It creates a small Interface: owner-approved trade intent in, GMX order out. It has high Leverage and better Locality than embedding all market price logic on-chain. If autonomous keeper rebalancing without per-order owner signatures is required, add GMX Reader validation.

Acceptance criteria:

- Keeper cannot submit an increase or decrease with an `acceptablePrice` outside the mandate/intent bounds.
- Tests cover malicious keeper prices for long and short, increase and decrease.
- Fork tests prove valid prices still create real GMX orders.

### 2. Add Position-Aware Exposure And Leverage Controls

Files:

- `src/StrategyVault.sol`
- `src/interfaces/IGmxV2Reader.sol` or equivalent new interface
- `test/StrategyVault.t.sol`
- `test/invariant/`

Problem:

The current leverage guard is explicitly per-order:

- `sizeDeltaUsd <= maxLeverage * addedCollateralUsd`
- zero-collateral size-ups are rejected

This prevents the simplest bypass, but it does not cap total gross exposure, per-market exposure, running leverage, cumulative repeated increases, or concentration across markets.

Plan:

1. Add GMX Reader support to read current positions and pending orders for the vault.
2. Extend `Mandate` with production risk limits. Consider:

   ```solidity
   uint256 maxGrossExposureUsd;
   uint256 maxMarketExposureUsd;
   uint256 maxNetExposureUsd;
   uint256 maxLeverageBps;
   uint256 maxOpenOrders;
   ```

3. Validate each increase against current position plus the proposed delta.
4. Decide whether decreases are always allowed or whether some decrease fields still need price/slippage validation.
5. Add tests for repeated keeper increases that individually pass but collectively exceed the mandate.

Acceptance criteria:

- A sequence of valid keeper calls cannot move total exposure above mandate limits.
- Invariants encode this property across randomized call sequences.
- Fork tests verify the Reader interface against supported GMX deployments.

### 3. Lock `cancelGmxOrder` To Pinned GMX Routing

Files:

- `src/StrategyVault.sol`
- `test/StrategyVault.t.sol`

Problem:

`cancelGmxOrder` accepts any nonzero `exchangeRouter`. The keeper can make the vault call arbitrary contracts with `cancelOrder(bytes32)`. This probably cannot move ERC20 collateral directly because the caller supplies `msg.value`, but it is an unnecessary Seam leak and inconsistent with the create-order paths.

Plan:

1. Require `exchangeRouter == gmxExchangeRouter`.
2. Consider rejecting `orderKey == bytes32(0)`.
3. Add a test that keeper cancellation through an unpinned exchange router reverts.
4. Add a happy-path test that pinned cancellation still works.

Acceptance criteria:

- Keeper can only cancel through the canonical pinned GMX exchange router.
- Unauthorized router calls are impossible through the keeper path.

### 4. Make Upgrade Authority Production-Safe

Files:

- `src/VaultFactory.sol`
- `script/DeployStrategyVault.s.sol`
- `test/StrategyVault.t.sol`
- new deployment docs/runbooks

Problem:

The beacon architecture gives one upgrade authority control over every vault. That has high operational Leverage, but it is also a systemic risk. The code comments correctly say the beacon owner should be a Timelock + multisig, but the deploy script defaults `BEACON_OWNER` to the vault owner.

Plan:

1. Make production deployment reject unsafe beacon ownership.
2. Require `BEACON_OWNER` explicitly for non-local chains.
3. Reject `BEACON_OWNER == deployer` and preferably `BEACON_OWNER == STRATEGY_VAULT_OWNER` on production chains.
4. Define the actual production admin stack: multisig, timelock delay, emergency pause owner, and upgrade proposal flow.
5. Add a post-deploy verification script that checks implementation, beacon, factory, beacon owner, asset, vault owner, and code lengths.
6. Add storage-layout diffing in CI before any upgrade.

Acceptance criteria:

- Mainnet deployment cannot accidentally use an EOA/deployer as beacon owner.
- Upgrade process is documented and tested on a fork.
- Storage compatibility is automatically checked for new implementations.

### 5. Add Security CI Before Mainnet

Files:

- `.github/workflows/`
- `contracts/package.json`
- `contracts/foundry.toml`

Problem:

There is no contracts CI workflow today. Existing workflows target app deployment and agent evals. Security checks must run continuously before the contracts become production-critical.

Plan:

1. Add a contracts workflow triggered by changes under `contracts/**`.
2. Run:
   
   ```sh
   pnpm --filter @agent-invest/contracts fmt:check
   pnpm --filter @agent-invest/contracts build
   pnpm --filter @agent-invest/contracts test
   forge build --sizes
   slither .
   ```

3. Add Aderyn as a second static analyzer if setup friction is low.
4. Add a high-fuzz nightly or manual workflow profile.
5. Add a separate fork-test job that requires `ARBITRUM_RPC_URL` and fails if it is missing.
6. Add storage-layout diff as a required upgrade check.

Acceptance criteria:

- PRs changing contracts run format, build, unit tests, static analysis, size checks, and storage checks.
- Fork tests are either clearly separate or required in release branches.

## Priority 1: Core Architecture Improvements

### 6. Deepen The GMX Adapter Seam

Files:

- `src/StrategyVault.sol`
- `src/interfaces/IGmxV2ExchangeRouter.sol`
- new `src/gmx/` or `src/libraries/` files

Problem:

`StrategyVault` is currently one very broad Module. It owns custody, access, mandates, GMX order construction, GMX external calls, pause behavior, arbitrary execution, and upgrade storage. This concentrates audit attention in one place, but it also reduces Locality: changing GMX struct shape or mandate policy requires editing the same file that holds custody logic.

Plan:

1. Keep `StrategyVault` as the custody and authorization Module.
2. Extract GMX order parameter construction into a small Adapter Module, for example `GmxV2OrderAdapter`.
3. Extract pure mandate validation into a library only if it clearly increases Locality. Do not create a pass-through Module just for style.
4. Keep storage ownership in `StrategyVault`; pass only the needed values into the Adapter.
5. Keep the external user Interface narrow: `createGmxMarketIncreaseOrder`, `createGmxMarketDecreaseOrder`, `cancelGmxOrder`.

Architecture target:

- `StrategyVault` Interface remains small and user-facing.
- GMX Adapter Implementation hides GMX struct construction and multicall result validation.
- Mandate validation is locally testable without duplicating custody setup.

Acceptance criteria:

- Deleting the Adapter would move GMX encoding and response validation back into multiple functions, so the Module has real Depth.
- Tests for malicious GMX responses target the Adapter behavior through the vault Interface.

### 7. Remove Ignored Fields From External Order Structs

Files:

- `src/StrategyVault.sol`
- agent/frontend callers if any
- tests

Problem:

`GmxMarketIncreaseOrder` and `GmxMarketDecreaseOrder` accept fields that the vault intentionally ignores or overrides:

- `receiver`
- `cancellationReceiver`
- `callbackContract`
- `uiFeeReceiver`
- `callbackGasLimit`

The overriding behavior is correct for safety, but accepting ignored fields makes the external Interface more complex than the Implementation. It can confuse frontend/backend integrations and auditors.

Plan:

1. Remove fields the caller cannot influence.
2. Keep only fields actually used to construct GMX orders.
3. Update tests to assert forced receiver/callback/UI-fee behavior through the built GMX params.
4. Update off-chain order builders to stop sending ignored fields.

Acceptance criteria:

- External order structs only expose meaningful user/keeper-controlled fields.
- Forced-safe GMX fields remain fixed inside the vault/Adapter.

### 8. Resolve The ERC-4626 Interface Mismatch

Files:

- `src/StrategyVault.sol`
- repo/product docs
- frontend integrations

Problem:

The repo-level description says `StrategyVault` is an ERC-4626 collateral vault, but the contract does not implement ERC-4626. It is a single-owner custody vault with `deposit(uint256)` and `withdraw(uint256,address)`, no shares, no `totalAssets`, no `convertToShares`, and no standard ERC-4626 Interface.

Plan:

Choose one:

1. If ERC-4626 is not required, update docs and naming to avoid implying standard vault semantics.
2. If ERC-4626 is required, implement it intentionally using OpenZeppelin ERC-4626 upgradeable patterns and handle single-owner restrictions carefully.

Recommendation:

Do not implement ERC-4626 unless there is a concrete integration that needs it. For a single-user self-custody vault with GMX positions and no share market, ERC-4626 may add shallow Interface complexity without useful Leverage.

Acceptance criteria:

- External docs accurately describe the deployed Interface.
- Integrators do not expect ERC-4626 behavior unless it is actually implemented.

### 9. Add Explicit Asset Configuration

Files:

- `src/VaultFactory.sol`
- `src/StrategyVault.sol`
- tests

Problem:

The factory accepts any `IERC20`, while the vault assumes stable collateral and computes USD scale from decimals. Most unit tests use an 18-decimal mock, but the intended production asset is likely 6-decimal USDC on Arbitrum.

Plan:

1. Add 6-decimal mock asset tests for all leverage and collateral scaling behavior.
2. Add asset code-length validation during initialization/factory creation.
3. Add a production asset allowlist in the factory or a chain-specific deploy config.
4. Consider an `AssetConfig` record containing token address, decimals, USD scale, GMX support, and enabled status.
5. Reject fee-on-transfer/rebasing/non-standard collateral unless explicitly supported.

Acceptance criteria:

- Unit tests cover 6-decimal USDC-style collateral.
- Production factory cannot deploy vaults for arbitrary unsupported tokens.
- Collateral USD scaling is tested at realistic token decimals.

## Priority 2: Testing And Verification

### 10. Add Invariant Tests For The Safety Model

Files:

- new `test/invariant/StrategyVaultInvariant.t.sol`
- `test/handlers/` if needed

Problem:

Current tests cover many individual paths, but the core claims are sequence properties. The keeper safety model should be encoded as invariants.

Plan:

Add Foundry invariant tests with a handler that randomly calls owner, keeper, and attacker actions.

Suggested invariants:

- Keeper can never withdraw ERC20 collateral.
- Keeper can never call `execute`.
- Keeper can never set routing, mandate, markets, owner, pause state, or beacon owner.
- Keeper-created orders always use pinned `exchangeRouter`, `router`, `orderVault`, and collateral asset.
- Keeper-created orders always force receiver and cancellation receiver to the vault.
- Keeper-created orders always force callback and UI fee receiver to zero.
- Keeper-created increases cannot exceed size, leverage, slippage, exposure, or interval limits.
- Pause blocks trading/execution but does not block owner withdrawal of idle collateral.
- Upgrades preserve owner, asset, keeper, mandate, markets, routing, and balances.

Acceptance criteria:

- Invariants run in CI with a reasonable budget.
- High-budget invariant runs are available locally or nightly.

### 11. Add Malicious GMX Adapter Tests

Files:

- `test/StrategyVault.t.sol` or new focused test file
- mock malicious routers/exchange routers

Problem:

The current GMX mocks are cooperative. Production tests should include hostile Adapters because GMX calls are the main external Seam.

Plan:

Add malicious mocks that attempt to:

- Reenter `withdraw`.
- Reenter `createGmxMarketIncreaseOrder`.
- Return too few multicall results.
- Return malformed order keys.
- Consume partial token allowance.
- Consume no token allowance.
- Revert after approval is set.
- Return an order key while not transferring tokens.

Acceptance criteria:

- Reentrancy attempts fail.
- Malformed GMX responses become clear custom errors.
- Token allowance behavior after success and failure is explicitly tested and documented.

### 12. Extend Fork Coverage

Files:

- `test/fork/GmxOrderFork.t.sol`
- package scripts

Problem:

Fork tests currently prove real GMX increase order creation and some mandate reverts. Decrease and cancellation flows are not covered against real GMX.

Plan:

1. Add a fork test for canceling a real pending order created by the vault.
2. Add a fork test for market decrease if practical.
3. If a filled position is hard to create in a deterministic fork test, document the limitation and test the closest feasible GMX acceptance path.
4. Split fork tests into an explicit package script, for example `test:fork`.
5. Ensure CI fork tests fail loudly if `ARBITRUM_RPC_URL` is missing.

Acceptance criteria:

- Real GMX cancel path is tested.
- Real GMX decrease path is tested or explicitly documented as requiring a non-deterministic/live setup.

### 13. Add Storage Layout Compatibility Checks

Files:

- `foundry.toml`
- CI workflow
- `test/StrategyVault.t.sol`

Problem:

Storage layout output is enabled and one upgrade test verifies basic preservation, but there is no automated layout diff against deployed versions.

Plan:

1. Generate and commit a baseline storage layout for deployed versions.
2. Add CI diffing for `StrategyVaultStorage` compatibility.
3. Add tests with V2/V3 implementations that append fields inside the ERC-7201 namespace.
4. Document storage extension rules next to `StrategyVaultStorage`.

Acceptance criteria:

- Unsafe storage changes fail CI before review.
- Upgrade tests cover appended storage and initializer/reinitializer behavior.

## Priority 3: Operational Hardening

### 14. Split Production Deployment From Testnet Deployment

Files:

- `script/DeployStrategyVault.s.sol`
- new production scripts
- deployment docs

Problem:

The current deploy script is useful for local/testnet work but too permissive for production. It defaults owner from deployer context, defaults beacon owner to owner, accepts any asset, does not configure GMX routing, does not configure mandate, and does not write a deployment manifest.

Plan:

1. Keep the existing script for local/testnet use or rename it accordingly.
2. Add a production deploy script that requires all critical addresses explicitly.
3. Add chain ID checks for supported networks.
4. Add code-length checks for asset and GMX addresses.
5. Add optional setup calls for routing, mandate, markets, and keeper.
6. Write or print a deployment manifest containing chain ID, implementation, factory, beacon, beacon owner, vault, owner, asset, GMX routing, and transaction hashes.
7. Add verification commands to the runbook.

Acceptance criteria:

- Production deployment cannot proceed with default owner/admin assumptions.
- A reviewer can reproduce and verify every deployed address.

### 15. Add Explicit Native ETH Recovery

Files:

- `src/StrategyVault.sol`
- tests

Problem:

The vault can receive native ETH, and GMX execution-fee refunds may leave ETH in the vault. Owner recovery is possible through `execute`, but that is not obvious and requires arbitrary call usage.

Plan:

1. Add `withdrawNative(address payable to, uint256 amount)` owner-only.
2. Add `NativeReceived` and `NativeWithdrawn` events if useful for indexing.
3. Keep withdrawal possible while paused, matching ERC20 idle withdrawal semantics.
4. Add tests for accidental ETH recovery and zero-address/zero-amount reverts.

Acceptance criteria:

- Owner can recover native ETH without using arbitrary `execute`.
- Pause does not trap native ETH.

### 16. Refine Pause And Emergency Controls

Files:

- `src/StrategyVault.sol`
- tests

Problem:

Pause currently blocks owner `execute`, increases, decreases, and cancels. That is simple, but during an incident the owner may need to cancel orders or decrease positions while preventing new increases.

Plan:

1. Decide emergency policy explicitly.

Option A: keep current global pause for simplicity.

Option B: split pause into `pauseIncreases`, `pauseKeeper`, and `pauseAllGmx`.

Option C: allow owner-only decrease/cancel while paused.

2. Add a guardian role only if there is a concrete operational owner for it.
3. Test emergency de-risking behavior.

Recommendation:

Use Option C first. It preserves simplicity while allowing the owner to reduce risk during an incident.

Acceptance criteria:

- Incident runbook says exactly which actions are allowed while paused.
- Tests enforce those semantics.

### 17. Make Factory Registry More Useful

Files:

- `src/VaultFactory.sol`
- tests

Problem:

The factory records `vaults` and `vaultOwner`, but not reverse indexes or asset metadata. Off-chain indexing can reconstruct this from events, but direct frontend queries are less ergonomic.

Plan:

Only add these if the frontend or backend needs direct reads:

- `mapping(address owner => address[] vaults) ownerVaults`.
- `mapping(address vault => address asset) vaultAsset`.
- Optional uniqueness guard for one vault per owner/asset.
- `FactoryDeployed` or config event with implementation, beacon, and beacon owner.
- Optional `CREATE2` deployment if predictable vault addresses become useful.

Acceptance criteria:

- Factory additions solve real integration needs, not speculative indexing.
- No unnecessary storage is added if events are sufficient.

## Priority 4: Style, Docs, And Standards

### 18. Complete NatSpec And Security Contact

Files:

- `src/StrategyVault.sol`
- `src/VaultFactory.sol`
- interfaces

Problem:

Top-level documentation is strong, but external functions need complete `@param`, `@return`, and security notes. Production contracts should also publish a security contact.

Plan:

1. Add `@custom:security-contact` to production contracts.
2. Add NatSpec for all external/public functions.
3. Document GMX units: `sizeDeltaUsd` is 1e30 USD, collateral amount uses token decimals.
4. Document owner compromise, keeper compromise, beacon owner compromise, and pause semantics.
5. Document which functions are safe for keeper and which are owner-only.

Acceptance criteria:

- An auditor can understand trust assumptions from the contracts and docs without reading frontend code.

### 19. Align Solidity Style With Production Standards

Files:

- all Solidity files

Plan:

1. Prefer absolute/named imports consistently. Current project imports mostly use remapped paths; avoid relative imports such as `./interfaces/...` if adopting the stricter standard.
2. Keep strict pragma for production contracts; floating pragma is fine for tests, abstract contracts, interfaces, and scripts.
3. Ensure function ordering follows:
   
   ```text
   constructor
   receive/fallback
   user-facing state-changing
   user-facing read-only
   internal state-changing
   internal read-only
   ```

4. Keep `nonReentrant` before other modifiers.
5. Avoid unused accepted calldata fields.
6. Avoid storage reads repeated by modifiers and function bodies where meaningful.

Acceptance criteria:

- Style is consistent and enforced by formatter/linter.
- Changes reduce audit friction without adding abstractions.

### 20. Document Chain Support And `ReentrancyGuardTransient`

Files:

- `src/StrategyVault.sol`
- `foundry.toml`
- deployment docs

Problem:

`ReentrancyGuardTransient` depends on transient storage support and `foundry.toml` targets Cancun. This must be explicitly valid for every deployment chain.

Plan:

1. Document supported chains and hardfork/EIP-1153 assumptions.
2. Confirm support on Arbitrum and any other GMX target chain.
3. If a target chain lacks support, switch to standard upgradeable reentrancy guard for that deployment line.
4. Add fork/build checks for supported chains.

Acceptance criteria:

- Deployment docs state exactly why transient reentrancy guard is safe on each supported chain.

## Suggested Implementation Order

1. Patch immediate low-risk safety issue: lock `cancelGmxOrder` to pinned routing.
2. Add 6-decimal asset tests and malicious GMX response tests.
3. Decide keeper price-safety design: GMX Reader validation or EIP-712 owner-signed intents.
4. Implement price/slippage safety.
5. Implement exposure/leverage controls or signed-intent limits.
6. Add invariant test suite for keeper safety claims.
7. Add contracts CI with forge, Slither/Aderyn, size checks, and storage layout checks.
8. Harden production deploy scripts and runbook.
9. Deepen GMX Adapter seam and remove ignored order fields.
10. Resolve ERC-4626 documentation mismatch.
11. Add native ETH recovery and refine pause semantics.
12. Complete NatSpec, security contact, and style cleanup.
13. Schedule audit after the security model and public Interface stop changing.

## Definition Of Done For Production Candidate

- All unit, fuzz, invariant, and fork tests pass.
- Static analysis findings are reviewed, remediated, or explicitly documented as false positives.
- Storage-layout compatibility is enforced in CI.
- Deployment scripts reject unsafe production admin defaults.
- Beacon owner is a multisig/timelock from first production deployment.
- Keeper orders have enforceable price/slippage and exposure bounds.
- Cancel/decrease emergency paths are documented and tested.
- Public docs match the actual contract Interface.
- Audit completed and remediation verified.
