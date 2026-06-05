// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {IERC20Metadata} from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Metadata.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {ECDSA} from "@openzeppelin/contracts/utils/cryptography/ECDSA.sol";
import {EnumerableSet} from "@openzeppelin/contracts/utils/structs/EnumerableSet.sol";
import {ReentrancyGuardTransient} from "@openzeppelin/contracts/utils/ReentrancyGuardTransient.sol";
import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {Ownable2StepUpgradeable} from "@openzeppelin/contracts-upgradeable/access/Ownable2StepUpgradeable.sol";
import {PausableUpgradeable} from "@openzeppelin/contracts-upgradeable/utils/PausableUpgradeable.sol";
import {EIP712Upgradeable} from "@openzeppelin/contracts-upgradeable/utils/cryptography/EIP712Upgradeable.sol";

import {IGmxV2ExchangeRouter} from "./interfaces/IGmxV2ExchangeRouter.sol";
import {StrategyVaultBase} from "./StrategyVaultBase.sol";
import {GmxOrderBuilder} from "./lib/GmxOrderBuilder.sol";

/**
 * @title StrategyVault
 * @notice Single-user, self-custody collateral vault for one Agent Invest strategy.
 * @dev Upgradeable (beacon proxy). The owner is the sole depositor; the vault holds USDC as
 *      collateral and executes GMX v2 perp orders. Strategy intelligence stays offchain.
 *
 *      Mandate enforcement (the core idea): the offchain AI agent runs as a bounded `keeper`. The
 *      owner commits a `Mandate` on-chain (allowed markets, max leverage, max per-order notional,
 *      keeper rebalance interval, max keeper slippage) and the contract enforces it on every GMX
 *      order. The keeper can ONLY submit conforming GMX orders — it can never move assets out
 *      (deposit/withdraw/execute stay owner-only). Even a fully compromised agent cannot exceed
 *      the user-approved limits.
 *
 *      Enforcement is risk-asymmetric: increases (taking on exposure) get the full mandate check;
 *      decreases (de-risking) only require an allowed market; keeper increases and decreases also
 *      require an owner-signed EIP-712 price intent. Cancels are unrestricted. The rebalance
 *      interval throttles the keeper only, never the human owner.
 *
 *      Routing is pinned: the owner sets the canonical GMX `exchangeRouter`/`router`/`orderVault`
 *      via {setGmxRouting} and every keeper order is validated against them + the vault asset, and
 *      all fund-relevant GMX addresses (receiver, cancellationReceiver, uiFeeReceiver, callback) are
 *      forced to the vault / zero — so the keeper cannot redirect collateral, PnL, or approvals.
 *
 *      v1 limitations (deferred to position-aware checks that need a GMX Reader read):
 *      - `maxLeverage` is a per-order ratio of added collateral to size delta, assuming $1 stable
 *        collateral; it does not track total position leverage. Zero-collateral size-ups are rejected
 *        so the cap cannot be bypassed, but the true running leverage of a position is not yet read.
 *      - No total gross-exposure cap and no autonomous oracle-based slippage bound yet; keeper
 *        price bounds rely on an owner-signed offchain reference price with a short expiry.
 */
contract StrategyVault is
    StrategyVaultBase,
    Initializable,
    Ownable2StepUpgradeable,
    PausableUpgradeable,
    EIP712Upgradeable,
    ReentrancyGuardTransient
{
    using SafeERC20 for IERC20;
    using EnumerableSet for EnumerableSet.AddressSet;

    /// @dev GMX orders may be submitted by the owner or the bounded keeper; both are mandate-checked.
    modifier onlyOwnerOrKeeper() {
        if (msg.sender != owner() && msg.sender != _strategyVaultStorage().keeper) {
            revert StrategyVault__NotAuthorized();
        }
        _;
    }

    constructor() {
        _disableInitializers();
    }

    function initialize(IERC20 asset_, address owner_) external initializer {
        if (address(asset_) == address(0)) revert StrategyVault__ZeroAsset();
        if (owner_ == address(0)) revert StrategyVault__ZeroOwner();

        __Ownable_init(owner_);
        __Ownable2Step_init();
        __Pausable_init();
        __EIP712_init("AgentInvestStrategyVault", "1");

        uint8 decimals = IERC20Metadata(address(asset_)).decimals();
        if (decimals > 30) revert StrategyVault__UnsupportedCollateralDecimals();

        StrategyVaultStorage storage $ = _strategyVaultStorage();
        $.asset = asset_;
        $.collateralUsdScale = 10 ** (30 - decimals);
    }

    receive() external payable {}

    /*//////////////////////////////////////////////////////////////
                                  VIEWS
    //////////////////////////////////////////////////////////////*/

    /// @notice The USDC collateral asset this vault holds.
    function asset() external view returns (IERC20) {
        return _strategyVaultStorage().asset;
    }

    /// @notice Idle (undeployed) collateral sitting in the vault, available to withdraw.
    function idleBalance() public view returns (uint256) {
        return _strategyVaultStorage().asset.balanceOf(address(this));
    }

    /// @notice The bounded keeper (the offchain agent), or address(0) if none.
    function keeper() external view returns (address) {
        return _strategyVaultStorage().keeper;
    }

    /// @notice The canonical GMX routing the keeper is pinned to.
    function gmxRouting() external view returns (address exchangeRouter, address router, address orderVault) {
        StrategyVaultStorage storage $ = _strategyVaultStorage();
        return ($.gmxExchangeRouter, $.gmxRouter, $.gmxOrderVault);
    }

    /// @notice The owner-committed mandate limits.
    function mandate() external view returns (Mandate memory) {
        return _strategyVaultStorage().mandate;
    }

    /// @notice Whether `market` is on the mandate allowlist.
    function isMarketAllowed(address market) external view returns (bool) {
        return _strategyVaultStorage().allowedMarkets.contains(market);
    }

    /// @notice All allowed GMX markets.
    function allowedMarkets() external view returns (address[] memory) {
        return _strategyVaultStorage().allowedMarkets.values();
    }

    /// @notice Whether a keeper order intent nonce has already been consumed.
    function isKeeperOrderIntentNonceUsed(uint256 nonce) external view returns (bool) {
        return _strategyVaultStorage().usedKeeperOrderIntentNonces[nonce];
    }

    /*//////////////////////////////////////////////////////////////
                              OWNER CONFIG
    //////////////////////////////////////////////////////////////*/

    /// @notice Set (or clear, with address(0)) the bounded keeper.
    function setKeeper(address keeper_) external onlyOwner {
        _strategyVaultStorage().keeper = keeper_;
        emit KeeperUpdated(keeper_);
    }

    /// @notice Pin the canonical GMX v2 routing. Keeper orders must match these exactly, so a
    ///         compromised keeper cannot redirect collateral/approvals to attacker contracts.
    function setGmxRouting(address exchangeRouter, address router, address orderVault) external onlyOwner {
        if (exchangeRouter == address(0) || router == address(0) || orderVault == address(0)) {
            revert StrategyVault__ZeroAddress();
        }
        StrategyVaultStorage storage $ = _strategyVaultStorage();
        $.gmxExchangeRouter = exchangeRouter;
        $.gmxRouter = router;
        $.gmxOrderVault = orderVault;
        emit GmxRoutingUpdated(exchangeRouter, router, orderVault);
    }

    /// @notice Commit the mandate scalar limits.
    function setMandate(Mandate calldata mandate_) external onlyOwner {
        if (
            mandate_.maxLeverage == 0 || mandate_.maxPositionSizeUsd == 0
                || mandate_.maxKeeperSlippageBps > MAX_KEEPER_SLIPPAGE_BPS
        ) revert StrategyVault__InvalidMandate();
        _strategyVaultStorage().mandate = mandate_;
        emit MandateUpdated(
            mandate_.maxLeverage,
            mandate_.maxPositionSizeUsd,
            mandate_.minRebalanceInterval,
            mandate_.maxKeeperSlippageBps
        );
    }

    /// @notice Add a GMX market to the mandate allowlist.
    function addMarket(address market) external onlyOwner {
        if (market == address(0)) revert StrategyVault__ZeroAddress();
        StrategyVaultStorage storage $ = _strategyVaultStorage();
        if ($.allowedMarkets.length() >= MAX_ALLOWED_MARKETS) revert StrategyVault__MaxMarketsExceeded();
        if ($.allowedMarkets.add(market)) emit MarketAllowed(market);
    }

    /// @notice Remove a GMX market from the mandate allowlist.
    function removeMarket(address market) external onlyOwner {
        if (_strategyVaultStorage().allowedMarkets.remove(market)) emit MarketDisallowed(market);
    }

    /*//////////////////////////////////////////////////////////////
                     USER-FACING STATE-CHANGING FUNCTIONS
    //////////////////////////////////////////////////////////////*/

    /// @notice Fund the vault with collateral. Withdrawals of deployed collateral require closing
    ///         the GMX position first; only idle balance is withdrawable.
    function deposit(uint256 amount) external onlyOwner nonReentrant {
        if (amount == 0) revert StrategyVault__ZeroAmount();
        _strategyVaultStorage().asset.safeTransferFrom(msg.sender, address(this), amount);
        emit Deposited(msg.sender, amount);
    }

    /// @notice Withdraw idle collateral. Pause does not trap withdrawals — the owner always keeps
    ///         custody of undeployed funds; pause only halts trading/execution.
    function withdraw(uint256 amount, address to) external onlyOwner nonReentrant {
        if (amount == 0) revert StrategyVault__ZeroAmount();
        if (to == address(0)) revert StrategyVault__ZeroAddress();

        IERC20 asset_ = _strategyVaultStorage().asset;
        if (amount > asset_.balanceOf(address(this))) revert StrategyVault__InsufficientIdleBalance();

        asset_.safeTransfer(to, amount);
        emit Withdrawn(to, amount);
    }

    function pause() external onlyOwner {
        _pause();
    }

    function unpause() external onlyOwner {
        _unpause();
    }

    /// @notice Owner-only escape hatch for arbitrary calls. Never granted to the keeper.
    function execute(address target, uint256 value, bytes calldata data)
        external
        onlyOwner
        nonReentrant
        whenNotPaused
        returns (bytes memory result)
    {
        if (target == address(0)) revert StrategyVault__ZeroTarget();

        bool success;
        (success, result) = target.call{value: value}(data);
        if (!success) revert StrategyVault__CallFailed(result);

        emit Executed(target, value, result);
    }

    function createGmxMarketIncreaseOrder(GmxMarketIncreaseOrder calldata order)
        external
        payable
        onlyOwnerOrKeeper
        nonReentrant
        whenNotPaused
        returns (bytes32 orderKey)
    {
        _requireValidIncreaseOrderShape(order);
        _requireExactExecutionFee(order.executionFee);
        _requireIncreaseWithinMandate(order);
        _requireKeeperIncreaseIntent(order);

        IERC20(order.collateralToken).forceApprove(order.router, order.collateralAmount);

        bytes[] memory calls = new bytes[](3);
        calls[0] = abi.encodeCall(IGmxV2ExchangeRouter.sendWnt, (order.orderVault, order.executionFee));
        calls[1] = abi.encodeCall(
            IGmxV2ExchangeRouter.sendTokens, (order.collateralToken, order.orderVault, order.collateralAmount)
        );
        calls[2] =
            abi.encodeCall(IGmxV2ExchangeRouter.createOrder, (GmxOrderBuilder.buildIncrease(order, address(this))));

        bytes[] memory results = IGmxV2ExchangeRouter(order.exchangeRouter).multicall{value: msg.value}(calls);
        orderKey = abi.decode(results[2], (bytes32));

        emit GmxMarketIncreaseOrderCreated(
            orderKey,
            order.exchangeRouter,
            order.market,
            order.collateralToken,
            order.isLong,
            order.sizeDeltaUsd,
            order.collateralAmount,
            order.executionFee
        );
    }

    function createGmxMarketDecreaseOrder(GmxMarketDecreaseOrder calldata order)
        external
        payable
        onlyOwnerOrKeeper
        nonReentrant
        whenNotPaused
        returns (bytes32 orderKey)
    {
        _requireValidDecreaseOrderShape(order);
        _requireExactExecutionFee(order.executionFee);
        StrategyVaultStorage storage $ = _strategyVaultStorage();
        if (
            order.exchangeRouter != $.gmxExchangeRouter || order.orderVault != $.gmxOrderVault
                || order.collateralToken != address($.asset)
        ) revert StrategyVault__UntrustedRouting();
        // De-risking: only require the market be on the allowlist; no size/leverage/interval gate.
        if (!$.allowedMarkets.contains(order.market)) revert StrategyVault__MarketNotAllowed(order.market);
        _requireKeeperDecreaseIntent(order, $.mandate.maxKeeperSlippageBps);

        bytes[] memory calls = new bytes[](2);
        calls[0] = abi.encodeCall(IGmxV2ExchangeRouter.sendWnt, (order.orderVault, order.executionFee));
        calls[1] =
            abi.encodeCall(IGmxV2ExchangeRouter.createOrder, (GmxOrderBuilder.buildDecrease(order, address(this))));

        bytes[] memory results = IGmxV2ExchangeRouter(order.exchangeRouter).multicall{value: msg.value}(calls);
        orderKey = abi.decode(results[1], (bytes32));

        emit GmxMarketDecreaseOrderCreated(
            orderKey,
            order.exchangeRouter,
            order.market,
            order.collateralToken,
            order.isLong,
            order.sizeDeltaUsd,
            order.collateralWithdrawalAmount,
            order.executionFee
        );
    }

    function cancelGmxOrder(address exchangeRouter, bytes32 orderKey, uint256 executionFee)
        external
        payable
        onlyOwnerOrKeeper
        nonReentrant
        whenNotPaused
    {
        if (exchangeRouter == address(0)) revert StrategyVault__ZeroAddress();
        _requireExactExecutionFee(executionFee);

        IGmxV2ExchangeRouter(exchangeRouter).cancelOrder{value: msg.value}(orderKey);

        emit GmxOrderCancelled(orderKey, exchangeRouter, executionFee);
    }

    /*//////////////////////////////////////////////////////////////
                            INTERNAL FUNCTIONS
    //////////////////////////////////////////////////////////////*/

    /// @dev Enforce the mandate on an increase order. Routing/collateral are pinned to the canonical
    ///      GMX config + vault asset; market allowlist + per-order notional + per-order leverage apply
    ///      to every caller; the rebalance interval throttles the keeper only.
    function _requireIncreaseWithinMandate(GmxMarketIncreaseOrder calldata order) internal {
        StrategyVaultStorage storage $ = _strategyVaultStorage();

        if (
            order.exchangeRouter != $.gmxExchangeRouter || order.router != $.gmxRouter
                || order.orderVault != $.gmxOrderVault || order.collateralToken != address($.asset)
        ) revert StrategyVault__UntrustedRouting();

        if (!$.allowedMarkets.contains(order.market)) revert StrategyVault__MarketNotAllowed(order.market);
        if (order.sizeDeltaUsd > $.mandate.maxPositionSizeUsd) revert StrategyVault__PositionSizeExceeded();

        // Per-order leverage guardrail: size delta vs added collateral (assumes $1 stable collateral).
        // Zero-collateral size-ups are rejected (collateralUsd == 0) so the cap cannot be bypassed by
        // adding size against existing collateral; position-aware leverage is deferred (needs Reader).
        uint256 collateralUsd = order.collateralAmount * $.collateralUsdScale;
        if (order.sizeDeltaUsd > $.mandate.maxLeverage * collateralUsd) revert StrategyVault__LeverageExceeded();

        if (msg.sender == $.keeper) {
            uint256 last = $.lastKeeperIncreaseAt;
            if (last != 0 && block.timestamp < last + $.mandate.minRebalanceInterval) {
                revert StrategyVault__RebalanceTooSoon();
            }
            $.lastKeeperIncreaseAt = block.timestamp;
        }
    }

    function _requireKeeperIncreaseIntent(GmxMarketIncreaseOrder calldata order) internal {
        StrategyVaultStorage storage $ = _strategyVaultStorage();
        if (msg.sender != $.keeper) return;

        _validateKeeperPriceBound(order.acceptablePrice, order.referencePrice, order.maxSlippageBps, true, order.isLong);
        if (order.maxSlippageBps > $.mandate.maxKeeperSlippageBps) revert StrategyVault__KeeperSlippageExceeded();

        bytes32 digest = _hashTypedDataV4(
            keccak256(
                abi.encode(
                    KEEPER_INCREASE_ORDER_INTENT_TYPEHASH,
                    msg.sender,
                    _hashKeeperIncreaseOrderFields(order),
                    order.referencePrice,
                    order.maxSlippageBps,
                    order.intentNonce,
                    order.intentDeadline
                )
            )
        );

        _consumeKeeperOrderIntent($, order.intentNonce, order.intentDeadline, digest, order.ownerSignature);
    }

    function _requireKeeperDecreaseIntent(GmxMarketDecreaseOrder calldata order, uint256 mandateMaxSlippageBps)
        internal
    {
        StrategyVaultStorage storage $ = _strategyVaultStorage();
        if (msg.sender != $.keeper) return;

        _validateKeeperPriceBound(
            order.acceptablePrice, order.referencePrice, order.maxSlippageBps, false, order.isLong
        );
        if (order.maxSlippageBps > mandateMaxSlippageBps) revert StrategyVault__KeeperSlippageExceeded();

        bytes32 digest = _hashTypedDataV4(
            keccak256(
                abi.encode(
                    KEEPER_DECREASE_ORDER_INTENT_TYPEHASH,
                    msg.sender,
                    _hashKeeperDecreaseOrderFields(order),
                    order.referencePrice,
                    order.maxSlippageBps,
                    order.intentNonce,
                    order.intentDeadline
                )
            )
        );

        _consumeKeeperOrderIntent($, order.intentNonce, order.intentDeadline, digest, order.ownerSignature);
    }

    function _hashKeeperIncreaseOrderFields(GmxMarketIncreaseOrder calldata order) internal pure returns (bytes32) {
        bytes32 routingHash = keccak256(
            abi.encode(
                order.exchangeRouter,
                order.router,
                order.orderVault,
                order.market,
                order.collateralToken,
                order.isLong,
                order.shouldUnwrapNativeToken
            )
        );
        bytes32 numbersHash = keccak256(
            abi.encode(order.sizeDeltaUsd, order.collateralAmount, order.acceptablePrice, order.executionFee)
        );
        return keccak256(abi.encode(routingHash, numbersHash, order.referralCode));
    }

    function _hashKeeperDecreaseOrderFields(GmxMarketDecreaseOrder calldata order) internal pure returns (bytes32) {
        bytes32 routingHash = keccak256(
            abi.encode(
                order.exchangeRouter,
                order.orderVault,
                order.market,
                order.collateralToken,
                order.isLong,
                order.shouldUnwrapNativeToken
            )
        );
        bytes32 numbersHash = keccak256(
            abi.encode(
                order.sizeDeltaUsd,
                order.collateralWithdrawalAmount,
                order.acceptablePrice,
                order.executionFee,
                order.minOutputAmount,
                uint8(order.decreasePositionSwapType)
            )
        );
        return keccak256(abi.encode(routingHash, numbersHash, order.referralCode));
    }

    function _consumeKeeperOrderIntent(
        StrategyVaultStorage storage $,
        uint256 nonce,
        uint256 deadline,
        bytes32 digest,
        bytes calldata signature
    ) internal {
        if (deadline < block.timestamp) revert StrategyVault__OrderIntentExpired();
        if ($.usedKeeperOrderIntentNonces[nonce]) revert StrategyVault__OrderIntentNonceUsed(nonce);

        (address recovered, ECDSA.RecoverError recoverError,) = ECDSA.tryRecoverCalldata(digest, signature);
        if (recoverError != ECDSA.RecoverError.NoError || recovered != owner()) {
            revert StrategyVault__InvalidOrderIntent();
        }

        $.usedKeeperOrderIntentNonces[nonce] = true;
        emit KeeperOrderIntentConsumed(digest, nonce, msg.sender);
    }

    function _validateKeeperPriceBound(
        uint256 acceptablePrice,
        uint256 referencePrice,
        uint256 maxSlippageBps,
        bool isIncrease,
        bool isLong
    ) internal pure {
        if (referencePrice == 0) revert StrategyVault__InvalidReferencePrice();
        if (maxSlippageBps > MAX_KEEPER_SLIPPAGE_BPS) revert StrategyVault__KeeperSlippageExceeded();

        if (_usesMaxAcceptablePrice(isIncrease, isLong)) {
            uint256 maxAcceptablePrice = referencePrice * (BPS_DIVISOR + maxSlippageBps) / BPS_DIVISOR;
            if (acceptablePrice > maxAcceptablePrice) revert StrategyVault__KeeperSlippageExceeded();
            return;
        }

        uint256 minAcceptablePrice = referencePrice * (BPS_DIVISOR - maxSlippageBps) / BPS_DIVISOR;
        if (acceptablePrice < minAcceptablePrice) revert StrategyVault__KeeperSlippageExceeded();
    }

    function _usesMaxAcceptablePrice(bool isIncrease, bool isLong) internal pure returns (bool) {
        return isIncrease == isLong;
    }

    function _requireExactExecutionFee(uint256 executionFee) internal view {
        if (msg.value != executionFee) revert StrategyVault__InvalidExecutionFee();
    }

    function _requireValidIncreaseOrderShape(GmxMarketIncreaseOrder calldata order) internal pure {
        if (
            order.exchangeRouter == address(0) || order.router == address(0) || order.orderVault == address(0)
                || order.market == address(0) || order.collateralToken == address(0)
        ) revert StrategyVault__ZeroAddress();
    }

    function _requireValidDecreaseOrderShape(GmxMarketDecreaseOrder calldata order) internal pure {
        if (
            order.exchangeRouter == address(0) || order.orderVault == address(0) || order.market == address(0)
                || order.collateralToken == address(0)
        ) revert StrategyVault__ZeroAddress();
    }
}
