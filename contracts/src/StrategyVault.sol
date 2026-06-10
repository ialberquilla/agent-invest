// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {IERC20Metadata} from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Metadata.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {EnumerableSet} from "@openzeppelin/contracts/utils/structs/EnumerableSet.sol";
import {ReentrancyGuardTransient} from "@openzeppelin/contracts/utils/ReentrancyGuardTransient.sol";
import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {Ownable2StepUpgradeable} from "@openzeppelin/contracts-upgradeable/access/Ownable2StepUpgradeable.sol";
import {PausableUpgradeable} from "@openzeppelin/contracts-upgradeable/utils/PausableUpgradeable.sol";

import {IGmxV2ExchangeRouter} from "./interfaces/IGmxV2ExchangeRouter.sol";
import {StrategyVaultBase} from "./StrategyVaultBase.sol";
import {GmxOrderBuilder} from "./lib/GmxOrderBuilder.sol";

/**
 * @title StrategyVault
 * @notice Single-user upgradeable collateral vault for explicit owner-approved GMX v2 orders.
 * @dev The vault only holds collateral and submits/cancels GMX orders requested by its owner.
 *      Strategy logic and position sizing live offchain.
 */
contract StrategyVault is
    StrategyVaultBase,
    Initializable,
    Ownable2StepUpgradeable,
    PausableUpgradeable,
    ReentrancyGuardTransient
{
    using SafeERC20 for IERC20;
    using EnumerableSet for EnumerableSet.AddressSet;

    /// @dev Mutating GMX order funcs may be called by the owner OR a delegated keeper. The keeper
    ///      can execute/adjust/close positions but can NEVER move funds out: every fund-exit and
    ///      config path ({withdraw}, {withdrawNative}, {setGmxRouting}, {setKeeper}, {setMandate},
    ///      pause) stays {onlyOwner}, and order `receiver`/`cancellationReceiver` are pinned to the
    ///      vault in {GmxOrderBuilder}. This separation is the vault's reason to exist over an EOA.
    modifier onlyOwnerOrKeeper() {
        if (msg.sender != owner() && msg.sender != _strategyVaultStorage().keeper) {
            revert StrategyVault__NotKeeperOrOwner();
        }
        _;
    }

    constructor() {
        _disableInitializers();
    }

    function initialize(IERC20 asset_, address owner_, address exchangeRouter, address router, address orderVault)
        external
        initializer
    {
        if (address(asset_) == address(0)) revert StrategyVault__ZeroAsset();
        if (owner_ == address(0)) revert StrategyVault__ZeroOwner();
        if (exchangeRouter == address(0) || router == address(0) || orderVault == address(0)) {
            revert StrategyVault__ZeroAddress();
        }

        __Ownable_init(owner_);
        __Ownable2Step_init();
        __Pausable_init();

        StrategyVaultStorage storage $ = _strategyVaultStorage();
        $.asset = asset_;
        $.gmxExchangeRouter = exchangeRouter;
        $.gmxRouter = router;
        $.gmxOrderVault = orderVault;

        emit GmxRoutingUpdated(exchangeRouter, router, orderVault);
    }

    receive() external payable {}

    /*//////////////////////////////////////////////////////////////
                                  VIEWS
    //////////////////////////////////////////////////////////////*/

    function asset() external view returns (IERC20) {
        return _strategyVaultStorage().asset;
    }

    function idleBalance() public view returns (uint256) {
        return _strategyVaultStorage().asset.balanceOf(address(this));
    }

    function nativeBalance() public view returns (uint256) {
        return address(this).balance;
    }

    function gmxRouting() external view returns (address exchangeRouter, address router, address orderVault) {
        StrategyVaultStorage storage $ = _strategyVaultStorage();
        return ($.gmxExchangeRouter, $.gmxRouter, $.gmxOrderVault);
    }

    /// @notice The keeper allowed to execute GMX orders (cannot withdraw). `address(0)` = owner-only.
    function keeper() external view returns (address) {
        return _strategyVaultStorage().keeper;
    }

    /// @notice The mandate bounds enforced on keeper/owner increase orders.
    /// @return maxLeverageBps Max leverage in bps of 1x (0 = unbounded).
    /// @return allowedMarkets GMX markets the agent may open into (empty = any market).
    function mandate() external view returns (uint256 maxLeverageBps, address[] memory allowedMarkets) {
        StrategyVaultStorage storage $ = _strategyVaultStorage();
        return ($.maxLeverageBps, $.allowedMarkets.values());
    }

    /*//////////////////////////////////////////////////////////////
                              OWNER CONFIG
    //////////////////////////////////////////////////////////////*/

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

    /// @notice Set (or revoke, with `address(0)`) the keeper allowed to execute GMX orders.
    /// @dev Owner-only: delegating execution must never delegate custody.
    function setKeeper(address keeper_) external onlyOwner {
        _strategyVaultStorage().keeper = keeper_;
        emit KeeperUpdated(keeper_);
    }

    /// @notice Set the mandate bounds enforced on every increase order (owner and keeper alike).
    /// @param maxLeverageBps_ Max leverage in bps of 1x (0 = unbounded). e.g. 30_000 = 3x.
    /// @param allowedMarkets The GMX markets the agent may open into. Empty = any market allowed.
    /// @dev Replaces the existing mandate wholesale. Owner-only. Decrease/close orders are never
    ///      constrained by the mandate so the agent can always de-risk an out-of-bounds position.
    function setMandate(uint256 maxLeverageBps_, address[] calldata allowedMarkets) external onlyOwner {
        if (allowedMarkets.length > MAX_ALLOWED_MARKETS) revert StrategyVault__TooManyMarkets();

        StrategyVaultStorage storage $ = _strategyVaultStorage();
        $.maxLeverageBps = maxLeverageBps_;

        // Clear then repopulate the allowed-markets set. `remove` swaps the last element into the
        // removed slot, so repeatedly removing index 0 drains the set in O(n).
        uint256 existing = $.allowedMarkets.length();
        for (uint256 i; i < existing; ++i) {
            $.allowedMarkets.remove($.allowedMarkets.at(0));
        }
        for (uint256 i; i < allowedMarkets.length; ++i) {
            $.allowedMarkets.add(allowedMarkets[i]);
        }

        emit MandateUpdated(maxLeverageBps_, allowedMarkets);
    }

    /*//////////////////////////////////////////////////////////////
                     USER-FACING STATE-CHANGING FUNCTIONS
    //////////////////////////////////////////////////////////////*/

    /// @notice Deposit USDC collateral, optionally topping up the native gas tank in the same tx.
    /// @dev `payable`: any attached ETH funds the GMX execution-fee gas tank (see {depositNative}).
    function deposit(uint256 amount) external payable onlyOwner nonReentrant {
        if (amount == 0) revert StrategyVault__ZeroAmount();
        _strategyVaultStorage().asset.safeTransferFrom(msg.sender, address(this), amount);
        emit Deposited(msg.sender, amount);
        if (msg.value > 0) emit NativeDeposited(msg.sender, msg.value);
    }

    function withdraw(uint256 amount, address to) external onlyOwner nonReentrant {
        if (amount == 0) revert StrategyVault__ZeroAmount();
        if (to == address(0)) revert StrategyVault__ZeroAddress();

        IERC20 asset_ = _strategyVaultStorage().asset;
        if (amount > asset_.balanceOf(address(this))) revert StrategyVault__InsufficientIdleBalance();

        asset_.safeTransfer(to, amount);
        emit Withdrawn(to, amount);
    }

    /// @notice Fund the vault's native ETH "gas tank" used to pay GMX execution fees.
    /// @dev Permissionless: anyone may top up the tank (it only adds ETH, which only the owner can withdraw).
    ///      Plain transfers to `receive()` also work; this exists for an explicit entrypoint + event.
    function depositNative() external payable {
        if (msg.value == 0) revert StrategyVault__ZeroAmount();
        emit NativeDeposited(msg.sender, msg.value);
    }

    /// @notice Sweep native ETH out of the vault (e.g. GMX execution-fee refunds accrued via `receive()`).
    /// @dev GMX returns unspent execution fees and cancellation refunds as native ETH to this vault, which
    ///      would otherwise be stranded since `withdraw` only moves the ERC20 asset.
    function withdrawNative(uint256 amount, address to) external onlyOwner nonReentrant {
        if (amount == 0) revert StrategyVault__ZeroAmount();
        if (to == address(0)) revert StrategyVault__ZeroAddress();
        if (amount > address(this).balance) revert StrategyVault__InsufficientNativeBalance();

        (bool ok,) = to.call{value: amount}("");
        if (!ok) revert StrategyVault__NativeTransferFailed();

        emit NativeWithdrawn(to, amount);
    }

    function pause() external onlyOwner {
        _pause();
    }

    function unpause() external onlyOwner {
        _unpause();
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
        _requireSufficientExecutionGas(order.executionFee);
        _requireTrustedIncreaseOrder(order);
        _requireWithinMandate(order);

        orderKey = _createGmxMarketIncreaseOrder(order, order.executionFee);
    }

    function createGmxMarketIncreaseOrders(GmxMarketIncreaseOrder[] calldata orders)
        external
        payable
        onlyOwnerOrKeeper
        nonReentrant
        whenNotPaused
        returns (bytes32[] memory orderKeys)
    {
        if (orders.length == 0) revert StrategyVault__ZeroAmount();

        uint256 totalExecutionFee;
        for (uint256 i; i < orders.length; ++i) {
            totalExecutionFee += orders[i].executionFee;
        }
        _requireSufficientExecutionGas(totalExecutionFee);

        orderKeys = new bytes32[](orders.length);
        for (uint256 i; i < orders.length; ++i) {
            GmxMarketIncreaseOrder calldata order = orders[i];
            _requireValidIncreaseOrderShape(order);
            _requireTrustedIncreaseOrder(order);
            _requireWithinMandate(order);
            orderKeys[i] = _createGmxMarketIncreaseOrder(order, order.executionFee);
        }
    }

    /// @dev Size-reducing/closing decreases are de-risking actions, so they are gated only on
    ///      `owner || keeper` and are NOT mandate-constrained: the agent must be able to reduce or close
    ///      a position even in a market later removed from the allowlist or now over the leverage ceiling.
    ///      A *collateral-withdrawing* decrease (`collateralWithdrawalAmount > 0`) is the exception — it
    ///      raises leverage, so {_requireDecreaseAllowed} restricts it to the owner (see I5/Finding 1).
    function createGmxMarketDecreaseOrder(GmxMarketDecreaseOrder calldata order)
        external
        payable
        onlyOwnerOrKeeper
        nonReentrant
        whenNotPaused
        returns (bytes32 orderKey)
    {
        _requireValidDecreaseOrderShape(order);
        _requireSufficientExecutionGas(order.executionFee);
        _requireTrustedDecreaseOrder(order);
        _requireDecreaseAllowed(order);

        orderKey = _createGmxMarketDecreaseOrder(order, order.executionFee);
    }

    function createGmxMarketDecreaseOrders(GmxMarketDecreaseOrder[] calldata orders)
        external
        payable
        onlyOwnerOrKeeper
        nonReentrant
        whenNotPaused
        returns (bytes32[] memory orderKeys)
    {
        if (orders.length == 0) revert StrategyVault__ZeroAmount();

        uint256 totalExecutionFee;
        for (uint256 i; i < orders.length; ++i) {
            totalExecutionFee += orders[i].executionFee;
        }
        _requireSufficientExecutionGas(totalExecutionFee);

        orderKeys = new bytes32[](orders.length);
        for (uint256 i; i < orders.length; ++i) {
            GmxMarketDecreaseOrder calldata order = orders[i];
            _requireValidDecreaseOrderShape(order);
            _requireTrustedDecreaseOrder(order);
            _requireDecreaseAllowed(order);
            orderKeys[i] = _createGmxMarketDecreaseOrder(order, order.executionFee);
        }
    }

    /// @dev Not `whenNotPaused`: cancelling a pending order is a de-risking action the owner must be able to
    ///      perform during an emergency pause. GMX `cancelOrder` charges no execution fee (it refunds the
    ///      original), so no exact-fee check is imposed; any forwarded `msg.value` is normally zero.
    function cancelGmxOrder(address exchangeRouter, bytes32 orderKey) external payable onlyOwnerOrKeeper nonReentrant {
        if (exchangeRouter == address(0)) revert StrategyVault__ZeroAddress();
        if (exchangeRouter != _strategyVaultStorage().gmxExchangeRouter) revert StrategyVault__UntrustedRouting();

        IGmxV2ExchangeRouter(exchangeRouter).cancelOrder{value: msg.value}(orderKey);

        emit GmxOrderCancelled(orderKey, exchangeRouter, msg.value);
    }

    /*//////////////////////////////////////////////////////////////
                            INTERNAL FUNCTIONS
    //////////////////////////////////////////////////////////////*/

    function _createGmxMarketIncreaseOrder(GmxMarketIncreaseOrder calldata order, uint256 executionFee)
        internal
        returns (bytes32 orderKey)
    {
        IERC20(order.collateralToken).forceApprove(order.router, order.collateralAmount);

        bytes[] memory calls = new bytes[](3);
        calls[0] = abi.encodeCall(IGmxV2ExchangeRouter.sendWnt, (order.orderVault, executionFee));
        calls[1] = abi.encodeCall(
            IGmxV2ExchangeRouter.sendTokens, (order.collateralToken, order.orderVault, order.collateralAmount)
        );
        calls[2] =
            abi.encodeCall(IGmxV2ExchangeRouter.createOrder, (GmxOrderBuilder.buildIncrease(order, address(this))));

        bytes[] memory results = IGmxV2ExchangeRouter(order.exchangeRouter).multicall{value: executionFee}(calls);
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

    function _createGmxMarketDecreaseOrder(GmxMarketDecreaseOrder calldata order, uint256 executionFee)
        internal
        returns (bytes32 orderKey)
    {
        bytes[] memory calls = new bytes[](2);
        calls[0] = abi.encodeCall(IGmxV2ExchangeRouter.sendWnt, (order.orderVault, executionFee));
        calls[1] =
            abi.encodeCall(IGmxV2ExchangeRouter.createOrder, (GmxOrderBuilder.buildDecrease(order, address(this))));

        bytes[] memory results = IGmxV2ExchangeRouter(order.exchangeRouter).multicall{value: executionFee}(calls);
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

    function _requireTrustedIncreaseOrder(GmxMarketIncreaseOrder calldata order) internal view {
        StrategyVaultStorage storage $ = _strategyVaultStorage();
        if (
            order.exchangeRouter != $.gmxExchangeRouter || order.router != $.gmxRouter
                || order.orderVault != $.gmxOrderVault || order.collateralToken != address($.asset)
        ) revert StrategyVault__UntrustedRouting();
    }

    function _requireTrustedDecreaseOrder(GmxMarketDecreaseOrder calldata order) internal view {
        StrategyVaultStorage storage $ = _strategyVaultStorage();
        if (
            order.exchangeRouter != $.gmxExchangeRouter || order.orderVault != $.gmxOrderVault
                || order.collateralToken != address($.asset)
        ) revert StrategyVault__UntrustedRouting();
    }

    /// @dev Enforces the mandate on an increase order. Two independent bounds, each opt-in:
    ///        1. Market allowlist — empty set means any GMX market is permitted (e.g. a momentum
    ///           strategy that trades the whole screened universe leaves it empty).
    ///        2. Leverage ceiling — `maxLeverageBps == 0` means unbounded. When set, the order MUST
    ///           add collateral (`collateralAmount > 0`), otherwise size could be added against zero
    ///           new margin and leverage would be unbounded (the "zero-collateral bypass").
    ///      NOTE: this bounds each order's OWN leverage (sizeDeltaUsd vs the collateral it adds); it
    ///      does not track cumulative position leverage across multiple increases — that needs a GMX
    ///      Reader call and is intentionally out of scope. Collateral is valued 1:1 in USD (the asset
    ///      is a USD stablecoin), so no price oracle is required.
    function _requireWithinMandate(GmxMarketIncreaseOrder calldata order) internal view {
        StrategyVaultStorage storage $ = _strategyVaultStorage();

        if ($.allowedMarkets.length() != 0 && !$.allowedMarkets.contains(order.market)) {
            revert StrategyVault__MarketNotAllowed();
        }

        uint256 maxLeverageBps = $.maxLeverageBps;
        if (maxLeverageBps != 0) {
            if (order.collateralAmount == 0) revert StrategyVault__ZeroCollateral();

            // sizeDeltaUsd is GMX 30-decimal USD; collateral is the asset's own decimals valued 1:1.
            // Scale collateral up to 30 decimals, then compare leverage in bps:
            //   sizeDeltaUsd / collateralUsd30 * ONE_X_BPS <= maxLeverageBps
            // rearranged to avoid division/precision loss.
            uint256 collateralUsd30 = order.collateralAmount * (10 ** (30 - _assetDecimals()));
            if (order.sizeDeltaUsd * ONE_X_BPS > maxLeverageBps * collateralUsd30) {
                revert StrategyVault__LeverageTooHigh();
            }
        }
    }

    function _assetDecimals() internal view returns (uint256) {
        return IERC20Metadata(address(_strategyVaultStorage().asset)).decimals();
    }

    /// @dev A decrease that withdraws collateral (`collateralWithdrawalAmount > 0`) without reducing
    ///      size RAISES the position's leverage — it is the opposite of de-risking and would let a
    ///      keeper defeat the mandate's leverage ceiling (open at-cap, then strip margin). Such bare
    ///      collateral withdrawals are therefore restricted to the owner. The keeper retains full
    ///      ability to reduce or fully close size (collateral returns to the vault on close), so the
    ///      protective lane is intact. See INVARIANTS.md I5.
    function _requireDecreaseAllowed(GmxMarketDecreaseOrder calldata order) internal view {
        if (order.collateralWithdrawalAmount > 0 && msg.sender != owner()) {
            revert StrategyVault__KeeperCannotWithdrawCollateral();
        }
    }

    /// @dev The execution fee is paid out of the vault's native balance (the "gas tank"). Any `msg.value`
    ///      attached to this call is already part of `address(this).balance`, so a caller can either pre-fund
    ///      the tank (keeper flow) or attach the fee inline (owner flow) — both satisfy this check.
    function _requireSufficientExecutionGas(uint256 executionFee) internal view {
        if (address(this).balance < executionFee) revert StrategyVault__InsufficientExecutionGas();
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
