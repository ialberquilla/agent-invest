// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
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

    /*//////////////////////////////////////////////////////////////
                     USER-FACING STATE-CHANGING FUNCTIONS
    //////////////////////////////////////////////////////////////*/

    function deposit(uint256 amount) external onlyOwner nonReentrant {
        if (amount == 0) revert StrategyVault__ZeroAmount();
        _strategyVaultStorage().asset.safeTransferFrom(msg.sender, address(this), amount);
        emit Deposited(msg.sender, amount);
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
        onlyOwner
        nonReentrant
        whenNotPaused
        returns (bytes32 orderKey)
    {
        _requireValidIncreaseOrderShape(order);
        _requireSufficientExecutionGas(order.executionFee);
        _requireTrustedIncreaseOrder(order);

        orderKey = _createGmxMarketIncreaseOrder(order, order.executionFee);
    }

    function createGmxMarketIncreaseOrders(GmxMarketIncreaseOrder[] calldata orders)
        external
        payable
        onlyOwner
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
            orderKeys[i] = _createGmxMarketIncreaseOrder(order, order.executionFee);
        }
    }

    function createGmxMarketDecreaseOrder(GmxMarketDecreaseOrder calldata order)
        external
        payable
        onlyOwner
        nonReentrant
        whenNotPaused
        returns (bytes32 orderKey)
    {
        _requireValidDecreaseOrderShape(order);
        _requireSufficientExecutionGas(order.executionFee);
        _requireTrustedDecreaseOrder(order);

        orderKey = _createGmxMarketDecreaseOrder(order, order.executionFee);
    }

    function createGmxMarketDecreaseOrders(GmxMarketDecreaseOrder[] calldata orders)
        external
        payable
        onlyOwner
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
            orderKeys[i] = _createGmxMarketDecreaseOrder(order, order.executionFee);
        }
    }

    /// @dev Not `whenNotPaused`: cancelling a pending order is a de-risking action the owner must be able to
    ///      perform during an emergency pause. GMX `cancelOrder` charges no execution fee (it refunds the
    ///      original), so no exact-fee check is imposed; any forwarded `msg.value` is normally zero.
    function cancelGmxOrder(address exchangeRouter, bytes32 orderKey) external payable onlyOwner nonReentrant {
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
