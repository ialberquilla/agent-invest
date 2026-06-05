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

    function initialize(IERC20 asset_, address owner_) external initializer {
        if (address(asset_) == address(0)) revert StrategyVault__ZeroAsset();
        if (owner_ == address(0)) revert StrategyVault__ZeroOwner();

        __Ownable_init(owner_);
        __Ownable2Step_init();
        __Pausable_init();

        _strategyVaultStorage().asset = asset_;
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
        _requireExactExecutionFee(order.executionFee);
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
        _requireExactExecutionFee(totalExecutionFee);

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
        _requireExactExecutionFee(order.executionFee);
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
        _requireExactExecutionFee(totalExecutionFee);

        orderKeys = new bytes32[](orders.length);
        for (uint256 i; i < orders.length; ++i) {
            GmxMarketDecreaseOrder calldata order = orders[i];
            _requireValidDecreaseOrderShape(order);
            _requireTrustedDecreaseOrder(order);
            orderKeys[i] = _createGmxMarketDecreaseOrder(order, order.executionFee);
        }
    }

    function cancelGmxOrder(address exchangeRouter, bytes32 orderKey, uint256 executionFee)
        external
        payable
        onlyOwner
        nonReentrant
        whenNotPaused
    {
        if (exchangeRouter == address(0)) revert StrategyVault__ZeroAddress();
        if (exchangeRouter != _strategyVaultStorage().gmxExchangeRouter) revert StrategyVault__UntrustedRouting();
        _requireExactExecutionFee(executionFee);

        IGmxV2ExchangeRouter(exchangeRouter).cancelOrder{value: msg.value}(orderKey);

        emit GmxOrderCancelled(orderKey, exchangeRouter, executionFee);
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
