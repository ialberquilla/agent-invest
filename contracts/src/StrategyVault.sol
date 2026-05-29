// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {ERC4626} from "@openzeppelin/contracts/token/ERC20/extensions/ERC4626.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {Ownable2Step} from "@openzeppelin/contracts/access/Ownable2Step.sol";
import {Pausable} from "@openzeppelin/contracts/utils/Pausable.sol";
import {ReentrancyGuardTransient} from "@openzeppelin/contracts/utils/ReentrancyGuardTransient.sol";

import {IGmxV2ExchangeRouter} from "./interfaces/IGmxV2ExchangeRouter.sol";

/**
 * @title StrategyVault
 * @notice ERC-4626 collateral vault for one user-owned Agent Invest strategy.
 * @dev Strategy intelligence stays offchain; the owner executes approved calls directly from the vault.
 */
contract StrategyVault is ERC4626, Ownable2Step, Pausable, ReentrancyGuardTransient {
    using SafeERC20 for IERC20;

    event Executed(address indexed target, uint256 value, bytes result);
    event GmxMarketIncreaseOrderCreated(
        address indexed exchangeRouter,
        address indexed market,
        address indexed collateralToken,
        bool isLong,
        uint256 sizeDeltaUsd,
        uint256 collateralAmount,
        uint256 executionFee
    );

    struct GmxMarketIncreaseOrder {
        address exchangeRouter;
        address router;
        address orderVault;
        address market;
        address collateralToken;
        address receiver;
        address cancellationReceiver;
        address callbackContract;
        address uiFeeReceiver;
        bool isLong;
        bool shouldUnwrapNativeToken;
        uint256 sizeDeltaUsd;
        uint256 collateralAmount;
        uint256 acceptablePrice;
        uint256 executionFee;
        uint256 callbackGasLimit;
        bytes32 referralCode;
    }

    error StrategyVault__ZeroOwner();
    error StrategyVault__ZeroTarget();
    error StrategyVault__ZeroAddress();
    error StrategyVault__InvalidExecutionFee();
    error StrategyVault__CallFailed(bytes returndata);

    constructor(IERC20 asset_, address owner_, string memory name_, string memory symbol_)
        ERC4626(asset_)
        ERC20(name_, symbol_)
        Ownable(owner_)
    {
        if (owner_ == address(0)) revert StrategyVault__ZeroOwner();
    }

    receive() external payable {}

    /*//////////////////////////////////////////////////////////////
                     USER-FACING STATE-CHANGING FUNCTIONS
    //////////////////////////////////////////////////////////////*/

    function pause() external onlyOwner {
        _pause();
    }

    function unpause() external onlyOwner {
        _unpause();
    }

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
        onlyOwner
        nonReentrant
        whenNotPaused
        returns (bytes[] memory results)
    {
        _validateGmxOrder(order);
        if (msg.value != order.executionFee) revert StrategyVault__InvalidExecutionFee();

        IERC20(order.collateralToken).forceApprove(order.router, order.collateralAmount);

        bytes[] memory calls = new bytes[](3);
        calls[0] = abi.encodeCall(IGmxV2ExchangeRouter.sendWnt, (order.orderVault, order.executionFee));
        calls[1] = abi.encodeCall(
            IGmxV2ExchangeRouter.sendTokens, (order.collateralToken, order.orderVault, order.collateralAmount)
        );
        calls[2] = abi.encodeCall(IGmxV2ExchangeRouter.createOrder, (_buildGmxCreateOrderParams(order)));

        results = IGmxV2ExchangeRouter(order.exchangeRouter).multicall{value: msg.value}(calls);

        emit GmxMarketIncreaseOrderCreated(
            order.exchangeRouter,
            order.market,
            order.collateralToken,
            order.isLong,
            order.sizeDeltaUsd,
            order.collateralAmount,
            order.executionFee
        );
    }

    /*//////////////////////////////////////////////////////////////
                            INTERNAL FUNCTIONS
    //////////////////////////////////////////////////////////////*/

    function _update(address from, address to, uint256 value) internal override whenNotPaused {
        super._update(from, to, value);
    }

    function _validateGmxOrder(GmxMarketIncreaseOrder calldata order) internal pure {
        if (
            order.exchangeRouter == address(0) || order.router == address(0) || order.orderVault == address(0)
                || order.market == address(0) || order.collateralToken == address(0)
        ) revert StrategyVault__ZeroAddress();
    }

    function _buildGmxCreateOrderParams(GmxMarketIncreaseOrder calldata order)
        internal
        view
        returns (IGmxV2ExchangeRouter.CreateOrderParams memory params)
    {
        address receiver = order.receiver == address(0) ? address(this) : order.receiver;
        address cancellationReceiver = order.cancellationReceiver == address(0) ? receiver : order.cancellationReceiver;
        address[] memory swapPath = new address[](0);
        bytes32[] memory dataList = new bytes32[](0);

        params = IGmxV2ExchangeRouter.CreateOrderParams({
            addresses: IGmxV2ExchangeRouter.CreateOrderParamsAddresses({
                receiver: receiver,
                cancellationReceiver: cancellationReceiver,
                callbackContract: order.callbackContract,
                uiFeeReceiver: order.uiFeeReceiver,
                market: order.market,
                initialCollateralToken: order.collateralToken,
                swapPath: swapPath
            }),
            numbers: IGmxV2ExchangeRouter.CreateOrderParamsNumbers({
                sizeDeltaUsd: order.sizeDeltaUsd,
                initialCollateralDeltaAmount: order.collateralAmount,
                triggerPrice: 0,
                acceptablePrice: order.acceptablePrice,
                executionFee: order.executionFee,
                callbackGasLimit: order.callbackGasLimit,
                minOutputAmount: 0,
                validFromTime: 0
            }),
            orderType: IGmxV2ExchangeRouter.OrderType.MarketIncrease,
            decreasePositionSwapType: IGmxV2ExchangeRouter.DecreasePositionSwapType.NoSwap,
            isLong: order.isLong,
            shouldUnwrapNativeToken: order.shouldUnwrapNativeToken,
            autoCancel: false,
            referralCode: order.referralCode,
            dataList: dataList
        });
    }
}
