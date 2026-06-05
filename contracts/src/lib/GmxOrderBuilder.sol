// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {IGmxV2ExchangeRouter} from "src/interfaces/IGmxV2ExchangeRouter.sol";
import {StrategyVaultBase} from "src/StrategyVaultBase.sol";

library GmxOrderBuilder {
    function buildIncrease(StrategyVaultBase.GmxMarketIncreaseOrder calldata order, address vault)
        internal
        pure
        returns (IGmxV2ExchangeRouter.CreateOrderParams memory params)
    {
        address[] memory swapPath = new address[](0);
        bytes32[] memory dataList = new bytes32[](0);

        params = IGmxV2ExchangeRouter.CreateOrderParams({
            addresses: IGmxV2ExchangeRouter.CreateOrderParamsAddresses({
                receiver: vault,
                cancellationReceiver: vault,
                callbackContract: address(0),
                uiFeeReceiver: address(0),
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
                callbackGasLimit: 0,
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

    function buildDecrease(StrategyVaultBase.GmxMarketDecreaseOrder calldata order, address vault)
        internal
        pure
        returns (IGmxV2ExchangeRouter.CreateOrderParams memory params)
    {
        address[] memory swapPath = new address[](0);
        bytes32[] memory dataList = new bytes32[](0);

        params = IGmxV2ExchangeRouter.CreateOrderParams({
            addresses: IGmxV2ExchangeRouter.CreateOrderParamsAddresses({
                receiver: vault,
                cancellationReceiver: vault,
                callbackContract: address(0),
                uiFeeReceiver: address(0),
                market: order.market,
                initialCollateralToken: order.collateralToken,
                swapPath: swapPath
            }),
            numbers: IGmxV2ExchangeRouter.CreateOrderParamsNumbers({
                sizeDeltaUsd: order.sizeDeltaUsd,
                initialCollateralDeltaAmount: order.collateralWithdrawalAmount,
                triggerPrice: 0,
                acceptablePrice: order.acceptablePrice,
                executionFee: order.executionFee,
                callbackGasLimit: 0,
                minOutputAmount: order.minOutputAmount,
                validFromTime: 0
            }),
            orderType: IGmxV2ExchangeRouter.OrderType.MarketDecrease,
            decreasePositionSwapType: order.decreasePositionSwapType,
            isLong: order.isLong,
            shouldUnwrapNativeToken: order.shouldUnwrapNativeToken,
            autoCancel: false,
            referralCode: order.referralCode,
            dataList: dataList
        });
    }
}
