// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {IGmxV2ExchangeRouter} from "src/interfaces/IGmxV2ExchangeRouter.sol";
import {Test} from "forge-std/Test.sol";
import {StrategyVault} from "src/StrategyVault.sol";
import {StrategyVaultBase} from "src/StrategyVaultBase.sol";
import {VaultFactory} from "src/VaultFactory.sol";

interface IGmxV2ExchangeRouterDataStore {
    function dataStore() external view returns (address);
}

interface IGmxV2Reader {
    struct OrderInfo {
        bytes32 orderKey;
        OrderProps order;
    }

    struct OrderProps {
        OrderAddresses addresses;
        OrderNumbers numbers;
        OrderFlags flags;
        bytes32[] dataList;
    }

    struct OrderAddresses {
        address account;
        address receiver;
        address cancellationReceiver;
        address callbackContract;
        address uiFeeReceiver;
        address market;
        address initialCollateralToken;
        address[] swapPath;
    }

    struct OrderNumbers {
        IGmxV2ExchangeRouter.OrderType orderType;
        IGmxV2ExchangeRouter.DecreasePositionSwapType decreasePositionSwapType;
        uint256 sizeDeltaUsd;
        uint256 initialCollateralDeltaAmount;
        uint256 triggerPrice;
        uint256 acceptablePrice;
        uint256 executionFee;
        uint256 callbackGasLimit;
        uint256 minOutputAmount;
        uint256 updatedAtTime;
        uint256 validFromTime;
        uint256 srcChainId;
    }

    struct OrderFlags {
        bool isLong;
        bool shouldUnwrapNativeToken;
        bool isFrozen;
        bool autoCancel;
    }

    function getAccountOrders(address dataStore, address account, uint256 start, uint256 end)
        external
        view
        returns (OrderInfo[] memory);
}

contract GmxOrderForkTest is Test {
    address internal constant ARBITRUM_USDC = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831;
    address internal constant GMX_EXCHANGE_ROUTER = 0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41;
    address internal constant GMX_ROUTER = 0x7452c558d45f8afC8c83dAe62C3f8A5BE19c71f6;
    address internal constant GMX_ORDER_VAULT = 0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5;
    address internal constant GMX_READER = 0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789;
    address internal constant GMX_BTC_USD_MARKET = 0x47c031236e19d024b42f8AE6780E44A573170703;

    address internal owner = address(0xA11CE);
    StrategyVault internal vault;

    function setUp() external {
        string memory rpcUrl = vm.envOr("ARBITRUM_RPC_URL", string(""));
        if (bytes(rpcUrl).length == 0) return;

        vm.createSelectFork(rpcUrl, 452_780_000);
        StrategyVault implementation = new StrategyVault();
        VaultFactory factory =
            new VaultFactory(address(implementation), address(this), GMX_EXCHANGE_ROUTER, GMX_ROUTER, GMX_ORDER_VAULT);
        vault = StrategyVault(payable(factory.createVault(IERC20(ARBITRUM_USDC), owner)));
    }

    function testFork_CanSubmitGmxLongMarketIncreaseOrder() external {
        if (address(vault) == address(0)) return;

        bytes32 orderKey = _submitOrder(true, 120_000e30);

        assertTrue(orderKey != bytes32(0));
        _assertPendingOrder(orderKey, true, IGmxV2ExchangeRouter.OrderType.MarketIncrease);
    }

    function testFork_CanSubmitGmxShortMarketIncreaseOrder() external {
        if (address(vault) == address(0)) return;

        bytes32 orderKey = _submitOrder(false, 80_000e30);

        assertTrue(orderKey != bytes32(0));
        _assertPendingOrder(orderKey, false, IGmxV2ExchangeRouter.OrderType.MarketIncrease);
    }

    function testFork_CanSubmitBatchGmxMarketIncreaseOrders() external {
        if (address(vault) == address(0)) return;

        StrategyVaultBase.GmxMarketIncreaseOrder[] memory orders = new StrategyVaultBase.GmxMarketIncreaseOrder[](2);
        orders[0] = _buildOrder(GMX_BTC_USD_MARKET, 120_000e30);
        orders[1] = _buildOrder(GMX_BTC_USD_MARKET, 80_000e30);
        orders[1].isLong = false;

        deal(ARBITRUM_USDC, address(vault), orders[0].collateralAmount + orders[1].collateralAmount);
        uint256 totalExecutionFee = orders[0].executionFee + orders[1].executionFee;
        vm.deal(owner, totalExecutionFee);

        vm.prank(owner);
        bytes32[] memory orderKeys = vault.createGmxMarketIncreaseOrders{value: totalExecutionFee}(orders);

        assertEq(orderKeys.length, 2);
        assertTrue(orderKeys[0] != bytes32(0));
        assertTrue(orderKeys[1] != bytes32(0));
        _assertPendingOrder(orderKeys[0], true, IGmxV2ExchangeRouter.OrderType.MarketIncrease);
        _assertPendingOrder(orderKeys[1], false, IGmxV2ExchangeRouter.OrderType.MarketIncrease);
    }

    function testFork_CanCancelPendingGmxOrder() external {
        if (address(vault) == address(0)) return;

        bytes32 orderKey = _submitOrder(true, 120_000e30);
        _assertPendingOrder(orderKey, true, IGmxV2ExchangeRouter.OrderType.MarketIncrease);

        vm.warp(block.timestamp + 301);
        vm.prank(owner);
        vault.cancelGmxOrder(GMX_EXCHANGE_ROUTER, orderKey, 0);

        _assertNoPendingOrder(orderKey);
    }

    function testFork_BlocksUntrustedRouting() external {
        if (address(vault) == address(0)) return;

        StrategyVaultBase.GmxMarketIncreaseOrder memory order = _buildOrder(GMX_BTC_USD_MARKET, 120_000e30);
        order.exchangeRouter = address(0xBAD);

        vm.prank(owner);
        vm.expectRevert(StrategyVaultBase.StrategyVault__UntrustedRouting.selector);
        vault.createGmxMarketIncreaseOrder{value: order.executionFee}(order);
    }

    function testFork_BlocksUntrustedCollateral() external {
        if (address(vault) == address(0)) return;

        StrategyVaultBase.GmxMarketIncreaseOrder memory order = _buildOrder(GMX_BTC_USD_MARKET, 120_000e30);
        order.collateralToken = address(0xBAD);

        vm.prank(owner);
        vm.expectRevert(StrategyVaultBase.StrategyVault__UntrustedRouting.selector);
        vault.createGmxMarketIncreaseOrder{value: order.executionFee}(order);
    }

    function _submitOrder(bool isLong, uint256 acceptablePrice) internal returns (bytes32 orderKey) {
        StrategyVaultBase.GmxMarketIncreaseOrder memory order = _buildOrder(GMX_BTC_USD_MARKET, acceptablePrice);
        order.isLong = isLong;

        vm.prank(owner);
        orderKey = vault.createGmxMarketIncreaseOrder{value: order.executionFee}(order);
    }

    function _buildOrder(address market, uint256 acceptablePrice)
        internal
        returns (StrategyVaultBase.GmxMarketIncreaseOrder memory order)
    {
        uint256 collateralAmount = 50e6;
        uint256 executionFee = 0.005 ether;

        deal(ARBITRUM_USDC, address(vault), collateralAmount);
        vm.deal(owner, executionFee);

        order = StrategyVaultBase.GmxMarketIncreaseOrder({
            exchangeRouter: GMX_EXCHANGE_ROUTER,
            router: GMX_ROUTER,
            orderVault: GMX_ORDER_VAULT,
            market: market,
            collateralToken: ARBITRUM_USDC,
            isLong: true,
            shouldUnwrapNativeToken: false,
            sizeDeltaUsd: 50e30,
            collateralAmount: collateralAmount,
            acceptablePrice: acceptablePrice,
            executionFee: executionFee,
            referralCode: bytes32("agent-invest")
        });
    }

    function _assertPendingOrder(
        bytes32 expectedOrderKey,
        bool expectedIsLong,
        IGmxV2ExchangeRouter.OrderType expectedOrderType
    ) internal {
        address dataStore = IGmxV2ExchangeRouterDataStore(GMX_EXCHANGE_ROUTER).dataStore();
        IGmxV2Reader.OrderInfo[] memory orders =
            IGmxV2Reader(GMX_READER).getAccountOrders(dataStore, address(vault), 0, 10);

        for (uint256 i; i < orders.length; ++i) {
            if (orders[i].orderKey != expectedOrderKey) continue;

            assertEq(orders[i].order.addresses.account, address(vault));
            assertEq(orders[i].order.addresses.receiver, address(vault));
            assertEq(orders[i].order.addresses.market, GMX_BTC_USD_MARKET);
            assertEq(orders[i].order.addresses.initialCollateralToken, ARBITRUM_USDC);
            assertEq(uint8(orders[i].order.numbers.orderType), uint8(expectedOrderType));
            assertEq(orders[i].order.numbers.sizeDeltaUsd, 50e30);
            assertEq(orders[i].order.numbers.initialCollateralDeltaAmount, 50e6);
            assertEq(orders[i].order.flags.isLong, expectedIsLong);
            return;
        }

        fail("pending GMX order not found");
    }

    function _assertNoPendingOrder(bytes32 orderKey) internal view {
        address dataStore = IGmxV2ExchangeRouterDataStore(GMX_EXCHANGE_ROUTER).dataStore();
        IGmxV2Reader.OrderInfo[] memory orders =
            IGmxV2Reader(GMX_READER).getAccountOrders(dataStore, address(vault), 0, 10);

        for (uint256 i; i < orders.length; ++i) {
            assertTrue(orders[i].orderKey != orderKey, "cancelled order is still pending");
        }
    }
}
