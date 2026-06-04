// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {IGmxV2ExchangeRouter} from "src/interfaces/IGmxV2ExchangeRouter.sol";
import {Test} from "forge-std/Test.sol";
import {StrategyVault} from "src/StrategyVault.sol";
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
    uint256 internal constant OWNER_PRIVATE_KEY = 0xA11CE;
    bytes32 internal constant EIP712_DOMAIN_TYPEHASH =
        keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)");
    bytes32 internal constant KEEPER_INCREASE_ORDER_INTENT_TYPEHASH = keccak256(
        "KeeperIncreaseOrderIntent(address keeper,bytes32 orderHash,uint256 referencePrice,uint256 maxSlippageBps,uint256 nonce,uint256 deadline)"
    );

    address internal constant ARBITRUM_USDC = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831;
    address internal constant GMX_EXCHANGE_ROUTER = 0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41;
    address internal constant GMX_ROUTER = 0x7452c558d45f8afC8c83dAe62C3f8A5BE19c71f6;
    address internal constant GMX_ORDER_VAULT = 0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5;
    address internal constant GMX_READER = 0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789;
    address internal constant GMX_BTC_USD_MARKET = 0x47c031236e19d024b42f8AE6780E44A573170703;

    address internal owner;
    address internal keeper = makeAddr("keeper");
    StrategyVault internal vault;

    function setUp() external {
        string memory rpcUrl = vm.envOr("ARBITRUM_RPC_URL", string(""));
        if (bytes(rpcUrl).length == 0) return;

        vm.createSelectFork(rpcUrl, 452_780_000);
        owner = vm.addr(OWNER_PRIVATE_KEY);
        StrategyVault implementation = new StrategyVault();
        VaultFactory factory = new VaultFactory(address(implementation), address(this));
        vault = StrategyVault(payable(factory.createVault(IERC20(ARBITRUM_USDC), owner)));

        vm.startPrank(owner);
        vault.setMandate(
            StrategyVault.Mandate({
                maxLeverage: 5, maxPositionSizeUsd: 1_000_000e30, minRebalanceInterval: 0, maxKeeperSlippageBps: 200
            })
        );
        vault.addMarket(GMX_BTC_USD_MARKET);
        vault.setGmxRouting(GMX_EXCHANGE_ROUTER, GMX_ROUTER, GMX_ORDER_VAULT);
        vault.setKeeper(keeper);
        vm.stopPrank();
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

    function testFork_KeeperCanSubmitConformingOrder() external {
        if (address(vault) == address(0)) return;

        // bounded keeper submits a mandate-conforming order against real GMX
        StrategyVault.GmxMarketIncreaseOrder memory order = _buildOrder(GMX_BTC_USD_MARKET, 50e30, 120_000e30);
        order = _signKeeperIncreaseOrder(order);
        vm.prank(keeper);
        bytes32 orderKey = vault.createGmxMarketIncreaseOrder{value: order.executionFee}(order);

        assertTrue(orderKey != bytes32(0));
        _assertPendingOrder(orderKey, true, IGmxV2ExchangeRouter.OrderType.MarketIncrease);
    }

    function testFork_MandateBlocksDisallowedMarket() external {
        if (address(vault) == address(0)) return;

        StrategyVault.GmxMarketIncreaseOrder memory order = _buildOrder(address(0xDEAD), 50e30, 120_000e30);
        vm.prank(keeper);
        vm.expectRevert(abi.encodeWithSelector(StrategyVault.StrategyVault__MarketNotAllowed.selector, address(0xDEAD)));
        vault.createGmxMarketIncreaseOrder{value: order.executionFee}(order);
    }

    function testFork_MandateBlocksExcessLeverage() external {
        if (address(vault) == address(0)) return;

        // $50 collateral, $1,000 size = 20x > 5x mandate cap → reverts in the vault, before GMX
        StrategyVault.GmxMarketIncreaseOrder memory order = _buildOrder(GMX_BTC_USD_MARKET, 1_000e30, 120_000e30);
        vm.prank(keeper);
        vm.expectRevert(StrategyVault.StrategyVault__LeverageExceeded.selector);
        vault.createGmxMarketIncreaseOrder{value: order.executionFee}(order);
    }

    function _submitOrder(bool isLong, uint256 acceptablePrice) internal returns (bytes32 orderKey) {
        StrategyVault.GmxMarketIncreaseOrder memory order = _buildOrder(GMX_BTC_USD_MARKET, 50e30, acceptablePrice);
        order.isLong = isLong;
        vm.prank(owner);
        orderKey = vault.createGmxMarketIncreaseOrder{value: order.executionFee}(order);
    }

    /// @dev Build a long increase order, funding the vault + keeper. Caller pranks separately.
    function _buildOrder(address market, uint256 sizeDeltaUsd, uint256 acceptablePrice)
        internal
        returns (StrategyVault.GmxMarketIncreaseOrder memory order)
    {
        uint256 collateralAmount = 50e6;
        uint256 executionFee = 0.005 ether;

        deal(ARBITRUM_USDC, address(vault), collateralAmount);
        vm.deal(owner, executionFee);
        vm.deal(keeper, executionFee);

        order = StrategyVault.GmxMarketIncreaseOrder({
            exchangeRouter: GMX_EXCHANGE_ROUTER,
            router: GMX_ROUTER,
            orderVault: GMX_ORDER_VAULT,
            market: market,
            collateralToken: ARBITRUM_USDC,
            receiver: address(0),
            cancellationReceiver: address(0),
            callbackContract: address(0),
            uiFeeReceiver: address(0),
            isLong: true,
            shouldUnwrapNativeToken: false,
            sizeDeltaUsd: sizeDeltaUsd,
            collateralAmount: collateralAmount,
            acceptablePrice: acceptablePrice,
            executionFee: executionFee,
            referencePrice: acceptablePrice,
            maxSlippageBps: 200,
            intentNonce: 0,
            intentDeadline: block.timestamp + 5 minutes,
            callbackGasLimit: 0,
            referralCode: bytes32("agent-invest"),
            ownerSignature: ""
        });
    }

    function _signKeeperIncreaseOrder(StrategyVault.GmxMarketIncreaseOrder memory order)
        internal
        view
        returns (StrategyVault.GmxMarketIncreaseOrder memory)
    {
        bytes32 structHash = keccak256(
            abi.encode(
                KEEPER_INCREASE_ORDER_INTENT_TYPEHASH,
                keeper,
                _hashKeeperIncreaseOrderFields(order),
                order.referencePrice,
                order.maxSlippageBps,
                order.intentNonce,
                order.intentDeadline
            )
        );
        bytes32 domainSeparator = keccak256(
            abi.encode(
                EIP712_DOMAIN_TYPEHASH,
                keccak256(bytes("AgentInvestStrategyVault")),
                keccak256(bytes("1")),
                block.chainid,
                address(vault)
            )
        );
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", domainSeparator, structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(OWNER_PRIVATE_KEY, digest);
        order.ownerSignature = abi.encodePacked(r, s, v);
        return order;
    }

    function _hashKeeperIncreaseOrderFields(StrategyVault.GmxMarketIncreaseOrder memory order)
        internal
        pure
        returns (bytes32)
    {
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
}
