// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {UpgradeableBeacon} from "@openzeppelin/contracts/proxy/beacon/UpgradeableBeacon.sol";
import {Test} from "forge-std/Test.sol";
import {IGmxV2ExchangeRouter} from "src/interfaces/IGmxV2ExchangeRouter.sol";
import {StrategyVault} from "src/StrategyVault.sol";
import {StrategyVaultBase} from "src/StrategyVaultBase.sol";
import {VaultFactory} from "src/VaultFactory.sol";

contract MockAsset is ERC20 {
    constructor() ERC20("Mock USDC", "mUSDC") {}

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}

contract MockGmxRouter {
    function pluginTransfer(address token, address account, address receiver, uint256 amount) external {
        IERC20(token).transferFrom(account, receiver, amount);
    }
}

contract MockGmxExchangeRouter is IGmxV2ExchangeRouter {
    address public router;
    address public multicallSender;
    address public lastWntReceiver;
    address public lastToken;
    address public lastTokenReceiver;
    uint256 public lastWntAmount;
    uint256 public lastTokenAmount;
    uint256 public lastMsgValue;
    uint256 public totalMsgValue;
    uint256 public multicallCount;
    bytes32 public lastCancelledOrderKey;
    uint256 public lastCancelMsgValue;
    CreateOrderParams public lastOrder;

    constructor(address router_) {
        router = router_;
    }

    function multicall(bytes[] calldata data) external payable returns (bytes[] memory results) {
        lastMsgValue = msg.value;
        totalMsgValue += msg.value;
        ++multicallCount;
        multicallSender = msg.sender;
        results = new bytes[](data.length);

        for (uint256 i; i < data.length; ++i) {
            bytes4 selector = bytes4(data[i]);

            if (selector == this.sendWnt.selector) {
                (address receiver, uint256 amount) = abi.decode(data[i][4:], (address, uint256));
                this.sendWnt(receiver, amount);
            } else if (selector == this.sendTokens.selector) {
                (address token, address receiver, uint256 amount) = abi.decode(data[i][4:], (address, address, uint256));
                this.sendTokens(token, receiver, amount);
            } else if (selector == this.createOrder.selector) {
                CreateOrderParams memory params = abi.decode(data[i][4:], (CreateOrderParams));
                bytes32 key = this.createOrder(params);
                results[i] = abi.encode(key);
            } else {
                revert("unknown selector");
            }
        }
    }

    function sendWnt(address receiver, uint256 amount) external payable {
        lastWntReceiver = receiver;
        lastWntAmount = amount;
    }

    function sendTokens(address token, address receiver, uint256 amount) external payable {
        MockGmxRouter(router).pluginTransfer(token, multicallSender, receiver, amount);
        lastToken = token;
        lastTokenReceiver = receiver;
        lastTokenAmount = amount;
    }

    function createOrder(CreateOrderParams calldata params) external payable returns (bytes32) {
        lastOrder.addresses.receiver = params.addresses.receiver;
        lastOrder.addresses.cancellationReceiver = params.addresses.cancellationReceiver;
        lastOrder.addresses.callbackContract = params.addresses.callbackContract;
        lastOrder.addresses.uiFeeReceiver = params.addresses.uiFeeReceiver;
        lastOrder.addresses.market = params.addresses.market;
        lastOrder.addresses.initialCollateralToken = params.addresses.initialCollateralToken;
        lastOrder.numbers = params.numbers;
        lastOrder.orderType = params.orderType;
        lastOrder.decreasePositionSwapType = params.decreasePositionSwapType;
        lastOrder.isLong = params.isLong;
        lastOrder.shouldUnwrapNativeToken = params.shouldUnwrapNativeToken;
        lastOrder.autoCancel = params.autoCancel;
        lastOrder.referralCode = params.referralCode;

        return keccak256("order-key");
    }

    function getLastOrder() external view returns (CreateOrderParams memory) {
        return lastOrder;
    }

    function cancelOrder(bytes32 key) external payable {
        lastCancelledOrderKey = key;
        lastCancelMsgValue = msg.value;
    }
}

contract StrategyVaultV2 is StrategyVault {
    function version() external pure returns (uint256) {
        return 2;
    }
}

contract StrategyVaultTest is Test {
    MockAsset internal asset;
    VaultFactory internal factory;
    StrategyVault internal vault;
    address internal owner = address(0xA11CE);
    address internal beaconOwner = address(0xBEAC0);
    address internal market = address(0xBEEF);

    function setUp() external {
        asset = new MockAsset();
        StrategyVault implementation = new StrategyVault();
        factory = new VaultFactory(address(implementation), beaconOwner);
        vault = StrategyVault(payable(factory.createVault(asset, owner)));
    }

    function _gmx() internal returns (MockGmxExchangeRouter exchangeRouter, address router, address orderVault) {
        router = address(new MockGmxRouter());
        exchangeRouter = new MockGmxExchangeRouter(router);
        orderVault = address(0x5678);

        vm.prank(owner);
        vault.setGmxRouting(address(exchangeRouter), router, orderVault);
    }

    function _increaseOrder(address exchangeRouter, address router, address orderVault)
        internal
        view
        returns (StrategyVaultBase.GmxMarketIncreaseOrder memory)
    {
        return StrategyVaultBase.GmxMarketIncreaseOrder({
            exchangeRouter: exchangeRouter,
            router: router,
            orderVault: orderVault,
            market: market,
            collateralToken: address(asset),
            isLong: true,
            shouldUnwrapNativeToken: false,
            sizeDeltaUsd: 1_000e30,
            collateralAmount: 1_000e18,
            acceptablePrice: 50_000e30,
            executionFee: 0.01 ether,
            referralCode: bytes32("agent-invest")
        });
    }

    function _decreaseOrder(address exchangeRouter, address orderVault)
        internal
        view
        returns (StrategyVaultBase.GmxMarketDecreaseOrder memory)
    {
        return StrategyVaultBase.GmxMarketDecreaseOrder({
            exchangeRouter: exchangeRouter,
            orderVault: orderVault,
            market: market,
            collateralToken: address(asset),
            isLong: true,
            shouldUnwrapNativeToken: false,
            sizeDeltaUsd: 1_000e30,
            collateralWithdrawalAmount: 1_000e18,
            acceptablePrice: 49_000e30,
            executionFee: 0.01 ether,
            minOutputAmount: 0,
            decreasePositionSwapType: IGmxV2ExchangeRouter.DecreasePositionSwapType.NoSwap,
            referralCode: bytes32("agent-invest")
        });
    }

    function _fundVault(uint256 amount) internal {
        asset.mint(owner, amount);
        vm.startPrank(owner);
        asset.approve(address(vault), amount);
        vault.deposit(amount);
        vm.stopPrank();
    }

    /*//////////////////////////////////////////////////////////////
                          FACTORY / UPGRADEABILITY
    //////////////////////////////////////////////////////////////*/

    function test_FactoryRecordsVault() external view {
        assertEq(factory.vaultCount(), 1);
        assertEq(factory.vaults(0), address(vault));
        assertEq(factory.vaultOwner(address(vault)), owner);
        assertEq(vault.owner(), owner);
        assertEq(address(vault.asset()), address(asset));
    }

    function test_ImplementationIsLocked() external {
        StrategyVault implementation = new StrategyVault();
        vm.expectRevert();
        implementation.initialize(asset, owner);
    }

    function test_InitializeCannotBeCalledTwice() external {
        vm.expectRevert();
        vault.initialize(asset, owner);
    }

    function test_BeaconUpgradeSwapsLogicAndPreservesStorage() external {
        _fundVault(1_000e18);

        StrategyVaultV2 v2 = new StrategyVaultV2();
        UpgradeableBeacon beacon = factory.beacon();
        vm.prank(beaconOwner);
        beacon.upgradeTo(address(v2));

        assertEq(StrategyVaultV2(payable(address(vault))).version(), 2);
        assertEq(address(vault.asset()), address(asset));
        assertEq(vault.idleBalance(), 1_000e18);
        assertEq(vault.owner(), owner);
    }

    function test_OnlyBeaconOwnerCanUpgrade() external {
        StrategyVaultV2 v2 = new StrategyVaultV2();
        UpgradeableBeacon beacon = factory.beacon();
        vm.expectRevert();
        beacon.upgradeTo(address(v2));
    }

    /*//////////////////////////////////////////////////////////////
                              DEPOSIT / WITHDRAW
    //////////////////////////////////////////////////////////////*/

    function test_DepositPullsFunds(uint256 amount) external {
        amount = bound(amount, 1, type(uint128).max);
        asset.mint(owner, amount);

        vm.startPrank(owner);
        asset.approve(address(vault), amount);
        vault.deposit(amount);
        vm.stopPrank();

        assertEq(asset.balanceOf(address(vault)), amount);
        assertEq(vault.idleBalance(), amount);
    }

    function test_OnlyOwnerCanDeposit() external {
        asset.mint(address(this), 1e18);
        asset.approve(address(vault), 1e18);

        vm.expectRevert();
        vault.deposit(1e18);
    }

    function test_WithdrawReturnsIdleFunds() external {
        _fundVault(1_000e18);

        vm.prank(owner);
        vault.withdraw(400e18, owner);

        assertEq(asset.balanceOf(owner), 400e18);
        assertEq(vault.idleBalance(), 600e18);
    }

    function test_WithdrawWorksWhilePaused() external {
        _fundVault(1_000e18);

        vm.startPrank(owner);
        vault.pause();
        vault.withdraw(1_000e18, owner);
        vm.stopPrank();

        assertEq(asset.balanceOf(owner), 1_000e18);
    }

    function test_WithdrawRevertsWhenExceedsIdle() external {
        _fundVault(1_000e18);

        vm.prank(owner);
        vm.expectRevert(StrategyVaultBase.StrategyVault__InsufficientIdleBalance.selector);
        vault.withdraw(1_001e18, owner);
    }

    /*//////////////////////////////////////////////////////////////
                                  CONFIG
    //////////////////////////////////////////////////////////////*/

    function test_SetGmxRouting() external {
        vm.prank(owner);
        vault.setGmxRouting(address(1), address(2), address(3));

        (address exchangeRouter, address router, address orderVault) = vault.gmxRouting();
        assertEq(exchangeRouter, address(1));
        assertEq(router, address(2));
        assertEq(orderVault, address(3));
    }

    function test_OnlyOwnerCanSetGmxRouting() external {
        vm.expectRevert();
        vault.setGmxRouting(address(1), address(2), address(3));
    }

    function test_SetGmxRoutingRevertsOnZero() external {
        vm.prank(owner);
        vm.expectRevert(StrategyVaultBase.StrategyVault__ZeroAddress.selector);
        vault.setGmxRouting(address(0), address(2), address(3));
    }

    /*//////////////////////////////////////////////////////////////
                                  GMX
    //////////////////////////////////////////////////////////////*/

    function test_CreateGmxMarketIncreaseOrder() external {
        (MockGmxExchangeRouter exchangeRouter, address router, address orderVault) = _gmx();
        _fundVault(1_000e18);
        StrategyVaultBase.GmxMarketIncreaseOrder memory order =
            _increaseOrder(address(exchangeRouter), router, orderVault);

        vm.deal(owner, order.executionFee);
        vm.prank(owner);
        bytes32 orderKey = vault.createGmxMarketIncreaseOrder{value: order.executionFee}(order);

        assertEq(orderKey, keccak256("order-key"));
        assertEq(exchangeRouter.lastMsgValue(), order.executionFee);
        assertEq(exchangeRouter.lastWntReceiver(), orderVault);
        assertEq(exchangeRouter.lastToken(), address(asset));
        assertEq(exchangeRouter.lastTokenReceiver(), orderVault);
        assertEq(exchangeRouter.lastTokenAmount(), order.collateralAmount);
        IGmxV2ExchangeRouter.CreateOrderParams memory lastOrder = exchangeRouter.getLastOrder();
        assertEq(uint256(lastOrder.orderType), uint256(IGmxV2ExchangeRouter.OrderType.MarketIncrease));
        assertEq(lastOrder.addresses.receiver, address(vault));
        assertEq(lastOrder.addresses.cancellationReceiver, address(vault));
        assertEq(lastOrder.addresses.callbackContract, address(0));
        assertEq(lastOrder.addresses.uiFeeReceiver, address(0));
    }

    function test_CreateGmxMarketIncreaseOrders() external {
        (MockGmxExchangeRouter exchangeRouter, address router, address orderVault) = _gmx();
        _fundVault(1_500e18);
        StrategyVaultBase.GmxMarketIncreaseOrder[] memory orders = new StrategyVaultBase.GmxMarketIncreaseOrder[](2);
        orders[0] = _increaseOrder(address(exchangeRouter), router, orderVault);
        orders[1] = _increaseOrder(address(exchangeRouter), router, orderVault);
        orders[1].isLong = false;
        orders[1].sizeDeltaUsd = 2_000e30;
        orders[1].collateralAmount = 500e18;
        orders[1].executionFee = 0.02 ether;

        uint256 totalExecutionFee = orders[0].executionFee + orders[1].executionFee;
        vm.deal(owner, totalExecutionFee);
        vm.prank(owner);
        bytes32[] memory orderKeys = vault.createGmxMarketIncreaseOrders{value: totalExecutionFee}(orders);

        assertEq(orderKeys.length, 2);
        assertEq(orderKeys[0], keccak256("order-key"));
        assertEq(orderKeys[1], keccak256("order-key"));
        assertEq(exchangeRouter.multicallCount(), 2);
        assertEq(exchangeRouter.totalMsgValue(), totalExecutionFee);
        assertEq(exchangeRouter.lastMsgValue(), orders[1].executionFee);
        assertEq(exchangeRouter.lastTokenAmount(), orders[1].collateralAmount);

        IGmxV2ExchangeRouter.CreateOrderParams memory lastOrder = exchangeRouter.getLastOrder();
        assertEq(uint256(lastOrder.orderType), uint256(IGmxV2ExchangeRouter.OrderType.MarketIncrease));
        assertEq(lastOrder.isLong, false);
        assertEq(lastOrder.numbers.sizeDeltaUsd, orders[1].sizeDeltaUsd);
        assertEq(lastOrder.numbers.initialCollateralDeltaAmount, orders[1].collateralAmount);
    }

    function test_CreateGmxMarketIncreaseOrdersRevertsOnBadTotalExecutionFee() external {
        (MockGmxExchangeRouter exchangeRouter, address router, address orderVault) = _gmx();
        StrategyVaultBase.GmxMarketIncreaseOrder[] memory orders = new StrategyVaultBase.GmxMarketIncreaseOrder[](2);
        orders[0] = _increaseOrder(address(exchangeRouter), router, orderVault);
        orders[1] = _increaseOrder(address(exchangeRouter), router, orderVault);

        uint256 totalExecutionFee = orders[0].executionFee + orders[1].executionFee;
        vm.deal(owner, totalExecutionFee - 1);
        vm.prank(owner);
        vm.expectRevert(StrategyVaultBase.StrategyVault__InvalidExecutionFee.selector);
        vault.createGmxMarketIncreaseOrders{value: totalExecutionFee - 1}(orders);
    }

    function test_CreateGmxMarketIncreaseOrdersRevertsOnEmptyOrders() external {
        StrategyVaultBase.GmxMarketIncreaseOrder[] memory orders = new StrategyVaultBase.GmxMarketIncreaseOrder[](0);

        vm.prank(owner);
        vm.expectRevert(StrategyVaultBase.StrategyVault__ZeroAmount.selector);
        vault.createGmxMarketIncreaseOrders(orders);
    }

    function test_CreateGmxMarketDecreaseOrder() external {
        (MockGmxExchangeRouter exchangeRouter,, address orderVault) = _gmx();
        StrategyVaultBase.GmxMarketDecreaseOrder memory order = _decreaseOrder(address(exchangeRouter), orderVault);

        vm.deal(owner, order.executionFee);
        vm.prank(owner);
        bytes32 orderKey = vault.createGmxMarketDecreaseOrder{value: order.executionFee}(order);

        assertEq(orderKey, keccak256("order-key"));
        assertEq(exchangeRouter.lastMsgValue(), order.executionFee);
        IGmxV2ExchangeRouter.CreateOrderParams memory lastOrder = exchangeRouter.getLastOrder();
        assertEq(uint256(lastOrder.orderType), uint256(IGmxV2ExchangeRouter.OrderType.MarketDecrease));
        assertEq(lastOrder.numbers.sizeDeltaUsd, order.sizeDeltaUsd);
        assertEq(lastOrder.numbers.initialCollateralDeltaAmount, order.collateralWithdrawalAmount);
    }

    function test_CreateGmxMarketDecreaseOrders() external {
        (MockGmxExchangeRouter exchangeRouter,, address orderVault) = _gmx();
        StrategyVaultBase.GmxMarketDecreaseOrder[] memory orders = new StrategyVaultBase.GmxMarketDecreaseOrder[](2);
        orders[0] = _decreaseOrder(address(exchangeRouter), orderVault);
        orders[1] = _decreaseOrder(address(exchangeRouter), orderVault);
        orders[1].isLong = false;
        orders[1].sizeDeltaUsd = 2_000e30;
        orders[1].collateralWithdrawalAmount = 500e18;
        orders[1].executionFee = 0.02 ether;

        uint256 totalExecutionFee = orders[0].executionFee + orders[1].executionFee;
        vm.deal(owner, totalExecutionFee);
        vm.prank(owner);
        bytes32[] memory orderKeys = vault.createGmxMarketDecreaseOrders{value: totalExecutionFee}(orders);

        assertEq(orderKeys.length, 2);
        assertEq(orderKeys[0], keccak256("order-key"));
        assertEq(orderKeys[1], keccak256("order-key"));
        assertEq(exchangeRouter.multicallCount(), 2);
        assertEq(exchangeRouter.totalMsgValue(), totalExecutionFee);
        assertEq(exchangeRouter.lastMsgValue(), orders[1].executionFee);

        IGmxV2ExchangeRouter.CreateOrderParams memory lastOrder = exchangeRouter.getLastOrder();
        assertEq(uint256(lastOrder.orderType), uint256(IGmxV2ExchangeRouter.OrderType.MarketDecrease));
        assertEq(lastOrder.isLong, false);
        assertEq(lastOrder.numbers.sizeDeltaUsd, orders[1].sizeDeltaUsd);
        assertEq(lastOrder.numbers.initialCollateralDeltaAmount, orders[1].collateralWithdrawalAmount);
    }

    function test_CreateGmxMarketDecreaseOrdersRevertsOnBadTotalExecutionFee() external {
        (MockGmxExchangeRouter exchangeRouter,, address orderVault) = _gmx();
        StrategyVaultBase.GmxMarketDecreaseOrder[] memory orders = new StrategyVaultBase.GmxMarketDecreaseOrder[](2);
        orders[0] = _decreaseOrder(address(exchangeRouter), orderVault);
        orders[1] = _decreaseOrder(address(exchangeRouter), orderVault);

        uint256 totalExecutionFee = orders[0].executionFee + orders[1].executionFee;
        vm.deal(owner, totalExecutionFee - 1);
        vm.prank(owner);
        vm.expectRevert(StrategyVaultBase.StrategyVault__InvalidExecutionFee.selector);
        vault.createGmxMarketDecreaseOrders{value: totalExecutionFee - 1}(orders);
    }

    function test_CreateGmxMarketDecreaseOrdersRevertsOnEmptyOrders() external {
        StrategyVaultBase.GmxMarketDecreaseOrder[] memory orders = new StrategyVaultBase.GmxMarketDecreaseOrder[](0);

        vm.prank(owner);
        vm.expectRevert(StrategyVaultBase.StrategyVault__ZeroAmount.selector);
        vault.createGmxMarketDecreaseOrders(orders);
    }

    function test_CancelGmxOrder() external {
        (MockGmxExchangeRouter exchangeRouter,,) = _gmx();
        bytes32 orderKey = keccak256("k");

        vm.deal(owner, 0.01 ether);
        vm.prank(owner);
        vault.cancelGmxOrder{value: 0.01 ether}(address(exchangeRouter), orderKey, 0.01 ether);

        assertEq(exchangeRouter.lastCancelledOrderKey(), orderKey);
        assertEq(exchangeRouter.lastCancelMsgValue(), 0.01 ether);
    }

    function test_OnlyOwnerCanCreateGmxOrder() external {
        (MockGmxExchangeRouter exchangeRouter, address router, address orderVault) = _gmx();
        StrategyVaultBase.GmxMarketIncreaseOrder memory order =
            _increaseOrder(address(exchangeRouter), router, orderVault);

        vm.expectRevert();
        vault.createGmxMarketIncreaseOrder{value: order.executionFee}(order);
    }

    function test_GmxOrderRevertsOnUntrustedRouting() external {
        (MockGmxExchangeRouter exchangeRouter, address router, address orderVault) = _gmx();
        StrategyVaultBase.GmxMarketIncreaseOrder memory order =
            _increaseOrder(address(exchangeRouter), router, orderVault);
        order.router = address(0xBAD);

        vm.deal(owner, order.executionFee);
        vm.prank(owner);
        vm.expectRevert(StrategyVaultBase.StrategyVault__UntrustedRouting.selector);
        vault.createGmxMarketIncreaseOrder{value: order.executionFee}(order);
    }

    function test_GmxOrderRevertsOnBadExecutionFee() external {
        (MockGmxExchangeRouter exchangeRouter, address router, address orderVault) = _gmx();
        StrategyVaultBase.GmxMarketIncreaseOrder memory order =
            _increaseOrder(address(exchangeRouter), router, orderVault);

        vm.deal(owner, order.executionFee + 1);
        vm.prank(owner);
        vm.expectRevert(StrategyVaultBase.StrategyVault__InvalidExecutionFee.selector);
        vault.createGmxMarketIncreaseOrder{value: order.executionFee + 1}(order);
    }

    function test_GmxOrderRevertsWhenPaused() external {
        (MockGmxExchangeRouter exchangeRouter, address router, address orderVault) = _gmx();
        StrategyVaultBase.GmxMarketIncreaseOrder memory order =
            _increaseOrder(address(exchangeRouter), router, orderVault);

        vm.deal(owner, order.executionFee);
        vm.startPrank(owner);
        vault.pause();
        vm.expectRevert();
        vault.createGmxMarketIncreaseOrder{value: order.executionFee}(order);
        vm.stopPrank();
    }

    function test_FactoryConstructorRevertsOnZeroImplementation() external {
        vm.expectRevert(VaultFactory.VaultFactory__ZeroAddress.selector);
        new VaultFactory(address(0), beaconOwner);
    }

    function test_FactoryConstructorRevertsOnZeroBeaconOwner() external {
        StrategyVault implementation = new StrategyVault();
        vm.expectRevert(VaultFactory.VaultFactory__ZeroAddress.selector);
        new VaultFactory(address(implementation), address(0));
    }

    function test_FactoryCreateVaultRevertsOnZeroAsset() external {
        vm.expectRevert(VaultFactory.VaultFactory__ZeroAddress.selector);
        factory.createVault(IERC20(address(0)), owner);
    }

    function test_FactoryCreateVaultRevertsOnZeroOwner() external {
        vm.expectRevert(VaultFactory.VaultFactory__ZeroAddress.selector);
        factory.createVault(asset, address(0));
    }
}
