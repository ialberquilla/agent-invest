// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {UpgradeableBeacon} from "@openzeppelin/contracts/proxy/beacon/UpgradeableBeacon.sol";
import {Test} from "forge-std/Test.sol";
import {IGmxV2ExchangeRouter} from "src/interfaces/IGmxV2ExchangeRouter.sol";
import {StrategyVault} from "src/StrategyVault.sol";
import {VaultFactory} from "src/VaultFactory.sol";

contract MockAsset is ERC20 {
    constructor() ERC20("Mock USDC", "mUSDC") {}

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}

contract CallTarget {
    uint256 public calls;

    function record() external payable returns (uint256) {
        ++calls;
        return calls;
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
    bytes32 public lastCancelledOrderKey;
    uint256 public lastCancelMsgValue;
    CreateOrderParams public lastOrder;

    constructor(address router_) {
        router = router_;
    }

    function multicall(bytes[] calldata data) external payable returns (bytes[] memory results) {
        lastMsgValue = msg.value;
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

    function cancelOrder(bytes32 key) external payable {
        lastCancelledOrderKey = key;
        lastCancelMsgValue = msg.value;
    }
}

/// @dev A v2 implementation used to prove beacon upgrades swap logic for live vaults while
///      preserving ERC-7201 namespaced storage (the original `asset` survives the upgrade).
contract StrategyVaultV2 is StrategyVault {
    function version() external pure returns (uint256) {
        return 2;
    }
}

contract StrategyVaultTest is Test {
    uint256 internal constant OWNER_PRIVATE_KEY = 0xA11CE;
    bytes32 internal constant EIP712_DOMAIN_TYPEHASH =
        keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)");
    bytes32 internal constant KEEPER_INCREASE_ORDER_INTENT_TYPEHASH = keccak256(
        "KeeperIncreaseOrderIntent(address keeper,bytes32 orderHash,uint256 referencePrice,uint256 maxSlippageBps,uint256 nonce,uint256 deadline)"
    );
    bytes32 internal constant KEEPER_DECREASE_ORDER_INTENT_TYPEHASH = keccak256(
        "KeeperDecreaseOrderIntent(address keeper,bytes32 orderHash,uint256 referencePrice,uint256 maxSlippageBps,uint256 nonce,uint256 deadline)"
    );

    MockAsset internal asset;
    VaultFactory internal factory;
    StrategyVault internal vault;
    address internal owner;
    address internal beaconOwner = address(0xBEAC0);
    address internal keeper = address(0xC0FFEE);
    address internal market = address(0xBEEF);

    function setUp() external {
        owner = vm.addr(OWNER_PRIVATE_KEY);
        asset = new MockAsset();
        StrategyVault implementation = new StrategyVault();
        factory = new VaultFactory(address(implementation), beaconOwner);
        vault = StrategyVault(payable(factory.createVault(asset, owner)));
    }

    /// @dev Standard permissive mandate so GMX happy-paths reach the router; per-test we tighten it.
    function _configure(uint256 minInterval, address keeper_) internal {
        vm.startPrank(owner);
        vault.setMandate(
            StrategyVault.Mandate({
                maxLeverage: 5,
                maxPositionSizeUsd: 1_000_000e30,
                minRebalanceInterval: minInterval,
                maxKeeperSlippageBps: 200
            })
        );
        vault.addMarket(market);
        if (keeper_ != address(0)) vault.setKeeper(keeper_);
        vm.stopPrank();
    }

    function _gmx() internal returns (MockGmxExchangeRouter exchangeRouter, address router) {
        router = address(new MockGmxRouter());
        exchangeRouter = new MockGmxExchangeRouter(router);
        // pin the vault's canonical routing to these mocks (orderVault matches the order structs)
        vm.prank(owner);
        vault.setGmxRouting(address(exchangeRouter), router, address(0x5678));
    }

    function _increaseOrder(address exchangeRouter, address router, uint256 sizeDeltaUsd, uint256 collateralAmount)
        internal
        view
        returns (StrategyVault.GmxMarketIncreaseOrder memory)
    {
        return StrategyVault.GmxMarketIncreaseOrder({
            exchangeRouter: exchangeRouter,
            router: router,
            orderVault: address(0x5678),
            market: market,
            collateralToken: address(asset),
            receiver: address(0),
            cancellationReceiver: address(0),
            callbackContract: address(0),
            uiFeeReceiver: address(0),
            isLong: true,
            shouldUnwrapNativeToken: false,
            sizeDeltaUsd: sizeDeltaUsd,
            collateralAmount: collateralAmount,
            acceptablePrice: 51_000e30,
            executionFee: 0.01 ether,
            referencePrice: 50_000e30,
            maxSlippageBps: 200,
            intentNonce: 0,
            intentDeadline: block.timestamp + 1 hours,
            callbackGasLimit: 0,
            referralCode: bytes32("agent-invest"),
            ownerSignature: ""
        });
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
        vm.expectRevert(); // InvalidInitialization: initializers disabled in the implementation
        implementation.initialize(asset, owner);
    }

    function test_InitializeCannotBeCalledTwice() external {
        vm.expectRevert(); // InvalidInitialization
        vault.initialize(asset, owner);
    }

    function test_BeaconUpgradeSwapsLogicAndPreservesStorage() external {
        asset.mint(owner, 1_000e18);
        vm.startPrank(owner);
        asset.approve(address(vault), 1_000e18);
        vault.deposit(1_000e18);
        vault.setKeeper(keeper);
        vm.stopPrank();

        StrategyVaultV2 v2 = new StrategyVaultV2();
        UpgradeableBeacon beacon = factory.beacon();
        vm.prank(beaconOwner);
        beacon.upgradeTo(address(v2));

        // new logic is live on the existing proxy...
        assertEq(StrategyVaultV2(payable(address(vault))).version(), 2);
        // ...and namespaced storage is intact.
        assertEq(address(vault.asset()), address(asset));
        assertEq(vault.idleBalance(), 1_000e18);
        assertEq(vault.owner(), owner);
        assertEq(vault.keeper(), keeper);
    }

    function test_OnlyBeaconOwnerCanUpgrade() external {
        StrategyVaultV2 v2 = new StrategyVaultV2();
        UpgradeableBeacon beacon = factory.beacon();
        vm.expectRevert(); // OwnableUnauthorizedAccount
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

        assertEq(vault.idleBalance(), amount);
        assertEq(asset.balanceOf(address(vault)), amount);
    }

    function test_OnlyOwnerCanDeposit() external {
        asset.mint(address(this), 1e18);
        asset.approve(address(vault), 1e18);
        vm.expectRevert(); // OwnableUnauthorizedAccount
        vault.deposit(1e18);
    }

    function test_DepositRevertsOnZero() external {
        vm.prank(owner);
        vm.expectRevert(StrategyVault.StrategyVault__ZeroAmount.selector);
        vault.deposit(0);
    }

    function test_WithdrawReturnsIdleFunds() external {
        asset.mint(owner, 500e18);
        vm.startPrank(owner);
        asset.approve(address(vault), 500e18);
        vault.deposit(500e18);
        vault.withdraw(200e18, owner);
        vm.stopPrank();

        assertEq(vault.idleBalance(), 300e18);
        assertEq(asset.balanceOf(owner), 200e18);
    }

    function test_OnlyOwnerCanWithdraw() external {
        asset.mint(owner, 100e18);
        vm.startPrank(owner);
        asset.approve(address(vault), 100e18);
        vault.deposit(100e18);
        vm.stopPrank();

        vm.expectRevert(); // OwnableUnauthorizedAccount
        vault.withdraw(1e18, address(this));
    }

    function test_WithdrawRevertsWhenExceedsIdle() external {
        asset.mint(owner, 100e18);
        vm.startPrank(owner);
        asset.approve(address(vault), 100e18);
        vault.deposit(100e18);
        vm.expectRevert(StrategyVault.StrategyVault__InsufficientIdleBalance.selector);
        vault.withdraw(100e18 + 1, owner);
        vm.stopPrank();
    }

    function test_WithdrawWorksWhilePaused() external {
        asset.mint(owner, 100e18);
        vm.startPrank(owner);
        asset.approve(address(vault), 100e18);
        vault.deposit(100e18);
        vault.pause();
        vault.withdraw(100e18, owner); // pause must not trap owner custody of idle funds
        vm.stopPrank();

        assertEq(asset.balanceOf(owner), 100e18);
    }

    /*//////////////////////////////////////////////////////////////
                          MANDATE / KEEPER CONFIG
    //////////////////////////////////////////////////////////////*/

    function test_OnlyOwnerCanSetKeeper() external {
        vm.expectRevert(); // OwnableUnauthorizedAccount
        vault.setKeeper(keeper);
    }

    function test_SetKeeper() external {
        vm.prank(owner);
        vault.setKeeper(keeper);
        assertEq(vault.keeper(), keeper);
    }

    function test_OnlyOwnerCanSetMandate() external {
        vm.expectRevert(); // OwnableUnauthorizedAccount
        vault.setMandate(
            StrategyVault.Mandate({
                maxLeverage: 5, maxPositionSizeUsd: 1e30, minRebalanceInterval: 0, maxKeeperSlippageBps: 200
            })
        );
    }

    function test_SetMandateRevertsOnZeroLeverage() external {
        vm.prank(owner);
        vm.expectRevert(StrategyVault.StrategyVault__InvalidMandate.selector);
        vault.setMandate(
            StrategyVault.Mandate({
                maxLeverage: 0, maxPositionSizeUsd: 1e30, minRebalanceInterval: 0, maxKeeperSlippageBps: 200
            })
        );
    }

    function test_SetMandateRevertsOnZeroPositionSize() external {
        vm.prank(owner);
        vm.expectRevert(StrategyVault.StrategyVault__InvalidMandate.selector);
        vault.setMandate(
            StrategyVault.Mandate({
                maxLeverage: 3, maxPositionSizeUsd: 0, minRebalanceInterval: 0, maxKeeperSlippageBps: 200
            })
        );
    }

    function test_SetMandateRevertsOnExcessKeeperSlippage() external {
        vm.prank(owner);
        vm.expectRevert(StrategyVault.StrategyVault__InvalidMandate.selector);
        vault.setMandate(
            StrategyVault.Mandate({
                maxLeverage: 3, maxPositionSizeUsd: 1e30, minRebalanceInterval: 0, maxKeeperSlippageBps: 1_001
            })
        );
    }

    function test_AddAndRemoveMarket() external {
        vm.startPrank(owner);
        vault.addMarket(market);
        assertTrue(vault.isMarketAllowed(market));
        assertEq(vault.allowedMarkets().length, 1);
        vault.removeMarket(market);
        assertFalse(vault.isMarketAllowed(market));
        assertEq(vault.allowedMarkets().length, 0);
        vm.stopPrank();
    }

    function test_OnlyOwnerCanAddMarket() external {
        vm.expectRevert(); // OwnableUnauthorizedAccount
        vault.addMarket(market);
    }

    function test_AddMarketRevertsAboveCap() external {
        vm.startPrank(owner);
        for (uint160 i = 1; i <= 32; ++i) {
            vault.addMarket(address(i));
        }
        vm.expectRevert(StrategyVault.StrategyVault__MaxMarketsExceeded.selector);
        vault.addMarket(address(0xDEAD));
        vm.stopPrank();
    }

    /*//////////////////////////////////////////////////////////////
                       MANDATE ENFORCEMENT (THE CORE)
    //////////////////////////////////////////////////////////////*/

    function test_KeeperCanSubmitConformingIncrease() external {
        _configure(0, keeper);
        (MockGmxExchangeRouter er, address router) = _gmx();
        asset.mint(address(vault), 1_000e18);
        vm.deal(keeper, 0.01 ether);

        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 2_000e30, 1_000e18);
        order = _signKeeperIncreaseOrder(order);

        vm.prank(keeper);
        bytes32 orderKey = vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
        assertEq(orderKey, keccak256("order-key"));
    }

    function test_RevertsOnDisallowedMarket() external {
        // mandate set but market NOT added
        vm.startPrank(owner);
        vault.setMandate(
            StrategyVault.Mandate({
                maxLeverage: 5, maxPositionSizeUsd: 1_000_000e30, minRebalanceInterval: 0, maxKeeperSlippageBps: 200
            })
        );
        vault.setKeeper(keeper);
        vm.stopPrank();

        (MockGmxExchangeRouter er, address router) = _gmx();
        asset.mint(address(vault), 1_000e18);
        vm.deal(keeper, 0.01 ether);
        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 2_000e30, 1_000e18);

        vm.prank(keeper);
        vm.expectRevert(abi.encodeWithSelector(StrategyVault.StrategyVault__MarketNotAllowed.selector, market));
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
    }

    function test_RevertsOnPositionSizeExceeded() external {
        vm.startPrank(owner);
        vault.setMandate(
            StrategyVault.Mandate({
                maxLeverage: 100, maxPositionSizeUsd: 1_000e30, minRebalanceInterval: 0, maxKeeperSlippageBps: 200
            })
        );
        vault.addMarket(market);
        vault.setKeeper(keeper);
        vm.stopPrank();

        (MockGmxExchangeRouter er, address router) = _gmx();
        asset.mint(address(vault), 1_000e18);
        vm.deal(keeper, 0.01 ether);
        // size 2_000e30 > cap 1_000e30
        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 2_000e30, 1_000e18);

        vm.prank(keeper);
        vm.expectRevert(StrategyVault.StrategyVault__PositionSizeExceeded.selector);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
    }

    function test_RevertsOnLeverageExceeded() external {
        vm.startPrank(owner);
        vault.setMandate(
            StrategyVault.Mandate({
                maxLeverage: 2, maxPositionSizeUsd: 1_000_000e30, minRebalanceInterval: 0, maxKeeperSlippageBps: 200
            })
        );
        vault.addMarket(market);
        vault.setKeeper(keeper);
        vm.stopPrank();

        (MockGmxExchangeRouter er, address router) = _gmx();
        asset.mint(address(vault), 1_000e18);
        vm.deal(keeper, 0.01 ether);
        // collateral $1000 (1_000e18 * 1e12 = 1e33), size 3_000e30 => 3x > 2x cap
        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 3_000e30, 1_000e18);

        vm.prank(keeper);
        vm.expectRevert(StrategyVault.StrategyVault__LeverageExceeded.selector);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
    }

    function test_KeeperRateLimited() external {
        _configure(1 hours, keeper);
        (MockGmxExchangeRouter er, address router) = _gmx();
        asset.mint(address(vault), 2_000e18);
        vm.deal(keeper, 1 ether);
        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 1_000e30, 1_000e18);
        order = _signKeeperIncreaseOrder(order);

        vm.prank(keeper);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);

        // second keeper order within the interval reverts
        vm.prank(keeper);
        vm.expectRevert(StrategyVault.StrategyVault__RebalanceTooSoon.selector);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);

        // after the interval, it succeeds
        vm.warp(block.timestamp + 1 hours);
        ++order.intentNonce;
        order.intentDeadline = block.timestamp + 1 hours;
        order = _signKeeperIncreaseOrder(order);
        vm.prank(keeper);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
    }

    function test_OwnerNotRateLimited() external {
        _configure(1 hours, keeper);
        (MockGmxExchangeRouter er, address router) = _gmx();
        asset.mint(address(vault), 2_000e18);
        vm.deal(owner, 1 ether);
        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 1_000e30, 1_000e18);

        vm.startPrank(owner);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order); // no throttle for the human owner
        vm.stopPrank();
    }

    function test_NonOwnerNonKeeperCannotSubmit() external {
        _configure(0, keeper);
        (MockGmxExchangeRouter er, address router) = _gmx();
        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 1_000e30, 1_000e18);

        vm.deal(address(this), 0.01 ether);
        vm.expectRevert(StrategyVault.StrategyVault__NotAuthorized.selector);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
    }

    function test_RevertsOnUntrustedRouter() external {
        // valid mandate + market, routing pinned to the real mocks, but the order points router elsewhere
        _configure(0, keeper);
        (MockGmxExchangeRouter er, address router) = _gmx();
        asset.mint(address(vault), 1_000e18);
        vm.deal(keeper, 0.01 ether);

        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 1_000e30, 1_000e18);
        order.router = address(0xBAD); // attacker-controlled spender

        vm.prank(keeper);
        vm.expectRevert(StrategyVault.StrategyVault__UntrustedRouting.selector);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
    }

    function test_RevertsOnUntrustedCollateralToken() external {
        _configure(0, keeper);
        (MockGmxExchangeRouter er, address router) = _gmx();
        vm.deal(keeper, 0.01 ether);

        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 1_000e30, 1_000e18);
        order.collateralToken = address(0xBAD); // not the vault asset

        vm.prank(keeper);
        vm.expectRevert(StrategyVault.StrategyVault__UntrustedRouting.selector);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
    }

    function test_ReceiverIsForcedToVault() external {
        // even if the keeper sets receiver to itself, the built GMX params point back at the vault
        _configure(0, keeper);
        (MockGmxExchangeRouter er, address router) = _gmx();
        asset.mint(address(vault), 1_000e18);
        vm.deal(keeper, 0.01 ether);

        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 1_000e30, 1_000e18);
        order.receiver = keeper;
        order.cancellationReceiver = keeper;
        order = _signKeeperIncreaseOrder(order);

        vm.prank(keeper);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);

        (IGmxV2ExchangeRouter.CreateOrderParamsAddresses memory addresses,,,,,,,) = er.lastOrder();
        assertEq(addresses.receiver, address(vault));
        assertEq(addresses.cancellationReceiver, address(vault));
        assertEq(addresses.uiFeeReceiver, address(0));
        assertEq(addresses.callbackContract, address(0));
    }

    function test_ZeroCollateralSizeUpReverts() external {
        // closing the leverage-bypass: an increase with no added collateral is rejected
        _configure(0, keeper);
        (MockGmxExchangeRouter er, address router) = _gmx();
        vm.deal(keeper, 0.01 ether);

        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 1_000e30, 0);

        vm.prank(keeper);
        vm.expectRevert(StrategyVault.StrategyVault__LeverageExceeded.selector);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
    }

    function test_KeeperIncreaseLongRevertsWhenAcceptablePriceAboveBound() external {
        _configure(0, keeper);
        (MockGmxExchangeRouter er, address router) = _gmx();
        asset.mint(address(vault), 1_000e18);
        vm.deal(keeper, 0.01 ether);

        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 1_000e30, 1_000e18);
        order.isLong = true;
        order.acceptablePrice = 51_000e30 + 1;

        vm.prank(keeper);
        vm.expectRevert(StrategyVault.StrategyVault__KeeperSlippageExceeded.selector);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
    }

    function test_KeeperIncreaseShortRevertsWhenAcceptablePriceBelowBound() external {
        _configure(0, keeper);
        (MockGmxExchangeRouter er, address router) = _gmx();
        asset.mint(address(vault), 1_000e18);
        vm.deal(keeper, 0.01 ether);

        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 1_000e30, 1_000e18);
        order.isLong = false;
        order.acceptablePrice = 49_000e30 - 1;

        vm.prank(keeper);
        vm.expectRevert(StrategyVault.StrategyVault__KeeperSlippageExceeded.selector);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
    }

    function test_KeeperOrderRevertsWhenIntentNonceIsReused() external {
        _configure(0, keeper);
        (MockGmxExchangeRouter er, address router) = _gmx();
        asset.mint(address(vault), 2_000e18);
        vm.deal(keeper, 0.02 ether);

        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 1_000e30, 1_000e18);
        order = _signKeeperIncreaseOrder(order);

        vm.prank(keeper);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);

        vm.prank(keeper);
        vm.expectRevert(abi.encodeWithSelector(StrategyVault.StrategyVault__OrderIntentNonceUsed.selector, 0));
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
    }

    function test_KeeperOrderRevertsWhenIntentExpired() external {
        vm.warp(10);
        _configure(0, keeper);
        (MockGmxExchangeRouter er, address router) = _gmx();
        asset.mint(address(vault), 1_000e18);
        vm.deal(keeper, 0.01 ether);

        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 1_000e30, 1_000e18);
        order.intentDeadline = block.timestamp - 1;
        order = _signKeeperIncreaseOrder(order);

        vm.prank(keeper);
        vm.expectRevert(StrategyVault.StrategyVault__OrderIntentExpired.selector);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
    }

    function test_KeeperOrderRevertsWhenSlippageExceedsMandate() external {
        vm.startPrank(owner);
        vault.setMandate(
            StrategyVault.Mandate({
                maxLeverage: 5, maxPositionSizeUsd: 1_000_000e30, minRebalanceInterval: 0, maxKeeperSlippageBps: 50
            })
        );
        vault.addMarket(market);
        vault.setKeeper(keeper);
        vm.stopPrank();

        (MockGmxExchangeRouter er, address router) = _gmx();
        asset.mint(address(vault), 1_000e18);
        vm.deal(keeper, 0.01 ether);

        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 1_000e30, 1_000e18);
        order.maxSlippageBps = 51;
        order.acceptablePrice = 50_255e30;
        order = _signKeeperIncreaseOrder(order);

        vm.prank(keeper);
        vm.expectRevert(StrategyVault.StrategyVault__KeeperSlippageExceeded.selector);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
    }

    function test_OnlyOwnerCanSetGmxRouting() external {
        vm.expectRevert(); // OwnableUnauthorizedAccount
        vault.setGmxRouting(address(1), address(2), address(3));
    }

    function test_SetGmxRoutingRevertsOnZero() external {
        vm.prank(owner);
        vm.expectRevert(StrategyVault.StrategyVault__ZeroAddress.selector);
        vault.setGmxRouting(address(0), address(2), address(3));
    }

    function test_DecreaseRevertsOnDisallowedMarket() external {
        // keeper set, mandate set, but market not allowed
        vm.startPrank(owner);
        vault.setMandate(
            StrategyVault.Mandate({
                maxLeverage: 5, maxPositionSizeUsd: 1_000_000e30, minRebalanceInterval: 0, maxKeeperSlippageBps: 200
            })
        );
        vault.setKeeper(keeper);
        vm.stopPrank();

        (MockGmxExchangeRouter er,) = _gmx();
        vm.deal(keeper, 0.01 ether);
        StrategyVault.GmxMarketDecreaseOrder memory order = _decreaseOrder(address(er));

        vm.prank(keeper);
        vm.expectRevert(abi.encodeWithSelector(StrategyVault.StrategyVault__MarketNotAllowed.selector, market));
        vault.createGmxMarketDecreaseOrder{value: 0.01 ether}(order);
    }

    function test_KeeperCanSubmitConformingDecrease() external {
        _configure(0, keeper);
        (MockGmxExchangeRouter er,) = _gmx();
        vm.deal(keeper, 0.01 ether);

        StrategyVault.GmxMarketDecreaseOrder memory order = _decreaseOrder(address(er));
        order.intentNonce = 9;
        order = _signKeeperDecreaseOrder(order);

        vm.prank(keeper);
        bytes32 orderKey = vault.createGmxMarketDecreaseOrder{value: 0.01 ether}(order);

        assertEq(orderKey, keccak256("order-key"));
    }

    function test_KeeperDecreaseLongRevertsWhenAcceptablePriceBelowBound() external {
        _configure(0, keeper);
        (MockGmxExchangeRouter er,) = _gmx();
        vm.deal(keeper, 0.01 ether);

        StrategyVault.GmxMarketDecreaseOrder memory order = _decreaseOrder(address(er));
        order.isLong = true;
        order.acceptablePrice = 49_000e30 - 1;

        vm.prank(keeper);
        vm.expectRevert(StrategyVault.StrategyVault__KeeperSlippageExceeded.selector);
        vault.createGmxMarketDecreaseOrder{value: 0.01 ether}(order);
    }

    function test_KeeperDecreaseShortRevertsWhenAcceptablePriceAboveBound() external {
        _configure(0, keeper);
        (MockGmxExchangeRouter er,) = _gmx();
        vm.deal(keeper, 0.01 ether);

        StrategyVault.GmxMarketDecreaseOrder memory order = _decreaseOrder(address(er));
        order.isLong = false;
        order.acceptablePrice = 51_000e30 + 1;

        vm.prank(keeper);
        vm.expectRevert(StrategyVault.StrategyVault__KeeperSlippageExceeded.selector);
        vault.createGmxMarketDecreaseOrder{value: 0.01 ether}(order);
    }

    function _decreaseOrder(address exchangeRouter)
        internal
        view
        returns (StrategyVault.GmxMarketDecreaseOrder memory)
    {
        return StrategyVault.GmxMarketDecreaseOrder({
            exchangeRouter: exchangeRouter,
            orderVault: address(0x5678),
            market: market,
            collateralToken: address(asset),
            receiver: address(0),
            cancellationReceiver: address(0),
            callbackContract: address(0),
            uiFeeReceiver: address(0),
            isLong: true,
            shouldUnwrapNativeToken: false,
            sizeDeltaUsd: 500e30,
            collateralWithdrawalAmount: 100e18,
            acceptablePrice: 49_000e30,
            executionFee: 0.01 ether,
            referencePrice: 50_000e30,
            maxSlippageBps: 200,
            intentNonce: 0,
            intentDeadline: block.timestamp + 1 hours,
            callbackGasLimit: 0,
            minOutputAmount: 95e18,
            decreasePositionSwapType: IGmxV2ExchangeRouter.DecreasePositionSwapType.NoSwap,
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
        order.ownerSignature = _signTypedData(structHash);
        return order;
    }

    function _signKeeperDecreaseOrder(StrategyVault.GmxMarketDecreaseOrder memory order)
        internal
        view
        returns (StrategyVault.GmxMarketDecreaseOrder memory)
    {
        bytes32 structHash = keccak256(
            abi.encode(
                KEEPER_DECREASE_ORDER_INTENT_TYPEHASH,
                keeper,
                _hashKeeperDecreaseOrderFields(order),
                order.referencePrice,
                order.maxSlippageBps,
                order.intentNonce,
                order.intentDeadline
            )
        );
        order.ownerSignature = _signTypedData(structHash);
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

    function _hashKeeperDecreaseOrderFields(StrategyVault.GmxMarketDecreaseOrder memory order)
        internal
        pure
        returns (bytes32)
    {
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

    function _signTypedData(bytes32 structHash) internal view returns (bytes memory) {
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
        return abi.encodePacked(r, s, v);
    }

    /*//////////////////////////////////////////////////////////////
                                 EXECUTE
    //////////////////////////////////////////////////////////////*/

    function test_ExecuteCallsTarget() external {
        CallTarget target = new CallTarget();

        vm.prank(owner);
        bytes memory result = vault.execute(address(target), 0, abi.encodeCall(CallTarget.record, ()));

        assertEq(abi.decode(result, (uint256)), 1);
        assertEq(target.calls(), 1);
    }

    function test_OnlyOwnerCanExecute() external {
        CallTarget target = new CallTarget();

        vm.expectRevert(); // OwnableUnauthorizedAccount
        vault.execute(address(target), 0, abi.encodeCall(CallTarget.record, ()));
    }

    function test_KeeperCannotExecute() external {
        _configure(0, keeper);
        CallTarget target = new CallTarget();

        vm.prank(keeper);
        vm.expectRevert(); // keeper is not the owner; execute is owner-only
        vault.execute(address(target), 0, abi.encodeCall(CallTarget.record, ()));
    }

    function test_ExecuteRevertsWhenPaused() external {
        CallTarget target = new CallTarget();

        vm.startPrank(owner);
        vault.pause();
        vm.expectRevert(); // EnforcedPause
        vault.execute(address(target), 0, abi.encodeCall(CallTarget.record, ()));
        vm.stopPrank();
    }

    /*//////////////////////////////////////////////////////////////
                          GMX ORDERS (HAPPY PATH)
    //////////////////////////////////////////////////////////////*/

    function test_CreateGmxMarketIncreaseOrderLong() external {
        _testCreateGmxMarketIncreaseOrder(true);
    }

    function test_CreateGmxMarketIncreaseOrderShort() external {
        _testCreateGmxMarketIncreaseOrder(false);
    }

    function _testCreateGmxMarketIncreaseOrder(bool isLong) internal {
        _configure(0, address(0));
        (MockGmxExchangeRouter exchangeRouter, address router) = _gmx();
        uint256 collateralAmount = 1_000e18;
        uint256 executionFee = 0.01 ether;

        asset.mint(address(vault), collateralAmount);
        vm.deal(owner, executionFee);

        StrategyVault.GmxMarketIncreaseOrder memory order =
            _increaseOrder(address(exchangeRouter), router, 2_000e30, collateralAmount);
        order.isLong = isLong;
        order.acceptablePrice = isLong ? 51_000e30 : 49_000e30;

        vm.prank(owner);
        bytes32 orderKey = vault.createGmxMarketIncreaseOrder{value: executionFee}(order);

        assertEq(orderKey, keccak256("order-key"));

        assertEq(exchangeRouter.lastMsgValue(), executionFee);
        assertEq(exchangeRouter.lastWntReceiver(), order.orderVault);
        assertEq(exchangeRouter.lastWntAmount(), executionFee);
        assertEq(exchangeRouter.lastToken(), address(asset));
        assertEq(exchangeRouter.lastTokenReceiver(), order.orderVault);
        assertEq(exchangeRouter.lastTokenAmount(), collateralAmount);
        assertEq(asset.balanceOf(order.orderVault), collateralAmount);

        (
            IGmxV2ExchangeRouter.CreateOrderParamsAddresses memory addresses,
            IGmxV2ExchangeRouter.CreateOrderParamsNumbers memory numbers,
            IGmxV2ExchangeRouter.OrderType orderType,,
            bool actualIsLong,,,
        ) = exchangeRouter.lastOrder();

        assertEq(addresses.receiver, address(vault));
        assertEq(addresses.cancellationReceiver, address(vault));
        assertEq(addresses.market, market);
        assertEq(addresses.initialCollateralToken, address(asset));
        assertEq(numbers.sizeDeltaUsd, 2_000e30);
        assertEq(numbers.initialCollateralDeltaAmount, collateralAmount);
        assertEq(numbers.acceptablePrice, order.acceptablePrice);
        assertEq(numbers.executionFee, executionFee);
        assertEq(uint8(orderType), uint8(IGmxV2ExchangeRouter.OrderType.MarketIncrease));
        assertEq(actualIsLong, isLong);
    }

    function test_CreateGmxOrderRevertsOnBadExecutionFee() external {
        _configure(0, address(0));
        (MockGmxExchangeRouter exchangeRouter, address router) = _gmx();
        asset.mint(address(vault), 1_000e18);
        vm.deal(owner, 1 ether);

        StrategyVault.GmxMarketIncreaseOrder memory order =
            _increaseOrder(address(exchangeRouter), router, 2_000e30, 1_000e18);

        vm.prank(owner);
        vm.expectRevert(StrategyVault.StrategyVault__InvalidExecutionFee.selector);
        vault.createGmxMarketIncreaseOrder{value: 0.02 ether}(order);
    }

    function test_CreateGmxMarketDecreaseOrder() external {
        _configure(0, address(0));
        uint256 executionFee = 0.01 ether;
        (MockGmxExchangeRouter exchangeRouter,) = _gmx();

        vm.deal(owner, executionFee);
        StrategyVault.GmxMarketDecreaseOrder memory order = _decreaseOrder(address(exchangeRouter));

        vm.prank(owner);
        bytes32 orderKey = vault.createGmxMarketDecreaseOrder{value: executionFee}(order);

        assertEq(orderKey, keccak256("order-key"));
        assertEq(exchangeRouter.lastMsgValue(), executionFee);
        assertEq(exchangeRouter.lastWntReceiver(), order.orderVault);
        assertEq(exchangeRouter.lastWntAmount(), executionFee);
        assertEq(exchangeRouter.lastTokenAmount(), 0);

        (
            IGmxV2ExchangeRouter.CreateOrderParamsAddresses memory addresses,
            IGmxV2ExchangeRouter.CreateOrderParamsNumbers memory numbers,
            IGmxV2ExchangeRouter.OrderType orderType,
            IGmxV2ExchangeRouter.DecreasePositionSwapType decreaseSwapType,
            bool actualIsLong,,,
        ) = exchangeRouter.lastOrder();

        assertEq(addresses.receiver, address(vault));
        assertEq(addresses.cancellationReceiver, address(vault));
        assertEq(addresses.market, market);
        assertEq(addresses.initialCollateralToken, address(asset));
        assertEq(numbers.sizeDeltaUsd, 500e30);
        assertEq(numbers.initialCollateralDeltaAmount, 100e18);
        assertEq(numbers.acceptablePrice, order.acceptablePrice);
        assertEq(numbers.minOutputAmount, 95e18);
        assertEq(uint8(orderType), uint8(IGmxV2ExchangeRouter.OrderType.MarketDecrease));
        assertEq(uint8(decreaseSwapType), uint8(IGmxV2ExchangeRouter.DecreasePositionSwapType.NoSwap));
        assertEq(actualIsLong, true);
    }

    function test_CancelGmxOrder() external {
        uint256 executionFee = 0.01 ether;
        bytes32 orderKey = keccak256("order-key");
        MockGmxExchangeRouter exchangeRouter = new MockGmxExchangeRouter(address(new MockGmxRouter()));

        vm.deal(owner, executionFee);

        vm.prank(owner);
        vault.cancelGmxOrder{value: executionFee}(address(exchangeRouter), orderKey, executionFee);

        assertEq(exchangeRouter.lastCancelledOrderKey(), orderKey);
        assertEq(exchangeRouter.lastCancelMsgValue(), executionFee);
    }

    function test_GmxOrderRevertsWhenPaused() external {
        MockGmxExchangeRouter exchangeRouter = new MockGmxExchangeRouter(address(new MockGmxRouter()));
        vm.deal(owner, 0.01 ether);

        vm.startPrank(owner);
        vault.pause();
        vm.expectRevert(); // EnforcedPause
        vault.cancelGmxOrder{value: 0.01 ether}(address(exchangeRouter), keccak256("k"), 0.01 ether);
        vm.stopPrank();
    }

    function test_OnlyOwnerOrKeeperCanCancelGmxOrder() external {
        MockGmxExchangeRouter exchangeRouter = new MockGmxExchangeRouter(address(new MockGmxRouter()));
        vm.expectRevert(StrategyVault.StrategyVault__NotAuthorized.selector);
        vault.cancelGmxOrder(address(exchangeRouter), keccak256("k"), 0);
    }

    /*//////////////////////////////////////////////////////////////
                          HARDENING / EDGE CASES
    //////////////////////////////////////////////////////////////*/

    function test_ExecuteRevertsOnZeroTarget() external {
        vm.prank(owner);
        vm.expectRevert(StrategyVault.StrategyVault__ZeroTarget.selector);
        vault.execute(address(0), 0, "");
    }

    function test_WithdrawRevertsOnZeroAddress() external {
        asset.mint(owner, 1e18);
        vm.startPrank(owner);
        asset.approve(address(vault), 1e18);
        vault.deposit(1e18);
        vm.expectRevert(StrategyVault.StrategyVault__ZeroAddress.selector);
        vault.withdraw(1e18, address(0));
        vm.stopPrank();
    }

    function test_IncreaseRevertsOnZeroAddressField() external {
        _configure(0, address(0));
        (MockGmxExchangeRouter er, address router) = _gmx();
        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 1_000e30, 1_000e18);
        order.exchangeRouter = address(0); // validated before the mandate/fee checks

        vm.deal(owner, 0.01 ether);
        vm.prank(owner);
        vm.expectRevert(StrategyVault.StrategyVault__ZeroAddress.selector);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
    }

    function test_SetKeeperToZeroDisablesKeeper() external {
        _configure(0, keeper);
        (MockGmxExchangeRouter er, address router) = _gmx();
        asset.mint(address(vault), 1_000e18);
        vm.deal(keeper, 0.01 ether);
        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 1_000e30, 1_000e18);

        vm.prank(owner);
        vault.setKeeper(address(0));

        vm.prank(keeper);
        vm.expectRevert(StrategyVault.StrategyVault__NotAuthorized.selector);
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
    }

    function test_UnpauseResumesTrading() external {
        _configure(0, address(0));
        (MockGmxExchangeRouter er, address router) = _gmx();
        asset.mint(address(vault), 1_000e18);
        vm.deal(owner, 0.02 ether);
        StrategyVault.GmxMarketIncreaseOrder memory order = _increaseOrder(address(er), router, 1_000e30, 1_000e18);

        vm.startPrank(owner);
        vault.pause();
        vm.expectRevert(); // EnforcedPause
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order);
        vault.unpause();
        vault.createGmxMarketIncreaseOrder{value: 0.01 ether}(order); // resumes
        vm.stopPrank();
    }

    /*//////////////////////////////////////////////////////////////
                                 FACTORY
    //////////////////////////////////////////////////////////////*/

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

    function test_FactoryDeploysIndependentVaults() external {
        address other = address(0xD00D);
        address vault2 = factory.createVault(asset, other);

        assertEq(factory.vaultCount(), 2);
        assertEq(factory.vaultOwner(vault2), other);
        assertTrue(vault2 != address(vault));
        assertEq(StrategyVault(payable(vault2)).owner(), other);
        assertEq(address(StrategyVault(payable(vault2)).asset()), address(asset));
    }
}
