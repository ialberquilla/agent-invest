// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {Test} from "forge-std/Test.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {IGmxV2ExchangeRouter} from "src/interfaces/IGmxV2ExchangeRouter.sol";
import {StrategyVault} from "src/StrategyVault.sol";

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
}

contract StrategyVaultTest is Test {
    MockAsset internal asset;
    StrategyVault internal vault;
    address internal owner = address(0xA11CE);

    function setUp() external {
        asset = new MockAsset();
        vault = new StrategyVault(asset, owner, "Agent Invest Strategy Vault", "aisUSDC");
    }

    function test_DepositMintsShares(uint256 amount) external {
        amount = bound(amount, 1, type(uint128).max);
        asset.mint(owner, amount);

        vm.startPrank(owner);
        asset.approve(address(vault), amount);
        uint256 shares = vault.deposit(amount, owner);
        vm.stopPrank();

        assertEq(shares, amount);
        assertEq(vault.balanceOf(owner), amount);
        assertEq(vault.totalAssets(), amount);
    }

    function test_PauseBlocksDeposits() external {
        asset.mint(owner, 1 ether);

        vm.prank(owner);
        vault.pause();

        vm.startPrank(owner);
        asset.approve(address(vault), 1 ether);
        vm.expectRevert();
        vault.deposit(1 ether, owner);
        vm.stopPrank();
    }

    function test_ExecuteCallsTarget() external {
        CallTarget target = new CallTarget();

        vm.prank(owner);
        bytes memory result = vault.execute(address(target), 0, abi.encodeCall(CallTarget.record, ()));

        assertEq(abi.decode(result, (uint256)), 1);
        assertEq(target.calls(), 1);
    }

    function test_OnlyOwnerCanExecute() external {
        CallTarget target = new CallTarget();

        vm.expectRevert();
        vault.execute(address(target), 0, abi.encodeCall(CallTarget.record, ()));
    }

    function test_CreateGmxMarketIncreaseOrderLong() external {
        _testCreateGmxMarketIncreaseOrder(true);
    }

    function test_CreateGmxMarketIncreaseOrderShort() external {
        _testCreateGmxMarketIncreaseOrder(false);
    }

    function _testCreateGmxMarketIncreaseOrder(bool isLong) internal {
        address router = address(new MockGmxRouter());
        address orderVault = address(0x5678);
        address market = address(0xBEEF);
        uint256 collateralAmount = 1_000e18;
        uint256 executionFee = 0.01 ether;
        MockGmxExchangeRouter exchangeRouter = new MockGmxExchangeRouter(router);

        asset.mint(address(vault), collateralAmount);
        vm.deal(owner, executionFee);

        StrategyVault.GmxMarketIncreaseOrder memory order = StrategyVault.GmxMarketIncreaseOrder({
            exchangeRouter: address(exchangeRouter),
            router: router,
            orderVault: orderVault,
            market: market,
            collateralToken: address(asset),
            receiver: address(0),
            cancellationReceiver: address(0),
            callbackContract: address(0),
            uiFeeReceiver: address(0),
            isLong: isLong,
            shouldUnwrapNativeToken: false,
            sizeDeltaUsd: 2_000e30,
            collateralAmount: collateralAmount,
            acceptablePrice: isLong ? 51_000e30 : 49_000e30,
            executionFee: executionFee,
            callbackGasLimit: 0,
            referralCode: bytes32("agent-invest")
        });

        vm.prank(owner);
        vault.createGmxMarketIncreaseOrder{value: executionFee}(order);

        assertEq(exchangeRouter.lastMsgValue(), executionFee);
        assertEq(exchangeRouter.lastWntReceiver(), orderVault);
        assertEq(exchangeRouter.lastWntAmount(), executionFee);
        assertEq(exchangeRouter.lastToken(), address(asset));
        assertEq(exchangeRouter.lastTokenReceiver(), orderVault);
        assertEq(exchangeRouter.lastTokenAmount(), collateralAmount);
        assertEq(asset.balanceOf(orderVault), collateralAmount);

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
}
