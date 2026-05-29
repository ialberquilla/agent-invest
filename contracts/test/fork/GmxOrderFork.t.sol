// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {Test} from "forge-std/Test.sol";
import {StrategyVault} from "src/StrategyVault.sol";

contract GmxOrderForkTest is Test {
    address internal constant ARBITRUM_USDC = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831;
    address internal constant GMX_EXCHANGE_ROUTER = 0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41;
    address internal constant GMX_ROUTER = 0x7452c558d45f8afC8c83dAe62C3f8A5BE19c71f6;
    address internal constant GMX_ORDER_VAULT = 0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5;
    address internal constant GMX_BTC_USD_MARKET = 0x47c031236e19d024b42f8AE6780E44A573170703;

    address internal owner = makeAddr("owner");
    StrategyVault internal vault;

    function setUp() external {
        string memory rpcUrl = vm.envOr("ARBITRUM_RPC_URL", string(""));
        if (bytes(rpcUrl).length == 0) return;

        vm.createSelectFork(rpcUrl, 452_780_000);
        vault = new StrategyVault(IERC20(ARBITRUM_USDC), owner, "Agent Invest Strategy Vault", "aisUSDC");
    }

    function testFork_CanSubmitGmxLongMarketIncreaseOrder() external {
        if (address(vault) == address(0)) return;

        bytes32 orderKey = _submitOrder(true, 120_000e30);

        assertTrue(orderKey != bytes32(0));
    }

    function testFork_CanSubmitGmxShortMarketIncreaseOrder() external {
        if (address(vault) == address(0)) return;

        bytes32 orderKey = _submitOrder(false, 80_000e30);

        assertTrue(orderKey != bytes32(0));
    }

    function _submitOrder(bool isLong, uint256 acceptablePrice) internal returns (bytes32 orderKey) {
        uint256 collateralAmount = 50e6;
        uint256 executionFee = 0.005 ether;

        deal(ARBITRUM_USDC, address(vault), collateralAmount);
        vm.deal(owner, executionFee);

        StrategyVault.GmxMarketIncreaseOrder memory order = StrategyVault.GmxMarketIncreaseOrder({
            exchangeRouter: GMX_EXCHANGE_ROUTER,
            router: GMX_ROUTER,
            orderVault: GMX_ORDER_VAULT,
            market: GMX_BTC_USD_MARKET,
            collateralToken: ARBITRUM_USDC,
            receiver: address(0),
            cancellationReceiver: address(0),
            callbackContract: address(0),
            uiFeeReceiver: address(0),
            isLong: isLong,
            shouldUnwrapNativeToken: false,
            sizeDeltaUsd: 50e30,
            collateralAmount: collateralAmount,
            acceptablePrice: acceptablePrice,
            executionFee: executionFee,
            callbackGasLimit: 0,
            referralCode: bytes32("agent-invest")
        });

        vm.prank(owner);
        bytes[] memory results = vault.createGmxMarketIncreaseOrder{value: executionFee}(order);

        orderKey = abi.decode(results[2], (bytes32));
    }
}
