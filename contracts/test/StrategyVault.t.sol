// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {Test} from "forge-std/Test.sol";
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
}
