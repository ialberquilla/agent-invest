// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {Script} from "forge-std/Script.sol";
import {StrategyVault} from "src/StrategyVault.sol";

contract DeployStrategyVault is Script {
    function run() external returns (StrategyVault vault) {
        IERC20 asset = IERC20(vm.envAddress("STRATEGY_VAULT_ASSET"));
        address owner = vm.envAddress("STRATEGY_VAULT_OWNER");

        vm.startBroadcast();
        vault = new StrategyVault(asset, owner, "Agent Invest Strategy Vault", "aisUSDC");
        vm.stopBroadcast();
    }
}
