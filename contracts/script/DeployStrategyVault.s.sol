// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {Script} from "forge-std/Script.sol";
import {StrategyVault} from "src/StrategyVault.sol";
import {VaultFactory} from "src/VaultFactory.sol";

/**
 * @notice Deploys the StrategyVault implementation, a VaultFactory (which creates the shared
 *         UpgradeableBeacon), and one vault for STRATEGY_VAULT_OWNER over STRATEGY_VAULT_ASSET.
 * @dev Set BEACON_OWNER to a Timelock + multisig in production; it is the upgrade authority over
 *      every vault. Defaults to STRATEGY_VAULT_OWNER if unset (fine for local/testnet only).
 */
contract DeployStrategyVault is Script {
    function run() external returns (StrategyVault implementation, VaultFactory factory, address vault) {
        IERC20 asset = IERC20(vm.envAddress("STRATEGY_VAULT_ASSET"));
        uint256 deployerPrivateKey = vm.envOr("TESTNET_PRIVATE_KEY", uint256(0));
        address owner = vm.envOr("STRATEGY_VAULT_OWNER", address(0));
        if (owner == address(0) && deployerPrivateKey != 0) owner = vm.addr(deployerPrivateKey);
        if (owner == address(0)) owner = msg.sender;
        address beaconOwner = vm.envOr("BEACON_OWNER", owner);
        address gmxExchangeRouter = vm.envAddress("GMX_EXCHANGE_ROUTER");
        address gmxRouter = vm.envAddress("GMX_ROUTER");
        address gmxOrderVault = vm.envAddress("GMX_ORDER_VAULT");

        if (deployerPrivateKey == 0) {
            vm.startBroadcast();
        } else {
            vm.startBroadcast(deployerPrivateKey);
        }
        implementation = new StrategyVault();
        factory = new VaultFactory(address(implementation), beaconOwner, gmxExchangeRouter, gmxRouter, gmxOrderVault);
        vault = factory.createVault(asset, owner);
        vm.stopBroadcast();
    }
}
