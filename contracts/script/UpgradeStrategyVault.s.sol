// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {Script} from "forge-std/Script.sol";
import {UpgradeableBeacon} from "@openzeppelin/contracts/proxy/beacon/UpgradeableBeacon.sol";
import {StrategyVault} from "src/StrategyVault.sol";
import {VaultFactory} from "src/VaultFactory.sol";

/**
 * @notice Deploys a fresh {StrategyVault} implementation and repoints the factory's shared
 *         {UpgradeableBeacon} at it, upgrading every existing vault atomically.
 * @dev The broadcast key MUST be the beacon owner (the `BEACON_OWNER` set at the original deploy —
 *      a Timelock + multisig in production). Reads `VAULT_FACTORY` from the environment and derives
 *      the beacon from it so the two can never drift.
 *
 *      Run:
 *        VAULT_FACTORY=0x... forge script script/UpgradeStrategyVault.s.sol:UpgradeStrategyVault \
 *          --rpc-url "$ARBITRUM_RPC_URL" --broadcast --account <beacon-owner-keystore>
 */
contract UpgradeStrategyVault is Script {
    function run() external returns (StrategyVault implementation, address beacon) {
        VaultFactory factory = VaultFactory(vm.envAddress("VAULT_FACTORY"));
        beacon = address(factory.beacon());
        address previousImplementation = UpgradeableBeacon(beacon).implementation();

        vm.startBroadcast();
        implementation = new StrategyVault();
        UpgradeableBeacon(beacon).upgradeTo(address(implementation));
        vm.stopBroadcast();

        require(
            UpgradeableBeacon(beacon).implementation() == address(implementation), "beacon upgrade did not take effect"
        );
        // Log via require message is not possible; the returned values + previousImplementation aid the runbook.
        previousImplementation;
    }
}
