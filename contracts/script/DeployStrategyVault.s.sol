// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {Script} from "forge-std/Script.sol";
import {StrategyVault} from "src/StrategyVault.sol";
import {VaultFactory} from "src/VaultFactory.sol";

/**
 * @notice Deploys the StrategyVault implementation, a VaultFactory (which creates the shared
 *         UpgradeableBeacon), and one vault for STRATEGY_VAULT_OWNER over STRATEGY_VAULT_ASSET.
 * @dev On Arbitrum mainnet, STRATEGY_VAULT_OWNER and BEACON_OWNER must be explicit, and GMX routing
 *      must match the known GMX v2 contracts. BEACON_OWNER is the upgrade authority over every vault;
 *      use a Timelock + multisig for production.
 */
contract DeployStrategyVault is Script {
    uint256 internal constant ARBITRUM_CHAIN_ID = 42161;
    address internal constant ARBITRUM_GMX_EXCHANGE_ROUTER = 0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41;
    address internal constant ARBITRUM_GMX_ROUTER = 0x7452c558d45f8afC8c83dAe62C3f8A5BE19c71f6;
    address internal constant ARBITRUM_GMX_ORDER_VAULT = 0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5;

    error DeployStrategyVault__ProductionRequiresExplicitOwner();
    error DeployStrategyVault__ProductionRequiresExplicitBeaconOwner();
    error DeployStrategyVault__UnexpectedProductionGmxRouting();

    function run() external returns (StrategyVault implementation, VaultFactory factory, address vault) {
        IERC20 asset = IERC20(vm.envAddress("STRATEGY_VAULT_ASSET"));
        address owner = vm.envOr("STRATEGY_VAULT_OWNER", address(0));
        bool ownerWasExplicit = owner != address(0);
        if (owner == address(0)) owner = msg.sender;
        address beaconOwner = vm.envOr("BEACON_OWNER", address(0));
        if (beaconOwner == address(0) && block.chainid != ARBITRUM_CHAIN_ID) beaconOwner = owner;
        address gmxExchangeRouter = vm.envAddress("GMX_EXCHANGE_ROUTER");
        address gmxRouter = vm.envAddress("GMX_ROUTER");
        address gmxOrderVault = vm.envAddress("GMX_ORDER_VAULT");

        _validateProductionConfig(ownerWasExplicit, beaconOwner, gmxExchangeRouter, gmxRouter, gmxOrderVault);

        vm.startBroadcast();
        implementation = new StrategyVault();
        factory = new VaultFactory(address(implementation), beaconOwner, gmxExchangeRouter, gmxRouter, gmxOrderVault);
        vault = factory.createVault(asset, owner);
        vm.stopBroadcast();
    }

    function _validateProductionConfig(
        bool ownerWasExplicit,
        address beaconOwner,
        address gmxExchangeRouter,
        address gmxRouter,
        address gmxOrderVault
    ) internal view {
        if (block.chainid != ARBITRUM_CHAIN_ID) return;

        if (!ownerWasExplicit) revert DeployStrategyVault__ProductionRequiresExplicitOwner();
        if (beaconOwner == address(0)) revert DeployStrategyVault__ProductionRequiresExplicitBeaconOwner();
        if (
            gmxExchangeRouter != ARBITRUM_GMX_EXCHANGE_ROUTER || gmxRouter != ARBITRUM_GMX_ROUTER
                || gmxOrderVault != ARBITRUM_GMX_ORDER_VAULT
        ) revert DeployStrategyVault__UnexpectedProductionGmxRouting();
    }
}
