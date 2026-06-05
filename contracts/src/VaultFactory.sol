// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {UpgradeableBeacon} from "@openzeppelin/contracts/proxy/beacon/UpgradeableBeacon.sol";
import {BeaconProxy} from "@openzeppelin/contracts/proxy/beacon/BeaconProxy.sol";

import {StrategyVault} from "./StrategyVault.sol";

/**
 * @title VaultFactory
 * @notice Deploys per-user {StrategyVault} beacon proxies. A single {UpgradeableBeacon} backs every
 *         vault, so one beacon upgrade fixes all of them at once.
 * @dev The beacon owner (the upgrade authority over every vault) should be a Timelock + multisig in
 *      production. Vault `owner` is the end user; the factory only deploys + records bindings.
 */
contract VaultFactory {
    /// @notice The shared beacon all vaults delegate to for their implementation.
    UpgradeableBeacon public immutable beacon;

    /// @notice All vaults deployed by this factory, in creation order.
    address[] public vaults;

    /// @notice Vault address => the user that owns it.
    mapping(address vault => address owner) public vaultOwner;

    event VaultCreated(address indexed vault, address indexed owner, address indexed asset);

    error VaultFactory__ZeroAddress();

    /// @param implementation_ The {StrategyVault} logic contract (must have disabled initializers).
    /// @param beaconOwner The upgrade authority over every vault (use a Timelock + multisig).
    constructor(address implementation_, address beaconOwner) {
        if (implementation_ == address(0) || beaconOwner == address(0)) revert VaultFactory__ZeroAddress();
        beacon = new UpgradeableBeacon(implementation_, beaconOwner);
    }

    /// @notice Deploy a new vault for `owner` over collateral `asset`.
    function createVault(IERC20 asset, address owner) external returns (address vault) {
        if (address(asset) == address(0) || owner == address(0)) revert VaultFactory__ZeroAddress();

        bytes memory initData = abi.encodeCall(StrategyVault.initialize, (asset, owner));
        vault = address(new BeaconProxy(address(beacon), initData));

        vaults.push(vault);
        vaultOwner[vault] = owner;

        emit VaultCreated(vault, owner, address(asset));
    }

    /// @notice Number of vaults deployed by this factory.
    function vaultCount() external view returns (uint256) {
        return vaults.length;
    }

    /// @notice The current shared implementation behind every vault.
    function implementation() external view returns (address) {
        return beacon.implementation();
    }
}
