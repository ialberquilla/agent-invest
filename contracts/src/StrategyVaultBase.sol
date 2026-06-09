// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import {IGmxV2ExchangeRouter} from "src/interfaces/IGmxV2ExchangeRouter.sol";

abstract contract StrategyVaultBase {
    // keccak256(abi.encode(uint256(keccak256("agentinvest.storage.StrategyVault")) - 1)) & ~bytes32(uint256(0xff))
    bytes32 private constant STRATEGY_VAULT_STORAGE_LOCATION =
        0xb9b2998beb57c4954282a856f7caa8a2becd30d6222cd15bddaf30bd0b902600;

    /// @custom:storage-location erc7201:agentinvest.storage.StrategyVault
    struct StrategyVaultStorage {
        IERC20 asset;
        address gmxExchangeRouter;
        address gmxRouter;
        address gmxOrderVault;
    }

    struct GmxMarketIncreaseOrder {
        address exchangeRouter;
        address router;
        address orderVault;
        address market;
        address collateralToken;
        bool isLong;
        bool shouldUnwrapNativeToken;
        uint256 sizeDeltaUsd;
        uint256 collateralAmount;
        uint256 acceptablePrice;
        uint256 executionFee;
        bytes32 referralCode;
    }

    struct GmxMarketDecreaseOrder {
        address exchangeRouter;
        address orderVault;
        address market;
        address collateralToken;
        bool isLong;
        bool shouldUnwrapNativeToken;
        uint256 sizeDeltaUsd;
        uint256 collateralWithdrawalAmount;
        uint256 acceptablePrice;
        uint256 executionFee;
        uint256 minOutputAmount;
        IGmxV2ExchangeRouter.DecreasePositionSwapType decreasePositionSwapType;
        bytes32 referralCode;
    }

    event Deposited(address indexed from, uint256 amount);
    event Withdrawn(address indexed to, uint256 amount);
    event GmxRoutingUpdated(address indexed exchangeRouter, address indexed router, address indexed orderVault);
    event GmxMarketIncreaseOrderCreated(
        bytes32 indexed orderKey,
        address indexed exchangeRouter,
        address indexed market,
        address collateralToken,
        bool isLong,
        uint256 sizeDeltaUsd,
        uint256 collateralAmount,
        uint256 executionFee
    );
    event GmxMarketDecreaseOrderCreated(
        bytes32 indexed orderKey,
        address indexed exchangeRouter,
        address indexed market,
        address collateralToken,
        bool isLong,
        uint256 sizeDeltaUsd,
        uint256 collateralWithdrawalAmount,
        uint256 executionFee
    );
    event GmxOrderCancelled(bytes32 indexed orderKey, address indexed exchangeRouter, uint256 executionFee);

    error StrategyVault__ZeroOwner();
    error StrategyVault__ZeroAsset();
    error StrategyVault__ZeroAmount();
    error StrategyVault__ZeroAddress();
    error StrategyVault__UntrustedRouting();
    error StrategyVault__InsufficientIdleBalance();
    error StrategyVault__InvalidExecutionFee();

    function _strategyVaultStorage() internal pure returns (StrategyVaultStorage storage $) {
        assembly {
            $.slot := STRATEGY_VAULT_STORAGE_LOCATION
        }
    }
}
