// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {EnumerableSet} from "@openzeppelin/contracts/utils/structs/EnumerableSet.sol";

import {IGmxV2ExchangeRouter} from "src/interfaces/IGmxV2ExchangeRouter.sol";

abstract contract StrategyVaultBase {
    // keccak256(abi.encode(uint256(keccak256("agentinvest.storage.StrategyVault")) - 1)) & ~bytes32(uint256(0xff))
    bytes32 private constant STRATEGY_VAULT_STORAGE_LOCATION =
        0xb9b2998beb57c4954282a856f7caa8a2becd30d6222cd15bddaf30bd0b902600;

    /// @notice Leverage is expressed in basis points of 1x (10_000 = 1x, 30_000 = 3x).
    uint256 internal constant ONE_X_BPS = 10_000;

    /// @notice Hard cap on the mandate's allowed-markets set to keep `setMandate` bounded.
    uint256 internal constant MAX_ALLOWED_MARKETS = 16;

    /// @custom:storage-location erc7201:agentinvest.storage.StrategyVault
    /// @dev New fields are APPENDED ONLY (never reordered) — the struct lives at a fixed
    ///      ERC-7201 slot and reordering would corrupt every already-deployed beacon vault.
    struct StrategyVaultStorage {
        IERC20 asset;
        address gmxExchangeRouter;
        address gmxRouter;
        address gmxOrderVault;
        // --- keeper + mandate (appended; see @dev above) ---
        address keeper; // address(0) = no keeper (owner-only)
        uint256 maxLeverageBps; // 0 = leverage unbounded
        EnumerableSet.AddressSet allowedMarkets; // empty = any GMX market allowed
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
    event NativeDeposited(address indexed from, uint256 amount);
    event NativeWithdrawn(address indexed to, uint256 amount);
    event GmxRoutingUpdated(address indexed exchangeRouter, address indexed router, address indexed orderVault);
    event KeeperUpdated(address indexed keeper);
    event MandateUpdated(uint256 maxLeverageBps, address[] allowedMarkets);
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
    error StrategyVault__InsufficientExecutionGas();
    error StrategyVault__InsufficientNativeBalance();
    error StrategyVault__NativeTransferFailed();
    error StrategyVault__NotKeeperOrOwner();
    error StrategyVault__KeeperCannotWithdrawCollateral();
    error StrategyVault__MarketNotAllowed();
    error StrategyVault__LeverageTooHigh();
    error StrategyVault__ZeroCollateral();
    error StrategyVault__TooManyMarkets();

    function _strategyVaultStorage() internal pure returns (StrategyVaultStorage storage $) {
        assembly {
            $.slot := STRATEGY_VAULT_STORAGE_LOCATION
        }
    }
}
