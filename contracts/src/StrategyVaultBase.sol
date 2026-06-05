// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {EnumerableSet} from "@openzeppelin/contracts/utils/structs/EnumerableSet.sol";

import {IGmxV2ExchangeRouter} from "src/interfaces/IGmxV2ExchangeRouter.sol";

abstract contract StrategyVaultBase {
    /// @notice Upper bound on the allowlist size (bounds gas + keeps the mandate legible).
    uint256 public constant MAX_ALLOWED_MARKETS = 32;

    /// @notice Keeper intents cannot allow more than 10% price slippage from the signed reference price.
    uint256 public constant MAX_KEEPER_SLIPPAGE_BPS = 1_000;

    uint256 internal constant BPS_DIVISOR = 10_000;

    bytes32 internal constant KEEPER_INCREASE_ORDER_INTENT_TYPEHASH = keccak256(
        "KeeperIncreaseOrderIntent(address keeper,bytes32 orderHash,uint256 referencePrice,uint256 maxSlippageBps,uint256 nonce,uint256 deadline)"
    );

    bytes32 internal constant KEEPER_DECREASE_ORDER_INTENT_TYPEHASH = keccak256(
        "KeeperDecreaseOrderIntent(address keeper,bytes32 orderHash,uint256 referencePrice,uint256 maxSlippageBps,uint256 nonce,uint256 deadline)"
    );

    // keccak256(abi.encode(uint256(keccak256("agentinvest.storage.StrategyVault")) - 1)) & ~bytes32(uint256(0xff))
    bytes32 private constant STRATEGY_VAULT_STORAGE_LOCATION =
        0xb9b2998beb57c4954282a856f7caa8a2becd30d6222cd15bddaf30bd0b902600;

    /// @notice Owner-committed limits the keeper is bound by. `allowedMarkets` is managed separately
    ///         (it is an EnumerableSet and cannot live in a memory struct).
    struct Mandate {
        uint256 maxLeverage; // integer multiple, e.g. 3 == 3x (per-order, see limitations)
        uint256 maxPositionSizeUsd; // GMX 1e30 USD, cap on a single increase's sizeDeltaUsd
        uint256 minRebalanceInterval; // seconds between keeper increase orders
        uint256 maxKeeperSlippageBps; // max owner-signed keeper slippage from the signed reference price
    }

    /// @custom:storage-location erc7201:agentinvest.storage.StrategyVault
    struct StrategyVaultStorage {
        IERC20 asset;
        address keeper;
        uint256 collateralUsdScale; // multiply token amount by this to get 1e30 USD (assumes $1)
        uint256 lastKeeperIncreaseAt;
        Mandate mandate;
        EnumerableSet.AddressSet allowedMarkets;
        // Canonical GMX v2 routing the keeper is pinned to. Every keeper order is validated against
        // these so a compromised keeper cannot point routing/collateral at attacker contracts.
        address gmxExchangeRouter;
        address gmxRouter;
        address gmxOrderVault;
        mapping(uint256 nonce => bool used) usedKeeperOrderIntentNonces;
    }

    struct GmxMarketIncreaseOrder {
        address exchangeRouter;
        address router;
        address orderVault;
        address market;
        address collateralToken;
        address receiver;
        address cancellationReceiver;
        address callbackContract;
        address uiFeeReceiver;
        bool isLong;
        bool shouldUnwrapNativeToken;
        uint256 sizeDeltaUsd;
        uint256 collateralAmount;
        uint256 acceptablePrice;
        uint256 executionFee;
        uint256 referencePrice;
        uint256 maxSlippageBps;
        uint256 intentNonce;
        uint256 intentDeadline;
        uint256 callbackGasLimit;
        bytes32 referralCode;
        bytes ownerSignature;
    }

    struct GmxMarketDecreaseOrder {
        address exchangeRouter;
        address orderVault;
        address market;
        address collateralToken;
        address receiver;
        address cancellationReceiver;
        address callbackContract;
        address uiFeeReceiver;
        bool isLong;
        bool shouldUnwrapNativeToken;
        uint256 sizeDeltaUsd;
        uint256 collateralWithdrawalAmount;
        uint256 acceptablePrice;
        uint256 executionFee;
        uint256 referencePrice;
        uint256 maxSlippageBps;
        uint256 intentNonce;
        uint256 intentDeadline;
        uint256 callbackGasLimit;
        uint256 minOutputAmount;
        IGmxV2ExchangeRouter.DecreasePositionSwapType decreasePositionSwapType;
        bytes32 referralCode;
        bytes ownerSignature;
    }

    event Deposited(address indexed from, uint256 amount);
    event Withdrawn(address indexed to, uint256 amount);
    event KeeperUpdated(address indexed keeper);
    event GmxRoutingUpdated(address indexed exchangeRouter, address indexed router, address indexed orderVault);
    event MandateUpdated(
        uint256 maxLeverage, uint256 maxPositionSizeUsd, uint256 minRebalanceInterval, uint256 maxKeeperSlippageBps
    );
    event MarketAllowed(address indexed market);
    event MarketDisallowed(address indexed market);
    event Executed(address indexed target, uint256 value, bytes result);
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
    event KeeperOrderIntentConsumed(bytes32 indexed digest, uint256 indexed nonce, address indexed keeper);

    error StrategyVault__ZeroOwner();
    error StrategyVault__ZeroAsset();
    error StrategyVault__ZeroAmount();
    error StrategyVault__ZeroTarget();
    error StrategyVault__ZeroAddress();
    error StrategyVault__NotAuthorized();
    error StrategyVault__UntrustedRouting();
    error StrategyVault__InvalidMandate();
    error StrategyVault__MaxMarketsExceeded();
    error StrategyVault__MarketNotAllowed(address market);
    error StrategyVault__PositionSizeExceeded();
    error StrategyVault__LeverageExceeded();
    error StrategyVault__RebalanceTooSoon();
    error StrategyVault__UnsupportedCollateralDecimals();
    error StrategyVault__InsufficientIdleBalance();
    error StrategyVault__InvalidExecutionFee();
    error StrategyVault__InvalidOrderIntent();
    error StrategyVault__OrderIntentExpired();
    error StrategyVault__OrderIntentNonceUsed(uint256 nonce);
    error StrategyVault__KeeperSlippageExceeded();
    error StrategyVault__InvalidReferencePrice();
    error StrategyVault__CallFailed(bytes returndata);

    function _strategyVaultStorage() internal pure returns (StrategyVaultStorage storage $) {
        assembly {
            $.slot := STRATEGY_VAULT_STORAGE_LOCATION
        }
    }
}
