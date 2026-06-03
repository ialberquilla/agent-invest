// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {IERC20Metadata} from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Metadata.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {EnumerableSet} from "@openzeppelin/contracts/utils/structs/EnumerableSet.sol";
import {ReentrancyGuardTransient} from "@openzeppelin/contracts/utils/ReentrancyGuardTransient.sol";
import {Initializable} from "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import {Ownable2StepUpgradeable} from "@openzeppelin/contracts-upgradeable/access/Ownable2StepUpgradeable.sol";
import {PausableUpgradeable} from "@openzeppelin/contracts-upgradeable/utils/PausableUpgradeable.sol";

import {IGmxV2ExchangeRouter} from "./interfaces/IGmxV2ExchangeRouter.sol";

/**
 * @title StrategyVault
 * @notice Single-user, self-custody collateral vault for one Agent Invest strategy.
 * @dev Upgradeable (beacon proxy). The owner is the sole depositor; the vault holds USDC as
 *      collateral and executes GMX v2 perp orders. Strategy intelligence stays offchain.
 *
 *      Mandate enforcement (the core idea): the offchain AI agent runs as a bounded `keeper`. The
 *      owner commits a `Mandate` on-chain (allowed markets, max leverage, max per-order notional,
 *      keeper rebalance interval) and the contract enforces it on every GMX order. The keeper can
 *      ONLY submit conforming GMX orders — it can never move assets out (deposit/withdraw/execute
 *      stay owner-only). Even a fully compromised agent cannot exceed the user-approved limits.
 *
 *      Enforcement is risk-asymmetric: increases (taking on exposure) get the full mandate check;
 *      decreases (de-risking) only require an allowed market; cancels are unrestricted. The
 *      rebalance interval throttles the keeper only, never the human owner.
 *
 *      Routing is pinned: the owner sets the canonical GMX `exchangeRouter`/`router`/`orderVault`
 *      via {setGmxRouting} and every keeper order is validated against them + the vault asset, and
 *      all fund-relevant GMX addresses (receiver, cancellationReceiver, uiFeeReceiver, callback) are
 *      forced to the vault / zero — so the keeper cannot redirect collateral, PnL, or approvals.
 *
 *      v1 limitations (deferred to position-aware checks that need a GMX Reader read):
 *      - `maxLeverage` is a per-order ratio of added collateral to size delta, assuming $1 stable
 *        collateral; it does not track total position leverage. Zero-collateral size-ups are rejected
 *        so the cap cannot be bypassed, but the true running leverage of a position is not yet read.
 *      - No total gross-exposure cap and no oracle-based slippage bound yet.
 */
contract StrategyVault is Initializable, Ownable2StepUpgradeable, PausableUpgradeable, ReentrancyGuardTransient {
    using SafeERC20 for IERC20;
    using EnumerableSet for EnumerableSet.AddressSet;

    /// @notice Upper bound on the allowlist size (bounds gas + keeps the mandate legible).
    uint256 public constant MAX_ALLOWED_MARKETS = 32;

    /// @notice Owner-committed limits the keeper is bound by. `allowedMarkets` is managed separately
    ///         (it is an EnumerableSet and cannot live in a memory struct).
    struct Mandate {
        uint256 maxLeverage; // integer multiple, e.g. 3 == 3x (per-order, see limitations)
        uint256 maxPositionSizeUsd; // GMX 1e30 USD, cap on a single increase's sizeDeltaUsd
        uint256 minRebalanceInterval; // seconds between keeper increase orders
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
    }

    // keccak256(abi.encode(uint256(keccak256("agentinvest.storage.StrategyVault")) - 1)) & ~bytes32(uint256(0xff))
    bytes32 private constant STRATEGY_VAULT_STORAGE_LOCATION =
        0xb9b2998beb57c4954282a856f7caa8a2becd30d6222cd15bddaf30bd0b902600;

    function _strategyVaultStorage() private pure returns (StrategyVaultStorage storage $) {
        assembly {
            $.slot := STRATEGY_VAULT_STORAGE_LOCATION
        }
    }

    event Deposited(address indexed from, uint256 amount);
    event Withdrawn(address indexed to, uint256 amount);
    event KeeperUpdated(address indexed keeper);
    event GmxRoutingUpdated(address indexed exchangeRouter, address indexed router, address indexed orderVault);
    event MandateUpdated(uint256 maxLeverage, uint256 maxPositionSizeUsd, uint256 minRebalanceInterval);
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
        uint256 callbackGasLimit;
        bytes32 referralCode;
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
        uint256 callbackGasLimit;
        uint256 minOutputAmount;
        IGmxV2ExchangeRouter.DecreasePositionSwapType decreasePositionSwapType;
        bytes32 referralCode;
    }

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
    error StrategyVault__CallFailed(bytes returndata);

    /// @dev GMX orders may be submitted by the owner or the bounded keeper; both are mandate-checked.
    modifier onlyOwnerOrKeeper() {
        if (msg.sender != owner() && msg.sender != _strategyVaultStorage().keeper) {
            revert StrategyVault__NotAuthorized();
        }
        _;
    }

    constructor() {
        _disableInitializers();
    }

    function initialize(IERC20 asset_, address owner_) external initializer {
        if (address(asset_) == address(0)) revert StrategyVault__ZeroAsset();
        if (owner_ == address(0)) revert StrategyVault__ZeroOwner();

        __Ownable_init(owner_);
        __Ownable2Step_init();
        __Pausable_init();

        uint8 decimals = IERC20Metadata(address(asset_)).decimals();
        if (decimals > 30) revert StrategyVault__UnsupportedCollateralDecimals();

        StrategyVaultStorage storage $ = _strategyVaultStorage();
        $.asset = asset_;
        $.collateralUsdScale = 10 ** (30 - decimals);
    }

    receive() external payable {}

    /*//////////////////////////////////////////////////////////////
                                  VIEWS
    //////////////////////////////////////////////////////////////*/

    /// @notice The USDC collateral asset this vault holds.
    function asset() external view returns (IERC20) {
        return _strategyVaultStorage().asset;
    }

    /// @notice Idle (undeployed) collateral sitting in the vault, available to withdraw.
    function idleBalance() public view returns (uint256) {
        return _strategyVaultStorage().asset.balanceOf(address(this));
    }

    /// @notice The bounded keeper (the offchain agent), or address(0) if none.
    function keeper() external view returns (address) {
        return _strategyVaultStorage().keeper;
    }

    /// @notice The canonical GMX routing the keeper is pinned to.
    function gmxRouting() external view returns (address exchangeRouter, address router, address orderVault) {
        StrategyVaultStorage storage $ = _strategyVaultStorage();
        return ($.gmxExchangeRouter, $.gmxRouter, $.gmxOrderVault);
    }

    /// @notice The owner-committed mandate limits.
    function mandate() external view returns (Mandate memory) {
        return _strategyVaultStorage().mandate;
    }

    /// @notice Whether `market` is on the mandate allowlist.
    function isMarketAllowed(address market) external view returns (bool) {
        return _strategyVaultStorage().allowedMarkets.contains(market);
    }

    /// @notice All allowed GMX markets.
    function allowedMarkets() external view returns (address[] memory) {
        return _strategyVaultStorage().allowedMarkets.values();
    }

    /*//////////////////////////////////////////////////////////////
                              OWNER CONFIG
    //////////////////////////////////////////////////////////////*/

    /// @notice Set (or clear, with address(0)) the bounded keeper.
    function setKeeper(address keeper_) external onlyOwner {
        _strategyVaultStorage().keeper = keeper_;
        emit KeeperUpdated(keeper_);
    }

    /// @notice Pin the canonical GMX v2 routing. Keeper orders must match these exactly, so a
    ///         compromised keeper cannot redirect collateral/approvals to attacker contracts.
    function setGmxRouting(address exchangeRouter, address router, address orderVault) external onlyOwner {
        if (exchangeRouter == address(0) || router == address(0) || orderVault == address(0)) {
            revert StrategyVault__ZeroAddress();
        }
        StrategyVaultStorage storage $ = _strategyVaultStorage();
        $.gmxExchangeRouter = exchangeRouter;
        $.gmxRouter = router;
        $.gmxOrderVault = orderVault;
        emit GmxRoutingUpdated(exchangeRouter, router, orderVault);
    }

    /// @notice Commit the mandate scalar limits.
    function setMandate(Mandate calldata mandate_) external onlyOwner {
        if (mandate_.maxLeverage == 0 || mandate_.maxPositionSizeUsd == 0) revert StrategyVault__InvalidMandate();
        _strategyVaultStorage().mandate = mandate_;
        emit MandateUpdated(mandate_.maxLeverage, mandate_.maxPositionSizeUsd, mandate_.minRebalanceInterval);
    }

    /// @notice Add a GMX market to the mandate allowlist.
    function addMarket(address market) external onlyOwner {
        if (market == address(0)) revert StrategyVault__ZeroAddress();
        StrategyVaultStorage storage $ = _strategyVaultStorage();
        if ($.allowedMarkets.length() >= MAX_ALLOWED_MARKETS) revert StrategyVault__MaxMarketsExceeded();
        if ($.allowedMarkets.add(market)) emit MarketAllowed(market);
    }

    /// @notice Remove a GMX market from the mandate allowlist.
    function removeMarket(address market) external onlyOwner {
        if (_strategyVaultStorage().allowedMarkets.remove(market)) emit MarketDisallowed(market);
    }

    /*//////////////////////////////////////////////////////////////
                     USER-FACING STATE-CHANGING FUNCTIONS
    //////////////////////////////////////////////////////////////*/

    /// @notice Fund the vault with collateral. Withdrawals of deployed collateral require closing
    ///         the GMX position first; only idle balance is withdrawable.
    function deposit(uint256 amount) external onlyOwner nonReentrant {
        if (amount == 0) revert StrategyVault__ZeroAmount();
        _strategyVaultStorage().asset.safeTransferFrom(msg.sender, address(this), amount);
        emit Deposited(msg.sender, amount);
    }

    /// @notice Withdraw idle collateral. Pause does not trap withdrawals — the owner always keeps
    ///         custody of undeployed funds; pause only halts trading/execution.
    function withdraw(uint256 amount, address to) external onlyOwner nonReentrant {
        if (amount == 0) revert StrategyVault__ZeroAmount();
        if (to == address(0)) revert StrategyVault__ZeroAddress();

        IERC20 asset_ = _strategyVaultStorage().asset;
        if (amount > asset_.balanceOf(address(this))) revert StrategyVault__InsufficientIdleBalance();

        asset_.safeTransfer(to, amount);
        emit Withdrawn(to, amount);
    }

    function pause() external onlyOwner {
        _pause();
    }

    function unpause() external onlyOwner {
        _unpause();
    }

    /// @notice Owner-only escape hatch for arbitrary calls. Never granted to the keeper.
    function execute(address target, uint256 value, bytes calldata data)
        external
        onlyOwner
        nonReentrant
        whenNotPaused
        returns (bytes memory result)
    {
        if (target == address(0)) revert StrategyVault__ZeroTarget();

        bool success;
        (success, result) = target.call{value: value}(data);
        if (!success) revert StrategyVault__CallFailed(result);

        emit Executed(target, value, result);
    }

    function createGmxMarketIncreaseOrder(GmxMarketIncreaseOrder calldata order)
        external
        payable
        onlyOwnerOrKeeper
        nonReentrant
        whenNotPaused
        returns (bytes32 orderKey)
    {
        _validateGmxOrder(order);
        if (msg.value != order.executionFee) revert StrategyVault__InvalidExecutionFee();
        _enforceIncreaseMandate(order);

        IERC20(order.collateralToken).forceApprove(order.router, order.collateralAmount);

        bytes[] memory calls = new bytes[](3);
        calls[0] = abi.encodeCall(IGmxV2ExchangeRouter.sendWnt, (order.orderVault, order.executionFee));
        calls[1] = abi.encodeCall(
            IGmxV2ExchangeRouter.sendTokens, (order.collateralToken, order.orderVault, order.collateralAmount)
        );
        calls[2] = abi.encodeCall(IGmxV2ExchangeRouter.createOrder, (_buildGmxCreateOrderParams(order)));

        bytes[] memory results = IGmxV2ExchangeRouter(order.exchangeRouter).multicall{value: msg.value}(calls);
        orderKey = abi.decode(results[2], (bytes32));

        emit GmxMarketIncreaseOrderCreated(
            orderKey,
            order.exchangeRouter,
            order.market,
            order.collateralToken,
            order.isLong,
            order.sizeDeltaUsd,
            order.collateralAmount,
            order.executionFee
        );
    }

    function createGmxMarketDecreaseOrder(GmxMarketDecreaseOrder calldata order)
        external
        payable
        onlyOwnerOrKeeper
        nonReentrant
        whenNotPaused
        returns (bytes32 orderKey)
    {
        _validateGmxDecreaseOrder(order);
        if (msg.value != order.executionFee) revert StrategyVault__InvalidExecutionFee();
        StrategyVaultStorage storage $ = _strategyVaultStorage();
        if (
            order.exchangeRouter != $.gmxExchangeRouter || order.orderVault != $.gmxOrderVault
                || order.collateralToken != address($.asset)
        ) revert StrategyVault__UntrustedRouting();
        // De-risking: only require the market be on the allowlist; no size/leverage/interval gate.
        if (!$.allowedMarkets.contains(order.market)) revert StrategyVault__MarketNotAllowed(order.market);

        bytes[] memory calls = new bytes[](2);
        calls[0] = abi.encodeCall(IGmxV2ExchangeRouter.sendWnt, (order.orderVault, order.executionFee));
        calls[1] = abi.encodeCall(IGmxV2ExchangeRouter.createOrder, (_buildGmxDecreaseOrderParams(order)));

        bytes[] memory results = IGmxV2ExchangeRouter(order.exchangeRouter).multicall{value: msg.value}(calls);
        orderKey = abi.decode(results[1], (bytes32));

        emit GmxMarketDecreaseOrderCreated(
            orderKey,
            order.exchangeRouter,
            order.market,
            order.collateralToken,
            order.isLong,
            order.sizeDeltaUsd,
            order.collateralWithdrawalAmount,
            order.executionFee
        );
    }

    function cancelGmxOrder(address exchangeRouter, bytes32 orderKey, uint256 executionFee)
        external
        payable
        onlyOwnerOrKeeper
        nonReentrant
        whenNotPaused
    {
        if (exchangeRouter == address(0)) revert StrategyVault__ZeroAddress();
        if (msg.value != executionFee) revert StrategyVault__InvalidExecutionFee();

        IGmxV2ExchangeRouter(exchangeRouter).cancelOrder{value: msg.value}(orderKey);

        emit GmxOrderCancelled(orderKey, exchangeRouter, executionFee);
    }

    /*//////////////////////////////////////////////////////////////
                            INTERNAL FUNCTIONS
    //////////////////////////////////////////////////////////////*/

    /// @dev Enforce the mandate on an increase order. Routing/collateral are pinned to the canonical
    ///      GMX config + vault asset; market allowlist + per-order notional + per-order leverage apply
    ///      to every caller; the rebalance interval throttles the keeper only.
    function _enforceIncreaseMandate(GmxMarketIncreaseOrder calldata order) internal {
        StrategyVaultStorage storage $ = _strategyVaultStorage();

        if (
            order.exchangeRouter != $.gmxExchangeRouter || order.router != $.gmxRouter
                || order.orderVault != $.gmxOrderVault || order.collateralToken != address($.asset)
        ) revert StrategyVault__UntrustedRouting();

        if (!$.allowedMarkets.contains(order.market)) revert StrategyVault__MarketNotAllowed(order.market);
        if (order.sizeDeltaUsd > $.mandate.maxPositionSizeUsd) revert StrategyVault__PositionSizeExceeded();

        // Per-order leverage guardrail: size delta vs added collateral (assumes $1 stable collateral).
        // Zero-collateral size-ups are rejected (collateralUsd == 0) so the cap cannot be bypassed by
        // adding size against existing collateral; position-aware leverage is deferred (needs Reader).
        uint256 collateralUsd = order.collateralAmount * $.collateralUsdScale;
        if (order.sizeDeltaUsd > $.mandate.maxLeverage * collateralUsd) revert StrategyVault__LeverageExceeded();

        if (msg.sender == $.keeper) {
            uint256 last = $.lastKeeperIncreaseAt;
            if (last != 0 && block.timestamp < last + $.mandate.minRebalanceInterval) {
                revert StrategyVault__RebalanceTooSoon();
            }
            $.lastKeeperIncreaseAt = block.timestamp;
        }
    }

    function _validateGmxOrder(GmxMarketIncreaseOrder calldata order) internal pure {
        if (
            order.exchangeRouter == address(0) || order.router == address(0) || order.orderVault == address(0)
                || order.market == address(0) || order.collateralToken == address(0)
        ) revert StrategyVault__ZeroAddress();
    }

    function _validateGmxDecreaseOrder(GmxMarketDecreaseOrder calldata order) internal pure {
        if (
            order.exchangeRouter == address(0) || order.orderVault == address(0) || order.market == address(0)
                || order.collateralToken == address(0)
        ) revert StrategyVault__ZeroAddress();
    }

    function _buildGmxCreateOrderParams(GmxMarketIncreaseOrder calldata order)
        internal
        view
        returns (IGmxV2ExchangeRouter.CreateOrderParams memory params)
    {
        // Fund-relevant addresses are pinned to the vault — the keeper cannot redirect collateral,
        // PnL, cancellation refunds, UI fees, or a callback target to itself.
        address[] memory swapPath = new address[](0);
        bytes32[] memory dataList = new bytes32[](0);

        params = IGmxV2ExchangeRouter.CreateOrderParams({
            addresses: IGmxV2ExchangeRouter.CreateOrderParamsAddresses({
                receiver: address(this),
                cancellationReceiver: address(this),
                callbackContract: address(0),
                uiFeeReceiver: address(0),
                market: order.market,
                initialCollateralToken: order.collateralToken,
                swapPath: swapPath
            }),
            numbers: IGmxV2ExchangeRouter.CreateOrderParamsNumbers({
                sizeDeltaUsd: order.sizeDeltaUsd,
                initialCollateralDeltaAmount: order.collateralAmount,
                triggerPrice: 0,
                acceptablePrice: order.acceptablePrice,
                executionFee: order.executionFee,
                callbackGasLimit: 0,
                minOutputAmount: 0,
                validFromTime: 0
            }),
            orderType: IGmxV2ExchangeRouter.OrderType.MarketIncrease,
            decreasePositionSwapType: IGmxV2ExchangeRouter.DecreasePositionSwapType.NoSwap,
            isLong: order.isLong,
            shouldUnwrapNativeToken: order.shouldUnwrapNativeToken,
            autoCancel: false,
            referralCode: order.referralCode,
            dataList: dataList
        });
    }

    function _buildGmxDecreaseOrderParams(GmxMarketDecreaseOrder calldata order)
        internal
        view
        returns (IGmxV2ExchangeRouter.CreateOrderParams memory params)
    {
        // Returned collateral + realized PnL go only to the vault; the keeper cannot redirect them.
        address[] memory swapPath = new address[](0);
        bytes32[] memory dataList = new bytes32[](0);

        params = IGmxV2ExchangeRouter.CreateOrderParams({
            addresses: IGmxV2ExchangeRouter.CreateOrderParamsAddresses({
                receiver: address(this),
                cancellationReceiver: address(this),
                callbackContract: address(0),
                uiFeeReceiver: address(0),
                market: order.market,
                initialCollateralToken: order.collateralToken,
                swapPath: swapPath
            }),
            numbers: IGmxV2ExchangeRouter.CreateOrderParamsNumbers({
                sizeDeltaUsd: order.sizeDeltaUsd,
                initialCollateralDeltaAmount: order.collateralWithdrawalAmount,
                triggerPrice: 0,
                acceptablePrice: order.acceptablePrice,
                executionFee: order.executionFee,
                callbackGasLimit: 0,
                minOutputAmount: order.minOutputAmount,
                validFromTime: 0
            }),
            orderType: IGmxV2ExchangeRouter.OrderType.MarketDecrease,
            decreasePositionSwapType: order.decreasePositionSwapType,
            isLong: order.isLong,
            shouldUnwrapNativeToken: order.shouldUnwrapNativeToken,
            autoCancel: false,
            referralCode: order.referralCode,
            dataList: dataList
        });
    }
}
