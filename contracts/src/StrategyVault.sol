// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import {ERC4626} from "@openzeppelin/contracts/token/ERC20/extensions/ERC4626.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {Ownable2Step} from "@openzeppelin/contracts/access/Ownable2Step.sol";
import {Pausable} from "@openzeppelin/contracts/utils/Pausable.sol";
import {ReentrancyGuardTransient} from "@openzeppelin/contracts/utils/ReentrancyGuardTransient.sol";

/**
 * @title StrategyVault
 * @notice ERC-4626 collateral vault for one user-owned Agent Invest strategy.
 * @dev Strategy intelligence stays offchain; the owner executes approved calls directly from the vault.
 */
contract StrategyVault is ERC4626, Ownable2Step, Pausable, ReentrancyGuardTransient {
    event Executed(address indexed target, uint256 value, bytes result);

    error StrategyVault__ZeroOwner();
    error StrategyVault__ZeroTarget();
    error StrategyVault__CallFailed(bytes returndata);

    constructor(IERC20 asset_, address owner_, string memory name_, string memory symbol_)
        ERC4626(asset_)
        ERC20(name_, symbol_)
        Ownable(owner_)
    {
        if (owner_ == address(0)) revert StrategyVault__ZeroOwner();
    }

    receive() external payable {}

    /*//////////////////////////////////////////////////////////////
                     USER-FACING STATE-CHANGING FUNCTIONS
    //////////////////////////////////////////////////////////////*/

    function pause() external onlyOwner {
        _pause();
    }

    function unpause() external onlyOwner {
        _unpause();
    }

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

    /*//////////////////////////////////////////////////////////////
                            INTERNAL FUNCTIONS
    //////////////////////////////////////////////////////////////*/

    function _update(address from, address to, uint256 value) internal override whenNotPaused {
        super._update(from, to, value);
    }
}
