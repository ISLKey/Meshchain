"""
MeshChain Consensus Module - Delegated Proof-of-Proximity (DPoP)

This module implements the DPoP consensus mechanism that selects validators
based on both their stake and their proximity to the proposer in the mesh network.

Key Components:
1. ValidatorRegistry: Manages validator stakes and information
2. DoPSelector: Selects validators based on proximity and stake
3. GiniCalculator: Calculates wealth distribution metric
4. StakeManager: Manages validator stakes and delegations
"""

import struct
import time
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field
from collections import defaultdict
import math


@dataclass
class Validator:
    """Represents a validator in the network."""
    node_id: bytes  # 8-byte node identifier
    stake: int  # Amount of MESH staked
    hop_distance: int  # Hops from proposer (0-255)
    is_active: bool = True  # Whether validator is active
    slashed_amount: int = 0  # Amount slashed
    last_validation_time: int = 0  # Timestamp of last validation
    validation_count: int = 0  # Number of blocks validated
    missed_validations: int = 0  # Number of missed validations
    
    def get_effective_stake(self) -> int:
        """Get stake after accounting for slashing."""
        return max(0, self.stake - self.slashed_amount)
    
    def get_weight(self) -> float:
        """Calculate validator weight for selection."""
        if not self.is_active or self.get_effective_stake() < 100:
            return 0.0
        
        # Weight = (1 / hop_distance) * stake
        # Closer nodes get higher weight
        proximity_weight = 1.0 / max(1, self.hop_distance)
        stake_weight = self.get_effective_stake()
        
        return proximity_weight * stake_weight


@dataclass
class ValidatorRegistry:
    """Manages all validators and their information."""
    validators: Dict[bytes, Validator] = field(default_factory=dict)
    total_stake: int = 0
    min_stake: int = 100  # Minimum stake to validate
    max_stake: int = 50000  # Maximum stake per validator
    
    def add_validator(self, node_id: bytes, stake: int, hop_distance: int) -> bool:
        """
        Add or update a validator.
        
        Args:
            node_id: 8-byte node identifier
            stake: Amount of MESH to stake
            hop_distance: Distance in hops from proposer
            
        Returns:
            True if validator was added/updated, False if stake too low
        """
        if stake < self.min_stake:
            return False
        
        # Cap stake at maximum
        capped_stake = min(stake, self.max_stake)
        
        if node_id in self.validators:
            # Update existing validator
            old_stake = self.validators[node_id].get_effective_stake()
            self.total_stake -= old_stake
        
        self.validators[node_id] = Validator(
            node_id=node_id,
            stake=capped_stake,
            hop_distance=hop_distance,
            is_active=True
        )
        self.total_stake += capped_stake
        
        return True
    
    def remove_validator(self, node_id: bytes) -> bool:
        """Remove a validator from registry."""
        if node_id not in self.validators:
            return False
        
        self.total_stake -= self.validators[node_id].get_effective_stake()
        del self.validators[node_id]
        return True
    
    def get_validator(self, node_id: bytes) -> Optional[Validator]:
        """Get validator by node ID."""
        return self.validators.get(node_id)
    
    def get_active_validators(self) -> List[Validator]:
        """Get list of all active validators."""
        return [v for v in self.validators.values() if v.is_active]
    
    def get_total_weight(self) -> float:
        """Calculate total weight of all validators."""
        return sum(v.get_weight() for v in self.validators.values())
    
    def update_hop_distance(self, node_id: bytes, hop_distance: int) -> bool:
        """Update hop distance for a validator."""
        if node_id not in self.validators:
            return False
        
        self.validators[node_id].hop_distance = hop_distance
        return True


class DoPSelector:
    """
    Delegated Proof-of-Proximity (DPoP) Validator Selector
    
    Selects validators based on:
    1. Proximity to proposer (hop distance)
    2. Stake amount
    3. Historical performance
    """
    
    def __init__(self, registry: ValidatorRegistry):
        """
        Initialize DPoP selector.
        
        Args:
            registry: ValidatorRegistry instance
        """
        self.registry = registry
        self.selection_history: List[bytes] = []
        self.last_selection_time = 0
    
    def select_validator(self, proposer_id: bytes = None) -> Optional[bytes]:
        """
        Select a validator using DPoP algorithm.
        
        Algorithm:
        1. Calculate weight for each validator: (1/hop_distance) * stake
        2. Select validator proportional to weight
        3. Prefer validators with good history
        
        Args:
            proposer_id: ID of the proposer (for reference)
            
        Returns:
            Node ID of selected validator, or None if no validators available
        """
        validators = self.registry.get_active_validators()
        
        if not validators:
            return None
        
        # Calculate weights
        weights = {}
        total_weight = 0.0
        
        for validator in validators:
            weight = validator.get_weight()
            if weight > 0:
                weights[validator.node_id] = weight
                total_weight += weight
        
        if total_weight == 0:
            return None
        
        # Select validator proportional to weight
        selected = self._weighted_selection(weights, total_weight)
        
        if selected:
            self.selection_history.append(selected)
            self.last_selection_time = time.time()
        
        return selected
    
    def select_multiple_validators(self, count: int) -> List[bytes]:
        """
        Select multiple validators for a committee.
        
        Args:
            count: Number of validators to select
            
        Returns:
            List of selected validator node IDs
        """
        validators = self.registry.get_active_validators()
        
        if not validators or count <= 0:
            return []
        
        count = min(count, len(validators))
        selected = []
        remaining_validators = set(v.node_id for v in validators)
        
        for _ in range(count):
            # Calculate weights for remaining validators
            weights = {}
            total_weight = 0.0
            
            for node_id in remaining_validators:
                validator = self.registry.get_validator(node_id)
                weight = validator.get_weight()
                if weight > 0:
                    weights[node_id] = weight
                    total_weight += weight
            
            if total_weight == 0:
                break
            
            # Select one validator
            selected_id = self._weighted_selection(weights, total_weight)
            if selected_id:
                selected.append(selected_id)
                remaining_validators.remove(selected_id)
        
        return selected
    
    @staticmethod
    def _weighted_selection(weights: Dict[bytes, float], 
                          total_weight: float) -> Optional[bytes]:
        """
        Select item from weighted dictionary.
        
        Args:
            weights: Dictionary of {item: weight}
            total_weight: Sum of all weights
            
        Returns:
            Selected item or None
        """
        if total_weight == 0:
            return None
        
        import random
        
        # Generate random number between 0 and total_weight
        random_value = random.uniform(0, total_weight)
        
        # Find selected item
        cumulative = 0.0
        for item, weight in weights.items():
            cumulative += weight
            if random_value <= cumulative:
                return item
        
        # Fallback to last item (shouldn't reach here)
        return list(weights.keys())[-1] if weights else None


class GiniCalculator:
    """
    Calculates Gini coefficient to measure wealth distribution.
    
    Gini coefficient ranges from 0 to 1:
    - 0 = perfect equality (everyone has same wealth)
    - 1 = perfect inequality (one person has all wealth)
    
    Target for MeshChain: 0.35 (moderate inequality, prevents oligarchy)
    """
    
    @staticmethod
    def calculate(registry: ValidatorRegistry) -> float:
        """
        Calculate Gini coefficient for current wealth distribution.
        
        Formula:
        G = (2 * sum(i * x_i)) / (n * sum(x_i)) - (n + 1) / n
        
        Where:
        - x_i = stake of validator i (sorted)
        - n = number of validators
        - i = index (1 to n)
        
        Args:
            registry: ValidatorRegistry instance
            
        Returns:
            Gini coefficient (0.0 to 1.0)
        """
        validators = registry.get_active_validators()
        
        if not validators:
            return 0.0
        
        # Get stakes sorted
        stakes = sorted([v.get_effective_stake() for v in validators])
        n = len(stakes)
        
        if n == 1:
            return 0.0
        
        # Calculate sum of stakes
        total_stake = sum(stakes)
        
        if total_stake == 0:
            return 0.0
        
        # Calculate numerator: 2 * sum(i * x_i)
        numerator = 0.0
        for i, stake in enumerate(stakes, 1):
            numerator += i * stake
        
        numerator *= 2
        
        # Calculate denominator: n * sum(x_i)
        denominator = n * total_stake
        
        # Calculate Gini coefficient
        gini = (numerator / denominator) - ((n + 1) / n)
        
        return max(0.0, min(1.0, gini))  # Clamp to [0, 1]
    
    @staticmethod
    def get_wealth_distribution(registry: ValidatorRegistry) -> Dict[str, float]:
        """
        Get detailed wealth distribution statistics.
        
        Args:
            registry: ValidatorRegistry instance
            
        Returns:
            Dictionary with distribution metrics
        """
        validators = registry.get_active_validators()
        
        if not validators:
            return {
                'gini': 0.0,
                'mean_stake': 0.0,
                'median_stake': 0.0,
                'max_stake': 0.0,
                'min_stake': 0.0,
                'total_stake': 0.0,
                'validator_count': 0
            }
        
        stakes = sorted([v.get_effective_stake() for v in validators])
        n = len(stakes)
        total_stake = sum(stakes)
        
        # Calculate median
        if n % 2 == 0:
            median = (stakes[n // 2 - 1] + stakes[n // 2]) / 2
        else:
            median = stakes[n // 2]
        
        return {
            'gini': GiniCalculator.calculate(registry),
            'mean_stake': total_stake / n if n > 0 else 0.0,
            'median_stake': median,
            'max_stake': max(stakes),
            'min_stake': min(stakes),
            'total_stake': total_stake,
            'validator_count': n
        }


class StakeManager:
    """
    Manages validator stakes, delegations, and slashing.
    """
    
    def __init__(self, registry: ValidatorRegistry):
        """
        Initialize stake manager.
        
        Args:
            registry: ValidatorRegistry instance
        """
        self.registry = registry
        self.delegations: Dict[bytes, List[Tuple[bytes, int]]] = defaultdict(list)
        # delegations[delegator] = [(delegatee, amount), ...]
        self.slashing_history: List[Dict] = []
    
    def delegate_stake(self, delegator_id: bytes, delegatee_id: bytes, 
                      amount: int) -> bool:
        """
        Delegate stake from one validator to another.
        
        Args:
            delegator_id: Node ID of stake owner
            delegatee_id: Node ID of validator receiving delegation
            amount: Amount to delegate
            
        Returns:
            True if delegation successful, False otherwise
        """
        delegator = self.registry.get_validator(delegator_id)
        delegatee = self.registry.get_validator(delegatee_id)
        
        if not delegator or not delegatee:
            return False
        
        if delegator.get_effective_stake() < amount:
            return False
        
        # Add delegation
        self.delegations[delegator_id].append((delegatee_id, amount))
        
        # Update delegatee's stake
        delegatee.stake += amount
        self.registry.total_stake += amount
        
        return True
    
    def revoke_delegation(self, delegator_id: bytes, delegatee_id: bytes) -> bool:
        """
        Revoke delegation from delegatee back to delegator.
        
        Args:
            delegator_id: Node ID of stake owner
            delegatee_id: Node ID of validator
            
        Returns:
            True if revocation successful, False otherwise
        """
        if delegator_id not in self.delegations:
            return False
        
        delegations = self.delegations[delegator_id]
        
        # Find and remove delegation
        for i, (delegatee, amount) in enumerate(delegations):
            if delegatee == delegatee_id:
                delegations.pop(i)
                
                # Update delegatee's stake
                delegatee_validator = self.registry.get_validator(delegatee_id)
                if delegatee_validator:
                    delegatee_validator.stake -= amount
                    self.registry.total_stake -= amount
                
                return True
        
        return False
    
    def slash_stake(self, node_id: bytes, penalty_percent: float) -> bool:
        """
        Apply slashing penalty to validator.
        
        Args:
            node_id: Node ID of validator to slash
            penalty_percent: Percentage of stake to slash (0-100)
            
        Returns:
            True if slashing successful, False otherwise
        """
        validator = self.registry.get_validator(node_id)
        
        if not validator:
            return False
        
        # Calculate slash amount
        effective_stake = validator.get_effective_stake()
        slash_amount = int(effective_stake * penalty_percent / 100)
        
        # Apply slash
        validator.slashed_amount += slash_amount
        
        # Record in history
        self.slashing_history.append({
            'node_id': node_id,
            'timestamp': time.time(),
            'amount': slash_amount,
            'percent': penalty_percent
        })
        
        return True
    
    def recover_slash(self, node_id: bytes, recovery_percent: float) -> bool:
        """
        Recover slashed stake after good behavior.
        
        Args:
            node_id: Node ID of validator
            recovery_percent: Percentage of slashed amount to recover (0-100)
            
        Returns:
            True if recovery successful, False otherwise
        """
        validator = self.registry.get_validator(node_id)
        
        if not validator or validator.slashed_amount == 0:
            return False
        
        # Calculate recovery amount
        recovery_amount = int(validator.slashed_amount * recovery_percent / 100)
        
        # Apply recovery
        validator.slashed_amount -= recovery_amount
        
        return True
    
    def get_delegation_rewards(self, delegatee_id: bytes, 
                              block_reward: int) -> Dict[bytes, int]:
        """
        Calculate delegation rewards for a block reward.
        
        Delegation fee: 5% of rewards go to delegator
        
        Args:
            delegatee_id: Node ID of validator who earned reward
            block_reward: Amount of reward earned
            
        Returns:
            Dictionary of {node_id: reward_amount}
        """
        rewards = {delegatee_id: block_reward}
        
        # Find delegators
        for delegator_id, delegations in self.delegations.items():
            for delegatee, amount in delegations:
                if delegatee == delegatee_id:
                    # Delegator gets 5% of reward
                    delegation_reward = int(block_reward * 0.05)
                    rewards[delegator_id] = delegation_reward
        
        return rewards


class ConsensusEngine:
    """
    Main consensus engine combining all components.
    """
    
    def __init__(self, target_gini: float = 0.35):
        """
        Initialize consensus engine.
        
        Args:
            target_gini: Target Gini coefficient for wealth distribution
        """
        self.registry = ValidatorRegistry()
        self.selector = DoPSelector(self.registry)
        self.stake_manager = StakeManager(self.registry)
        self.target_gini = target_gini
        self.current_gini = 0.0
        self.epoch = 0
        self.last_epoch_time = time.time()
    
    def add_validator(self, node_id: bytes, stake: int, 
                     hop_distance: int) -> bool:
        """Add validator to network."""
        return self.registry.add_validator(node_id, stake, hop_distance)
    
    def select_validator(self) -> Optional[bytes]:
        """Select next validator for block proposal."""
        return self.selector.select_validator()
    
    def select_committee(self, size: int) -> List[bytes]:
        """Select committee of validators."""
        return self.selector.select_multiple_validators(size)
    
    def update_gini(self):
        """Update Gini coefficient."""
        self.current_gini = GiniCalculator.calculate(self.registry)
    
    def get_statistics(self) -> Dict:
        """Get consensus statistics."""
        self.update_gini()
        
        return {
            'epoch': self.epoch,
            'validator_count': len(self.registry.get_active_validators()),
            'total_stake': self.registry.total_stake,
            'current_gini': self.current_gini,
            'target_gini': self.target_gini,
            'gini_distance': abs(self.current_gini - self.target_gini),
            'wealth_distribution': GiniCalculator.get_wealth_distribution(self.registry)
        }
