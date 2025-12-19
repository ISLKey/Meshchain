"""
Testnet Validator Configuration System

Implements:
1. Validator setup
2. Validator key management
3. Validator stake management
4. Validator rotation
5. Validator monitoring

This module manages validator configuration and lifecycle
for testnet deployment.
"""

import time
import logging
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ValidatorStatus(Enum):
    """Validator status."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    SLASHED = "slashed"
    EXITING = "exiting"


@dataclass
class ValidatorKeys:
    """Validator cryptographic keys."""
    node_id: int
    public_key: str  # Ed25519 public key (hex)
    signing_key: str  # Ed25519 signing key (encrypted hex)
    consensus_key: str  # Consensus key (hex)
    withdrawal_key: str  # Withdrawal key (hex)


@dataclass
class ValidatorStake:
    """Validator stake information."""
    node_id: int
    amount: int
    locked_until: int  # Timestamp
    withdrawal_address: int  # Node ID to receive withdrawals


@dataclass
class ValidatorMetrics:
    """Validator performance metrics."""
    blocks_proposed: int = 0
    blocks_missed: int = 0
    attestations_made: int = 0
    attestations_missed: int = 0
    slashing_events: int = 0
    last_block_time: int = 0
    uptime_percentage: float = 100.0


@dataclass
class ValidatorConfig:
    """Complete validator configuration."""
    node_id: int
    status: ValidatorStatus
    keys: ValidatorKeys
    stake: ValidatorStake
    metrics: ValidatorMetrics
    created_time: int
    version: str = "1.0"


class ValidatorManager:
    """Manages validator configurations and lifecycle."""
    
    def __init__(self):
        """Initialize validator manager."""
        self.validators: Dict[int, ValidatorConfig] = {}
        self.validators_lock = __import__('threading').Lock()
        logger.info("Validator manager initialized")
    
    def register_validator(
        self,
        node_id: int,
        public_key: str,
        signing_key: str,
        stake_amount: int,
        withdrawal_address: int
    ) -> ValidatorConfig:
        """
        Register a new validator.
        
        Args:
            node_id: Validator node ID
            public_key: Public key (hex)
            signing_key: Signing key (encrypted hex)
            stake_amount: Initial stake amount
            withdrawal_address: Withdrawal address
        
        Returns:
            Validator configuration
        """
        with self.validators_lock:
            if node_id in self.validators:
                raise ValueError(f"Validator {hex(node_id)} already registered")
            
            keys = ValidatorKeys(
                node_id=node_id,
                public_key=public_key,
                signing_key=signing_key,
                consensus_key='0' * 64,  # Placeholder
                withdrawal_key='0' * 64  # Placeholder
            )
            
            stake = ValidatorStake(
                node_id=node_id,
                amount=stake_amount,
                locked_until=int(time.time()) + 86400,  # 24 hours
                withdrawal_address=withdrawal_address
            )
            
            metrics = ValidatorMetrics()
            
            config = ValidatorConfig(
                node_id=node_id,
                status=ValidatorStatus.ACTIVE,
                keys=keys,
                stake=stake,
                metrics=metrics,
                created_time=int(time.time())
            )
            
            self.validators[node_id] = config
            logger.info(f"Registered validator {hex(node_id)} with stake {stake_amount}")
            
            return config
    
    def get_validator(self, node_id: int) -> Optional[ValidatorConfig]:
        """
        Get validator configuration.
        
        Args:
            node_id: Validator node ID
        
        Returns:
            Validator configuration or None
        """
        with self.validators_lock:
            return self.validators.get(node_id)
    
    def get_active_validators(self) -> List[ValidatorConfig]:
        """
        Get all active validators.
        
        Returns:
            List of active validators
        """
        with self.validators_lock:
            return [
                v for v in self.validators.values()
                if v.status == ValidatorStatus.ACTIVE
            ]
    
    def get_total_stake(self) -> int:
        """
        Get total stake across all validators.
        
        Returns:
            Total stake amount
        """
        with self.validators_lock:
            return sum(v.stake.amount for v in self.validators.values())
    
    def update_validator_metrics(
        self,
        node_id: int,
        blocks_proposed: int = 0,
        blocks_missed: int = 0,
        attestations_made: int = 0,
        attestations_missed: int = 0
    ) -> None:
        """
        Update validator metrics.
        
        Args:
            node_id: Validator node ID
            blocks_proposed: Blocks proposed
            blocks_missed: Blocks missed
            attestations_made: Attestations made
            attestations_missed: Attestations missed
        """
        with self.validators_lock:
            if node_id not in self.validators:
                logger.warning(f"Validator {hex(node_id)} not found")
                return
            
            validator = self.validators[node_id]
            validator.metrics.blocks_proposed += blocks_proposed
            validator.metrics.blocks_missed += blocks_missed
            validator.metrics.attestations_made += attestations_made
            validator.metrics.attestations_missed += attestations_missed
            validator.metrics.last_block_time = int(time.time())
            
            # Calculate uptime
            total_blocks = validator.metrics.blocks_proposed + validator.metrics.blocks_missed
            if total_blocks > 0:
                validator.metrics.uptime_percentage = (
                    validator.metrics.blocks_proposed / total_blocks * 100
                )
    
    def slash_validator(self, node_id: int, reason: str = "") -> None:
        """
        Slash a validator.
        
        Args:
            node_id: Validator node ID
            reason: Reason for slashing
        """
        with self.validators_lock:
            if node_id not in self.validators:
                logger.warning(f"Validator {hex(node_id)} not found")
                return
            
            validator = self.validators[node_id]
            validator.status = ValidatorStatus.SLASHED
            validator.metrics.slashing_events += 1
            
            # Reduce stake by 32% (standard slashing penalty)
            penalty = int(validator.stake.amount * 0.32)
            validator.stake.amount -= penalty
            
            logger.warning(f"Validator {hex(node_id)} slashed: {reason}. Penalty: {penalty}")
    
    def deactivate_validator(self, node_id: int) -> None:
        """
        Deactivate a validator.
        
        Args:
            node_id: Validator node ID
        """
        with self.validators_lock:
            if node_id not in self.validators:
                logger.warning(f"Validator {hex(node_id)} not found")
                return
            
            validator = self.validators[node_id]
            validator.status = ValidatorStatus.INACTIVE
            logger.info(f"Validator {hex(node_id)} deactivated")
    
    def get_validator_stats(self) -> Dict[str, Any]:
        """
        Get validator statistics.
        
        Returns:
            Statistics dictionary
        """
        with self.validators_lock:
            active_validators = [
                v for v in self.validators.values()
                if v.status == ValidatorStatus.ACTIVE
            ]
            
            total_stake = sum(v.stake.amount for v in self.validators.values())
            total_blocks = sum(
                v.metrics.blocks_proposed + v.metrics.blocks_missed
                for v in self.validators.values()
            )
            
            return {
                'total_validators': len(self.validators),
                'active_validators': len(active_validators),
                'total_stake': total_stake,
                'average_stake': total_stake / len(self.validators) if self.validators else 0,
                'total_blocks_proposed': sum(v.metrics.blocks_proposed for v in self.validators.values()),
                'total_blocks_missed': sum(v.metrics.blocks_missed for v in self.validators.values()),
                'average_uptime': sum(v.metrics.uptime_percentage for v in self.validators.values()) / len(self.validators) if self.validators else 0,
            }
    
    def save_to_file(self, filepath: str) -> None:
        """
        Save validator configurations to file.
        
        Args:
            filepath: Path to save file
        """
        with self.validators_lock:
            configs = {
                hex(node_id): {
                    'node_id': hex(config.node_id),
                    'status': config.status.value,
                    'keys': asdict(config.keys),
                    'stake': asdict(config.stake),
                    'metrics': asdict(config.metrics),
                    'created_time': config.created_time,
                    'version': config.version,
                }
                for node_id, config in self.validators.items()
            }
        
        with open(filepath, 'w') as f:
            json.dump(configs, f, indent=2)
        
        logger.info(f"Saved {len(self.validators)} validator configurations to {filepath}")
    
    def load_from_file(self, filepath: str) -> None:
        """
        Load validator configurations from file.
        
        Args:
            filepath: Path to configuration file
        """
        with open(filepath, 'r') as f:
            configs = json.load(f)
        
        with self.validators_lock:
            for node_id_hex, config_data in configs.items():
                node_id = int(node_id_hex, 16)
                
                keys = ValidatorKeys(**config_data['keys'])
                stake = ValidatorStake(**config_data['stake'])
                metrics = ValidatorMetrics(**config_data['metrics'])
                
                config = ValidatorConfig(
                    node_id=node_id,
                    status=ValidatorStatus(config_data['status']),
                    keys=keys,
                    stake=stake,
                    metrics=metrics,
                    created_time=config_data['created_time'],
                    version=config_data.get('version', '1.0')
                )
                
                self.validators[node_id] = config
        
        logger.info(f"Loaded {len(self.validators)} validator configurations from {filepath}")


class TestnetValidatorSetup:
    """Helper class for testnet validator setup."""
    
    @staticmethod
    def setup_5_node_validators() -> ValidatorManager:
        """
        Setup validators for 5-node testnet.
        
        Returns:
            ValidatorManager with 5 validators
        """
        manager = ValidatorManager()
        
        validators = [
            {
                'node_id': 0x11111111,
                'public_key': 'a' * 64,
                'signing_key': 'a' * 64,
                'stake': 10000,
                'withdrawal': 0x11111111,
            },
            {
                'node_id': 0x22222222,
                'public_key': 'b' * 64,
                'signing_key': 'b' * 64,
                'stake': 10000,
                'withdrawal': 0x22222222,
            },
            {
                'node_id': 0x33333333,
                'public_key': 'c' * 64,
                'signing_key': 'c' * 64,
                'stake': 10000,
                'withdrawal': 0x33333333,
            },
            {
                'node_id': 0x44444444,
                'public_key': 'd' * 64,
                'signing_key': 'd' * 64,
                'stake': 10000,
                'withdrawal': 0x44444444,
            },
            {
                'node_id': 0x55555555,
                'public_key': 'e' * 64,
                'signing_key': 'e' * 64,
                'stake': 10000,
                'withdrawal': 0x55555555,
            },
        ]
        
        for v in validators:
            manager.register_validator(
                node_id=v['node_id'],
                public_key=v['public_key'],
                signing_key=v['signing_key'],
                stake_amount=v['stake'],
                withdrawal_address=v['withdrawal']
            )
        
        return manager
    
    @staticmethod
    def setup_6_node_validators() -> ValidatorManager:
        """
        Setup validators for 6-node testnet.
        
        Returns:
            ValidatorManager with 6 validators
        """
        manager = ValidatorManager()
        
        validators = [
            {
                'node_id': 0x11111111,
                'public_key': 'a' * 64,
                'signing_key': 'a' * 64,
                'stake': 8000,
                'withdrawal': 0x11111111,
            },
            {
                'node_id': 0x22222222,
                'public_key': 'b' * 64,
                'signing_key': 'b' * 64,
                'stake': 8000,
                'withdrawal': 0x22222222,
            },
            {
                'node_id': 0x33333333,
                'public_key': 'c' * 64,
                'signing_key': 'c' * 64,
                'stake': 8000,
                'withdrawal': 0x33333333,
            },
            {
                'node_id': 0x44444444,
                'public_key': 'd' * 64,
                'signing_key': 'd' * 64,
                'stake': 8000,
                'withdrawal': 0x44444444,
            },
            {
                'node_id': 0x55555555,
                'public_key': 'e' * 64,
                'signing_key': 'e' * 64,
                'stake': 8000,
                'withdrawal': 0x55555555,
            },
            {
                'node_id': 0x66666666,
                'public_key': 'f' * 64,
                'signing_key': 'f' * 64,
                'stake': 8000,
                'withdrawal': 0x66666666,
            },
        ]
        
        for v in validators:
            manager.register_validator(
                node_id=v['node_id'],
                public_key=v['public_key'],
                signing_key=v['signing_key'],
                stake_amount=v['stake'],
                withdrawal_address=v['withdrawal']
            )
        
        return manager
