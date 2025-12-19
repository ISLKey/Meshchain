"""
Genesis Block Creation System for MeshChain Testnet

Implements:
1. Genesis block creation
2. Initial validator setup
3. Initial UTXO distribution
4. Testnet parameters
5. Genesis validation

This module handles the creation of the genesis block that
initializes a new MeshChain testnet.
"""

import time
import json
import hashlib
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class GenesisValidator:
    """Genesis validator configuration."""
    node_id: int
    stake: int
    public_key: str
    description: str = ""


@dataclass
class GenesisUTXO:
    """Genesis UTXO allocation."""
    owner: int
    amount: int
    description: str = ""


class GenesisBlockCreator:
    """Creates genesis blocks for MeshChain testnets."""
    
    # Genesis block constants
    GENESIS_VERSION = 1
    GENESIS_HEIGHT = 0
    GENESIS_PARENT_HASH = "0" * 64  # No parent for genesis
    
    # Testnet parameters
    TESTNET_BLOCK_TIME = 10  # 10 seconds
    TESTNET_MAX_BLOCK_SIZE = 1000000  # 1 MB
    TESTNET_MIN_STAKE = 1000  # Minimum stake for validator
    
    def __init__(self, testnet_name: str = "meshchain-testnet"):
        """
        Initialize genesis block creator.
        
        Args:
            testnet_name: Name of the testnet
        """
        self.testnet_name = testnet_name
        self.validators: List[GenesisValidator] = []
        self.utxos: List[GenesisUTXO] = []
        self.created_time = int(time.time())
        
        logger.info(f"Genesis block creator initialized for '{testnet_name}'")
    
    def add_validator(
        self,
        node_id: int,
        stake: int,
        public_key: str,
        description: str = ""
    ) -> None:
        """
        Add a genesis validator.
        
        Args:
            node_id: Validator node ID
            stake: Initial stake amount
            public_key: Validator public key (hex)
            description: Description of validator
        """
        if stake < self.TESTNET_MIN_STAKE:
            raise ValueError(f"Stake {stake} is below minimum {self.TESTNET_MIN_STAKE}")
        
        validator = GenesisValidator(
            node_id=node_id,
            stake=stake,
            public_key=public_key,
            description=description
        )
        
        self.validators.append(validator)
        logger.info(f"Added validator: {hex(node_id)} with stake {stake}")
    
    def add_utxo(
        self,
        owner: int,
        amount: int,
        description: str = ""
    ) -> None:
        """
        Add initial UTXO allocation.
        
        Args:
            owner: Owner node ID
            amount: Amount to allocate
            description: Description of allocation
        """
        utxo = GenesisUTXO(
            owner=owner,
            amount=amount,
            description=description
        )
        
        self.utxos.append(utxo)
        logger.info(f"Added UTXO: {hex(owner)} -> {amount} units")
    
    def create_genesis_block(self) -> Dict[str, Any]:
        """
        Create the genesis block.
        
        Returns:
            Genesis block dictionary
        """
        if not self.validators:
            raise ValueError("No validators configured for genesis block")
        
        # Calculate total stake
        total_stake = sum(v.stake for v in self.validators)
        
        # Calculate total UTXO allocation
        total_utxo = sum(u.amount for u in self.utxos)
        
        logger.info(f"Creating genesis block with {len(self.validators)} validators and {len(self.utxos)} UTXOs")
        logger.info(f"Total stake: {total_stake}, Total UTXO: {total_utxo}")
        
        # Create genesis block data
        genesis_data = {
            'version': self.GENESIS_VERSION,
            'height': self.GENESIS_HEIGHT,
            'timestamp': self.created_time,
            'parent_hash': self.GENESIS_PARENT_HASH,
            'testnet_name': self.testnet_name,
            'testnet_params': {
                'block_time': self.TESTNET_BLOCK_TIME,
                'max_block_size': self.TESTNET_MAX_BLOCK_SIZE,
                'min_stake': self.TESTNET_MIN_STAKE,
            },
            'validators': [asdict(v) for v in self.validators],
            'utxos': [asdict(u) for u in self.utxos],
            'total_stake': total_stake,
            'total_supply': total_utxo,
        }
        
        # Calculate genesis block hash
        genesis_hash = self._calculate_hash(genesis_data)
        genesis_data['hash'] = genesis_hash
        
        logger.info(f"Genesis block created: {genesis_hash}")
        
        return genesis_data
    
    def _calculate_hash(self, data: Dict[str, Any]) -> str:
        """
        Calculate hash of genesis block data.
        
        Args:
            data: Block data
        
        Returns:
            Hash as hex string
        """
        # Create a copy without the hash field
        data_copy = {k: v for k, v in data.items() if k != 'hash'}
        
        # Serialize to JSON
        json_str = json.dumps(data_copy, sort_keys=True)
        
        # Calculate SHA256
        hash_obj = hashlib.sha256(json_str.encode('utf-8'))
        return hash_obj.hexdigest()
    
    def validate_genesis_block(self, genesis_block: Dict[str, Any]) -> bool:
        """
        Validate a genesis block.
        
        Args:
            genesis_block: Genesis block to validate
        
        Returns:
            True if valid
        """
        # Check required fields
        required_fields = ['version', 'height', 'timestamp', 'parent_hash', 'hash', 'validators', 'utxos']
        for field in required_fields:
            if field not in genesis_block:
                logger.error(f"Missing required field: {field}")
                return False
        
        # Check height is 0
        if genesis_block['height'] != 0:
            logger.error(f"Genesis block height must be 0, got {genesis_block['height']}")
            return False
        
        # Check parent hash is all zeros
        if genesis_block['parent_hash'] != self.GENESIS_PARENT_HASH:
            logger.error(f"Genesis block parent hash must be all zeros")
            return False
        
        # Check hash is valid
        expected_hash = self._calculate_hash(genesis_block)
        if genesis_block['hash'] != expected_hash:
            logger.error(f"Genesis block hash mismatch")
            return False
        
        # Check validators
        if not genesis_block['validators']:
            logger.error("Genesis block must have at least one validator")
            return False
        
        # Check UTXOs
        if not genesis_block['utxos']:
            logger.error("Genesis block must have at least one UTXO")
            return False
        
        logger.info("Genesis block validation passed")
        return True
    
    def save_genesis_block(self, genesis_block: Dict[str, Any], filepath: str) -> None:
        """
        Save genesis block to file.
        
        Args:
            genesis_block: Genesis block to save
            filepath: Path to save file
        """
        with open(filepath, 'w') as f:
            json.dump(genesis_block, f, indent=2)
        
        logger.info(f"Genesis block saved to {filepath}")
    
    def load_genesis_block(self, filepath: str) -> Dict[str, Any]:
        """
        Load genesis block from file.
        
        Args:
            filepath: Path to genesis block file
        
        Returns:
            Genesis block dictionary
        """
        with open(filepath, 'r') as f:
            genesis_block = json.load(f)
        
        logger.info(f"Genesis block loaded from {filepath}")
        return genesis_block


class TestnetGenesisBuilder:
    """Helper class to build testnet genesis blocks."""
    
    @staticmethod
    def create_5_node_testnet() -> Dict[str, Any]:
        """
        Create a 5-node testnet genesis block.
        
        Returns:
            Genesis block dictionary
        """
        creator = GenesisBlockCreator("meshchain-5node-testnet")
        
        # Add 5 validators
        validators = [
            {
                'node_id': 0x11111111,
                'stake': 10000,
                'public_key': 'a' * 64,
                'description': 'Validator 1'
            },
            {
                'node_id': 0x22222222,
                'stake': 10000,
                'public_key': 'b' * 64,
                'description': 'Validator 2'
            },
            {
                'node_id': 0x33333333,
                'stake': 10000,
                'public_key': 'c' * 64,
                'description': 'Validator 3'
            },
            {
                'node_id': 0x44444444,
                'stake': 10000,
                'public_key': 'd' * 64,
                'description': 'Validator 4'
            },
            {
                'node_id': 0x55555555,
                'stake': 10000,
                'public_key': 'e' * 64,
                'description': 'Validator 5'
            },
        ]
        
        for v in validators:
            creator.add_validator(**v)
        
        # Add initial UTXOs (1000 units per validator)
        for v in validators:
            creator.add_utxo(
                owner=v['node_id'],
                amount=1000,
                description=f"Initial allocation for {v['description']}"
            )
        
        return creator.create_genesis_block()
    
    @staticmethod
    def create_6_node_testnet() -> Dict[str, Any]:
        """
        Create a 6-node testnet genesis block.
        
        Returns:
            Genesis block dictionary
        """
        creator = GenesisBlockCreator("meshchain-6node-testnet")
        
        # Add 6 validators
        validators = [
            {
                'node_id': 0x11111111,
                'stake': 8000,
                'public_key': 'a' * 64,
                'description': 'Validator 1'
            },
            {
                'node_id': 0x22222222,
                'stake': 8000,
                'public_key': 'b' * 64,
                'description': 'Validator 2'
            },
            {
                'node_id': 0x33333333,
                'stake': 8000,
                'public_key': 'c' * 64,
                'description': 'Validator 3'
            },
            {
                'node_id': 0x44444444,
                'stake': 8000,
                'public_key': 'd' * 64,
                'description': 'Validator 4'
            },
            {
                'node_id': 0x55555555,
                'stake': 8000,
                'public_key': 'e' * 64,
                'description': 'Validator 5'
            },
            {
                'node_id': 0x66666666,
                'stake': 8000,
                'public_key': 'f' * 64,
                'description': 'Validator 6'
            },
        ]
        
        for v in validators:
            creator.add_validator(**v)
        
        # Add initial UTXOs (800 units per validator)
        for v in validators:
            creator.add_utxo(
                owner=v['node_id'],
                amount=800,
                description=f"Initial allocation for {v['description']}"
            )
        
        return creator.create_genesis_block()
