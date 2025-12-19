"""
MeshChain Secure Storage Module

Enhanced storage with:
1. Atomic writes (prevents corruption on power loss)
2. Block validation before storage
3. Hash verification on read
4. Transaction-block consistency checks
5. Chain continuity validation
6. Data integrity verification

Integrates security fixes from crypto_security module.
"""

import json
import os
import hashlib
import time
import tempfile
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from dataclasses import dataclass, asdict, field
import threading
import logging

from meshchain.crypto_security import AtomicFileWriter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class BlockMetadata:
    """Metadata for stored block."""
    height: int
    block_hash: str  # Hex-encoded hash
    timestamp: int
    size: int
    tx_count: int
    previous_hash: str
    stored_at: float = field(default_factory=time.time)


@dataclass
class StorageIntegrityCheck:
    """Result of integrity check."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    blocks_checked: int = 0
    blocks_corrupted: int = 0
    transactions_checked: int = 0
    transactions_orphaned: int = 0


class SecureStorage:
    """
    Secure storage with atomic writes and integrity checks.
    
    Features:
    - Atomic writes prevent corruption on power loss
    - Block validation before storage
    - Hash verification on read
    - Transaction-block consistency
    - Chain continuity validation
    - Data integrity verification
    """
    
    def __init__(self, db_path: str = "/mnt/microsd/blockchain"):
        """
        Initialize secure storage.
        
        Args:
            db_path: Path to blockchain directory
        """
        self.db_path = Path(db_path)
        self.db_path.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        (self.db_path / "blocks").mkdir(exist_ok=True)
        (self.db_path / "transactions").mkdir(exist_ok=True)
        (self.db_path / "utxos").mkdir(exist_ok=True)
        (self.db_path / "metadata").mkdir(exist_ok=True)
        
        # State tracking
        self.latest_block_height = self._load_latest_height()
        self.lock = threading.Lock()
        
        # Statistics
        self.stats = {
            'blocks_stored': 0,
            'blocks_validated': 0,
            'blocks_corrupted': 0,
            'transactions_stored': 0,
            'transactions_orphaned': 0,
            'reads': 0,
            'writes': 0,
            'integrity_checks': 0,
            'atomic_writes': 0,
            'atomic_write_failures': 0
        }
    
    def _load_latest_height(self) -> int:
        """Load latest block height from state with integrity check."""
        state_file = self.db_path / "state.json"
        if state_file.exists():
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                    height = state.get('latest_block_height', -1)
                    logger.info(f"Loaded latest block height: {height}")
                    return height
            except Exception as e:
                logger.error(f"Error loading state: {e}")
                return -1
        return -1
    
    def _save_state_atomic(self) -> bool:
        """Save node state atomically."""
        state = {
            'latest_block_height': self.latest_block_height,
            'timestamp': int(time.time()),
            'version': '2.0',
            'integrity_check_timestamp': int(time.time())
        }
        
        state_file = self.db_path / "state.json"
        state_json = json.dumps(state, indent=2).encode('utf-8')
        
        # Use atomic write
        success = AtomicFileWriter.write_atomic(str(state_file), state_json)
        
        if success:
            logger.info("State saved atomically")
        else:
            logger.error("Failed to save state atomically")
        
        return success
    
    def _compute_block_hash(self, block_data: bytes) -> str:
        """Compute SHA256 hash of block data."""
        return hashlib.sha256(block_data).hexdigest()
    
    def _validate_block(self, height: int, block_data: bytes, 
                       block_hash: str, previous_hash: str = None) -> Tuple[bool, List[str]]:
        """
        Validate block before storage.
        
        Args:
            height: Block height
            block_data: Block data
            block_hash: Expected block hash
            previous_hash: Expected previous block hash
        
        Returns:
            Tuple of (is_valid, error_list)
        """
        errors = []
        
        # Check block data is not empty
        if not block_data or len(block_data) == 0:
            errors.append("Block data is empty")
            return False, errors
        
        # Verify block hash
        computed_hash = self._compute_block_hash(block_data)
        if computed_hash != block_hash:
            errors.append(f"Block hash mismatch: expected {block_hash}, got {computed_hash}")
            return False, errors
        
        # Check block height is valid
        if height < 0:
            errors.append(f"Invalid block height: {height}")
            return False, errors
        
        # Check chain continuity if previous hash provided
        if previous_hash is not None:
            if height > 0:
                prev_block = self.get_block(height - 1)
                if prev_block is None:
                    errors.append(f"Previous block not found at height {height - 1}")
                    return False, errors
                
                # Verify previous hash matches
                prev_hash_computed = self._compute_block_hash(prev_block)
                if prev_hash_computed != previous_hash:
                    errors.append(f"Previous hash mismatch at height {height - 1}")
                    return False, errors
        
        return True, errors
    
    def add_block(self, height: int, block_hash: str, block_data: bytes,
                 previous_hash: str = None) -> Tuple[bool, Optional[str]]:
        """
        Add block with validation and atomic write.
        
        Args:
            height: Block height
            block_hash: Block hash (hex-encoded)
            block_data: Serialized block data
            previous_hash: Previous block hash for chain validation
        
        Returns:
            Tuple of (success, error_message)
        """
        with self.lock:
            # Validate block
            is_valid, errors = self._validate_block(height, block_data, block_hash, previous_hash)
            if not is_valid:
                error_msg = "; ".join(errors)
                logger.error(f"Block validation failed: {error_msg}")
                return False, error_msg
            
            self.stats['blocks_validated'] += 1
            
            try:
                # Write block file atomically
                block_file = self.db_path / "blocks" / f"{height:06d}.bin"
                success = AtomicFileWriter.write_atomic(str(block_file), block_data)
                
                if not success:
                    logger.error(f"Failed to write block {height} atomically")
                    self.stats['atomic_write_failures'] += 1
                    return False, "Atomic write failed"
                
                self.stats['atomic_writes'] += 1
                
                # Save block metadata
                metadata = BlockMetadata(
                    height=height,
                    block_hash=block_hash,
                    timestamp=int(time.time()),
                    size=len(block_data),
                    tx_count=0,  # Would be extracted from block data
                    previous_hash=previous_hash or ""
                )
                
                metadata_file = self.db_path / "metadata" / f"{height:06d}.json"
                metadata_json = json.dumps(asdict(metadata), indent=2).encode('utf-8')
                AtomicFileWriter.write_atomic(str(metadata_file), metadata_json)
                
                # Update state
                if height > self.latest_block_height:
                    self.latest_block_height = height
                    self._save_state_atomic()
                
                self.stats['blocks_stored'] += 1
                self.stats['writes'] += 1
                
                logger.info(f"Block {height} stored successfully (hash: {block_hash[:16]}...)")
                return True, None
            
            except Exception as e:
                error_msg = f"Error adding block: {str(e)}"
                logger.error(error_msg)
                self.stats['atomic_write_failures'] += 1
                return False, error_msg
    
    def get_block(self, height: int) -> Optional[bytes]:
        """
        Get block with hash verification.
        
        Args:
            height: Block height
        
        Returns:
            Block data or None if not found/corrupted
        """
        with self.lock:
            self.stats['reads'] += 1
            
            block_file = self.db_path / "blocks" / f"{height:06d}.bin"
            if not block_file.exists():
                logger.warning(f"Block {height} not found")
                return None
            
            try:
                # Read block data
                with open(block_file, 'rb') as f:
                    block_data = f.read()
                
                # Load metadata
                metadata_file = self.db_path / "metadata" / f"{height:06d}.json"
                if metadata_file.exists():
                    with open(metadata_file, 'r') as f:
                        metadata = json.load(f)
                        stored_hash = metadata.get('block_hash')
                        
                        # Verify hash
                        computed_hash = self._compute_block_hash(block_data)
                        if computed_hash != stored_hash:
                            logger.error(f"Block {height} hash mismatch: expected {stored_hash}, got {computed_hash}")
                            self.stats['blocks_corrupted'] += 1
                            return None
                
                return block_data
            
            except Exception as e:
                logger.error(f"Error reading block {height}: {e}")
                self.stats['blocks_corrupted'] += 1
                return None
    
    def add_transaction(self, tx_hash: str, block_height: int, tx_data: bytes) -> Tuple[bool, Optional[str]]:
        """
        Add transaction with consistency check.
        
        Args:
            tx_hash: Transaction hash (hex-encoded)
            block_height: Height of block containing transaction
            tx_data: Serialized transaction data
        
        Returns:
            Tuple of (success, error_message)
        """
        with self.lock:
            # Verify block exists
            block = self.get_block(block_height)
            if block is None:
                error_msg = f"Block {block_height} not found for transaction {tx_hash}"
                logger.error(error_msg)
                self.stats['transactions_orphaned'] += 1
                return False, error_msg
            
            try:
                # Write transaction file atomically
                tx_file = self.db_path / "transactions" / f"{tx_hash}.bin"
                success = AtomicFileWriter.write_atomic(str(tx_file), tx_data)
                
                if not success:
                    logger.error(f"Failed to write transaction {tx_hash} atomically")
                    return False, "Atomic write failed"
                
                # Create index entry
                index_entry = {
                    'tx_hash': tx_hash,
                    'block_height': block_height,
                    'timestamp': int(time.time()),
                    'size': len(tx_data)
                }
                
                index_file = self.db_path / "transactions" / f"{tx_hash}.json"
                index_json = json.dumps(index_entry, indent=2).encode('utf-8')
                AtomicFileWriter.write_atomic(str(index_file), index_json)
                
                self.stats['transactions_stored'] += 1
                self.stats['writes'] += 1
                
                logger.info(f"Transaction {tx_hash[:16]}... stored in block {block_height}")
                return True, None
            
            except Exception as e:
                error_msg = f"Error adding transaction: {str(e)}"
                logger.error(error_msg)
                return False, error_msg
    
    def get_transaction(self, tx_hash: str) -> Optional[bytes]:
        """
        Get transaction with consistency check.
        
        Args:
            tx_hash: Transaction hash (hex-encoded)
        
        Returns:
            Transaction data or None if not found
        """
        with self.lock:
            self.stats['reads'] += 1
            
            tx_file = self.db_path / "transactions" / f"{tx_hash}.bin"
            if not tx_file.exists():
                logger.warning(f"Transaction {tx_hash} not found")
                return None
            
            try:
                # Load index to verify consistency
                index_file = self.db_path / "transactions" / f"{tx_hash}.json"
                if index_file.exists():
                    with open(index_file, 'r') as f:
                        index = json.load(f)
                        block_height = index.get('block_height')
                        
                        # Verify block exists
                        block = self.get_block(block_height)
                        if block is None:
                            logger.error(f"Transaction {tx_hash} references missing block {block_height}")
                            self.stats['transactions_orphaned'] += 1
                            return None
                
                # Read transaction data
                with open(tx_file, 'rb') as f:
                    return f.read()
            
            except Exception as e:
                logger.error(f"Error reading transaction {tx_hash}: {e}")
                return None
    
    def verify_chain_integrity(self) -> StorageIntegrityCheck:
        """
        Verify chain integrity by checking all blocks.
        
        Returns:
            StorageIntegrityCheck with results
        """
        with self.lock:
            result = StorageIntegrityCheck(is_valid=True)
            self.stats['integrity_checks'] += 1
            
            blocks_dir = self.db_path / "blocks"
            if not blocks_dir.exists():
                result.errors.append("Blocks directory not found")
                result.is_valid = False
                return result
            
            # Get all block files
            block_files = sorted(blocks_dir.glob("*.bin"))
            
            logger.info(f"Verifying {len(block_files)} blocks...")
            
            for block_file in block_files:
                try:
                    # Extract height from filename
                    height = int(block_file.stem)
                    result.blocks_checked += 1
                    
                    # Read block
                    with open(block_file, 'rb') as f:
                        block_data = f.read()
                    
                    # Load metadata
                    metadata_file = self.db_path / "metadata" / f"{height:06d}.json"
                    if not metadata_file.exists():
                        result.warnings.append(f"Block {height} missing metadata")
                        continue
                    
                    with open(metadata_file, 'r') as f:
                        metadata = json.load(f)
                    
                    # Verify hash
                    stored_hash = metadata.get('block_hash')
                    computed_hash = self._compute_block_hash(block_data)
                    
                    if computed_hash != stored_hash:
                        result.errors.append(f"Block {height} hash mismatch")
                        result.blocks_corrupted += 1
                        result.is_valid = False
                    
                    # Check chain continuity
                    if height > 0:
                        prev_metadata_file = self.db_path / "metadata" / f"{height - 1:06d}.json"
                        if prev_metadata_file.exists():
                            with open(prev_metadata_file, 'r') as f:
                                prev_metadata = json.load(f)
                                expected_prev_hash = prev_metadata.get('block_hash')
                                actual_prev_hash = metadata.get('previous_hash')
                                
                                if expected_prev_hash != actual_prev_hash:
                                    result.errors.append(f"Block {height} chain continuity broken")
                                    result.is_valid = False
                
                except Exception as e:
                    result.errors.append(f"Error checking block {height}: {str(e)}")
                    result.blocks_corrupted += 1
                    result.is_valid = False
            
            # Check transaction consistency
            tx_dir = self.db_path / "transactions"
            if tx_dir.exists():
                tx_files = list(tx_dir.glob("*.json"))
                result.transactions_checked = len(tx_files)
                
                for tx_file in tx_files:
                    try:
                        with open(tx_file, 'r') as f:
                            index = json.load(f)
                            block_height = index.get('block_height')
                            
                            # Verify block exists
                            block_file = self.db_path / "blocks" / f"{block_height:06d}.bin"
                            if not block_file.exists():
                                result.warnings.append(f"Transaction references missing block {block_height}")
                                result.transactions_orphaned += 1
                    
                    except Exception as e:
                        result.warnings.append(f"Error checking transaction: {str(e)}")
            
            logger.info(f"Integrity check complete: {result.blocks_checked} blocks, "
                       f"{result.blocks_corrupted} corrupted, "
                       f"{result.transactions_orphaned} orphaned transactions")
            
            return result
    
    def get_stats(self) -> Dict[str, int]:
        """Get storage statistics."""
        with self.lock:
            return self.stats.copy()
    
    def get_latest_block_height(self) -> int:
        """Get height of latest block."""
        return self.latest_block_height


# Backward compatibility wrapper
class StorageAdapter:
    """Adapter for backward compatibility with existing storage interface."""
    
    def __init__(self, db_path: str = "/mnt/microsd/blockchain"):
        self.storage = SecureStorage(db_path)
    
    def add_block(self, height: int, block_hash: bytes, block_data: bytes) -> bool:
        """Add block (backward compatible)."""
        block_hash_hex = block_hash.hex() if isinstance(block_hash, bytes) else block_hash
        success, _ = self.storage.add_block(height, block_hash_hex, block_data)
        return success
    
    def get_block(self, height: int) -> Optional[bytes]:
        """Get block (backward compatible)."""
        return self.storage.get_block(height)
    
    def add_transaction(self, tx_hash: bytes, block_height: int, tx_data: bytes) -> bool:
        """Add transaction (backward compatible)."""
        tx_hash_hex = tx_hash.hex() if isinstance(tx_hash, bytes) else tx_hash
        success, _ = self.storage.add_transaction(tx_hash_hex, block_height, tx_data)
        return success
    
    def get_transaction(self, tx_hash: bytes) -> Optional[bytes]:
        """Get transaction (backward compatible)."""
        tx_hash_hex = tx_hash.hex() if isinstance(tx_hash, bytes) else tx_hash
        return self.storage.get_transaction(tx_hash_hex)
    
    def get_latest_block_height(self) -> int:
        """Get latest block height (backward compatible)."""
        return self.storage.get_latest_block_height()
    
    def verify_integrity(self) -> bool:
        """Verify storage integrity."""
        result = self.storage.verify_chain_integrity()
        return result.is_valid
