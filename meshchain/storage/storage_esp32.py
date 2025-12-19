"""
MeshChain ESP32 Storage Module

Lightweight storage adapter for ESP32 devices using JSON-based storage
instead of SQLite. Designed for low memory footprint while maintaining
compatibility with existing BlockchainStorage interface.

Key Components:
1. StorageAdapter: Abstract interface for storage operations
2. LiteDBStorage: Lightweight JSON-based storage
3. MemoryCache: In-memory cache for hot data
4. BlockCache: Recent blocks cache
5. UTXOCache: UTXO set cache
"""

import json
import os
import hashlib
import time
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from dataclasses import dataclass, asdict, field
from collections import OrderedDict
import threading


@dataclass
class CacheEntry:
    """Cache entry with TTL and size tracking."""
    key: str
    value: Any
    timestamp: float = field(default_factory=time.time)
    size: int = 0  # Size in bytes
    access_count: int = 0
    last_access: float = field(default_factory=time.time)
    
    def is_expired(self, ttl: float = 3600.0) -> bool:
        """Check if entry has expired."""
        return time.time() - self.timestamp > ttl
    
    def update_access(self):
        """Update access time and count."""
        self.last_access = time.time()
        self.access_count += 1


class MemoryCache:
    """
    In-memory cache for frequently accessed data.
    
    Features:
    - LRU eviction when size limit exceeded
    - TTL-based expiration
    - Thread-safe operations
    - Size tracking
    """
    
    def __init__(self, max_size_kb: int = 50, ttl: float = 3600.0):
        """
        Initialize memory cache.
        
        Args:
            max_size_kb: Maximum cache size in kilobytes
            ttl: Time-to-live for entries in seconds
        """
        self.max_size = max_size_kb * 1024  # Convert to bytes
        self.ttl = ttl
        self.cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self.current_size = 0
        self.lock = threading.Lock()
        self.hits = 0
        self.misses = 0
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
        
        Returns:
            Cached value or None if not found/expired
        """
        with self.lock:
            if key not in self.cache:
                self.misses += 1
                return None
            
            entry = self.cache[key]
            
            # Check if expired
            if entry.is_expired(self.ttl):
                self.current_size -= entry.size
                del self.cache[key]
                self.misses += 1
                return None
            
            # Update access info and move to end (LRU)
            entry.update_access()
            self.cache.move_to_end(key)
            self.hits += 1
            
            return entry.value
    
    def set(self, key: str, value: Any, size: int = 0) -> None:
        """
        Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            size: Size in bytes (estimated if 0)
        """
        with self.lock:
            # Estimate size if not provided
            if size == 0:
                try:
                    size = len(json.dumps(value).encode())
                except:
                    size = 100  # Default estimate
            
            # Remove old entry if exists
            if key in self.cache:
                self.current_size -= self.cache[key].size
            
            # Evict LRU entries if needed
            while self.current_size + size > self.max_size and self.cache:
                lru_key, lru_entry = self.cache.popitem(last=False)
                self.current_size -= lru_entry.size
            
            # Add new entry
            entry = CacheEntry(key=key, value=value, size=size)
            self.cache[key] = entry
            self.current_size += size
    
    def delete(self, key: str) -> bool:
        """Delete entry from cache."""
        with self.lock:
            if key in self.cache:
                self.current_size -= self.cache[key].size
                del self.cache[key]
                return True
            return False
    
    def clear(self) -> None:
        """Clear all cache entries."""
        with self.lock:
            self.cache.clear()
            self.current_size = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            total_requests = self.hits + self.misses
            hit_rate = self.hits / total_requests if total_requests > 0 else 0
            
            return {
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate': hit_rate,
                'entries': len(self.cache),
                'size_bytes': self.current_size,
                'max_size_bytes': self.max_size,
                'utilization': self.current_size / self.max_size if self.max_size > 0 else 0
            }


class BlockCache:
    """
    Specialized cache for recent blocks.
    
    Keeps last N blocks in memory for fast access.
    """
    
    def __init__(self, max_blocks: int = 10):
        """
        Initialize block cache.
        
        Args:
            max_blocks: Maximum number of blocks to keep
        """
        self.max_blocks = max_blocks
        self.blocks: OrderedDict[int, bytes] = OrderedDict()  # height -> serialized block
        self.lock = threading.Lock()
    
    def add_block(self, height: int, block_data: bytes) -> None:
        """Add block to cache."""
        with self.lock:
            self.blocks[height] = block_data
            
            # Evict oldest if needed
            while len(self.blocks) > self.max_blocks:
                self.blocks.popitem(last=False)
    
    def get_block(self, height: int) -> Optional[bytes]:
        """Get block by height."""
        with self.lock:
            return self.blocks.get(height)
    
    def get_latest_height(self) -> Optional[int]:
        """Get height of latest block."""
        with self.lock:
            if self.blocks:
                return max(self.blocks.keys())
            return None
    
    def clear(self) -> None:
        """Clear cache."""
        with self.lock:
            self.blocks.clear()


class UTXOCache:
    """
    Specialized cache for UTXO set.
    
    Keeps frequently accessed UTXOs in memory.
    """
    
    def __init__(self, max_utxos: int = 1000):
        """
        Initialize UTXO cache.
        
        Args:
            max_utxos: Maximum number of UTXOs to keep
        """
        self.max_utxos = max_utxos
        self.utxos: Dict[bytes, Dict] = {}  # utxo_id -> utxo_data
        self.lock = threading.Lock()
    
    def add_utxo(self, utxo_id: bytes, utxo_data: Dict) -> None:
        """Add UTXO to cache."""
        with self.lock:
            self.utxos[utxo_id] = utxo_data
            
            # Evict random UTXO if over limit
            if len(self.utxos) > self.max_utxos:
                # Remove first item (simple eviction)
                self.utxos.pop(next(iter(self.utxos)))
    
    def get_utxo(self, utxo_id: bytes) -> Optional[Dict]:
        """Get UTXO by ID."""
        with self.lock:
            return self.utxos.get(utxo_id)
    
    def remove_utxo(self, utxo_id: bytes) -> bool:
        """Remove UTXO from cache."""
        with self.lock:
            if utxo_id in self.utxos:
                del self.utxos[utxo_id]
                return True
            return False
    
    def clear(self) -> None:
        """Clear cache."""
        with self.lock:
            self.utxos.clear()


class LiteDBStorage:
    """
    Lightweight JSON-based storage for MeshChain on ESP32.
    
    Replaces SQLite with simple JSON files for lower memory footprint.
    Uses memory caches for frequently accessed data.
    
    Directory structure:
    /blockchain/
    ├── blocks/           # Individual block files
    ├── transactions/     # Transaction index
    ├── utxos/           # UTXO set
    ├── state.json       # Node state
    └── metadata.json    # Storage metadata
    """
    
    def __init__(self, db_path: str = "/mnt/microsd/blockchain"):
        """
        Initialize LiteDB storage.
        
        Args:
            db_path: Path to blockchain directory
        """
        self.db_path = Path(db_path)
        self.db_path.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        (self.db_path / "blocks").mkdir(exist_ok=True)
        (self.db_path / "transactions").mkdir(exist_ok=True)
        (self.db_path / "utxos").mkdir(exist_ok=True)
        
        # Initialize caches
        self.memory_cache = MemoryCache(max_size_kb=50)
        self.block_cache = BlockCache(max_blocks=10)
        self.utxo_cache = UTXOCache(max_utxos=1000)
        
        # State tracking
        self.latest_block_height = self._load_latest_height()
        self.lock = threading.Lock()
        
        # Statistics
        self.stats = {
            'blocks_stored': 0,
            'transactions_stored': 0,
            'utxos_stored': 0,
            'reads': 0,
            'writes': 0,
            'cache_hits': 0,
            'cache_misses': 0
        }
    
    def _load_latest_height(self) -> int:
        """Load latest block height from state."""
        state_file = self.db_path / "state.json"
        if state_file.exists():
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                    return state.get('latest_block_height', -1)
            except:
                return -1
        return -1
    
    def _save_state(self) -> None:
        """Save node state to file."""
        state = {
            'latest_block_height': self.latest_block_height,
            'timestamp': int(time.time()),
            'version': '1.0'
        }
        
        state_file = self.db_path / "state.json"
        try:
            with open(state_file, 'w') as f:
                json.dump(state, f)
        except Exception as e:
            print(f"Error saving state: {e}")
    
    def add_block(self, height: int, block_hash: bytes, block_data: bytes) -> bool:
        """
        Add a block to storage.
        
        Args:
            height: Block height
            block_hash: Block hash (for verification)
            block_data: Serialized block data
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.lock:
                # Save block file
                block_file = self.db_path / "blocks" / f"{height:06d}.bin"
                with open(block_file, 'wb') as f:
                    f.write(block_data)
                
                # Update caches
                self.block_cache.add_block(height, block_data)
                self.memory_cache.set(f"block:{height}", block_data, len(block_data))
                
                # Update state
                if height > self.latest_block_height:
                    self.latest_block_height = height
                    self._save_state()
                
                self.stats['blocks_stored'] += 1
                self.stats['writes'] += 1
                
                return True
        except Exception as e:
            print(f"Error adding block: {e}")
            return False
    
    def get_block(self, height: int) -> Optional[bytes]:
        """
        Get block by height.
        
        Args:
            height: Block height
        
        Returns:
            Serialized block data or None
        """
        with self.lock:
            self.stats['reads'] += 1
            
            # Try memory cache first
            cached = self.memory_cache.get(f"block:{height}")
            if cached:
                self.stats['cache_hits'] += 1
                return cached
            
            # Try block cache
            cached = self.block_cache.get_block(height)
            if cached:
                self.stats['cache_hits'] += 1
                return cached
            
            # Load from disk
            block_file = self.db_path / "blocks" / f"{height:06d}.bin"
            if block_file.exists():
                try:
                    with open(block_file, 'rb') as f:
                        data = f.read()
                        # Cache for future access
                        self.memory_cache.set(f"block:{height}", data, len(data))
                        self.block_cache.add_block(height, data)
                        return data
                except Exception as e:
                    print(f"Error reading block: {e}")
            
            self.stats['cache_misses'] += 1
            return None
    
    def get_latest_block_height(self) -> int:
        """Get height of latest block."""
        return self.latest_block_height
    
    def add_transaction(self, tx_hash: bytes, block_height: int, tx_data: bytes) -> bool:
        """
        Add transaction to storage.
        
        Args:
            tx_hash: Transaction hash
            block_height: Block height containing transaction
            tx_data: Serialized transaction data
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.lock:
                # Save transaction file
                tx_file = self.db_path / "transactions" / tx_hash.hex()
                with open(tx_file, 'wb') as f:
                    f.write(tx_data)
                
                # Cache transaction
                self.memory_cache.set(f"tx:{tx_hash.hex()}", tx_data, len(tx_data))
                
                self.stats['transactions_stored'] += 1
                self.stats['writes'] += 1
                
                return True
        except Exception as e:
            print(f"Error adding transaction: {e}")
            return False
    
    def get_transaction(self, tx_hash: bytes) -> Optional[bytes]:
        """
        Get transaction by hash.
        
        Args:
            tx_hash: Transaction hash
        
        Returns:
            Serialized transaction data or None
        """
        with self.lock:
            self.stats['reads'] += 1
            
            # Try cache first
            cached = self.memory_cache.get(f"tx:{tx_hash.hex()}")
            if cached:
                self.stats['cache_hits'] += 1
                return cached
            
            # Load from disk
            tx_file = self.db_path / "transactions" / tx_hash.hex()
            if tx_file.exists():
                try:
                    with open(tx_file, 'rb') as f:
                        data = f.read()
                        self.memory_cache.set(f"tx:{tx_hash.hex()}", data, len(data))
                        return data
                except Exception as e:
                    print(f"Error reading transaction: {e}")
            
            self.stats['cache_misses'] += 1
            return None
    
    def add_utxo(self, utxo_id: bytes, utxo_data: Dict) -> bool:
        """
        Add UTXO to storage.
        
        Args:
            utxo_id: UTXO ID
            utxo_data: UTXO data dictionary
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.lock:
                # Save UTXO file
                utxo_file = self.db_path / "utxos" / utxo_id.hex()
                with open(utxo_file, 'w') as f:
                    json.dump(utxo_data, f)
                
                # Cache UTXO
                self.utxo_cache.add_utxo(utxo_id, utxo_data)
                
                self.stats['utxos_stored'] += 1
                self.stats['writes'] += 1
                
                return True
        except Exception as e:
            print(f"Error adding UTXO: {e}")
            return False
    
    def get_utxo(self, utxo_id: bytes) -> Optional[Dict]:
        """
        Get UTXO by ID.
        
        Args:
            utxo_id: UTXO ID
        
        Returns:
            UTXO data dictionary or None
        """
        with self.lock:
            self.stats['reads'] += 1
            
            # Try cache first
            cached = self.utxo_cache.get_utxo(utxo_id)
            if cached:
                self.stats['cache_hits'] += 1
                return cached
            
            # Load from disk
            utxo_file = self.db_path / "utxos" / utxo_id.hex()
            if utxo_file.exists():
                try:
                    with open(utxo_file, 'r') as f:
                        data = json.load(f)
                        self.utxo_cache.add_utxo(utxo_id, data)
                        return data
                except Exception as e:
                    print(f"Error reading UTXO: {e}")
            
            self.stats['cache_misses'] += 1
            return None
    
    def remove_utxo(self, utxo_id: bytes) -> bool:
        """
        Remove UTXO from storage (mark as spent).
        
        Args:
            utxo_id: UTXO ID
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.lock:
                utxo_file = self.db_path / "utxos" / utxo_id.hex()
                if utxo_file.exists():
                    utxo_file.unlink()
                
                self.utxo_cache.remove_utxo(utxo_id)
                self.stats['writes'] += 1
                
                return True
        except Exception as e:
            print(f"Error removing UTXO: {e}")
            return False
    
    def get_all_utxos(self) -> List[Tuple[bytes, Dict]]:
        """
        Get all UTXOs.
        
        Returns:
            List of (utxo_id, utxo_data) tuples
        """
        utxos = []
        utxo_dir = self.db_path / "utxos"
        
        if utxo_dir.exists():
            for utxo_file in utxo_dir.iterdir():
                if utxo_file.is_file():
                    try:
                        utxo_id = bytes.fromhex(utxo_file.stem)
                        with open(utxo_file, 'r') as f:
                            data = json.load(f)
                            utxos.append((utxo_id, data))
                    except Exception as e:
                        print(f"Error reading UTXO file: {e}")
        
        return utxos
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get storage statistics."""
        cache_stats = self.memory_cache.get_stats()
        
        return {
            'blocks_stored': self.stats['blocks_stored'],
            'transactions_stored': self.stats['transactions_stored'],
            'utxos_stored': self.stats['utxos_stored'],
            'total_reads': self.stats['reads'],
            'total_writes': self.stats['writes'],
            'cache_hits': cache_stats['hits'],
            'cache_misses': cache_stats['misses'],
            'cache_hit_rate': cache_stats['hit_rate'],
            'cache_utilization': cache_stats['utilization'],
            'latest_block_height': self.latest_block_height
        }
    
    def clear_cache(self) -> None:
        """Clear all caches."""
        self.memory_cache.clear()
        self.block_cache.clear()
        self.utxo_cache.clear()
    
    def close(self) -> None:
        """Close storage and save state."""
        self._save_state()
        self.clear_cache()


# Compatibility wrapper for existing code
class BlockchainStorageESP32(LiteDBStorage):
    """
    Drop-in replacement for BlockchainStorage that works on ESP32.
    
    Maintains same interface as original SQLite-based storage.
    """
    
    def __init__(self, db_path: str = "/mnt/microsd/blockchain.db"):
        """
        Initialize storage.
        
        Args:
            db_path: Path to blockchain directory (ignores .db extension)
        """
        # Remove .db extension if present
        if db_path.endswith('.db'):
            db_path = db_path[:-3]
        
        super().__init__(db_path)
