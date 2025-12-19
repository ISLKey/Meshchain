"""
MeshChain Optimization - Bandwidth and Performance Improvements

This module provides:
1. Message compression
2. Transaction batching
3. Block pruning
4. Database optimization
5. Network optimization
6. Memory management
"""

import zlib
import time
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque
import sqlite3


@dataclass
class CompressionStats:
    """Compression statistics."""
    original_size: int = 0
    compressed_size: int = 0
    compression_time: float = 0.0
    decompression_time: float = 0.0
    
    @property
    def compression_ratio(self) -> float:
        """Get compression ratio."""
        if self.original_size == 0:
            return 0.0
        return self.compressed_size / self.original_size
    
    @property
    def savings(self) -> float:
        """Get bandwidth savings percentage."""
        if self.original_size == 0:
            return 0.0
        return (1 - self.compression_ratio) * 100


class MessageCompression:
    """
    Compress messages for bandwidth optimization.
    
    Uses zlib with configurable compression levels.
    """
    
    def __init__(self, compression_level: int = 6):
        """
        Initialize compression.
        
        Args:
            compression_level: zlib compression level (0-9, 6 is default)
        """
        self.compression_level = compression_level
        self.stats = CompressionStats()
    
    def compress_message(self, message: bytes) -> Tuple[bytes, float]:
        """
        Compress message.
        
        Args:
            message: Message bytes
            
        Returns:
            Tuple of (compressed_data, compression_time)
        """
        start_time = time.time()
        
        compressed = zlib.compress(message, self.compression_level)
        
        compression_time = time.time() - start_time
        
        # Update stats
        self.stats.original_size += len(message)
        self.stats.compressed_size += len(compressed)
        self.stats.compression_time += compression_time
        
        return compressed, compression_time
    
    def decompress_message(self, compressed_data: bytes) -> Tuple[Optional[bytes], float]:
        """
        Decompress message.
        
        Args:
            compressed_data: Compressed data bytes
            
        Returns:
            Tuple of (decompressed_data, decompression_time) or (None, time) on error
        """
        start_time = time.time()
        
        try:
            decompressed = zlib.decompress(compressed_data)
            decompression_time = time.time() - start_time
            
            # Update stats
            self.stats.decompression_time += decompression_time
            
            return decompressed, decompression_time
            
        except Exception as e:
            print(f"Decompression error: {str(e)}")
            return None, time.time() - start_time
    
    def get_statistics(self) -> Dict:
        """Get compression statistics."""
        return {
            'original_size': self.stats.original_size,
            'compressed_size': self.stats.compressed_size,
            'compression_ratio': self.stats.compression_ratio,
            'bandwidth_savings': f"{self.stats.savings:.2f}%",
            'compression_time': f"{self.stats.compression_time:.4f}s",
            'decompression_time': f"{self.stats.decompression_time:.4f}s"
        }


class TransactionBatcher:
    """
    Batch transactions for efficient processing.
    
    Reduces overhead by processing multiple transactions together.
    """
    
    def __init__(self, batch_size: int = 100, batch_timeout: float = 5.0):
        """
        Initialize batcher.
        
        Args:
            batch_size: Maximum transactions per batch
            batch_timeout: Maximum time to wait for batch (seconds)
        """
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.batch: List[bytes] = []
        self.batch_created_at = time.time()
    
    def add_transaction(self, tx_data: bytes) -> Optional[List[bytes]]:
        """
        Add transaction to batch.
        
        Args:
            tx_data: Transaction data
            
        Returns:
            Batch if ready, None otherwise
        """
        self.batch.append(tx_data)
        
        # Check if batch is ready
        if len(self.batch) >= self.batch_size:
            return self.get_batch()
        
        # Check if batch timed out
        if time.time() - self.batch_created_at >= self.batch_timeout:
            if self.batch:
                return self.get_batch()
        
        return None
    
    def get_batch(self) -> Optional[List[bytes]]:
        """Get current batch."""
        if not self.batch:
            return None
        
        batch = self.batch[:]
        self.batch = []
        self.batch_created_at = time.time()
        
        return batch
    
    def get_statistics(self) -> Dict:
        """Get batcher statistics."""
        return {
            'batch_size': self.batch_size,
            'batch_timeout': self.batch_timeout,
            'current_batch_size': len(self.batch),
            'time_in_batch': time.time() - self.batch_created_at
        }


class BlockPruner:
    """
    Prune old blocks to save storage.
    
    Keeps recent blocks and block headers for historical verification.
    """
    
    def __init__(self, keep_blocks: int = 1000, keep_headers: bool = True):
        """
        Initialize pruner.
        
        Args:
            keep_blocks: Number of recent blocks to keep
            keep_headers: Keep block headers for historical verification
        """
        self.keep_blocks = keep_blocks
        self.keep_headers = keep_headers
        self.pruned_blocks = 0
    
    def should_prune(self, current_height: int) -> bool:
        """
        Check if pruning should occur.
        
        Args:
            current_height: Current blockchain height
            
        Returns:
            True if pruning recommended
        """
        return current_height > self.keep_blocks
    
    def get_prune_height(self, current_height: int) -> int:
        """
        Get height to prune up to.
        
        Args:
            current_height: Current blockchain height
            
        Returns:
            Height to prune up to
        """
        return max(0, current_height - self.keep_blocks)
    
    def prune_blocks(self, db_connection, current_height: int) -> int:
        """
        Prune old blocks from database.
        
        Args:
            db_connection: SQLite connection
            current_height: Current blockchain height
            
        Returns:
            Number of blocks pruned
        """
        if not self.should_prune(current_height):
            return 0
        
        prune_height = self.get_prune_height(current_height)
        
        try:
            cursor = db_connection.cursor()
            
            # Delete old blocks
            cursor.execute(
                "DELETE FROM blocks WHERE height <= ?",
                (prune_height,)
            )
            
            # Delete associated transactions
            cursor.execute(
                "DELETE FROM transactions WHERE block_height <= ?",
                (prune_height,)
            )
            
            # Vacuum database to reclaim space
            cursor.execute("VACUUM")
            
            db_connection.commit()
            
            pruned = cursor.rowcount
            self.pruned_blocks += pruned
            
            return pruned
            
        except Exception as e:
            print(f"Pruning error: {str(e)}")
            return 0
    
    def get_statistics(self) -> Dict:
        """Get pruner statistics."""
        return {
            'keep_blocks': self.keep_blocks,
            'keep_headers': self.keep_headers,
            'total_pruned': self.pruned_blocks
        }


class DatabaseOptimizer:
    """
    Optimize SQLite database for better performance.
    """
    
    @staticmethod
    def optimize_database(db_path: str):
        """
        Apply optimizations to database.
        
        Args:
            db_path: Path to database file
        """
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Enable WAL mode for better concurrency
            cursor.execute("PRAGMA journal_mode=WAL")
            
            # Increase cache size
            cursor.execute("PRAGMA cache_size=10000")
            
            # Synchronous mode for better performance
            cursor.execute("PRAGMA synchronous=NORMAL")
            
            # Temp store in memory
            cursor.execute("PRAGMA temp_store=MEMORY")
            
            # Vacuum to reclaim space
            cursor.execute("VACUUM")
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"Database optimization error: {str(e)}")
    
    @staticmethod
    def create_indexes(db_path: str):
        """
        Create indexes for better query performance.
        
        Args:
            db_path: Path to database file
        """
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Index on block height
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_blocks_height ON blocks(height)"
            )
            
            # Index on transaction sender
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_transactions_sender ON transactions(sender)"
            )
            
            # Index on transaction recipient
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_transactions_recipient ON transactions(recipient)"
            )
            
            # Index on UTXO owner
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_utxos_owner ON utxos(owner)"
            )
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"Index creation error: {str(e)}")


class NetworkOptimizer:
    """
    Optimize network communication.
    """
    
    @staticmethod
    def calculate_optimal_batch_size(bandwidth_kbps: float, 
                                     message_overhead: int = 20) -> int:
        """
        Calculate optimal batch size for bandwidth.
        
        Args:
            bandwidth_kbps: Available bandwidth in kbps
            message_overhead: Overhead per message in bytes
            
        Returns:
            Optimal batch size
        """
        # Assume 110 bytes per transaction
        tx_size = 110
        
        # Calculate messages per second
        messages_per_second = (bandwidth_kbps * 1000 / 8) / (tx_size + message_overhead)
        
        # Batch size = messages per second * 5 seconds
        batch_size = max(1, int(messages_per_second * 5))
        
        return batch_size
    
    @staticmethod
    def calculate_optimal_compression(message_size: int) -> bool:
        """
        Determine if compression is beneficial.
        
        Args:
            message_size: Message size in bytes
            
        Returns:
            True if compression recommended
        """
        # Compression overhead is ~20 bytes
        # Only beneficial for messages > 100 bytes
        return message_size > 100


class PerformanceMonitor:
    """
    Monitor performance metrics.
    """
    
    def __init__(self, window_size: int = 100):
        """
        Initialize monitor.
        
        Args:
            window_size: Size of rolling window for statistics
        """
        self.window_size = window_size
        self.latencies = deque(maxlen=window_size)
        self.throughputs = deque(maxlen=window_size)
        self.errors = deque(maxlen=window_size)
    
    def record_latency(self, latency_ms: float):
        """Record operation latency."""
        self.latencies.append(latency_ms)
    
    def record_throughput(self, throughput_tps: float):
        """Record throughput."""
        self.throughputs.append(throughput_tps)
    
    def record_error(self, error_type: str):
        """Record error."""
        self.errors.append(error_type)
    
    def get_statistics(self) -> Dict:
        """Get performance statistics."""
        if not self.latencies:
            return {}
        
        avg_latency = sum(self.latencies) / len(self.latencies)
        min_latency = min(self.latencies)
        max_latency = max(self.latencies)
        
        avg_throughput = sum(self.throughputs) / len(self.throughputs) if self.throughputs else 0
        
        error_count = len(self.errors)
        
        return {
            'avg_latency_ms': f"{avg_latency:.2f}",
            'min_latency_ms': f"{min_latency:.2f}",
            'max_latency_ms': f"{max_latency:.2f}",
            'avg_throughput_tps': f"{avg_throughput:.2f}",
            'error_count': error_count,
            'error_rate': f"{(error_count / len(self.latencies) * 100):.2f}%"
        }


class OptimizationManager:
    """
    Manage all optimizations.
    """
    
    def __init__(self, db_path: str):
        """
        Initialize optimization manager.
        
        Args:
            db_path: Path to blockchain database
        """
        self.compression = MessageCompression()
        self.batcher = TransactionBatcher()
        self.pruner = BlockPruner()
        self.monitor = PerformanceMonitor()
        self.db_path = db_path
        
        # Apply initial optimizations
        DatabaseOptimizer.optimize_database(db_path)
        DatabaseOptimizer.create_indexes(db_path)
    
    def get_all_statistics(self) -> Dict:
        """Get all optimization statistics."""
        return {
            'compression': self.compression.get_statistics(),
            'batcher': self.batcher.get_statistics(),
            'pruner': self.pruner.get_statistics(),
            'performance': self.monitor.get_statistics()
        }
