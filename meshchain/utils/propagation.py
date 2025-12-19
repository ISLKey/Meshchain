"""
MeshChain Propagation Module - Block Broadcasting & Transaction Propagation

This module handles:
1. Block broadcasting to network
2. Transaction propagation
3. Mempool management (pending transactions)
4. Duplicate detection
5. Rate limiting
6. Propagation statistics

Key Components:
1. Mempool: Manages pending transactions
2. BlockPropagator: Broadcasts blocks
3. TransactionPropagator: Propagates transactions
4. PropagationStats: Tracks propagation metrics
"""

import time
import threading
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from collections import OrderedDict
import hashlib


@dataclass
class MempoolTransaction:
    """Transaction in mempool."""
    tx_id: bytes                    # Transaction hash
    tx_data: bytes                  # Serialized transaction
    timestamp: float = field(default_factory=time.time)
    fee: int = 0                    # Transaction fee
    size: int = 0                   # Transaction size
    propagated_to: Set[bytes] = field(default_factory=set)  # Peers we sent to
    received_from: Optional[bytes] = None  # Peer we received from
    
    def is_stale(self, timeout: float = 3600.0) -> bool:
        """Check if transaction is too old."""
        return time.time() - self.timestamp > timeout
    
    def get_fee_rate(self) -> float:
        """Get fee per byte."""
        return self.fee / self.size if self.size > 0 else 0.0


@dataclass
class MempoolStats:
    """Mempool statistics."""
    transaction_count: int = 0
    total_size: int = 0
    total_fees: int = 0
    avg_fee_rate: float = 0.0
    max_size: int = 0
    evictions: int = 0
    
    def get_summary(self) -> Dict:
        """Get summary statistics."""
        return {
            'transaction_count': self.transaction_count,
            'total_size': self.total_size,
            'total_fees': self.total_fees,
            'avg_fee_rate': self.avg_fee_rate,
            'max_size': self.max_size,
            'evictions': self.evictions
        }


class Mempool:
    """
    Manages pending transactions (mempool).
    
    Stores transactions that haven't been included in a block yet.
    """
    
    def __init__(self, max_size: int = 1000, max_bytes: int = 1000000):
        """
        Initialize mempool.
        
        Args:
            max_size: Maximum transactions in mempool
            max_bytes: Maximum mempool size in bytes
        """
        self.max_size = max_size
        self.max_bytes = max_bytes
        self.transactions: Dict[bytes, MempoolTransaction] = OrderedDict()
        self.stats = MempoolStats()
        self.lock = threading.RLock()
    
    def add_transaction(self, tx_id: bytes, tx_data: bytes, 
                       fee: int = 0, received_from: Optional[bytes] = None) -> bool:
        """
        Add transaction to mempool.
        
        Args:
            tx_id: Transaction hash
            tx_data: Serialized transaction
            fee: Transaction fee
            received_from: Peer we received from
            
        Returns:
            True if added, False if rejected
        """
        with self.lock:
            # Check if already in mempool
            if tx_id in self.transactions:
                return False
            
            tx_size = len(tx_data)
            
            # Check size limits
            if len(self.transactions) >= self.max_size:
                # Evict lowest fee transaction
                self._evict_lowest_fee()
            
            if self.stats.total_size + tx_size > self.max_bytes:
                # Evict lowest fee transaction
                self._evict_lowest_fee()
            
            # Add transaction
            tx = MempoolTransaction(
                tx_id=tx_id,
                tx_data=tx_data,
                fee=fee,
                size=tx_size,
                received_from=received_from
            )
            
            self.transactions[tx_id] = tx
            self.stats.transaction_count += 1
            self.stats.total_size += tx_size
            self.stats.total_fees += fee
            self.stats.max_size = max(self.stats.max_size, self.stats.total_size)
            
            # Update average fee rate
            if self.stats.transaction_count > 0:
                self.stats.avg_fee_rate = self.stats.total_fees / self.stats.transaction_count
            
            return True
    
    def remove_transaction(self, tx_id: bytes) -> bool:
        """Remove transaction from mempool."""
        with self.lock:
            if tx_id in self.transactions:
                tx = self.transactions[tx_id]
                del self.transactions[tx_id]
                
                self.stats.transaction_count -= 1
                self.stats.total_size -= tx.size
                self.stats.total_fees -= tx.fee
                
                return True
            return False
    
    def get_transaction(self, tx_id: bytes) -> Optional[MempoolTransaction]:
        """Get transaction from mempool."""
        with self.lock:
            return self.transactions.get(tx_id)
    
    def get_all_transactions(self) -> List[MempoolTransaction]:
        """Get all transactions."""
        with self.lock:
            return list(self.transactions.values())
    
    def get_transactions_by_fee(self, count: int = 10) -> List[MempoolTransaction]:
        """Get top N transactions by fee rate."""
        with self.lock:
            txs = sorted(self.transactions.values(),
                        key=lambda t: t.get_fee_rate(),
                        reverse=True)
            return txs[:count]
    
    def _evict_lowest_fee(self):
        """Evict transaction with lowest fee."""
        if not self.transactions:
            return
        
        # Find lowest fee transaction
        lowest_tx = min(self.transactions.values(),
                       key=lambda t: t.get_fee_rate())
        
        self.remove_transaction(lowest_tx.tx_id)
        self.stats.evictions += 1
    
    def cleanup_stale(self, timeout: float = 3600.0) -> int:
        """
        Remove stale transactions.
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            Number of transactions removed
        """
        with self.lock:
            stale = [tx.tx_id for tx in self.transactions.values()
                    if tx.is_stale(timeout)]
            
            for tx_id in stale:
                self.remove_transaction(tx_id)
            
            return len(stale)
    
    def get_statistics(self) -> Dict:
        """Get mempool statistics."""
        with self.lock:
            return self.stats.get_summary()
    
    def is_empty(self) -> bool:
        """Check if mempool is empty."""
        with self.lock:
            return len(self.transactions) == 0
    
    def clear(self):
        """Clear all transactions."""
        with self.lock:
            self.transactions.clear()
            self.stats.transaction_count = 0
            self.stats.total_size = 0
            self.stats.total_fees = 0


@dataclass
class PropagationStats:
    """Transaction/block propagation statistics."""
    transactions_propagated: int = 0
    blocks_propagated: int = 0
    bytes_propagated: int = 0
    avg_propagation_time: float = 0.0
    peers_reached: int = 0
    duplicate_blocks: int = 0
    duplicate_transactions: int = 0
    
    def get_summary(self) -> Dict:
        """Get summary statistics."""
        return {
            'transactions_propagated': self.transactions_propagated,
            'blocks_propagated': self.blocks_propagated,
            'bytes_propagated': self.bytes_propagated,
            'avg_propagation_time': self.avg_propagation_time,
            'peers_reached': self.peers_reached,
            'duplicate_blocks': self.duplicate_blocks,
            'duplicate_transactions': self.duplicate_transactions
        }


class BlockPropagator:
    """
    Handles block broadcasting to network.
    
    Ensures blocks are efficiently distributed to all peers.
    """
    
    def __init__(self, network_manager=None):
        """
        Initialize block propagator.
        
        Args:
            network_manager: NetworkManager instance
        """
        self.network = network_manager
        self.seen_blocks: Set[bytes] = set()  # Block hashes we've seen
        self.stats = PropagationStats()
        self.lock = threading.RLock()
    
    def broadcast_block(self, block_hash: bytes, block_data: bytes,
                       peer_manager=None) -> int:
        """
        Broadcast block to network.
        
        Args:
            block_hash: Block hash
            block_data: Serialized block
            peer_manager: PeerManager instance
            
        Returns:
            Number of peers reached
        """
        with self.lock:
            # Check if we've seen this block
            if block_hash in self.seen_blocks:
                self.stats.duplicate_blocks += 1
                return 0
            
            self.seen_blocks.add(block_hash)
            
            # Broadcast via network
            if self.network:
                success = self.network.broadcast_block(block_data)
                
                if success:
                    self.stats.blocks_propagated += 1
                    self.stats.bytes_propagated += len(block_data)
                    
                    # Count peers reached
                    peers_reached = 0
                    if peer_manager:
                        peers_reached = len(peer_manager.get_active_peers())
                    
                    self.stats.peers_reached = peers_reached
                    
                    return peers_reached
            
            return 0
    
    def mark_block_seen(self, block_hash: bytes):
        """Mark block as seen (to prevent re-broadcasting)."""
        with self.lock:
            self.seen_blocks.add(block_hash)
    
    def is_block_seen(self, block_hash: bytes) -> bool:
        """Check if we've seen this block."""
        with self.lock:
            return block_hash in self.seen_blocks
    
    def get_statistics(self) -> Dict:
        """Get propagation statistics."""
        with self.lock:
            return self.stats.get_summary()


class TransactionPropagator:
    """
    Handles transaction propagation to network.
    
    Manages mempool and broadcasts transactions to peers.
    """
    
    def __init__(self, network_manager=None, mempool: Optional[Mempool] = None):
        """
        Initialize transaction propagator.
        
        Args:
            network_manager: NetworkManager instance
            mempool: Mempool instance
        """
        self.network = network_manager
        self.mempool = mempool or Mempool()
        self.stats = PropagationStats()
        self.lock = threading.RLock()
        
        # Cleanup thread
        self.cleanup_thread = None
        self.running = False
    
    def start(self):
        """Start transaction propagator (cleanup thread)."""
        if not self.running:
            self.running = True
            self.cleanup_thread = threading.Thread(
                target=self._cleanup_loop,
                daemon=True
            )
            self.cleanup_thread.start()
    
    def stop(self):
        """Stop transaction propagator."""
        self.running = False
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5.0)
    
    def _cleanup_loop(self):
        """Periodically clean up stale transactions."""
        while self.running:
            try:
                self.mempool.cleanup_stale(timeout=3600.0)
                time.sleep(300)  # Cleanup every 5 minutes
            except Exception as e:
                print(f"Cleanup error: {str(e)}")
    
    def propagate_transaction(self, tx_id: bytes, tx_data: bytes,
                             fee: int = 0, received_from: Optional[bytes] = None,
                             peer_manager=None) -> int:
        """
        Propagate transaction to network.
        
        Args:
            tx_id: Transaction hash
            tx_data: Serialized transaction
            fee: Transaction fee
            received_from: Peer we received from
            peer_manager: PeerManager instance
            
        Returns:
            Number of peers reached
        """
        with self.lock:
            # Add to mempool
            added = self.mempool.add_transaction(
                tx_id, tx_data, fee, received_from
            )
            
            if not added:
                self.stats.duplicate_transactions += 1
                return 0
            
            # Broadcast via network
            if self.network:
                success = self.network.broadcast_transaction(tx_data)
                
                if success:
                    self.stats.transactions_propagated += 1
                    self.stats.bytes_propagated += len(tx_data)
                    
                    # Count peers reached
                    peers_reached = 0
                    if peer_manager:
                        peers_reached = len(peer_manager.get_active_peers())
                    
                    self.stats.peers_reached = peers_reached
                    
                    return peers_reached
            
            return 0
    
    def get_mempool(self) -> Mempool:
        """Get mempool instance."""
        return self.mempool
    
    def get_pending_transactions(self) -> List[MempoolTransaction]:
        """Get all pending transactions."""
        return self.mempool.get_all_transactions()
    
    def remove_transaction(self, tx_id: bytes) -> bool:
        """Remove transaction from mempool (e.g., when included in block)."""
        return self.mempool.remove_transaction(tx_id)
    
    def remove_transactions(self, tx_ids: List[bytes]) -> int:
        """Remove multiple transactions."""
        count = 0
        for tx_id in tx_ids:
            if self.remove_transaction(tx_id):
                count += 1
        return count
    
    def get_statistics(self) -> Dict:
        """Get propagation statistics."""
        with self.lock:
            stats = self.stats.get_summary()
            stats['mempool'] = self.mempool.get_statistics()
            return stats


class PropagationManager:
    """
    High-level manager for block and transaction propagation.
    
    Combines block and transaction propagation with mempool management.
    """
    
    def __init__(self, network_manager=None):
        """
        Initialize propagation manager.
        
        Args:
            network_manager: NetworkManager instance
        """
        self.network = network_manager
        self.block_propagator = BlockPropagator(network_manager)
        self.tx_propagator = TransactionPropagator(network_manager)
    
    def start(self):
        """Start propagation manager."""
        self.tx_propagator.start()
    
    def stop(self):
        """Stop propagation manager."""
        self.tx_propagator.stop()
    
    def broadcast_block(self, block_hash: bytes, block_data: bytes,
                       peer_manager=None) -> int:
        """Broadcast block."""
        return self.block_propagator.broadcast_block(
            block_hash, block_data, peer_manager
        )
    
    def propagate_transaction(self, tx_id: bytes, tx_data: bytes,
                             fee: int = 0, received_from: Optional[bytes] = None,
                             peer_manager=None) -> int:
        """Propagate transaction."""
        return self.tx_propagator.propagate_transaction(
            tx_id, tx_data, fee, received_from, peer_manager
        )
    
    def get_mempool(self) -> Mempool:
        """Get mempool."""
        return self.tx_propagator.get_mempool()
    
    def get_pending_transactions(self) -> List[MempoolTransaction]:
        """Get pending transactions."""
        return self.tx_propagator.get_pending_transactions()
    
    def remove_transaction(self, tx_id: bytes) -> bool:
        """Remove transaction from mempool."""
        return self.tx_propagator.remove_transaction(tx_id)
    
    def remove_transactions_from_block(self, tx_ids: List[bytes]) -> int:
        """Remove transactions that were included in block."""
        return self.tx_propagator.remove_transactions(tx_ids)
    
    def get_statistics(self) -> Dict:
        """Get all propagation statistics."""
        return {
            'blocks': self.block_propagator.get_statistics(),
            'transactions': self.tx_propagator.get_statistics()
        }
