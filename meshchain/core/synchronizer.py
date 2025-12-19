"""
MeshChain Synchronizer - Blockchain Synchronization

This module handles:
1. Blockchain state synchronization between nodes
2. Block fetching and validation
3. Chain reorganization (reorg) handling
4. Sync progress tracking
5. Conflict resolution

Key Components:
1. SyncState: Tracks synchronization state
2. BlockFetcher: Fetches blocks from peers
3. ChainSynchronizer: Synchronizes blockchain state
4. SyncManager: High-level sync interface
"""

import time
import threading
from typing import Dict, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
from enum import IntEnum


class SyncState(IntEnum):
    """Blockchain synchronization state."""
    IDLE = 0              # Not syncing
    SYNCING = 1           # Currently syncing
    SYNCED = 2            # Fully synced
    BEHIND = 3            # Behind network
    ERROR = 4             # Sync error


@dataclass
class SyncProgress:
    """Tracks synchronization progress."""
    state: SyncState = SyncState.IDLE
    current_height: int = 0         # Our current block height
    target_height: int = 0          # Target block height
    blocks_synced: int = 0          # Blocks synced this session
    blocks_remaining: int = 0       # Blocks remaining
    start_time: float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)
    error_message: str = ""
    
    def get_progress_percent(self) -> float:
        """Get sync progress as percentage (0-100)."""
        if self.target_height <= 0:
            return 0.0
        
        return min(100.0, (self.current_height / self.target_height) * 100.0)
    
    def get_elapsed_time(self) -> float:
        """Get elapsed time in seconds."""
        return time.time() - self.start_time
    
    def get_estimated_time_remaining(self) -> float:
        """Estimate time remaining in seconds."""
        elapsed = self.get_elapsed_time()
        
        if self.blocks_synced == 0 or elapsed == 0:
            return 0.0
        
        blocks_per_second = self.blocks_synced / elapsed
        if blocks_per_second == 0:
            return 0.0
        
        return self.blocks_remaining / blocks_per_second
    
    def is_synced(self) -> bool:
        """Check if we're fully synced."""
        return self.state == SyncState.SYNCED and self.current_height >= self.target_height


@dataclass
class SyncStats:
    """Synchronization statistics."""
    total_syncs: int = 0
    successful_syncs: int = 0
    failed_syncs: int = 0
    total_blocks_synced: int = 0
    total_time_syncing: float = 0.0
    avg_sync_time: float = 0.0
    reorgs: int = 0
    last_sync_time: float = 0.0
    
    def get_success_rate(self) -> float:
        """Get sync success rate (0.0 to 1.0)."""
        if self.total_syncs == 0:
            return 0.0
        return self.successful_syncs / self.total_syncs
    
    def get_summary(self) -> Dict:
        """Get summary statistics."""
        return {
            'total_syncs': self.total_syncs,
            'successful_syncs': self.successful_syncs,
            'failed_syncs': self.failed_syncs,
            'success_rate': self.get_success_rate(),
            'total_blocks_synced': self.total_blocks_synced,
            'avg_sync_time': self.avg_sync_time,
            'reorgs': self.reorgs,
            'last_sync_time': self.last_sync_time
        }


class BlockFetcher:
    """
    Fetches blocks from peers.
    
    Handles:
    1. Requesting blocks from peers
    2. Validating received blocks
    3. Retry logic for failed requests
    """
    
    def __init__(self, network_manager=None, validator=None):
        """
        Initialize block fetcher.
        
        Args:
            network_manager: NetworkManager instance
            validator: BlockValidator instance
        """
        self.network = network_manager
        self.validator = validator
        self.pending_requests: Dict[int, Tuple[bytes, float]] = {}  # height -> (peer_id, time)
        self.lock = threading.RLock()
    
    def request_block(self, height: int, peer_id: bytes) -> bool:
        """
        Request a block from a peer.
        
        Args:
            height: Block height to request
            peer_id: Peer to request from
            
        Returns:
            True if request sent, False otherwise
        """
        with self.lock:
            # Check if already requested
            if height in self.pending_requests:
                return False
            
            # Record request
            self.pending_requests[height] = (peer_id, time.time())
            
            # In real implementation, would send request via network
            # For now, just record it
            return True
    
    def request_blocks(self, from_height: int, to_height: int, 
                      peer_id: bytes) -> int:
        """
        Request multiple blocks from a peer.
        
        Args:
            from_height: Starting height
            to_height: Ending height
            peer_id: Peer to request from
            
        Returns:
            Number of requests sent
        """
        count = 0
        for height in range(from_height, to_height + 1):
            if self.request_block(height, peer_id):
                count += 1
        
        return count
    
    def mark_block_received(self, height: int) -> bool:
        """Mark block as received."""
        with self.lock:
            if height in self.pending_requests:
                del self.pending_requests[height]
                return True
            return False
    
    def get_pending_requests(self) -> Dict[int, Tuple[bytes, float]]:
        """Get pending block requests."""
        with self.lock:
            return dict(self.pending_requests)
    
    def cleanup_stale_requests(self, timeout: float = 30.0) -> int:
        """
        Remove stale block requests.
        
        Args:
            timeout: Request timeout in seconds
            
        Returns:
            Number of requests removed
        """
        with self.lock:
            current_time = time.time()
            stale = [h for h, (_, req_time) in self.pending_requests.items()
                    if current_time - req_time > timeout]
            
            for height in stale:
                del self.pending_requests[height]
            
            return len(stale)


class ChainSynchronizer:
    """
    Synchronizes blockchain state with network.
    
    Handles:
    1. Determining sync target
    2. Fetching blocks from peers
    3. Validating blocks
    4. Updating local chain
    5. Handling chain reorgs
    """
    
    def __init__(self, network_manager=None, peer_manager=None, 
                 validator=None, storage=None):
        """
        Initialize chain synchronizer.
        
        Args:
            network_manager: NetworkManager instance
            peer_manager: PeerManager instance
            validator: BlockValidator instance
            storage: Storage instance
        """
        self.network = network_manager
        self.peers = peer_manager
        self.validator = validator
        self.storage = storage
        
        self.progress = SyncProgress()
        self.stats = SyncStats()
        self.fetcher = BlockFetcher(network_manager, validator)
        
        self.lock = threading.RLock()
        
        # Callbacks
        self.on_block_received: Optional[Callable] = None
        self.on_sync_complete: Optional[Callable] = None
        self.on_sync_error: Optional[Callable] = None
    
    def start_sync(self, current_height: int, peer_manager=None) -> bool:
        """
        Start blockchain synchronization.
        
        Args:
            current_height: Our current block height
            peer_manager: PeerManager instance
            
        Returns:
            True if sync started, False otherwise
        """
        with self.lock:
            if self.progress.state == SyncState.SYNCING:
                return False  # Already syncing
            
            # Determine target height from peers
            if peer_manager:
                validators = peer_manager.get_validators()
                if validators:
                    target_height = max(p.block_height for p in validators)
                else:
                    target_height = current_height
            else:
                target_height = current_height
            
            # Initialize progress
            self.progress.state = SyncState.SYNCING
            self.progress.current_height = current_height
            self.progress.target_height = target_height
            self.progress.blocks_remaining = max(0, target_height - current_height)
            self.progress.blocks_synced = 0
            self.progress.start_time = time.time()
            self.progress.error_message = ""
            
            self.stats.total_syncs += 1
            
            return True
    
    def add_block(self, block_height: int, block_data: bytes) -> bool:
        """
        Add block to chain during sync.
        
        Args:
            block_height: Block height
            block_data: Serialized block
            
        Returns:
            True if block added, False if rejected
        """
        with self.lock:
            if self.progress.state != SyncState.SYNCING:
                return False
            
            # In real implementation, would validate and store block
            # For now, just update progress
            
            self.progress.current_height = block_height
            self.progress.blocks_synced += 1
            self.progress.blocks_remaining = max(
                0, 
                self.progress.target_height - block_height
            )
            self.progress.last_update = time.time()
            
            # Call callback
            if self.on_block_received:
                self.on_block_received(block_height, block_data)
            
            return True
    
    def complete_sync(self, success: bool = True):
        """
        Mark synchronization as complete.
        
        Args:
            success: Whether sync was successful
        """
        with self.lock:
            if success:
                self.progress.state = SyncState.SYNCED
                self.stats.successful_syncs += 1
            else:
                self.progress.state = SyncState.ERROR
                self.stats.failed_syncs += 1
            
            # Update statistics
            sync_time = time.time() - self.progress.start_time
            self.stats.total_time_syncing += sync_time
            self.stats.total_blocks_synced += self.progress.blocks_synced
            self.stats.last_sync_time = sync_time
            
            if self.stats.successful_syncs > 0:
                self.stats.avg_sync_time = (
                    self.stats.total_time_syncing / self.stats.successful_syncs
                )
            
            # Call callback
            if success and self.on_sync_complete:
                self.on_sync_complete()
            elif not success and self.on_sync_error:
                self.on_sync_error(self.progress.error_message)
    
    def set_error(self, error_message: str):
        """Set sync error."""
        with self.lock:
            self.progress.error_message = error_message
            self.progress.state = SyncState.ERROR
    
    def handle_chain_reorg(self, reorg_depth: int) -> bool:
        """
        Handle chain reorganization (reorg).
        
        Args:
            reorg_depth: Number of blocks to reorg
            
        Returns:
            True if reorg handled, False otherwise
        """
        with self.lock:
            # In real implementation, would:
            # 1. Remove blocks from chain
            # 2. Fetch replacement blocks
            # 3. Update state
            
            self.stats.reorgs += 1
            return True
    
    def get_progress(self) -> SyncProgress:
        """Get synchronization progress."""
        with self.lock:
            return SyncProgress(
                state=self.progress.state,
                current_height=self.progress.current_height,
                target_height=self.progress.target_height,
                blocks_synced=self.progress.blocks_synced,
                blocks_remaining=self.progress.blocks_remaining,
                start_time=self.progress.start_time,
                last_update=self.progress.last_update,
                error_message=self.progress.error_message
            )
    
    def get_statistics(self) -> Dict:
        """Get synchronization statistics."""
        with self.lock:
            return self.stats.get_summary()
    
    def is_synced(self) -> bool:
        """Check if blockchain is synced."""
        with self.lock:
            return self.progress.is_synced()
    
    def is_syncing(self) -> bool:
        """Check if currently syncing."""
        with self.lock:
            return self.progress.state == SyncState.SYNCING


class SyncManager:
    """
    High-level synchronization manager.
    
    Provides simplified interface for blockchain synchronization.
    """
    
    def __init__(self, network_manager=None, peer_manager=None,
                 validator=None, storage=None):
        """
        Initialize sync manager.
        
        Args:
            network_manager: NetworkManager instance
            peer_manager: PeerManager instance
            validator: BlockValidator instance
            storage: Storage instance
        """
        self.synchronizer = ChainSynchronizer(
            network_manager, peer_manager, validator, storage
        )
        self.sync_thread = None
        self.running = False
    
    def start(self):
        """Start sync manager."""
        if not self.running:
            self.running = True
            self.sync_thread = threading.Thread(
                target=self._sync_loop,
                daemon=True
            )
            self.sync_thread.start()
    
    def stop(self):
        """Stop sync manager."""
        self.running = False
        if self.sync_thread:
            self.sync_thread.join(timeout=5.0)
    
    def _sync_loop(self):
        """Main synchronization loop."""
        while self.running:
            try:
                # Check if we need to sync
                if not self.synchronizer.is_syncing():
                    # Check if we're behind
                    progress = self.synchronizer.get_progress()
                    if progress.current_height < progress.target_height:
                        # Start sync
                        self.synchronizer.start_sync(progress.current_height)
                
                time.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                print(f"Sync loop error: {str(e)}")
                time.sleep(10)
    
    def sync_blockchain(self, current_height: int, peer_manager=None) -> bool:
        """Start blockchain synchronization."""
        return self.synchronizer.start_sync(current_height, peer_manager)
    
    def add_synced_block(self, height: int, block_data: bytes) -> bool:
        """Add block received during sync."""
        return self.synchronizer.add_block(height, block_data)
    
    def complete_sync(self, success: bool = True):
        """Mark sync as complete."""
        self.synchronizer.complete_sync(success)
    
    def handle_reorg(self, depth: int) -> bool:
        """Handle chain reorganization."""
        return self.synchronizer.handle_chain_reorg(depth)
    
    def get_progress(self) -> SyncProgress:
        """Get sync progress."""
        return self.synchronizer.get_progress()
    
    def get_statistics(self) -> Dict:
        """Get sync statistics."""
        return self.synchronizer.get_statistics()
    
    def is_synced(self) -> bool:
        """Check if synced."""
        return self.synchronizer.is_synced()
    
    def is_syncing(self) -> bool:
        """Check if syncing."""
        return self.synchronizer.is_syncing()
    
    def register_on_block_received(self, callback: Callable):
        """Register callback for block received."""
        self.synchronizer.on_block_received = callback
    
    def register_on_sync_complete(self, callback: Callable):
        """Register callback for sync complete."""
        self.synchronizer.on_sync_complete = callback
    
    def register_on_sync_error(self, callback: Callable):
        """Register callback for sync error."""
        self.synchronizer.on_sync_error = callback
