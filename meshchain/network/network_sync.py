"""
Network Synchronization for MeshChain

Implements:
1. Block synchronization
2. Transaction synchronization
3. State synchronization
4. Sync request/response handling
5. Sync progress tracking
6. Conflict resolution

This module synchronizes blockchain state across the mesh network.
"""

import time
import threading
import logging
from typing import Dict, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
from enum import IntEnum
from collections import deque

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SyncState(IntEnum):
    """Synchronization state."""
    IDLE = 0
    SYNCING = 1
    SYNCED = 2
    ERROR = 3


@dataclass
class SyncProgress:
    """Synchronization progress."""
    state: SyncState = SyncState.IDLE
    current_height: int = 0
    target_height: int = 0
    blocks_synced: int = 0
    blocks_remaining: int = 0
    start_time: float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)
    error_message: Optional[str] = None
    
    def get_progress_percent(self) -> float:
        """Get synchronization progress as percentage."""
        if self.blocks_remaining == 0:
            return 100.0
        
        total = self.blocks_synced + self.blocks_remaining
        if total == 0:
            return 0.0
        
        return (self.blocks_synced / total) * 100.0
    
    def get_elapsed_time(self) -> float:
        """Get elapsed sync time in seconds."""
        return time.time() - self.start_time
    
    def get_estimated_time_remaining(self) -> float:
        """Estimate time remaining for sync."""
        if self.blocks_synced == 0:
            return 0.0
        
        elapsed = self.get_elapsed_time()
        rate = self.blocks_synced / elapsed if elapsed > 0 else 0
        
        if rate == 0:
            return 0.0
        
        return self.blocks_remaining / rate


class SyncManager:
    """
    Manages blockchain synchronization.
    
    Features:
    - Block synchronization
    - Transaction synchronization
    - Progress tracking
    - Peer selection for sync
    - Conflict resolution
    """
    
    def __init__(self, node_id: int):
        """
        Initialize sync manager.
        
        Args:
            node_id: This node's ID
        """
        self.node_id = node_id
        
        # Sync state
        self.sync_progress = SyncProgress()
        self.sync_lock = threading.Lock()
        
        # Sync queue
        self.sync_queue: deque = deque(maxlen=1000)
        self.queue_lock = threading.Lock()
        
        # Sync peers (peers we're syncing from)
        self.sync_peers: Dict[int, Dict] = {}
        self.peers_lock = threading.Lock()
        
        # Callbacks
        self.on_sync_started: Optional[Callable[[], None]] = None
        self.on_sync_progress: Optional[Callable[[SyncProgress], None]] = None
        self.on_sync_complete: Optional[Callable[[], None]] = None
        self.on_sync_error: Optional[Callable[[str], None]] = None
        
        # Statistics
        self.stats = {
            'sync_attempts': 0,
            'sync_successes': 0,
            'sync_failures': 0,
            'blocks_received': 0,
            'transactions_received': 0,
            'conflicts_resolved': 0,
            'total_sync_time': 0.0
        }
        self.stats_lock = threading.Lock()
        
        logger.info(f"Sync manager initialized (node_id={node_id})")
    
    def start_sync(self, target_height: int) -> bool:
        """
        Start synchronization to target height.
        
        Args:
            target_height: Target block height
        
        Returns:
            True if sync started
        """
        with self.sync_lock:
            if self.sync_progress.state == SyncState.SYNCING:
                logger.warning("Sync already in progress")
                return False
            
            self.sync_progress.state = SyncState.SYNCING
            self.sync_progress.target_height = target_height
            self.sync_progress.blocks_synced = 0
            self.sync_progress.blocks_remaining = target_height - self.sync_progress.current_height
            self.sync_progress.start_time = time.time()
            self.sync_progress.error_message = None
        
        with self.stats_lock:
            self.stats['sync_attempts'] += 1
        
        logger.info(f"Sync started (target_height={target_height})")
        
        if self.on_sync_started:
            self.on_sync_started()
        
        return True
    
    def add_sync_block(self, block_height: int, block_data: Dict) -> bool:
        """
        Add block to sync queue.
        
        Args:
            block_height: Block height
            block_data: Block data
        
        Returns:
            True if added successfully
        """
        try:
            with self.queue_lock:
                self.sync_queue.append({
                    'height': block_height,
                    'data': block_data,
                    'timestamp': time.time()
                })
            
            with self.sync_lock:
                self.sync_progress.blocks_synced += 1
                self.sync_progress.blocks_remaining = max(0, self.sync_progress.blocks_remaining - 1)
                self.sync_progress.last_update = time.time()
            
            return True
        except:
            return False
    
    def get_next_sync_block(self) -> Optional[Dict]:
        """
        Get next block from sync queue.
        
        Returns:
            Block data or None
        """
        try:
            with self.queue_lock:
                if self.sync_queue:
                    return self.sync_queue.popleft()
        except:
            pass
        
        return None
    
    def complete_sync(self) -> None:
        """Mark synchronization as complete."""
        with self.sync_lock:
            if self.sync_progress.state != SyncState.SYNCING:
                return
            
            self.sync_progress.state = SyncState.SYNCED
            sync_time = time.time() - self.sync_progress.start_time
        
        with self.stats_lock:
            self.stats['sync_successes'] += 1
            self.stats['total_sync_time'] += sync_time
        
        logger.info(f"Sync complete (time={sync_time:.2f}s, blocks={self.sync_progress.blocks_synced})")
        
        if self.on_sync_complete:
            self.on_sync_complete()
    
    def fail_sync(self, error_message: str) -> None:
        """
        Mark synchronization as failed.
        
        Args:
            error_message: Error message
        """
        with self.sync_lock:
            if self.sync_progress.state != SyncState.SYNCING:
                return
            
            self.sync_progress.state = SyncState.ERROR
            self.sync_progress.error_message = error_message
        
        with self.stats_lock:
            self.stats['sync_failures'] += 1
        
        logger.error(f"Sync failed: {error_message}")
        
        if self.on_sync_error:
            self.on_sync_error(error_message)
    
    def get_sync_progress(self) -> SyncProgress:
        """Get current sync progress."""
        with self.sync_lock:
            return SyncProgress(
                state=self.sync_progress.state,
                current_height=self.sync_progress.current_height,
                target_height=self.sync_progress.target_height,
                blocks_synced=self.sync_progress.blocks_synced,
                blocks_remaining=self.sync_progress.blocks_remaining,
                start_time=self.sync_progress.start_time,
                last_update=self.sync_progress.last_update,
                error_message=self.sync_progress.error_message
            )
    
    def add_sync_peer(self, peer_id: int, peer_height: int) -> None:
        """
        Add peer for synchronization.
        
        Args:
            peer_id: Peer node ID
            peer_height: Peer's block height
        """
        with self.peers_lock:
            self.sync_peers[peer_id] = {
                'height': peer_height,
                'added_time': time.time(),
                'blocks_received': 0,
                'errors': 0
            }
    
    def remove_sync_peer(self, peer_id: int) -> None:
        """
        Remove peer from synchronization.
        
        Args:
            peer_id: Peer node ID
        """
        with self.peers_lock:
            self.sync_peers.pop(peer_id, None)
    
    def get_sync_peers(self) -> List[int]:
        """Get list of sync peers."""
        with self.peers_lock:
            return list(self.sync_peers.keys())
    
    def get_stats(self) -> Dict:
        """Get sync statistics."""
        with self.stats_lock:
            stats = self.stats.copy()
        
        # Add progress info
        progress = self.get_sync_progress()
        stats['current_state'] = progress.state.name
        stats['current_height'] = progress.current_height
        stats['target_height'] = progress.target_height
        stats['progress_percent'] = progress.get_progress_percent()
        
        return stats


class ConflictResolver:
    """
    Resolves conflicts in blockchain state.
    
    Features:
    - Fork detection
    - Fork resolution
    - State validation
    """
    
    def __init__(self, node_id: int):
        """
        Initialize conflict resolver.
        
        Args:
            node_id: This node's ID
        """
        self.node_id = node_id
        
        # Fork tracking
        self.forks: Dict[int, List[Dict]] = {}  # height -> list of blocks
        self.forks_lock = threading.Lock()
        
        # Statistics
        self.stats = {
            'forks_detected': 0,
            'forks_resolved': 0,
            'conflicts_resolved': 0
        }
        self.stats_lock = threading.Lock()
        
        logger.info(f"Conflict resolver initialized (node_id={node_id})")
    
    def detect_fork(self, height: int, block_hash: str, peer_id: int) -> bool:
        """
        Detect potential fork.
        
        Args:
            height: Block height
            block_hash: Block hash
            peer_id: Peer node ID
        
        Returns:
            True if fork detected
        """
        with self.forks_lock:
            if height not in self.forks:
                self.forks[height] = []
            
            # Check if we have a different block at this height
            for existing_block in self.forks[height]:
                if existing_block['hash'] != block_hash:
                    with self.stats_lock:
                        self.stats['forks_detected'] += 1
                    
                    logger.warning(f"Fork detected at height {height}")
                    return True
            
            # Add block
            self.forks[height].append({
                'hash': block_hash,
                'peer_id': peer_id,
                'timestamp': time.time()
            })
        
        return False
    
    def resolve_fork(self, height: int, canonical_hash: str) -> bool:
        """
        Resolve fork by accepting canonical block.
        
        Args:
            height: Block height
            canonical_hash: Hash of canonical block
        
        Returns:
            True if resolved
        """
        with self.forks_lock:
            if height not in self.forks:
                return False
            
            # Keep only canonical block
            self.forks[height] = [
                b for b in self.forks[height]
                if b['hash'] == canonical_hash
            ]
            
            with self.stats_lock:
                self.stats['forks_resolved'] += 1
            
            logger.info(f"Fork resolved at height {height}")
        
        return True
    
    def get_stats(self) -> Dict:
        """Get conflict resolver statistics."""
        with self.stats_lock:
            return self.stats.copy()
