"""
MeshChain Peer Manager - Peer Discovery and Management

This module handles:
1. Peer discovery through HELLO messages
2. Peer information tracking and updates
3. Peer reputation and scoring
4. Peer selection for synchronization
5. Stale peer cleanup
6. Network topology tracking

Key Components:
1. PeerScore: Scoring system for peer reliability
2. PeerDiscovery: Discovers peers in network
3. PeerManager: Manages all peers
4. TopologyManager: Tracks network topology
"""

import time
import threading
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import IntEnum
import random


class PeerScore(IntEnum):
    """Peer scoring for reliability."""
    EXCELLENT = 100    # Peer is reliable
    GOOD = 75          # Peer is mostly reliable
    FAIR = 50          # Peer is somewhat reliable
    POOR = 25          # Peer is unreliable
    FAILED = 0         # Peer has failed


@dataclass
class PeerMetrics:
    """Metrics for peer performance."""
    messages_sent: int = 0          # Messages sent to peer
    messages_received: int = 0      # Messages received from peer
    blocks_received: int = 0        # Blocks received from peer
    transactions_received: int = 0  # Transactions received from peer
    sync_requests: int = 0          # Sync requests to peer
    sync_successes: int = 0         # Successful syncs
    sync_failures: int = 0          # Failed syncs
    latency_ms: float = 0.0         # Average latency
    last_message_time: float = 0.0  # Last message timestamp
    connection_failures: int = 0    # Connection failures
    
    def get_reliability(self) -> float:
        """
        Calculate peer reliability (0.0 to 1.0).
        
        Based on:
        - Success rate of syncs
        - Message delivery rate
        - Connection stability
        """
        if self.sync_requests == 0:
            return 0.5  # Unknown reliability
        
        sync_success_rate = self.sync_successes / self.sync_requests
        
        # Penalize connection failures
        failure_penalty = self.connection_failures * 0.05
        
        reliability = sync_success_rate - failure_penalty
        return max(0.0, min(1.0, reliability))
    
    def get_score(self) -> PeerScore:
        """Get peer score based on metrics."""
        reliability = self.get_reliability()
        
        if reliability >= 0.9:
            return PeerScore.EXCELLENT
        elif reliability >= 0.7:
            return PeerScore.GOOD
        elif reliability >= 0.5:
            return PeerScore.FAIR
        elif reliability > 0.0:
            return PeerScore.POOR
        else:
            return PeerScore.FAILED


@dataclass
class PeerData:
    """Complete peer information."""
    node_id: bytes                          # 8-byte node ID
    block_height: int = 0                   # Last known block height
    stake: int = 0                          # Validator stake
    hop_distance: int = 255                 # Hops away (255 = unknown)
    is_validator: bool = False              # Is this a validator?
    version: str = "1.0"                    # Protocol version
    first_seen: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    metrics: PeerMetrics = field(default_factory=PeerMetrics)
    
    def update_seen(self):
        """Update last seen timestamp."""
        self.last_seen = time.time()
    
    def is_stale(self, timeout: float = 300.0) -> bool:
        """Check if peer hasn't been seen recently."""
        return time.time() - self.last_seen > timeout
    
    def get_uptime(self) -> float:
        """Get uptime in seconds."""
        return time.time() - self.first_seen
    
    def get_score(self) -> PeerScore:
        """Get peer score."""
        return self.metrics.get_score()


class PeerDiscovery:
    """
    Discovers peers in the network through HELLO messages.
    
    Maintains a list of known peers and their information.
    """
    
    def __init__(self, max_peers: int = 100, timeout: float = 300.0):
        """
        Initialize peer discovery.
        
        Args:
            max_peers: Maximum number of peers to track
            timeout: Peer timeout in seconds
        """
        self.max_peers = max_peers
        self.timeout = timeout
        self.peers: Dict[bytes, PeerData] = {}
        self.lock = threading.RLock()
    
    def add_peer(self, node_id: bytes, block_height: int, stake: int,
                 hop_distance: int = 255, is_validator: bool = False) -> PeerData:
        """
        Add or update a peer.
        
        Args:
            node_id: Peer's node ID
            block_height: Peer's current block height
            stake: Peer's validator stake
            hop_distance: Hops to peer
            is_validator: Whether peer is a validator
            
        Returns:
            PeerData for the peer
        """
        with self.lock:
            if node_id in self.peers:
                # Update existing peer
                peer = self.peers[node_id]
                peer.block_height = block_height
                peer.stake = stake
                peer.hop_distance = hop_distance
                peer.is_validator = is_validator
                peer.update_seen()
            else:
                # Add new peer
                if len(self.peers) >= self.max_peers:
                    # Remove oldest peer
                    oldest = min(self.peers.values(), 
                               key=lambda p: p.last_seen)
                    del self.peers[oldest.node_id]
                
                peer = PeerData(
                    node_id=node_id,
                    block_height=block_height,
                    stake=stake,
                    hop_distance=hop_distance,
                    is_validator=is_validator
                )
                self.peers[node_id] = peer
            
            return peer
    
    def get_peer(self, node_id: bytes) -> Optional[PeerData]:
        """Get peer information."""
        with self.lock:
            return self.peers.get(node_id)
    
    def get_all_peers(self) -> List[PeerData]:
        """Get all known peers."""
        with self.lock:
            return list(self.peers.values())
    
    def get_active_peers(self) -> List[PeerData]:
        """Get peers that are currently active."""
        with self.lock:
            return [p for p in self.peers.values() 
                   if not p.is_stale(self.timeout)]
    
    def get_validators(self) -> List[PeerData]:
        """Get validator peers."""
        with self.lock:
            return [p for p in self.peers.values() 
                   if p.is_validator and not p.is_stale(self.timeout)]
    
    def get_peers_by_score(self, min_score: PeerScore = PeerScore.FAIR) -> List[PeerData]:
        """Get peers with minimum score."""
        with self.lock:
            return [p for p in self.peers.values()
                   if p.get_score() >= min_score 
                   and not p.is_stale(self.timeout)]
    
    def remove_peer(self, node_id: bytes) -> bool:
        """Remove a peer."""
        with self.lock:
            if node_id in self.peers:
                del self.peers[node_id]
                return True
            return False
    
    def cleanup_stale(self) -> int:
        """
        Remove stale peers.
        
        Returns:
            Number of peers removed
        """
        with self.lock:
            stale = [p.node_id for p in self.peers.values()
                    if p.is_stale(self.timeout)]
            
            for node_id in stale:
                del self.peers[node_id]
            
            return len(stale)
    
    def get_stats(self) -> Dict:
        """Get discovery statistics."""
        with self.lock:
            active = self.get_active_peers()
            validators = self.get_validators()
            
            return {
                'total_peers': len(self.peers),
                'active_peers': len(active),
                'validator_peers': len(validators),
                'avg_block_height': sum(p.block_height for p in self.peers.values()) / len(self.peers) if self.peers else 0,
                'total_stake': sum(p.stake for p in validators)
            }


class PeerManager:
    """
    Manages all peer-related operations.
    
    Includes:
    1. Peer discovery
    2. Peer metrics tracking
    3. Peer selection for operations
    4. Peer reputation management
    """
    
    def __init__(self, node_id: bytes, max_peers: int = 100):
        """
        Initialize peer manager.
        
        Args:
            node_id: This node's ID
            max_peers: Maximum peers to track
        """
        self.node_id = node_id
        self.discovery = PeerDiscovery(max_peers=max_peers)
        self.lock = threading.RLock()
        
        # Cleanup thread
        self.cleanup_thread = None
        self.running = False
    
    def start(self):
        """Start peer manager (cleanup thread)."""
        if not self.running:
            self.running = True
            self.cleanup_thread = threading.Thread(
                target=self._cleanup_loop,
                daemon=True
            )
            self.cleanup_thread.start()
    
    def stop(self):
        """Stop peer manager."""
        self.running = False
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5.0)
    
    def _cleanup_loop(self):
        """Periodically clean up stale peers."""
        while self.running:
            try:
                self.discovery.cleanup_stale()
                time.sleep(60)  # Cleanup every minute
            except Exception as e:
                print(f"Cleanup error: {str(e)}")
    
    def add_peer(self, node_id: bytes, block_height: int, stake: int,
                 hop_distance: int = 255) -> PeerData:
        """Add or update a peer."""
        return self.discovery.add_peer(
            node_id, block_height, stake, hop_distance,
            is_validator=(stake > 0)
        )
    
    def record_message(self, node_id: bytes):
        """Record that we received a message from peer."""
        peer = self.discovery.get_peer(node_id)
        if peer:
            peer.metrics.messages_received += 1
            peer.update_seen()
    
    def record_block(self, node_id: bytes):
        """Record that we received a block from peer."""
        peer = self.discovery.get_peer(node_id)
        if peer:
            peer.metrics.blocks_received += 1
            peer.metrics.messages_received += 1
            peer.update_seen()
    
    def record_transaction(self, node_id: bytes):
        """Record that we received a transaction from peer."""
        peer = self.discovery.get_peer(node_id)
        if peer:
            peer.metrics.transactions_received += 1
            peer.metrics.messages_received += 1
            peer.update_seen()
    
    def record_sync_request(self, node_id: bytes):
        """Record that we sent a sync request to peer."""
        peer = self.discovery.get_peer(node_id)
        if peer:
            peer.metrics.sync_requests += 1
    
    def record_sync_success(self, node_id: bytes, latency_ms: float = 0.0):
        """Record successful sync with peer."""
        peer = self.discovery.get_peer(node_id)
        if peer:
            peer.metrics.sync_successes += 1
            if latency_ms > 0:
                # Update average latency
                old_latency = peer.metrics.latency_ms
                total = peer.metrics.sync_successes
                peer.metrics.latency_ms = (
                    (old_latency * (total - 1) + latency_ms) / total
                )
            peer.update_seen()
    
    def record_sync_failure(self, node_id: bytes):
        """Record failed sync with peer."""
        peer = self.discovery.get_peer(node_id)
        if peer:
            peer.metrics.sync_failures += 1
            peer.metrics.connection_failures += 1
    
    def select_peer_for_sync(self, 
                            exclude: Optional[Set[bytes]] = None) -> Optional[PeerData]:
        """
        Select best peer for synchronization.
        
        Uses weighted selection based on:
        1. Peer score (reliability)
        2. Block height (more advanced = better)
        3. Latency (lower = better)
        
        Args:
            exclude: Set of node IDs to exclude
            
        Returns:
            Selected peer or None
        """
        exclude = exclude or set()
        
        # Get good peers
        candidates = self.discovery.get_peers_by_score(PeerScore.FAIR)
        candidates = [p for p in candidates if p.node_id not in exclude]
        
        if not candidates:
            return None
        
        # Calculate weights
        weights = []
        for peer in candidates:
            # Score weight (0.0 to 1.0)
            score_weight = peer.get_score() / 100.0
            
            # Block height weight (prefer higher)
            max_height = max(p.block_height for p in candidates)
            height_weight = peer.block_height / max_height if max_height > 0 else 0.5
            
            # Latency weight (prefer lower)
            max_latency = max(p.metrics.latency_ms for p in candidates) or 1000
            latency_weight = 1.0 - (peer.metrics.latency_ms / max_latency)
            
            # Combined weight
            weight = (score_weight * 0.5 + height_weight * 0.3 + latency_weight * 0.2)
            weights.append(weight)
        
        # Weighted random selection
        total_weight = sum(weights)
        if total_weight <= 0:
            return random.choice(candidates)
        
        rand = random.uniform(0, total_weight)
        cumulative = 0
        for peer, weight in zip(candidates, weights):
            cumulative += weight
            if rand <= cumulative:
                return peer
        
        return candidates[-1]
    
    def select_peers_for_broadcast(self, count: int = 3,
                                   exclude: Optional[Set[bytes]] = None) -> List[PeerData]:
        """
        Select peers for block/transaction broadcast.
        
        Args:
            count: Number of peers to select
            exclude: Set of node IDs to exclude
            
        Returns:
            List of selected peers
        """
        exclude = exclude or set()
        
        # Get active peers
        candidates = self.discovery.get_active_peers()
        candidates = [p for p in candidates if p.node_id not in exclude]
        
        if not candidates:
            return []
        
        # Select top peers by score
        candidates.sort(key=lambda p: p.get_score(), reverse=True)
        return candidates[:count]
    
    def get_peer(self, node_id: bytes) -> Optional[PeerData]:
        """Get peer information."""
        return self.discovery.get_peer(node_id)
    
    def get_all_peers(self) -> List[PeerData]:
        """Get all peers."""
        return self.discovery.get_all_peers()
    
    def get_active_peers(self) -> List[PeerData]:
        """Get active peers."""
        return self.discovery.get_active_peers()
    
    def get_validators(self) -> List[PeerData]:
        """Get validator peers."""
        return self.discovery.get_validators()
    
    def get_statistics(self) -> Dict:
        """Get peer manager statistics."""
        stats = self.discovery.get_stats()
        
        # Add additional stats
        all_peers = self.discovery.get_all_peers()
        if all_peers:
            avg_score = sum(p.get_score() for p in all_peers) / len(all_peers)
            stats['avg_peer_score'] = avg_score
        
        return stats


class TopologyManager:
    """
    Tracks network topology based on hop distances.
    
    Helps understand network structure and optimize routing.
    """
    
    def __init__(self):
        """Initialize topology manager."""
        self.hop_distances: Dict[bytes, int] = {}
        self.lock = threading.RLock()
    
    def update_hop_distance(self, node_id: bytes, distance: int):
        """Update hop distance to a node."""
        with self.lock:
            self.hop_distances[node_id] = distance
    
    def get_hop_distance(self, node_id: bytes) -> int:
        """Get hop distance to a node."""
        with self.lock:
            return self.hop_distances.get(node_id, 255)
    
    def get_neighbors(self, max_hops: int = 2) -> List[bytes]:
        """Get neighbors within max hops."""
        with self.lock:
            return [node_id for node_id, distance in self.hop_distances.items()
                   if distance <= max_hops]
    
    def get_topology_stats(self) -> Dict:
        """Get topology statistics."""
        with self.lock:
            if not self.hop_distances:
                return {'nodes': 0, 'avg_hops': 0, 'max_hops': 0}
            
            distances = list(self.hop_distances.values())
            return {
                'nodes': len(distances),
                'avg_hops': sum(distances) / len(distances),
                'max_hops': max(distances),
                'min_hops': min(distances)
            }
