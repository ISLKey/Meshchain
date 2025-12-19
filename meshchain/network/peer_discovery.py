"""
Peer Discovery and Network Management for MeshChain

Implements:
1. Peer discovery protocol
2. Peer information tracking
3. Network topology management
4. Peer health monitoring
5. Peer reputation system
6. Connection management

This module enables nodes to discover and manage peers on the LoRa mesh network.
"""

import time
import threading
import logging
from typing import Dict, List, Optional, Callable, Set
from dataclasses import dataclass, field
from enum import IntEnum
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PeerStatus(IntEnum):
    """Peer connection status."""
    UNKNOWN = 0
    DISCOVERED = 1
    CONNECTED = 2
    DISCONNECTED = 3
    UNREACHABLE = 4


@dataclass
class PeerMetrics:
    """Metrics for a peer."""
    messages_sent: int = 0
    messages_received: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    errors: int = 0
    last_message_time: float = field(default_factory=time.time)
    latency_ms: float = 0.0
    reliability: float = 1.0  # 0.0-1.0
    reputation: float = 1.0   # 0.0-1.0


@dataclass
class PeerInfo:
    """Information about a peer."""
    node_id: int
    status: PeerStatus = PeerStatus.UNKNOWN
    block_height: int = 0
    stake: int = 0
    hop_distance: int = 255
    is_validator: bool = False
    version: str = "1.0"
    last_seen: float = field(default_factory=time.time)
    metrics: PeerMetrics = field(default_factory=PeerMetrics)
    
    def is_stale(self, timeout: float = 300.0) -> bool:
        """Check if peer info is stale."""
        return time.time() - self.last_seen > timeout
    
    def update_seen(self):
        """Update last seen timestamp."""
        self.last_seen = time.time()
    
    def is_healthy(self) -> bool:
        """Check if peer is healthy."""
        return (
            self.status in (PeerStatus.CONNECTED, PeerStatus.DISCOVERED) and
            not self.is_stale() and
            self.metrics.reliability > 0.5
        )


class PeerDiscovery:
    """
    Discovers peers on the mesh network.
    
    Features:
    - Periodic hello messages
    - Peer information exchange
    - Network topology discovery
    - Peer health monitoring
    """
    
    def __init__(self, node_id: int):
        """
        Initialize peer discovery.
        
        Args:
            node_id: This node's ID
        """
        self.node_id = node_id
        self.peers: Dict[int, PeerInfo] = {}
        self.peers_lock = threading.Lock()
        
        # Discovery state
        self.is_discovering = False
        self.discovery_thread: Optional[threading.Thread] = None
        
        # Callbacks
        self.on_peer_discovered: Optional[Callable[[int], None]] = None
        self.on_peer_lost: Optional[Callable[[int], None]] = None
        self.on_peer_updated: Optional[Callable[[int], None]] = None
        
        # Statistics
        self.stats = {
            'peers_discovered': 0,
            'peers_lost': 0,
            'hello_messages_sent': 0,
            'hello_messages_received': 0,
            'peer_updates_received': 0
        }
        self.stats_lock = threading.Lock()
        
        logger.info(f"Peer discovery initialized (node_id={node_id})")
    
    def add_peer(self, peer_info: PeerInfo) -> None:
        """
        Add or update peer information.
        
        Args:
            peer_info: Peer information
        """
        with self.peers_lock:
            is_new = peer_info.node_id not in self.peers
            self.peers[peer_info.node_id] = peer_info
        
        if is_new:
            with self.stats_lock:
                self.stats['peers_discovered'] += 1
            
            logger.info(f"Peer discovered: {peer_info.node_id}")
            
            if self.on_peer_discovered:
                self.on_peer_discovered(peer_info.node_id)
        else:
            if self.on_peer_updated:
                self.on_peer_updated(peer_info.node_id)
    
    def get_peer(self, node_id: int) -> Optional[PeerInfo]:
        """
        Get peer information.
        
        Args:
            node_id: Peer node ID
        
        Returns:
            Peer info or None
        """
        with self.peers_lock:
            return self.peers.get(node_id)
    
    def get_all_peers(self) -> List[PeerInfo]:
        """
        Get all known peers.
        
        Returns:
            List of peer info
        """
        with self.peers_lock:
            return list(self.peers.values())
    
    def get_healthy_peers(self) -> List[PeerInfo]:
        """
        Get healthy peers.
        
        Returns:
            List of healthy peer info
        """
        with self.peers_lock:
            return [p for p in self.peers.values() if p.is_healthy()]
    
    def get_validators(self) -> List[PeerInfo]:
        """
        Get validator peers.
        
        Returns:
            List of validator peer info
        """
        with self.peers_lock:
            return [p for p in self.peers.values() if p.is_validator]
    
    def cleanup_stale_peers(self, timeout: float = 300.0) -> int:
        """
        Remove stale peers.
        
        Args:
            timeout: Peer timeout in seconds
        
        Returns:
            Number of peers removed
        """
        removed = 0
        
        with self.peers_lock:
            stale_peers = [
                node_id for node_id, peer in self.peers.items()
                if peer.is_stale(timeout)
            ]
            
            for node_id in stale_peers:
                del self.peers[node_id]
                removed += 1
                
                with self.stats_lock:
                    self.stats['peers_lost'] += 1
                
                logger.info(f"Peer lost (stale): {node_id}")
                
                if self.on_peer_lost:
                    self.on_peer_lost(node_id)
        
        return removed
    
    def start_discovery(self) -> None:
        """Start peer discovery."""
        if self.is_discovering:
            return
        
        self.is_discovering = True
        self.discovery_thread = threading.Thread(target=self._discovery_loop, daemon=True)
        self.discovery_thread.start()
        logger.info("Peer discovery started")
    
    def stop_discovery(self) -> None:
        """Stop peer discovery."""
        self.is_discovering = False
        
        if self.discovery_thread:
            self.discovery_thread.join(timeout=5.0)
        
        logger.info("Peer discovery stopped")
    
    def _discovery_loop(self) -> None:
        """Main discovery loop."""
        while self.is_discovering:
            try:
                # Cleanup stale peers
                self.cleanup_stale_peers()
                
                # Sleep before next iteration
                time.sleep(30.0)
            
            except Exception as e:
                logger.error(f"Error in discovery loop: {e}")
                time.sleep(5.0)
    
    def get_stats(self) -> Dict[str, int]:
        """Get discovery statistics."""
        with self.stats_lock:
            return self.stats.copy()


class NetworkManager:
    """
    Manages network connections and peers.
    
    Features:
    - Peer management
    - Connection tracking
    - Network topology
    - Health monitoring
    """
    
    def __init__(self, node_id: int):
        """
        Initialize network manager.
        
        Args:
            node_id: This node's ID
        """
        self.node_id = node_id
        self.peer_discovery = PeerDiscovery(node_id)
        
        # Connected peers
        self.connected_peers: Set[int] = set()
        self.connected_lock = threading.Lock()
        
        # Network stats
        self.stats = {
            'total_peers': 0,
            'connected_peers': 0,
            'healthy_peers': 0,
            'validators': 0,
            'network_diameter': 0,
            'average_hop_distance': 0.0
        }
        self.stats_lock = threading.Lock()
        
        logger.info(f"Network manager initialized (node_id={node_id})")
    
    def add_peer(self, peer_info: PeerInfo) -> None:
        """
        Add peer to network.
        
        Args:
            peer_info: Peer information
        """
        self.peer_discovery.add_peer(peer_info)
        self._update_stats()
    
    def connect_peer(self, node_id: int) -> bool:
        """
        Mark peer as connected.
        
        Args:
            node_id: Peer node ID
        
        Returns:
            True if successful
        """
        peer = self.peer_discovery.get_peer(node_id)
        if not peer:
            return False
        
        with self.connected_lock:
            self.connected_peers.add(node_id)
        
        peer.status = PeerStatus.CONNECTED
        peer.update_seen()
        
        self._update_stats()
        logger.info(f"Peer connected: {node_id}")
        
        return True
    
    def disconnect_peer(self, node_id: int) -> bool:
        """
        Mark peer as disconnected.
        
        Args:
            node_id: Peer node ID
        
        Returns:
            True if successful
        """
        peer = self.peer_discovery.get_peer(node_id)
        if not peer:
            return False
        
        with self.connected_lock:
            self.connected_peers.discard(node_id)
        
        peer.status = PeerStatus.DISCONNECTED
        
        self._update_stats()
        logger.info(f"Peer disconnected: {node_id}")
        
        return True
    
    def get_network_stats(self) -> Dict[str, int]:
        """Get network statistics."""
        with self.stats_lock:
            return self.stats.copy()
    
    def _update_stats(self) -> None:
        """Update network statistics."""
        all_peers = self.peer_discovery.get_all_peers()
        healthy_peers = self.peer_discovery.get_healthy_peers()
        validators = self.peer_discovery.get_validators()
        
        with self.connected_lock:
            connected_count = len(self.connected_peers)
        
        # Calculate average hop distance
        hop_distances = [p.hop_distance for p in all_peers if p.hop_distance < 255]
        avg_hop = sum(hop_distances) / len(hop_distances) if hop_distances else 0.0
        
        with self.stats_lock:
            self.stats['total_peers'] = len(all_peers)
            self.stats['connected_peers'] = connected_count
            self.stats['healthy_peers'] = len(healthy_peers)
            self.stats['validators'] = len(validators)
            self.stats['network_diameter'] = max(hop_distances) if hop_distances else 0
            self.stats['average_hop_distance'] = avg_hop
    
    def start(self) -> None:
        """Start network manager."""
        self.peer_discovery.start_discovery()
        logger.info("Network manager started")
    
    def stop(self) -> None:
        """Stop network manager."""
        self.peer_discovery.stop_discovery()
        logger.info("Network manager stopped")
