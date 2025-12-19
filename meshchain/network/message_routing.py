"""
Message Routing and Propagation Protocol for MeshChain over LoRa

Handles:
1. Message routing through mesh network
2. Block and transaction propagation
3. Duplicate detection and prevention
4. Message prioritization
5. Hop limit management
6. Broadcast and unicast routing
7. Message acknowledgment tracking
8. Network flooding control

This module implements a lightweight routing protocol optimized for
low-bandwidth LoRa mesh networks with limited resources.
"""

import time
import threading
import logging
from typing import Dict, List, Optional, Callable, Set, Tuple
from dataclasses import dataclass, field
from enum import IntEnum
from collections import deque
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MessagePriority(IntEnum):
    """Message priority levels."""
    LOW = 0
    NORMAL = 64
    HIGH = 128
    CRITICAL = 192


@dataclass
class RoutingEntry:
    """Entry in routing table."""
    destination: int                   # Destination node ID
    next_hop: int                      # Next hop node ID
    hop_count: int                     # Hops to destination
    last_updated: float = field(default_factory=time.time)
    is_valid: bool = True
    metric: int = 255                  # Route metric (lower is better)
    
    def is_stale(self, timeout: float = 300.0) -> bool:
        """Check if route is stale."""
        return time.time() - self.last_updated > timeout


@dataclass
class MessageRecord:
    """Record of recently seen message for deduplication."""
    message_hash: bytes
    sender_id: int
    timestamp: float = field(default_factory=time.time)
    hop_count: int = 0
    
    def is_expired(self, timeout: float = 60.0) -> bool:
        """Check if record is expired."""
        return time.time() - self.timestamp > timeout


class MessageRouter:
    """
    Routes messages through mesh network.
    
    Features:
    - Duplicate detection
    - Hop limit management
    - Priority-based routing
    - Route caching
    - Broadcast flooding control
    """
    
    def __init__(self, node_id: int, max_hops: int = 3, message_timeout: float = 60.0):
        """
        Initialize router.
        
        Args:
            node_id: This node's ID
            max_hops: Maximum hops for messages
            message_timeout: Timeout for seen messages in seconds
        """
        self.node_id = node_id
        self.max_hops = max_hops
        self.message_timeout = message_timeout
        
        # Routing table
        self.routing_table: Dict[int, RoutingEntry] = {}
        self.routing_lock = threading.Lock()
        
        # Message deduplication (with timestamp for cleanup)
        self.seen_messages: Dict[bytes, MessageRecord] = {}
        self.seen_lock = threading.Lock()
        
        # Broadcast tracking
        self.broadcast_cache: Dict[bytes, float] = {}
        self.broadcast_lock = threading.Lock()
        
        # Cleanup thread
        self.cleanup_thread: Optional[threading.Thread] = None
        self.is_running = False
        
        # Callbacks
        self.on_route_discovered: Optional[Callable[[int, int, int], None]] = None
        self.on_route_expired: Optional[Callable[[int], None]] = None
        
        # Statistics
        self.stats = {
            'messages_routed': 0,
            'duplicates_dropped': 0,
            'hop_limit_exceeded': 0,
            'routes_discovered': 0,
            'routes_expired': 0,
            'broadcasts_flooded': 0,
            'messages_cleaned': 0,
            'broadcasts_cleaned': 0
        }
        self.stats_lock = threading.Lock()
        
        logger.info(f"Message router initialized (node_id={node_id}, max_hops={max_hops}, timeout={message_timeout}s)")
    
    def add_route(self, destination: int, next_hop: int, hop_count: int, metric: int = 255) -> None:
        """
        Add or update route in routing table.
        
        Args:
            destination: Destination node ID
            next_hop: Next hop node ID
            hop_count: Hops to destination
            metric: Route metric (lower is better)
        """
        with self.routing_lock:
            # Check if route already exists
            existing = self.routing_table.get(destination)
            
            # Only update if new route is better
            if existing and existing.metric <= metric:
                return
            
            # Add or update route
            self.routing_table[destination] = RoutingEntry(
                destination=destination,
                next_hop=next_hop,
                hop_count=hop_count,
                metric=metric
            )
            
            with self.stats_lock:
                self.stats['routes_discovered'] += 1
            
            logger.info(f"Route added: {destination} via {next_hop} ({hop_count} hops)")
            
            # Call callback
            if self.on_route_discovered:
                self.on_route_discovered(destination, next_hop, hop_count)
    
    def get_route(self, destination: int) -> Optional[RoutingEntry]:
        """
        Get route to destination.
        
        Args:
            destination: Destination node ID
        
        Returns:
            Route entry or None
        """
        with self.routing_lock:
            route = self.routing_table.get(destination)
            
            if route and route.is_stale():
                # Remove stale route
                del self.routing_table[destination]
                
                with self.stats_lock:
                    self.stats['routes_expired'] += 1
                
                logger.info(f"Route expired: {destination}")
                
                if self.on_route_expired:
                    self.on_route_expired(destination)
                
                return None
            
            return route
    
    def should_forward_message(self, message_hash: bytes, sender_id: int) -> bool:
        """
        Check if message should be forwarded (deduplication).
        
        Args:
            message_hash: Hash of message
            sender_id: Sender node ID
        
        Returns:
            True if message is new and should be forwarded
        """
        with self.seen_lock:
            # Check if we've seen this message
            if message_hash in self.seen_messages:
                record = self.seen_messages[message_hash]
                # Check if record is still valid
                if not record.is_expired(self.message_timeout):
                    with self.stats_lock:
                        self.stats['duplicates_dropped'] += 1
                    return False
                else:
                    # Record is expired, remove it
                    del self.seen_messages[message_hash]
            
            # Add to seen messages
            self.seen_messages[message_hash] = MessageRecord(message_hash, sender_id)
            return True
    
    def should_broadcast_flood(self, message_hash: bytes, timeout: float = 5.0) -> bool:
        """
        Check if message should be broadcast flooded.
        
        Uses cache to prevent duplicate floods within timeout period.
        
        Args:
            message_hash: Hash of message
            timeout: Flood timeout in seconds
        
        Returns:
            True if message should be flooded
        """
        with self.broadcast_lock:
            # Check if recently flooded
            last_flood = self.broadcast_cache.get(message_hash)
            if last_flood and time.time() - last_flood < timeout:
                return False
            
            # Update flood cache
            self.broadcast_cache[message_hash] = time.time()
            
            with self.stats_lock:
                self.stats['broadcasts_flooded'] += 1
            
            return True
    
    def calculate_hop_limit(self, destination: int) -> int:
        """
        Calculate hop limit for message to destination.
        
        Args:
            destination: Destination node ID
        
        Returns:
            Hop limit to use
        """
        route = self.get_route(destination)
        
        if route:
            # Use route hop count + buffer
            return min(route.hop_count + 1, self.max_hops)
        else:
            # Use maximum hops for unknown destinations
            return self.max_hops
    
    def cleanup_stale_routes(self, timeout: float = 300.0) -> int:
        """
        Remove stale routes from routing table.
        
        Args:
            timeout: Route timeout in seconds
        
        Returns:
            Number of routes removed
        """
        removed = 0
        
        with self.routing_lock:
            stale_routes = [
                dest for dest, route in self.routing_table.items()
                if route.is_stale(timeout)
            ]
            
            for dest in stale_routes:
                del self.routing_table[dest]
                removed += 1
                
                with self.stats_lock:
                    self.stats['routes_expired'] += 1
                
                if self.on_route_expired:
                    self.on_route_expired(dest)
        
        if removed > 0:
            logger.info(f"Removed {removed} stale routes")
        
        return removed
    
    def start_cleanup(self) -> None:
        """Start background cleanup thread."""
        if self.is_running:
            return
        
        self.is_running = True
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        logger.info("Message router cleanup thread started")
    
    def stop_cleanup(self) -> None:
        """Stop background cleanup thread."""
        self.is_running = False
        
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5.0)
        
        logger.info("Message router cleanup thread stopped")
    
    def _cleanup_loop(self) -> None:
        """Background cleanup loop for expired messages."""
        while self.is_running:
            try:
                time.sleep(30.0)  # Cleanup every 30 seconds
                
                # Cleanup seen messages
                with self.seen_lock:
                    expired = [
                        msg_hash for msg_hash, record in self.seen_messages.items()
                        if record.is_expired(self.message_timeout)
                    ]
                    
                    for msg_hash in expired:
                        del self.seen_messages[msg_hash]
                    
                    if expired:
                        with self.stats_lock:
                            self.stats['messages_cleaned'] += len(expired)
                        logger.info(f"Cleaned {len(expired)} expired messages")
                
                # Cleanup broadcast cache
                with self.broadcast_lock:
                    current_time = time.time()
                    expired_broadcasts = [
                        msg_hash for msg_hash, flood_time in self.broadcast_cache.items()
                        if current_time - flood_time > 300.0  # 5 minute timeout
                    ]
                    
                    for msg_hash in expired_broadcasts:
                        del self.broadcast_cache[msg_hash]
                    
                    if expired_broadcasts:
                        with self.stats_lock:
                            self.stats['broadcasts_cleaned'] += len(expired_broadcasts)
                        logger.info(f"Cleaned {len(expired_broadcasts)} expired broadcasts")
            
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                time.sleep(10.0)
    
    def get_stats(self) -> Dict[str, int]:
        """Get router statistics."""
        with self.stats_lock:
            stats = self.stats.copy()
        
        # Add memory usage info
        with self.seen_lock:
            stats['seen_messages_count'] = len(self.seen_messages)
        
        with self.broadcast_lock:
            stats['broadcast_cache_count'] = len(self.broadcast_cache)
        
        return stats


class MessagePropagator:
    """
    Propagates blockchain messages through network.
    
    Handles:
    - Block propagation
    - Transaction propagation
    - Sync request/response
    - Message prioritization
    - Rate limiting
    """
    
    def __init__(self, node_id: int, router: MessageRouter):
        """
        Initialize propagator.
        
        Args:
            node_id: This node's ID
            router: Message router instance
        """
        self.node_id = node_id
        self.router = router
        
        # Message queues by priority
        self.critical_queue: deque = deque(maxlen=50)
        self.high_queue: deque = deque(maxlen=100)
        self.normal_queue: deque = deque(maxlen=200)
        self.low_queue: deque = deque(maxlen=100)
        
        self.queue_lock = threading.Lock()
        
        # Rate limiting
        self.rate_limits: Dict[int, Tuple[int, float]] = {}  # node_id -> (count, timestamp)
        self.rate_lock = threading.Lock()
        
        # Callbacks
        self.on_message_ready: Optional[Callable[[Dict], None]] = None
        
        # Statistics
        self.stats = {
            'messages_queued': 0,
            'messages_sent': 0,
            'messages_dropped': 0,
            'rate_limited': 0
        }
        self.stats_lock = threading.Lock()
        
        logger.info(f"Message propagator initialized (node_id={node_id})")
    
    def queue_message(self, message: Dict, priority: int = MessagePriority.NORMAL) -> bool:
        """
        Queue message for propagation.
        
        Args:
            message: Message to queue
            priority: Message priority
        
        Returns:
            True if queued successfully
        """
        with self.queue_lock:
            try:
                if priority == MessagePriority.CRITICAL:
                    self.critical_queue.append(message)
                elif priority == MessagePriority.HIGH:
                    self.high_queue.append(message)
                elif priority == MessagePriority.NORMAL:
                    self.normal_queue.append(message)
                else:
                    self.low_queue.append(message)
                
                with self.stats_lock:
                    self.stats['messages_queued'] += 1
                
                return True
            except:
                with self.stats_lock:
                    self.stats['messages_dropped'] += 1
                return False
    
    def get_next_message(self) -> Optional[Dict]:
        """
        Get next message to send (priority order).
        
        Returns:
            Message or None if no messages
        """
        with self.queue_lock:
            # Check queues in priority order
            if self.critical_queue:
                return self.critical_queue.popleft()
            elif self.high_queue:
                return self.high_queue.popleft()
            elif self.normal_queue:
                return self.normal_queue.popleft()
            elif self.low_queue:
                return self.low_queue.popleft()
            
            return None
    
    def check_rate_limit(self, peer_id: int, limit: int = 10, window: float = 60.0) -> bool:
        """
        Check if peer is rate limited.
        
        Args:
            peer_id: Peer node ID
            limit: Message limit per window
            window: Time window in seconds
        
        Returns:
            True if peer is not rate limited
        """
        with self.rate_lock:
            now = time.time()
            count, timestamp = self.rate_limits.get(peer_id, (0, now))
            
            # Reset if window expired
            if now - timestamp > window:
                count = 0
                timestamp = now
            
            # Check limit
            if count >= limit:
                with self.stats_lock:
                    self.stats['rate_limited'] += 1
                return False
            
            # Update counter
            self.rate_limits[peer_id] = (count + 1, timestamp)
            return True
    
    def get_queue_depth(self) -> Dict[str, int]:
        """Get queue depths."""
        with self.queue_lock:
            return {
                'critical': len(self.critical_queue),
                'high': len(self.high_queue),
                'normal': len(self.normal_queue),
                'low': len(self.low_queue),
                'total': len(self.critical_queue) + len(self.high_queue) + 
                        len(self.normal_queue) + len(self.low_queue)
            }
    
    def get_stats(self) -> Dict[str, int]:
        """Get propagator statistics."""
        with self.stats_lock:
            return self.stats.copy()


class RoutingProtocol:
    """
    Routing protocol for mesh network.
    
    Implements:
    - Route discovery
    - Route maintenance
    - Periodic route updates
    - Route metric calculation
    """
    
    def __init__(self, node_id: int, router: MessageRouter):
        """
        Initialize routing protocol.
        
        Args:
            node_id: This node's ID
            router: Message router instance
        """
        self.node_id = node_id
        self.router = router
        
        # Sequence number for route updates
        self.sequence_number = 0
        self.seq_lock = threading.Lock()
        
        logger.info(f"Routing protocol initialized (node_id={node_id})")
    
    def process_route_update(self, source: int, routes: List[Tuple[int, int, int]]) -> None:
        """
        Process route update from peer.
        
        Args:
            source: Source node ID
            routes: List of (destination, next_hop, hop_count) tuples
        """
        for destination, next_hop, hop_count in routes:
            # Add route with source as next hop
            metric = hop_count + 1
            self.router.add_route(destination, source, hop_count + 1, metric)
    
    def get_next_sequence_number(self) -> int:
        """Get next sequence number for route updates."""
        with self.seq_lock:
            self.sequence_number = (self.sequence_number + 1) & 0xFFFFFFFF
            return self.sequence_number
    
    def create_route_update(self) -> Dict:
        """
        Create route update message.
        
        Returns:
            Route update message
        """
        with self.router.routing_lock:
            routes = [
                (entry.destination, entry.next_hop, entry.hop_count)
                for entry in self.router.routing_table.values()
            ]
        
        return {
            'type': 'route_update',
            'source': self.node_id,
            'sequence': self.get_next_sequence_number(),
            'routes': routes,
            'timestamp': int(time.time())
        }
