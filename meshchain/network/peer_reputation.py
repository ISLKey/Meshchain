"""
Peer Reputation System for MeshChain

Implements:
1. Peer reputation tracking
2. Behavior-based scoring
3. Reputation decay over time
4. Reputation rewards and penalties
5. Peer trustworthiness calculation

This module tracks peer behavior and assigns reputation scores
to help identify reliable peers and avoid unreliable ones.
"""

import time
import logging
from typing import Dict, Optional
from dataclasses import dataclass, field
import math

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ReputationEvent:
    """Single reputation event."""
    event_type: str  # 'message_received', 'message_invalid', 'block_valid', 'block_invalid', etc.
    timestamp: float = field(default_factory=time.time)
    value: float = 0.0  # Points added/subtracted
    reason: str = ""  # Description


class PeerReputation:
    """
    Tracks and manages peer reputation.
    
    Features:
    - Event-based reputation updates
    - Time-based reputation decay
    - Reputation bounds (0.0-1.0)
    - Reputation history
    - Trustworthiness calculation
    """
    
    # Reputation event values
    EVENTS = {
        # Positive events
        'valid_message': 0.01,
        'valid_block': 0.02,
        'valid_transaction': 0.01,
        'sync_success': 0.015,
        'fast_response': 0.01,
        
        # Negative events
        'invalid_message': -0.05,
        'invalid_block': -0.1,
        'invalid_transaction': -0.05,
        'sync_failure': -0.03,
        'slow_response': -0.01,
        'timeout': -0.02,
        'duplicate_message': -0.01,
        'malformed_message': -0.05,
        'double_spend_attempt': -0.2,
        'consensus_violation': -0.15,
    }
    
    # Reputation decay
    DECAY_RATE = 0.001  # Per second
    DECAY_HALF_LIFE = 86400  # 1 day in seconds
    
    def __init__(self, node_id: int, initial_reputation: float = 0.5):
        """
        Initialize peer reputation.
        
        Args:
            node_id: Peer node ID
            initial_reputation: Initial reputation (0.0-1.0)
        """
        self.node_id = node_id
        self.reputation = max(0.0, min(1.0, initial_reputation))
        self.created_time = time.time()
        self.last_updated = time.time()
        
        # Event history (keep last 100 events)
        self.events: list = []
        self.max_events = 100
        
        # Statistics
        self.total_messages = 0
        self.valid_messages = 0
        self.invalid_messages = 0
        self.total_blocks = 0
        self.valid_blocks = 0
        self.invalid_blocks = 0
        
        logger.info(f"Peer reputation initialized (node_id={hex(node_id)}, reputation={self.reputation:.2f})")
    
    def add_event(self, event_type: str, reason: str = "") -> None:
        """
        Add reputation event.
        
        Args:
            event_type: Type of event (from EVENTS dict)
            reason: Reason for event
        """
        # Get event value
        value = self.EVENTS.get(event_type, 0.0)
        
        if value == 0.0:
            logger.warning(f"Unknown event type: {event_type}")
            return
        
        # Apply decay before updating
        self._apply_decay()
        
        # Create event
        event = ReputationEvent(
            event_type=event_type,
            value=value,
            reason=reason
        )
        
        # Add to history
        self.events.append(event)
        if len(self.events) > self.max_events:
            self.events.pop(0)
        
        # Update reputation
        self.reputation = max(0.0, min(1.0, self.reputation + value))
        self.last_updated = time.time()
        
        # Update statistics
        self._update_statistics(event_type)
        
        logger.debug(f"Reputation event: {event_type} ({value:+.3f}) -> {self.reputation:.2f}")
    
    def _apply_decay(self) -> None:
        """Apply time-based reputation decay."""
        current_time = time.time()
        time_elapsed = current_time - self.last_updated
        
        if time_elapsed <= 0:
            return
        
        # Exponential decay: reputation = reputation * e^(-decay_rate * time)
        decay_factor = math.exp(-self.DECAY_RATE * time_elapsed)
        
        # Decay towards 0.5 (neutral)
        neutral = 0.5
        self.reputation = neutral + (self.reputation - neutral) * decay_factor
        
        # Clamp to valid range
        self.reputation = max(0.0, min(1.0, self.reputation))
    
    def _update_statistics(self, event_type: str) -> None:
        """Update statistics based on event type."""
        if 'message' in event_type:
            self.total_messages += 1
            if 'invalid' in event_type or 'malformed' in event_type:
                self.invalid_messages += 1
            elif 'valid' in event_type:
                self.valid_messages += 1
        
        if 'block' in event_type:
            self.total_blocks += 1
            if 'invalid' in event_type:
                self.invalid_blocks += 1
            elif 'valid' in event_type:
                self.valid_blocks += 1
    
    def get_reputation(self) -> float:
        """
        Get current reputation with decay applied.
        
        Returns:
            Reputation (0.0-1.0)
        """
        self._apply_decay()
        return self.reputation
    
    def get_trustworthiness(self) -> float:
        """
        Get trustworthiness score (0.0-1.0).
        
        Combines reputation with message/block validity statistics.
        
        Returns:
            Trustworthiness (0.0-1.0)
        """
        # Base trustworthiness from reputation
        trust = self.get_reputation()
        
        # Adjust based on message validity
        if self.total_messages > 0:
            message_validity = self.valid_messages / self.total_messages
            trust = trust * 0.7 + message_validity * 0.3
        
        # Adjust based on block validity
        if self.total_blocks > 0:
            block_validity = self.valid_blocks / self.total_blocks
            trust = trust * 0.7 + block_validity * 0.3
        
        return max(0.0, min(1.0, trust))
    
    def is_trustworthy(self, threshold: float = 0.5) -> bool:
        """
        Check if peer is trustworthy.
        
        Args:
            threshold: Trustworthiness threshold (0.0-1.0)
        
        Returns:
            True if trustworthy
        """
        return self.get_trustworthiness() >= threshold
    
    def get_quality_rating(self) -> str:
        """
        Get human-readable quality rating.
        
        Returns:
            Quality rating string
        """
        trust = self.get_trustworthiness()
        
        if trust >= 0.9:
            return "Excellent"
        elif trust >= 0.7:
            return "Good"
        elif trust >= 0.5:
            return "Fair"
        elif trust >= 0.3:
            return "Poor"
        else:
            return "Very Poor"
    
    def get_stats(self) -> Dict[str, any]:
        """
        Get reputation statistics.
        
        Returns:
            Dictionary of statistics
        """
        message_validity = 0.0
        if self.total_messages > 0:
            message_validity = self.valid_messages / self.total_messages
        
        block_validity = 0.0
        if self.total_blocks > 0:
            block_validity = self.valid_blocks / self.total_blocks
        
        return {
            'node_id': hex(self.node_id),
            'reputation': f"{self.get_reputation():.3f}",
            'trustworthiness': f"{self.get_trustworthiness():.3f}",
            'quality_rating': self.get_quality_rating(),
            'total_messages': self.total_messages,
            'valid_messages': self.valid_messages,
            'invalid_messages': self.invalid_messages,
            'message_validity': f"{message_validity:.2%}",
            'total_blocks': self.total_blocks,
            'valid_blocks': self.valid_blocks,
            'invalid_blocks': self.invalid_blocks,
            'block_validity': f"{block_validity:.2%}",
            'recent_events': len(self.events),
            'age_seconds': int(time.time() - self.created_time)
        }


class ReputationManager:
    """
    Manages reputation for all peers.
    
    Features:
    - Per-peer reputation tracking
    - Reputation aggregation
    - Peer ranking
    - Reputation persistence
    """
    
    def __init__(self):
        """Initialize reputation manager."""
        self.peers: Dict[int, PeerReputation] = {}
        self.peers_lock = __import__('threading').Lock()
        logger.info("Reputation manager initialized")
    
    def get_peer_reputation(self, node_id: int) -> PeerReputation:
        """
        Get or create peer reputation.
        
        Args:
            node_id: Peer node ID
        
        Returns:
            PeerReputation object
        """
        with self.peers_lock:
            if node_id not in self.peers:
                self.peers[node_id] = PeerReputation(node_id)
            return self.peers[node_id]
    
    def add_event(self, node_id: int, event_type: str, reason: str = "") -> None:
        """
        Add reputation event for peer.
        
        Args:
            node_id: Peer node ID
            event_type: Type of event
            reason: Reason for event
        """
        reputation = self.get_peer_reputation(node_id)
        reputation.add_event(event_type, reason)
    
    def get_trustworthy_peers(self, threshold: float = 0.5, limit: int = None) -> list:
        """
        Get list of trustworthy peers, sorted by trustworthiness.
        
        Args:
            threshold: Minimum trustworthiness threshold
            limit: Maximum number of peers to return
        
        Returns:
            List of (node_id, trustworthiness) tuples
        """
        with self.peers_lock:
            trustworthy = [
                (node_id, rep.get_trustworthiness())
                for node_id, rep in self.peers.items()
                if rep.get_trustworthiness() >= threshold
            ]
        
        # Sort by trustworthiness (descending)
        trustworthy.sort(key=lambda x: x[1], reverse=True)
        
        if limit:
            trustworthy = trustworthy[:limit]
        
        return trustworthy
    
    def get_peer_stats(self, node_id: int) -> Dict[str, any]:
        """
        Get statistics for peer.
        
        Args:
            node_id: Peer node ID
        
        Returns:
            Dictionary of statistics
        """
        reputation = self.get_peer_reputation(node_id)
        return reputation.get_stats()
    
    def get_all_stats(self) -> Dict[int, Dict[str, any]]:
        """
        Get statistics for all peers.
        
        Returns:
            Dictionary mapping node_id to statistics
        """
        with self.peers_lock:
            return {
                node_id: rep.get_stats()
                for node_id, rep in self.peers.items()
            }
    
    def get_average_reputation(self) -> float:
        """
        Get average reputation across all peers.
        
        Returns:
            Average reputation (0.0-1.0)
        """
        with self.peers_lock:
            if not self.peers:
                return 0.5
            
            total = sum(rep.get_reputation() for rep in self.peers.values())
            return total / len(self.peers)
    
    def cleanup_old_peers(self, max_age_seconds: float = 2592000) -> int:
        """
        Remove peers that haven't been updated in a long time.
        
        Args:
            max_age_seconds: Maximum age in seconds (default: 30 days)
        
        Returns:
            Number of peers removed
        """
        current_time = time.time()
        removed = 0
        
        with self.peers_lock:
            to_remove = [
                node_id for node_id, rep in self.peers.items()
                if (current_time - rep.last_updated) > max_age_seconds
            ]
            
            for node_id in to_remove:
                del self.peers[node_id]
                removed += 1
        
        if removed > 0:
            logger.info(f"Cleaned up {removed} old peer reputation records")
        
        return removed


# Global instance
_manager = None


def get_manager() -> ReputationManager:
    """Get global reputation manager instance."""
    global _manager
    if _manager is None:
        _manager = ReputationManager()
    return _manager
