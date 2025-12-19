"""
MeshChain MicroNode - Core Node Implementation for ESP32

Lightweight node implementation that orchestrates all MeshChain components
for embedded systems. Designed to work with limited resources while
maintaining full blockchain functionality.

Key Components:
1. MicroNode: Main node class
2. NodeConfig: Configuration management
3. LifecycleManager: Startup/shutdown/recovery
4. StatusMonitor: Health monitoring and diagnostics
5. NodeMetrics: Performance tracking
"""

import time
import json
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field, asdict
from pathlib import Path
from enum import IntEnum
import threading

from meshchain.async_core import (
    EventLoop, EventType, Event, NodeState, Message
)
from meshchain.storage_esp32 import LiteDBStorage
from meshchain.consensus import ConsensusEngine, ValidatorRegistry
from meshchain.peer_manager import PeerManager
from meshchain.synchronizer import SyncManager
from meshchain.propagation import BlockPropagator, TransactionPropagator
from meshchain.validator import ChainValidator
from meshchain.wallet import WalletManager


@dataclass
class NodeConfig:
    """
    MicroNode configuration.
    
    Attributes:
        node_id: Unique node identifier (8 bytes)
        node_name: Human-readable node name
        role: Node role (validator, relay, light)
        stake: Amount of MESH staked (for validators)
        storage_path: Path to blockchain storage
        wallet_path: Path to wallet storage
        max_peers: Maximum number of peers
        max_block_size: Maximum block size in bytes
        block_time: Target block time in seconds
        sync_timeout: Synchronization timeout in seconds
    """
    node_id: bytes = b'\x00' * 8
    node_name: str = "MeshChain Node"
    role: str = "relay"  # validator, relay, light
    stake: int = 0
    storage_path: str = "/tmp/meshchain/blockchain"
    wallet_path: str = "/tmp/meshchain/wallets"
    max_peers: int = 20
    max_block_size: int = 1024
    block_time: int = 10
    sync_timeout: float = 300.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        config_dict = asdict(self)
        config_dict['node_id'] = self.node_id.hex()
        return config_dict
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeConfig':
        """Create from dictionary."""
        if isinstance(data.get('node_id'), str):
            data['node_id'] = bytes.fromhex(data['node_id'])
        return cls(**data)
    
    @classmethod
    def from_file(cls, path: str) -> 'NodeConfig':
        """Load configuration from file."""
        try:
            with open(path, 'r') as f:
                data = json.load(f)
                return cls.from_dict(data)
        except Exception as e:
            print(f"Error loading config: {e}")
            return cls()
    
    def save_to_file(self, path: str) -> bool:
        """Save configuration to file."""
        try:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w') as f:
                json.dump(self.to_dict(), f, indent=2)
            return True
        except Exception as e:
            print(f"Error saving config: {e}")
            return False


@dataclass
class NodeMetrics:
    """
    Node performance metrics.
    
    Attributes:
        uptime: How long node has been running
        blocks_processed: Number of blocks processed
        transactions_processed: Number of transactions processed
        peers_connected: Number of connected peers
        memory_used: Memory usage in bytes
        storage_used: Storage usage in bytes
    """
    uptime: float = 0.0
    blocks_processed: int = 0
    transactions_processed: int = 0
    peers_connected: int = 0
    memory_used: int = 0
    storage_used: int = 0
    last_block_time: float = 0.0
    last_sync_time: float = 0.0
    sync_progress: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class StatusMonitor:
    """
    Monitors node health and performance.
    
    Features:
    - Periodic health checks
    - Metrics collection
    - Error tracking
    - Recovery suggestions
    """
    
    def __init__(self, node: 'MicroNode'):
        """
        Initialize status monitor.
        
        Args:
            node: MicroNode instance to monitor
        """
        self.node = node
        self.metrics = NodeMetrics()
        self.start_time = time.time()
        self.last_check = time.time()
        self.check_interval = 60.0  # Check every 60 seconds
        self.errors: List[Dict[str, Any]] = []
        self.lock = threading.Lock()
    
    def check_health(self) -> bool:
        """
        Perform health check.
        
        Returns:
            True if healthy, False if issues detected
        """
        with self.lock:
            current_time = time.time()
            
            # Update uptime
            self.metrics.uptime = current_time - self.start_time
            
            # Check storage
            try:
                stats = self.node.storage.get_statistics()
                self.metrics.storage_used = stats.get('blocks_stored', 0) * 500  # Estimate
            except Exception as e:
                self._record_error("storage_check", str(e))
                return False
            
            # Check peers
            try:
                peers = self.node.peer_manager.get_active_peers()
                self.metrics.peers_connected = len(peers)
            except Exception as e:
                self._record_error("peer_check", str(e))
            
            # Check event loop
            try:
                stats = self.node.event_loop.get_stats()
                if stats['errors'] > 0:
                    self._record_error("event_loop", f"{stats['errors']} errors")
            except Exception as e:
                self._record_error("event_loop_check", str(e))
            
            self.last_check = current_time
            
            # Consider healthy if no critical errors
            return len(self.errors) < 5
    
    def _record_error(self, error_type: str, message: str) -> None:
        """Record an error."""
        error = {
            'type': error_type,
            'message': message,
            'timestamp': time.time()
        }
        self.errors.append(error)
        
        # Keep only last 100 errors
        if len(self.errors) > 100:
            self.errors = self.errors[-100:]
    
    def get_metrics(self) -> NodeMetrics:
        """Get current metrics."""
        with self.lock:
            return NodeMetrics(**asdict(self.metrics))
    
    def get_status_report(self) -> Dict[str, Any]:
        """Get detailed status report."""
        with self.lock:
            return {
                'node_id': self.node.config.node_id.hex(),
                'node_name': self.node.config.node_name,
                'state': self.node.event_loop.state_manager.get_state().name,
                'metrics': self.metrics.to_dict(),
                'errors': self.errors[-10:],  # Last 10 errors
                'timestamp': time.time()
            }


class LifecycleManager:
    """
    Manages node startup, shutdown, and recovery.
    
    Features:
    - Graceful startup
    - Graceful shutdown
    - Error recovery
    - State persistence
    """
    
    def __init__(self, node: 'MicroNode'):
        """
        Initialize lifecycle manager.
        
        Args:
            node: MicroNode instance
        """
        self.node = node
        self.startup_time = None
        self.shutdown_time = None
        self.recovery_count = 0
        self.max_recovery_attempts = 3
    
    def startup(self) -> bool:
        """
        Perform node startup.
        
        Returns:
            True if successful, False if failed
        """
        try:
            print(f"[MicroNode] Starting {self.node.config.node_name}...")
            self.startup_time = time.time()
            
            # Initialize storage
            print("[MicroNode] Initializing storage...")
            self.node.storage = LiteDBStorage(self.node.config.storage_path)
            
            # Initialize consensus
            print("[MicroNode] Initializing consensus...")
            self.node.consensus = ConsensusEngine()
            
            # Initialize peer manager
            print("[MicroNode] Initializing peer manager...")
            self.node.peer_manager = PeerManager(self.node.config.node_id)
            
            # Initialize synchronizer
            print("[MicroNode] Initializing synchronizer...")
            self.node.synchronizer = SyncManager()
            
            # Initialize propagators
            print("[MicroNode] Initializing propagators...")
            self.node.block_propagator = BlockPropagator()
            self.node.tx_propagator = TransactionPropagator()
            
            # Initialize wallet manager
            print("[MicroNode] Initializing wallet manager...")
            self.node.wallet_manager = WalletManager(self.node.config.wallet_path)
            
            # Change state to waiting for peers
            self.node.event_loop.state_manager.set_state(NodeState.WAITING_PEERS)
            
            # Emit startup event
            self.node.event_loop.emit_event(Event(
                event_type=EventType.NODE_STARTED,
                source="lifecycle"
            ))
            
            print(f"[MicroNode] {self.node.config.node_name} started successfully")
            return True
        
        except Exception as e:
            print(f"[MicroNode] Startup failed: {e}")
            self.node.event_loop.emit_event(Event(
                event_type=EventType.NODE_ERROR,
                source="lifecycle",
                data={'error': str(e)}
            ))
            return False
    
    def shutdown(self) -> bool:
        """
        Perform graceful shutdown.
        
        Returns:
            True if successful
        """
        try:
            print(f"[MicroNode] Shutting down {self.node.config.node_name}...")
            self.shutdown_time = time.time()
            
            # Stop event loop
            self.node.event_loop.stop()
            
            # Close storage
            if self.node.storage:
                self.node.storage.close()
            
            # Emit shutdown event
            self.node.event_loop.emit_event(Event(
                event_type=EventType.NODE_STOPPED,
                source="lifecycle"
            ))
            
            print(f"[MicroNode] {self.node.config.node_name} shut down successfully")
            return True
        
        except Exception as e:
            print(f"[MicroNode] Shutdown error: {e}")
            return False
    
    def recover_from_error(self) -> bool:
        """
        Attempt to recover from error.
        
        Returns:
            True if recovery successful, False if max attempts exceeded
        """
        if self.recovery_count >= self.max_recovery_attempts:
            print(f"[MicroNode] Max recovery attempts ({self.max_recovery_attempts}) exceeded")
            return False
        
        self.recovery_count += 1
        print(f"[MicroNode] Recovery attempt {self.recovery_count}...")
        
        try:
            # Clear caches
            if self.node.storage:
                self.node.storage.clear_cache()
            
            # Reset state
            self.node.event_loop.state_manager.set_state(NodeState.WAITING_PEERS)
            
            # Emit recovery event
            self.node.event_loop.emit_event(Event(
                event_type=EventType.NODE_STARTED,
                source="lifecycle",
                data={'recovery': True}
            ))
            
            print(f"[MicroNode] Recovery attempt {self.recovery_count} successful")
            return True
        
        except Exception as e:
            print(f"[MicroNode] Recovery failed: {e}")
            return False


class MicroNode:
    """
    Lightweight MeshChain node for ESP32 devices.
    
    Orchestrates all blockchain components with minimal resource usage.
    
    Features:
    - Event-driven architecture
    - Async message processing
    - Lightweight storage
    - Peer management
    - Consensus participation
    - Wallet management
    """
    
    def __init__(self, config: Optional[NodeConfig] = None):
        """
        Initialize MicroNode.
        
        Args:
            config: NodeConfig instance (uses defaults if None)
        """
        self.config = config or NodeConfig()
        
        # Core components
        self.event_loop = EventLoop()
        self.storage: Optional[LiteDBStorage] = None
        self.consensus: Optional[ConsensusEngine] = None
        self.peer_manager: Optional[PeerManager] = None
        self.synchronizer: Optional[SyncManager] = None
        self.block_propagator: Optional[BlockPropagator] = None
        self.tx_propagator: Optional[TransactionPropagator] = None
        self.wallet_manager: Optional[WalletManager] = None
        self.validator: Optional[ChainValidator] = None
        
        # Management components
        self.lifecycle = LifecycleManager(self)
        self.status_monitor = StatusMonitor(self)
        
        # State
        self.running = False
        self.lock = threading.Lock()
    
    def start(self) -> bool:
        """
        Start the node.
        
        Returns:
            True if successful, False if failed
        """
        with self.lock:
            if self.running:
                print("[MicroNode] Node already running")
                return False
            
            # Perform startup
            if not self.lifecycle.startup():
                return False
            
            self.running = True
            
            # Schedule periodic health checks
            self.event_loop.task_scheduler.schedule(
                "health_check",
                self.status_monitor.check_health,
                interval=60.0
            )
            
            return True
    
    def stop(self) -> bool:
        """
        Stop the node.
        
        Returns:
            True if successful
        """
        with self.lock:
            if not self.running:
                return True
            
            self.running = False
            self.lifecycle.shutdown()
            
            return True
    
    def run(self, duration: float = 0.0) -> None:
        """
        Run the node event loop.
        
        Args:
            duration: How long to run (0 = until stopped)
        """
        if not self.running:
            print("[MicroNode] Node not started")
            return
        
        print(f"[MicroNode] Running event loop...")
        self.event_loop.run(duration=duration)
    
    def run_once(self) -> None:
        """Run one iteration of event loop."""
        if not self.running:
            return
        
        self.event_loop.run_once(timeout=0.1)
    
    def get_status(self) -> Dict[str, Any]:
        """Get node status."""
        return self.status_monitor.get_status_report()
    
    def get_metrics(self) -> NodeMetrics:
        """Get node metrics."""
        return self.status_monitor.get_metrics()
    
    def get_block_height(self) -> int:
        """Get current block height."""
        if self.storage:
            return self.storage.get_latest_block_height()
        return -1
    
    def get_peer_count(self) -> int:
        """Get number of connected peers."""
        if self.peer_manager:
            return len(self.peer_manager.get_active_peers())
        return 0
    
    def is_synced(self) -> bool:
        """Check if node is synchronized."""
        state = self.event_loop.state_manager.get_state()
        return state == NodeState.SYNCHRONIZED
    
    def is_validator(self) -> bool:
        """Check if node is a validator."""
        return self.config.role == "validator" and self.config.stake > 0
    
    def emit_event(self, event_type: EventType, data: Optional[Dict] = None) -> None:
        """
        Emit an event.
        
        Args:
            event_type: Type of event
            data: Event data
        """
        event = Event(
            event_type=event_type,
            source="node",
            data=data or {}
        )
        self.event_loop.emit_event(event)
    
    def enqueue_message(self, message_type: str, data: Any) -> bool:
        """
        Enqueue a message for processing.
        
        Args:
            message_type: Type of message
            data: Message data
        
        Returns:
            True if successful
        """
        message = Message(message_type=message_type, data=data)
        return self.event_loop.enqueue_message(message)
    
    def register_event_handler(self, event_type: EventType, 
                              handler: Callable) -> None:
        """
        Register event handler.
        
        Args:
            event_type: Type of event to handle
            handler: Callback function
        """
        self.event_loop.register_handler(event_type, handler)
    
    def schedule_task(self, task_id: str, callback: Callable, 
                     interval: float = 0.0) -> bool:
        """
        Schedule a task.
        
        Args:
            task_id: Unique task identifier
            callback: Function to call
            interval: Interval in seconds (0 = one-time)
        
        Returns:
            True if successful
        """
        return self.event_loop.task_scheduler.schedule(task_id, callback, interval)
    
    def get_state(self) -> NodeState:
        """Get current node state."""
        return self.event_loop.state_manager.get_state()
    
    def set_state(self, state: NodeState) -> bool:
        """
        Set node state.
        
        Args:
            state: New state
        
        Returns:
            True if state changed
        """
        return self.event_loop.state_manager.set_state(state)
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"MicroNode(name={self.config.node_name}, "
            f"role={self.config.role}, "
            f"state={self.event_loop.state_manager.get_state().name})"
        )


# Example usage
if __name__ == "__main__":
    # Create configuration
    config = NodeConfig(
        node_id=b'\x01\x02\x03\x04\x05\x06\x07\x08',
        node_name="Test Node",
        role="validator",
        stake=1000
    )
    
    # Create and start node
    node = MicroNode(config)
    
    if node.start():
        print(f"Node started: {node}")
        
        # Run for a few seconds
        import time
        start = time.time()
        while time.time() - start < 5:
            node.run_once()
        
        # Get status
        status = node.get_status()
        print(f"Status: {json.dumps(status, indent=2)}")
        
        # Stop node
        node.stop()
        print("Node stopped")
    else:
        print("Failed to start node")
