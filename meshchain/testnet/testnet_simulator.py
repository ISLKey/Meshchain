"""
Testnet Multi-Device Simulator

Implements:
1. Virtual device simulation
2. Network simulation
3. Message routing simulation
4. Consensus simulation
5. Stress testing

This module provides a simulator for testing testnet
behavior without physical hardware.
"""

import time
import logging
import random
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimulationEventType(Enum):
    """Simulation event types."""
    DEVICE_START = "device_start"
    DEVICE_STOP = "device_stop"
    BLOCK_PROPOSED = "block_proposed"
    BLOCK_VALIDATED = "block_validated"
    TRANSACTION_RECEIVED = "transaction_received"
    PEER_DISCOVERED = "peer_discovered"
    PEER_DISCONNECTED = "peer_disconnected"
    CONSENSUS_REACHED = "consensus_reached"
    FORK_DETECTED = "fork_detected"
    ERROR = "error"


@dataclass
class SimulationEvent:
    """Single simulation event."""
    event_type: SimulationEventType
    timestamp: float
    device_id: str
    data: Dict[str, Any]


class VirtualDevice:
    """Simulates a single blockchain device."""
    
    def __init__(self, device_id: str, node_id: int, is_validator: bool = False):
        """
        Initialize virtual device.
        
        Args:
            device_id: Device identifier
            node_id: Node ID
            is_validator: Whether device is a validator
        """
        self.device_id = device_id
        self.node_id = node_id
        self.is_validator = is_validator
        
        self.is_running = False
        self.block_height = 0
        self.peers: set = set()
        self.blocks_proposed = 0
        self.blocks_validated = 0
        self.transactions_processed = 0
        
        logger.info(f"Virtual device created: {device_id} (validator={is_validator})")
    
    def start(self) -> None:
        """Start the device."""
        self.is_running = True
        logger.info(f"Device {self.device_id} started")
    
    def stop(self) -> None:
        """Stop the device."""
        self.is_running = False
        logger.info(f"Device {self.device_id} stopped")
    
    def add_peer(self, peer_id: int) -> None:
        """
        Add a peer.
        
        Args:
            peer_id: Peer node ID
        """
        self.peers.add(peer_id)
    
    def remove_peer(self, peer_id: int) -> None:
        """
        Remove a peer.
        
        Args:
            peer_id: Peer node ID
        """
        self.peers.discard(peer_id)
    
    def propose_block(self) -> Optional[Dict[str, Any]]:
        """
        Propose a block.
        
        Returns:
            Block data or None
        """
        if not self.is_running or not self.is_validator:
            return None
        
        block = {
            'height': self.block_height + 1,
            'proposer': self.node_id,
            'timestamp': int(time.time()),
            'transactions': random.randint(1, 10),
        }
        
        self.block_height += 1
        self.blocks_proposed += 1
        
        return block
    
    def validate_block(self) -> bool:
        """
        Validate a block.
        
        Returns:
            True if valid
        """
        # 95% chance of valid block
        return random.random() < 0.95
    
    def process_transaction(self) -> None:
        """Process a transaction."""
        self.transactions_processed += 1
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get device status.
        
        Returns:
            Status dictionary
        """
        return {
            'device_id': self.device_id,
            'node_id': hex(self.node_id),
            'is_running': self.is_running,
            'is_validator': self.is_validator,
            'block_height': self.block_height,
            'peers': len(self.peers),
            'blocks_proposed': self.blocks_proposed,
            'blocks_validated': self.blocks_validated,
            'transactions_processed': self.transactions_processed,
        }


class TestnetSimulator:
    """Simulates an entire testnet."""
    
    def __init__(self, name: str = "testnet-sim"):
        """
        Initialize testnet simulator.
        
        Args:
            name: Simulator name
        """
        self.name = name
        self.devices: Dict[str, VirtualDevice] = {}
        self.events: List[SimulationEvent] = []
        self.start_time = 0
        self.current_time = 0
        
        logger.info(f"Testnet simulator initialized: {name}")
    
    def add_device(self, device_id: str, node_id: int, is_validator: bool = False) -> VirtualDevice:
        """
        Add a device to the simulation.
        
        Args:
            device_id: Device identifier
            node_id: Node ID
            is_validator: Whether device is a validator
        
        Returns:
            Virtual device
        """
        device = VirtualDevice(device_id, node_id, is_validator)
        self.devices[device_id] = device
        return device
    
    def start_simulation(self) -> None:
        """Start the simulation."""
        self.start_time = time.time()
        self.current_time = self.start_time
        
        # Start all devices
        for device in self.devices.values():
            device.start()
            self._record_event(SimulationEventType.DEVICE_START, device.device_id, {})
        
        logger.info(f"Simulation started with {len(self.devices)} devices")
    
    def stop_simulation(self) -> None:
        """Stop the simulation."""
        # Stop all devices
        for device in self.devices.values():
            device.stop()
            self._record_event(SimulationEventType.DEVICE_STOP, device.device_id, {})
        
        logger.info(f"Simulation stopped. Total events: {len(self.events)}")
    
    def run_tick(self) -> None:
        """Run a single simulation tick."""
        self.current_time = time.time()
        
        # Simulate block proposals
        for device in self.devices.values():
            if device.is_validator and device.is_running:
                # 10% chance to propose a block each tick
                if random.random() < 0.1:
                    block = device.propose_block()
                    if block:
                        self._record_event(
                            SimulationEventType.BLOCK_PROPOSED,
                            device.device_id,
                            block
                        )
                        
                        # Broadcast to peers
                        for peer_device in self.devices.values():
                            if peer_device.device_id != device.device_id and peer_device.is_running:
                                if peer_device.validate_block():
                                    peer_device.blocks_validated += 1
                                    self._record_event(
                                        SimulationEventType.BLOCK_VALIDATED,
                                        peer_device.device_id,
                                        {'block_height': block['height']}
                                    )
        
        # Simulate transactions
        for device in self.devices.values():
            if device.is_running:
                # 5% chance to process a transaction each tick
                if random.random() < 0.05:
                    device.process_transaction()
                    self._record_event(
                        SimulationEventType.TRANSACTION_RECEIVED,
                        device.device_id,
                        {}
                    )
    
    def run_simulation(self, duration_seconds: int, tick_interval: float = 1.0) -> None:
        """
        Run the simulation for a specified duration.
        
        Args:
            duration_seconds: Duration in seconds
            tick_interval: Interval between ticks in seconds
        """
        self.start_simulation()
        
        end_time = time.time() + duration_seconds
        
        try:
            while time.time() < end_time:
                self.run_tick()
                time.sleep(tick_interval)
        finally:
            self.stop_simulation()
    
    def _record_event(self, event_type: SimulationEventType, device_id: str, data: Dict[str, Any]) -> None:
        """
        Record a simulation event.
        
        Args:
            event_type: Event type
            device_id: Device identifier
            data: Event data
        """
        event = SimulationEvent(
            event_type=event_type,
            timestamp=self.current_time,
            device_id=device_id,
            data=data
        )
        
        self.events.append(event)
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get simulation summary.
        
        Returns:
            Summary dictionary
        """
        total_blocks = sum(d.blocks_proposed for d in self.devices.values())
        total_transactions = sum(d.transactions_processed for d in self.devices.values())
        
        return {
            'name': self.name,
            'duration_seconds': self.current_time - self.start_time,
            'total_devices': len(self.devices),
            'total_events': len(self.events),
            'total_blocks': total_blocks,
            'total_transactions': total_transactions,
            'devices': [d.get_status() for d in self.devices.values()],
        }
    
    def get_event_summary(self) -> Dict[str, int]:
        """
        Get summary of events by type.
        
        Returns:
            Event count by type
        """
        summary = defaultdict(int)
        
        for event in self.events:
            summary[event.event_type.value] += 1
        
        return dict(summary)


class StressTestRunner:
    """Runs stress tests on the testnet simulator."""
    
    @staticmethod
    def test_5_node_network() -> Dict[str, Any]:
        """
        Test a 5-node network.
        
        Returns:
            Test results
        """
        logger.info("Running 5-node network stress test")
        
        simulator = TestnetSimulator("5-node-stress-test")
        
        # Add 5 validators
        for i in range(5):
            device_id = f"device-{i+1}"
            node_id = 0x11111111 + i
            simulator.add_device(device_id, node_id, is_validator=True)
        
        # Run simulation for 60 seconds
        simulator.run_simulation(duration_seconds=60, tick_interval=0.1)
        
        summary = simulator.get_summary()
        event_summary = simulator.get_event_summary()
        
        return {
            'test_name': '5-node-stress-test',
            'summary': summary,
            'event_summary': event_summary,
            'status': 'completed',
        }
    
    @staticmethod
    def test_6_node_network() -> Dict[str, Any]:
        """
        Test a 6-node network.
        
        Returns:
            Test results
        """
        logger.info("Running 6-node network stress test")
        
        simulator = TestnetSimulator("6-node-stress-test")
        
        # Add 6 validators
        for i in range(6):
            device_id = f"device-{i+1}"
            node_id = 0x11111111 + i
            simulator.add_device(device_id, node_id, is_validator=True)
        
        # Run simulation for 60 seconds
        simulator.run_simulation(duration_seconds=60, tick_interval=0.1)
        
        summary = simulator.get_summary()
        event_summary = simulator.get_event_summary()
        
        return {
            'test_name': '6-node-stress-test',
            'summary': summary,
            'event_summary': event_summary,
            'status': 'completed',
        }
