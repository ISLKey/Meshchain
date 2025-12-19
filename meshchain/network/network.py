"""
MeshChain Network Module - Meshtastic MQTT Integration

This module handles all network communication for MeshChain using Meshtastic's
MQTT infrastructure. It enables:
1. Connection to Meshtastic MQTT broker
2. Message serialization and deserialization
3. Block and transaction broadcasting
4. Peer discovery and management
5. Blockchain synchronization

Key Components:
1. MeshtasticNetwork: Main network interface
2. NetworkMessage: Message wrapper for blockchain data
3. MessageSerializer: Serializes/deserializes messages
4. PeerInfo: Information about network peers
5. NetworkStats: Network statistics
"""

import json
import struct
import time
import threading
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field, asdict
from enum import IntEnum
import hashlib

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None


class MessageType(IntEnum):
    """Message types for blockchain network communication."""
    TRANSACTION = 0      # Individual transaction
    BLOCK = 1            # Block proposal
    BLOCK_ACK = 2        # Block acknowledgment
    SYNC_REQUEST = 3     # Request blockchain sync
    SYNC_RESPONSE = 4    # Sync response with blocks
    PEER_HELLO = 5       # Peer discovery message
    PEER_INFO = 6        # Peer information
    STATUS = 7           # Node status update


@dataclass
class PeerInfo:
    """Information about a network peer."""
    node_id: bytes              # 8-byte node identifier
    mqtt_topic: str             # MQTT topic for this peer
    last_seen: float = 0.0      # Timestamp of last message
    block_height: int = 0       # Last known block height
    stake: int = 0              # Validator stake
    hop_distance: int = 255     # Hops from us (255 = unknown)
    is_validator: bool = False  # Whether peer is a validator
    version: str = "1.0"        # Protocol version
    
    def is_stale(self, timeout: float = 300.0) -> bool:
        """Check if peer info is stale (not seen recently)."""
        return time.time() - self.last_seen > timeout
    
    def update_seen(self):
        """Update last seen timestamp."""
        self.last_seen = time.time()


@dataclass
class NetworkMessage:
    """Wrapper for blockchain messages over network."""
    message_type: MessageType
    sender_id: bytes              # 8-byte sender node ID
    timestamp: int                # Message timestamp
    sequence: int                 # Sequence number for ordering
    payload: bytes = b''          # Message payload
    hop_limit: int = 3            # Hops remaining (for LoRa)
    
    def serialize(self) -> bytes:
        """
        Serialize message to compact binary format.
        
        Format:
        - message_type (1 byte)
        - sender_id (8 bytes)
        - timestamp (4 bytes)
        - sequence (4 bytes)
        - hop_limit (1 byte)
        - payload_length (2 bytes)
        - payload (variable)
        
        Total overhead: 20 bytes
        """
        data = bytearray()
        
        # Message type (1 byte)
        data.append(self.message_type)
        
        # Sender ID (8 bytes)
        data.extend(self.sender_id)
        
        # Timestamp (4 bytes, big-endian)
        data.extend(struct.pack('>I', self.timestamp & 0xFFFFFFFF))
        
        # Sequence (4 bytes, big-endian)
        data.extend(struct.pack('>I', self.sequence & 0xFFFFFFFF))
        
        # Hop limit (1 byte)
        data.append(self.hop_limit)
        
        # Payload length (2 bytes, big-endian)
        payload_len = len(self.payload)
        data.extend(struct.pack('>H', payload_len))
        
        # Payload
        data.extend(self.payload)
        
        return bytes(data)
    
    @staticmethod
    def deserialize(data: bytes) -> Optional['NetworkMessage']:
        """
        Deserialize message from binary format.
        
        Args:
            data: Binary message data
            
        Returns:
            NetworkMessage or None if invalid
        """
        if len(data) < 20:
            return None
        
        try:
            offset = 0
            
            # Message type (1 byte)
            message_type = MessageType(data[offset])
            offset += 1
            
            # Sender ID (8 bytes)
            sender_id = data[offset:offset+8]
            offset += 8
            
            # Timestamp (4 bytes)
            timestamp = struct.unpack('>I', data[offset:offset+4])[0]
            offset += 4
            
            # Sequence (4 bytes)
            sequence = struct.unpack('>I', data[offset:offset+4])[0]
            offset += 4
            
            # Hop limit (1 byte)
            hop_limit = data[offset]
            offset += 1
            
            # Payload length (2 bytes)
            payload_len = struct.unpack('>H', data[offset:offset+2])[0]
            offset += 2
            
            # Payload
            payload = data[offset:offset+payload_len]
            
            return NetworkMessage(
                message_type=message_type,
                sender_id=sender_id,
                timestamp=timestamp,
                sequence=sequence,
                payload=payload,
                hop_limit=hop_limit
            )
            
        except (struct.error, ValueError):
            return None


@dataclass
class NetworkStats:
    """Network statistics."""
    messages_sent: int = 0
    messages_received: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    peer_count: int = 0
    blocks_synced: int = 0
    transactions_propagated: int = 0
    last_block_time: float = 0.0
    connection_uptime: float = 0.0
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        return {
            'messages_sent': self.messages_sent,
            'messages_received': self.messages_received,
            'bytes_sent': self.bytes_sent,
            'bytes_received': self.bytes_received,
            'peer_count': self.peer_count,
            'blocks_synced': self.blocks_synced,
            'transactions_propagated': self.transactions_propagated,
            'avg_block_time': self.last_block_time,
            'uptime_seconds': self.connection_uptime
        }


class MessageSerializer:
    """Serializes and deserializes blockchain messages."""
    
    @staticmethod
    def serialize_transaction(tx_data: bytes) -> bytes:
        """
        Wrap transaction in network message.
        
        Args:
            tx_data: Serialized transaction
            
        Returns:
            Network message with transaction
        """
        # Create network message
        msg = NetworkMessage(
            message_type=MessageType.TRANSACTION,
            sender_id=b'\x00' * 8,  # Will be set by sender
            timestamp=int(time.time()),
            sequence=0,  # Will be set by sender
            payload=tx_data,
            hop_limit=3
        )
        
        return msg.serialize()
    
    @staticmethod
    def serialize_block(block_data: bytes, sender_id: bytes) -> bytes:
        """
        Wrap block in network message.
        
        Args:
            block_data: Serialized block
            sender_id: Sender's node ID
            
        Returns:
            Network message with block
        """
        msg = NetworkMessage(
            message_type=MessageType.BLOCK,
            sender_id=sender_id,
            timestamp=int(time.time()),
            sequence=0,
            payload=block_data,
            hop_limit=3
        )
        
        return msg.serialize()
    
    @staticmethod
    def serialize_sync_request(from_height: int, to_height: int) -> bytes:
        """
        Create sync request message.
        
        Args:
            from_height: Starting block height
            to_height: Ending block height
            
        Returns:
            Serialized sync request
        """
        payload = struct.pack('>II', from_height, to_height)
        
        msg = NetworkMessage(
            message_type=MessageType.SYNC_REQUEST,
            sender_id=b'\x00' * 8,
            timestamp=int(time.time()),
            sequence=0,
            payload=payload,
            hop_limit=3
        )
        
        return msg.serialize()
    
    @staticmethod
    def serialize_peer_hello(node_id: bytes, block_height: int, 
                           stake: int) -> bytes:
        """
        Create peer discovery message.
        
        Args:
            node_id: This node's ID
            block_height: Current block height
            stake: Validator stake
            
        Returns:
            Serialized peer hello
        """
        payload = struct.pack('>8sII', node_id, block_height, stake)
        
        msg = NetworkMessage(
            message_type=MessageType.PEER_HELLO,
            sender_id=node_id,
            timestamp=int(time.time()),
            sequence=0,
            payload=payload,
            hop_limit=3
        )
        
        return msg.serialize()


class MeshtasticNetwork:
    """
    Main network interface for MeshChain using Meshtastic MQTT.
    
    Handles:
    1. MQTT connection and message handling
    2. Block and transaction broadcasting
    3. Peer discovery and management
    4. Blockchain synchronization
    """
    
    def __init__(self, node_id: bytes, mqtt_broker: str = "localhost",
                 mqtt_port: int = 1883, region: str = "US"):
        """
        Initialize network interface.
        
        Args:
            node_id: This node's 8-byte identifier
            mqtt_broker: MQTT broker address
            mqtt_port: MQTT broker port
            region: Meshtastic region (US, EU, etc.)
        """
        self.node_id = node_id
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.region = region
        
        # MQTT client
        self.client = None
        self.connected = False
        
        # Peers
        self.peers: Dict[bytes, PeerInfo] = {}
        
        # Message handlers
        self.message_handlers: Dict[MessageType, List[Callable]] = {
            msg_type: [] for msg_type in MessageType
        }
        
        # Network statistics
        self.stats = NetworkStats()
        
        # Message sequence counter
        self.sequence = 0
        self.sequence_lock = threading.Lock()
        
        # MQTT topics
        self.base_topic = f"msh/{region}/2/e/LongFast"
        self.broadcast_topic = f"{self.base_topic}/!ffffffff"  # Broadcast
        self.node_topic = f"{self.base_topic}/!{node_id.hex()}"
        
        # Connection time
        self.connection_time = 0.0
    
    def connect(self) -> bool:
        """
        Connect to MQTT broker.
        
        Returns:
            True if connection successful, False otherwise
        """
        if mqtt is None:
            print("ERROR: paho-mqtt not installed. Install with: pip install paho-mqtt")
            return False
        
        try:
            self.client = mqtt.Client(
                client_id=f"meshchain_{self.node_id.hex()}",
                clean_session=True
            )
            
            # Set callbacks
            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_message = self._on_message
            
            # Connect
            self.client.connect(self.mqtt_broker, self.mqtt_port, keepalive=60)
            
            # Start network loop
            self.client.loop_start()
            
            # Wait for connection
            timeout = 5.0
            start = time.time()
            while not self.connected and (time.time() - start) < timeout:
                time.sleep(0.1)
            
            if self.connected:
                self.connection_time = time.time()
                print(f"Connected to MQTT broker at {self.mqtt_broker}:{self.mqtt_port}")
                return True
            else:
                print(f"Failed to connect to MQTT broker")
                return False
                
        except Exception as e:
            print(f"Connection error: {str(e)}")
            return False
    
    def disconnect(self):
        """Disconnect from MQTT broker."""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            self.connected = False
    
    def _on_connect(self, client, userdata, flags, rc):
        """MQTT connect callback."""
        if rc == 0:
            self.connected = True
            
            # Subscribe to broadcast topic
            client.subscribe(self.broadcast_topic)
            
            # Subscribe to node-specific topic
            client.subscribe(self.node_topic)
            
            print(f"MQTT connected (rc={rc})")
        else:
            print(f"MQTT connection failed (rc={rc})")
    
    def _on_disconnect(self, client, userdata, rc):
        """MQTT disconnect callback."""
        self.connected = False
        if rc != 0:
            print(f"MQTT disconnected unexpectedly (rc={rc})")
    
    def _on_message(self, client, userdata, msg):
        """MQTT message callback."""
        try:
            # Deserialize message
            network_msg = NetworkMessage.deserialize(msg.payload)
            
            if not network_msg:
                return
            
            # Update statistics
            self.stats.messages_received += 1
            self.stats.bytes_received += len(msg.payload)
            
            # Update peer info
            self._update_peer(network_msg.sender_id)
            
            # Call handlers for this message type
            for handler in self.message_handlers.get(network_msg.message_type, []):
                try:
                    handler(network_msg)
                except Exception as e:
                    print(f"Handler error: {str(e)}")
                    
        except Exception as e:
            print(f"Message processing error: {str(e)}")
    
    def broadcast_block(self, block_data: bytes) -> bool:
        """
        Broadcast a block to all peers.
        
        Args:
            block_data: Serialized block data
            
        Returns:
            True if broadcast successful, False otherwise
        """
        if not self.connected:
            return False
        
        try:
            # Serialize message
            msg_data = MessageSerializer.serialize_block(block_data, self.node_id)
            
            # Publish to broadcast topic
            result = self.client.publish(
                self.broadcast_topic,
                msg_data,
                qos=1
            )
            
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                self.stats.messages_sent += 1
                self.stats.bytes_sent += len(msg_data)
                self.stats.blocks_synced += 1
                self.stats.last_block_time = time.time()
                return True
            else:
                return False
                
        except Exception as e:
            print(f"Broadcast error: {str(e)}")
            return False
    
    def broadcast_transaction(self, tx_data: bytes) -> bool:
        """
        Broadcast a transaction to all peers.
        
        Args:
            tx_data: Serialized transaction data
            
        Returns:
            True if broadcast successful, False otherwise
        """
        if not self.connected:
            return False
        
        try:
            # Serialize message
            msg_data = MessageSerializer.serialize_transaction(tx_data)
            
            # Publish to broadcast topic
            result = self.client.publish(
                self.broadcast_topic,
                msg_data,
                qos=1
            )
            
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                self.stats.messages_sent += 1
                self.stats.bytes_sent += len(msg_data)
                self.stats.transactions_propagated += 1
                return True
            else:
                return False
                
        except Exception as e:
            print(f"Broadcast error: {str(e)}")
            return False
    
    def request_sync(self, from_height: int, to_height: int) -> bool:
        """
        Request blockchain synchronization from peers.
        
        Args:
            from_height: Starting block height
            to_height: Ending block height
            
        Returns:
            True if request sent successfully, False otherwise
        """
        if not self.connected:
            return False
        
        try:
            msg_data = MessageSerializer.serialize_sync_request(from_height, to_height)
            
            result = self.client.publish(
                self.broadcast_topic,
                msg_data,
                qos=1
            )
            
            return result.rc == mqtt.MQTT_ERR_SUCCESS
            
        except Exception as e:
            print(f"Sync request error: {str(e)}")
            return False
    
    def announce_peer(self, block_height: int, stake: int) -> bool:
        """
        Announce this node to the network.
        
        Args:
            block_height: Current block height
            stake: Validator stake
            
        Returns:
            True if announcement sent successfully, False otherwise
        """
        if not self.connected:
            return False
        
        try:
            msg_data = MessageSerializer.serialize_peer_hello(
                self.node_id, block_height, stake
            )
            
            result = self.client.publish(
                self.broadcast_topic,
                msg_data,
                qos=1
            )
            
            return result.rc == mqtt.MQTT_ERR_SUCCESS
            
        except Exception as e:
            print(f"Peer announcement error: {str(e)}")
            return False
    
    def register_handler(self, message_type: MessageType, 
                        handler: Callable[[NetworkMessage], None]):
        """
        Register a handler for a message type.
        
        Args:
            message_type: Type of message to handle
            handler: Callable that processes the message
        """
        self.message_handlers[message_type].append(handler)
    
    def _update_peer(self, node_id: bytes):
        """Update peer information."""
        if node_id not in self.peers:
            self.peers[node_id] = PeerInfo(
                node_id=node_id,
                mqtt_topic=f"{self.base_topic}/!{node_id.hex()}"
            )
        
        self.peers[node_id].update_seen()
    
    def get_peers(self) -> List[PeerInfo]:
        """Get list of known peers."""
        return list(self.peers.values())
    
    def get_active_peers(self, timeout: float = 300.0) -> List[PeerInfo]:
        """
        Get list of active peers (seen recently).
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            List of active peers
        """
        return [p for p in self.peers.values() if not p.is_stale(timeout)]
    
    def get_validator_peers(self) -> List[PeerInfo]:
        """Get list of validator peers."""
        return [p for p in self.peers.values() if p.is_validator]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get network statistics."""
        uptime = time.time() - self.connection_time if self.connection_time else 0
        self.stats.connection_uptime = uptime
        self.stats.peer_count = len(self.get_active_peers())
        
        return self.stats.get_summary()
    
    def is_connected(self) -> bool:
        """Check if connected to MQTT broker."""
        return self.connected


class NetworkManager:
    """
    High-level network manager for MeshChain.
    
    Provides simplified interface for:
    1. Network operations
    2. Message handling
    3. Peer management
    4. Synchronization
    """
    
    def __init__(self, node_id: bytes, mqtt_broker: str = "localhost",
                 mqtt_port: int = 1883):
        """
        Initialize network manager.
        
        Args:
            node_id: This node's 8-byte identifier
            mqtt_broker: MQTT broker address
            mqtt_port: MQTT broker port
        """
        self.network = MeshtasticNetwork(node_id, mqtt_broker, mqtt_port)
        self.node_id = node_id
        
        # Callbacks for blockchain events
        self.on_block_received: Optional[Callable] = None
        self.on_transaction_received: Optional[Callable] = None
        self.on_sync_requested: Optional[Callable] = None
        self.on_peer_discovered: Optional[Callable] = None
        
        # Register default handlers
        self._register_handlers()
    
    def _register_handlers(self):
        """Register message handlers."""
        self.network.register_handler(
            MessageType.BLOCK,
            self._handle_block
        )
        self.network.register_handler(
            MessageType.TRANSACTION,
            self._handle_transaction
        )
        self.network.register_handler(
            MessageType.SYNC_REQUEST,
            self._handle_sync_request
        )
        self.network.register_handler(
            MessageType.PEER_HELLO,
            self._handle_peer_hello
        )
    
    def _handle_block(self, msg: NetworkMessage):
        """Handle incoming block."""
        if self.on_block_received:
            self.on_block_received(msg.payload, msg.sender_id)
    
    def _handle_transaction(self, msg: NetworkMessage):
        """Handle incoming transaction."""
        if self.on_transaction_received:
            self.on_transaction_received(msg.payload, msg.sender_id)
    
    def _handle_sync_request(self, msg: NetworkMessage):
        """Handle sync request."""
        if self.on_sync_requested:
            from_height, to_height = struct.unpack('>II', msg.payload)
            self.on_sync_requested(from_height, to_height, msg.sender_id)
    
    def _handle_peer_hello(self, msg: NetworkMessage):
        """Handle peer discovery."""
        if self.on_peer_discovered:
            node_id, block_height, stake = struct.unpack('>8sII', msg.payload)
            self.on_peer_discovered(node_id, block_height, stake)
    
    def connect(self) -> bool:
        """Connect to network."""
        return self.network.connect()
    
    def disconnect(self):
        """Disconnect from network."""
        self.network.disconnect()
    
    def broadcast_block(self, block_data: bytes) -> bool:
        """Broadcast block."""
        return self.network.broadcast_block(block_data)
    
    def broadcast_transaction(self, tx_data: bytes) -> bool:
        """Broadcast transaction."""
        return self.network.broadcast_transaction(tx_data)
    
    def request_sync(self, from_height: int, to_height: int) -> bool:
        """Request synchronization."""
        return self.network.request_sync(from_height, to_height)
    
    def announce(self, block_height: int, stake: int) -> bool:
        """Announce this node."""
        return self.network.announce_peer(block_height, stake)
    
    def get_peers(self) -> List[PeerInfo]:
        """Get known peers."""
        return self.network.get_peers()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get network statistics."""
        return self.network.get_statistics()
    
    def is_connected(self) -> bool:
        """Check if connected."""
        return self.network.is_connected()
