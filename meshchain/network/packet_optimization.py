"""
Packet Optimization for LoRa Mesh Network

Optimizes blockchain messages to fit within Meshtastic's 237-byte packet limit:
1. Message compression
2. Field encoding optimization
3. Variable-length encoding
4. Payload batching
5. Compression algorithm selection
6. Size estimation

This module ensures blockchain messages can be efficiently transmitted
over low-bandwidth LoRa networks with 237-byte MTU.
"""

import struct
import zlib
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import IntEnum
import io

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CompressionMethod(IntEnum):
    """Compression methods."""
    NONE = 0
    ZLIB = 1
    LZ4 = 2  # For future use


@dataclass
class OptimizationStats:
    """Statistics for packet optimization."""
    original_size: int = 0
    optimized_size: int = 0
    compression_ratio: float = 0.0
    compression_method: CompressionMethod = CompressionMethod.NONE
    messages_optimized: int = 0
    bytes_saved: int = 0


class VariableLengthEncoder:
    """
    Encodes integers using variable-length encoding.
    
    Saves space for small integers:
    - 0-127: 1 byte
    - 128-16383: 2 bytes
    - 16384+: 4 bytes
    """
    
    @staticmethod
    def encode(value: int) -> bytes:
        """
        Encode integer using variable-length encoding.
        
        Args:
            value: Integer to encode
        
        Returns:
            Encoded bytes
        """
        if value < 0:
            raise ValueError("Cannot encode negative values")
        
        if value < 128:
            # 1 byte: 0xxxxxxx
            return bytes([value])
        elif value < 16384:
            # 2 bytes: 10xxxxxx xxxxxxxx
            b1 = 0x80 | ((value >> 8) & 0x3F)
            b2 = value & 0xFF
            return bytes([b1, b2])
        else:
            # 4 bytes: 11xxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
            b1 = 0xC0 | ((value >> 24) & 0x3F)
            b2 = (value >> 16) & 0xFF
            b3 = (value >> 8) & 0xFF
            b4 = value & 0xFF
            return bytes([b1, b2, b3, b4])
    
    @staticmethod
    def decode(data: bytes, offset: int = 0) -> Tuple[int, int]:
        """
        Decode variable-length integer.
        
        Args:
            data: Data to decode
            offset: Offset in data
        
        Returns:
            Tuple of (value, bytes_consumed)
        """
        if offset >= len(data):
            raise ValueError("Offset out of range")
        
        b1 = data[offset]
        
        if b1 < 128:
            # 1 byte
            return b1, 1
        elif b1 < 192:
            # 2 bytes
            if offset + 1 >= len(data):
                raise ValueError("Incomplete data")
            b2 = data[offset + 1]
            value = ((b1 & 0x3F) << 8) | b2
            return value, 2
        else:
            # 4 bytes
            if offset + 3 >= len(data):
                raise ValueError("Incomplete data")
            b2 = data[offset + 1]
            b3 = data[offset + 2]
            b4 = data[offset + 3]
            value = ((b1 & 0x3F) << 24) | (b2 << 16) | (b3 << 8) | b4
            return value, 4


class CompactMessageEncoder:
    """
    Encodes blockchain messages in compact format.
    
    Optimizations:
    - Variable-length integers
    - Bit-packed flags
    - Field reordering
    - Type-specific compression
    """
    
    # Message type codes (1 byte each)
    MSG_TRANSACTION = 0x01
    MSG_BLOCK = 0x02
    MSG_SYNC_REQUEST = 0x03
    MSG_SYNC_RESPONSE = 0x04
    MSG_PEER_INFO = 0x05
    MSG_ROUTE_UPDATE = 0x06
    
    @staticmethod
    def encode_transaction(tx: Dict[str, Any]) -> bytes:
        """
        Encode transaction in compact format.
        
        Args:
            tx: Transaction dictionary
        
        Returns:
            Encoded bytes
        """
        data = bytearray()
        
        # Message type
        data.append(CompactMessageEncoder.MSG_TRANSACTION)
        
        # Transaction hash (32 bytes, required)
        if 'hash' in tx:
            tx_hash = tx['hash']
            if isinstance(tx_hash, str):
                tx_hash = bytes.fromhex(tx_hash)
            data.extend(tx_hash[:32])
        
        # Sender (variable-length)
        if 'sender' in tx:
            sender = tx['sender']
            if isinstance(sender, str):
                sender = int(sender, 16) if sender.startswith('0x') else int(sender)
            data.extend(VariableLengthEncoder.encode(sender))
        
        # Receiver (variable-length)
        if 'receiver' in tx:
            receiver = tx['receiver']
            if isinstance(receiver, str):
                receiver = int(receiver, 16) if receiver.startswith('0x') else int(receiver)
            data.extend(VariableLengthEncoder.encode(receiver))
        
        # Amount (variable-length)
        if 'amount' in tx:
            amount = int(tx['amount'])
            data.extend(VariableLengthEncoder.encode(amount))
        
        # Fee (variable-length)
        if 'fee' in tx:
            fee = int(tx['fee'])
            data.extend(VariableLengthEncoder.encode(fee))
        
        # Nonce (variable-length)
        if 'nonce' in tx:
            nonce = int(tx['nonce'])
            data.extend(VariableLengthEncoder.encode(nonce))
        
        # Signature (variable-length length + data)
        if 'signature' in tx:
            sig = tx['signature']
            if isinstance(sig, str):
                sig = bytes.fromhex(sig)
            data.extend(VariableLengthEncoder.encode(len(sig)))
            data.extend(sig)
        
        return bytes(data)
    
    @staticmethod
    def encode_block(block: Dict[str, Any]) -> bytes:
        """
        Encode block in compact format.
        
        Args:
            block: Block dictionary
        
        Returns:
            Encoded bytes
        """
        data = bytearray()
        
        # Message type
        data.append(CompactMessageEncoder.MSG_BLOCK)
        
        # Block height (variable-length)
        if 'height' in block:
            height = int(block['height'])
            data.extend(VariableLengthEncoder.encode(height))
        
        # Block hash (32 bytes)
        if 'hash' in block:
            block_hash = block['hash']
            if isinstance(block_hash, str):
                block_hash = bytes.fromhex(block_hash)
            data.extend(block_hash[:32])
        
        # Parent hash (32 bytes)
        if 'parent_hash' in block:
            parent_hash = block['parent_hash']
            if isinstance(parent_hash, str):
                parent_hash = bytes.fromhex(parent_hash)
            data.extend(parent_hash[:32])
        
        # Timestamp (4 bytes)
        if 'timestamp' in block:
            timestamp = int(block['timestamp'])
            data.extend(struct.pack('<I', timestamp & 0xFFFFFFFF))
        
        # Proposer (variable-length)
        if 'proposer' in block:
            proposer = block['proposer']
            if isinstance(proposer, str):
                proposer = int(proposer, 16) if proposer.startswith('0x') else int(proposer)
            data.extend(VariableLengthEncoder.encode(proposer))
        
        # Transaction count (variable-length)
        if 'transactions' in block:
            tx_count = len(block['transactions'])
            data.extend(VariableLengthEncoder.encode(tx_count))
        
        return bytes(data)
    
    @staticmethod
    def encode_sync_request(request: Dict[str, Any]) -> bytes:
        """
        Encode sync request in compact format.
        
        Args:
            request: Sync request dictionary
        
        Returns:
            Encoded bytes
        """
        data = bytearray()
        
        # Message type
        data.append(CompactMessageEncoder.MSG_SYNC_REQUEST)
        
        # From height (variable-length)
        if 'from_height' in request:
            from_height = int(request['from_height'])
            data.extend(VariableLengthEncoder.encode(from_height))
        
        # To height (variable-length)
        if 'to_height' in request:
            to_height = int(request['to_height'])
            data.extend(VariableLengthEncoder.encode(to_height))
        
        return bytes(data)
    
    @staticmethod
    def encode_peer_info(peer_info: Dict[str, Any]) -> bytes:
        """
        Encode peer info in compact format.
        
        Args:
            peer_info: Peer info dictionary
        
        Returns:
            Encoded bytes
        """
        data = bytearray()
        
        # Message type
        data.append(CompactMessageEncoder.MSG_PEER_INFO)
        
        # Node ID (variable-length)
        if 'node_id' in peer_info:
            node_id = peer_info['node_id']
            if isinstance(node_id, str):
                node_id = int(node_id, 16) if node_id.startswith('0x') else int(node_id)
            data.extend(VariableLengthEncoder.encode(node_id))
        
        # Block height (variable-length)
        if 'block_height' in peer_info:
            height = int(peer_info['block_height'])
            data.extend(VariableLengthEncoder.encode(height))
        
        # Stake (variable-length)
        if 'stake' in peer_info:
            stake = int(peer_info['stake'])
            data.extend(VariableLengthEncoder.encode(stake))
        
        # Hop distance (1 byte)
        if 'hop_distance' in peer_info:
            hop_distance = int(peer_info['hop_distance'])
            data.append(min(hop_distance, 255))
        
        # Flags (1 byte)
        flags = 0
        if peer_info.get('is_validator'):
            flags |= 0x01
        data.append(flags)
        
        return bytes(data)


class PacketOptimizer:
    """
    Optimizes packets for LoRa transmission.
    
    Features:
    - Automatic compression
    - Size estimation
    - Compression method selection
    - Statistics tracking
    """
    
    # Meshtastic MTU (Maximum Transmission Unit)
    MESHTASTIC_MTU = 237
    
    # Overhead for Meshtastic header
    MESHTASTIC_HEADER = 20  # Estimated header size
    
    # Effective payload size
    EFFECTIVE_MTU = MESHTASTIC_MTU - MESHTASTIC_HEADER
    
    def __init__(self):
        """Initialize packet optimizer."""
        self.stats = OptimizationStats()
        logger.info(f"Packet optimizer initialized (MTU={self.MESHTASTIC_MTU}, effective={self.EFFECTIVE_MTU})")
    
    def optimize_message(self, message: Dict[str, Any]) -> Tuple[bytes, CompressionMethod]:
        """
        Optimize message for transmission.
        
        Args:
            message: Message to optimize
        
        Returns:
            Tuple of (optimized_data, compression_method)
        """
        msg_type = message.get('type')
        
        # Encode message based on type
        if msg_type == 'transaction':
            encoded = CompactMessageEncoder.encode_transaction(message)
        elif msg_type == 'block':
            encoded = CompactMessageEncoder.encode_block(message)
        elif msg_type == 'sync_request':
            encoded = CompactMessageEncoder.encode_sync_request(message)
        elif msg_type == 'peer_info':
            encoded = CompactMessageEncoder.encode_peer_info(message)
        else:
            # Fallback: use JSON encoding
            import json
            encoded = json.dumps(message).encode('utf-8')
        
        # Try compression if message is large
        compression_method = CompressionMethod.NONE
        compressed = encoded
        
        if len(encoded) > self.EFFECTIVE_MTU * 0.7:
            # Try ZLIB compression
            try:
                zlib_compressed = zlib.compress(encoded, level=6)
                
                # Add compression header (1 byte)
                compressed_with_header = bytes([CompressionMethod.ZLIB]) + zlib_compressed
                
                # Use compression if it saves space
                if len(compressed_with_header) < len(encoded):
                    compressed = compressed_with_header
                    compression_method = CompressionMethod.ZLIB
            except:
                pass
        
        # Update statistics
        self.stats.original_size = len(encoded)
        self.stats.optimized_size = len(compressed)
        self.stats.compression_method = compression_method
        self.stats.messages_optimized += 1
        
        if len(compressed) < len(encoded):
            self.stats.bytes_saved += len(encoded) - len(compressed)
            self.stats.compression_ratio = 1.0 - (len(compressed) / len(encoded))
        
        return compressed, compression_method
    
    def estimate_size(self, message: Dict[str, Any]) -> int:
        """
        Estimate optimized message size.
        
        Args:
            message: Message to estimate
        
        Returns:
            Estimated size in bytes
        """
        msg_type = message.get('type')
        
        # Estimate based on message type
        if msg_type == 'transaction':
            # Type (1) + hash (32) + sender (4) + receiver (4) + amount (4) + fee (2) + nonce (2) + sig (65)
            return 1 + 32 + 4 + 4 + 4 + 2 + 2 + 65
        elif msg_type == 'block':
            # Type (1) + height (4) + hash (32) + parent_hash (32) + timestamp (4) + proposer (4) + tx_count (2)
            return 1 + 4 + 32 + 32 + 4 + 4 + 2
        elif msg_type == 'sync_request':
            # Type (1) + from_height (4) + to_height (4)
            return 1 + 4 + 4
        elif msg_type == 'peer_info':
            # Type (1) + node_id (4) + height (4) + stake (4) + hop_distance (1) + flags (1)
            return 1 + 4 + 4 + 4 + 1 + 1
        else:
            return 100  # Default estimate
    
    def fits_in_packet(self, message: Dict[str, Any]) -> bool:
        """
        Check if message fits in single packet.
        
        Args:
            message: Message to check
        
        Returns:
            True if message fits
        """
        estimated_size = self.estimate_size(message)
        return estimated_size <= self.EFFECTIVE_MTU
    
    def batch_messages(self, messages: List[Dict[str, Any]]) -> List[bytes]:
        """
        Batch multiple messages into packets.
        
        Args:
            messages: Messages to batch
        
        Returns:
            List of optimized packets
        """
        packets = []
        current_packet = bytearray()
        
        for message in messages:
            optimized, _ = self.optimize_message(message)
            
            # Check if message fits in current packet
            if len(current_packet) + len(optimized) <= self.EFFECTIVE_MTU:
                current_packet.extend(optimized)
            else:
                # Start new packet
                if current_packet:
                    packets.append(bytes(current_packet))
                current_packet = bytearray(optimized)
        
        # Add last packet
        if current_packet:
            packets.append(bytes(current_packet))
        
        return packets
    
    def get_stats(self) -> Dict[str, Any]:
        """Get optimization statistics."""
        return {
            'original_size': self.stats.original_size,
            'optimized_size': self.stats.optimized_size,
            'compression_ratio': self.stats.compression_ratio,
            'compression_method': self.stats.compression_method.name,
            'messages_optimized': self.stats.messages_optimized,
            'bytes_saved': self.stats.bytes_saved,
            'avg_compression_ratio': (
                self.stats.bytes_saved / (self.stats.original_size * self.stats.messages_optimized)
                if self.stats.messages_optimized > 0 else 0
            )
        }
