"""
MeshChain Transaction Module

Handles transaction creation, validation, and serialization.
"""

from dataclasses import dataclass
from typing import List, Optional
import hashlib
import struct
from enum import IntEnum


class TransactionType(IntEnum):
    """Transaction type enumeration."""
    TRANSFER = 0
    STAKE = 1
    VOTE = 2


@dataclass
class Transaction:
    """
    Represents a MeshChain transaction.
    
    Attributes:
        version: Protocol version
        tx_type: Transaction type
        nonce: Sequence number for replay protection
        fee: Transaction fee in satoshis
        ring_size: Number of ring members (privacy)
        ring_members: List of ring member IDs (8 bytes each)
        stealth_address: Receiver's stealth address (16 bytes)
        amount_encrypted: Encrypted transaction amount (8 bytes)
        signature: Ring signature (32 bytes)
        timestamp: Block height hint (for ordering)
    """
    
    version: int
    tx_type: TransactionType
    nonce: int
    fee: int
    ring_size: int
    ring_members: List[bytes]
    stealth_address: bytes
    amount_encrypted: bytes
    signature: bytes
    timestamp: int
    
    def __post_init__(self) -> None:
        """Validate transaction fields."""
        if self.version < 0 or self.version > 255:
            raise ValueError("Version must be 0-255")
        
        if self.fee < 0 or self.fee > 255:
            raise ValueError("Fee must be 0-255")
        
        if self.ring_size < 2 or self.ring_size > 16:
            raise ValueError("Ring size must be 2-16")
        
        if len(self.ring_members) != self.ring_size:
            raise ValueError(f"Expected {self.ring_size} ring members")
        
        if len(self.stealth_address) != 16:
            raise ValueError("Stealth address must be 16 bytes")
        
        if len(self.amount_encrypted) != 8:
            raise ValueError("Encrypted amount must be 8 bytes")
        
        if len(self.signature) != 32:
            raise ValueError("Signature must be 32 bytes")
    
    def serialize(self) -> bytes:
        """
        Serialize transaction to compact binary format.
        
        Returns:
            Serialized transaction (approximately 110 bytes)
        """
        data = bytearray()
        
        # Version (1 byte)
        data.append(self.version)
        
        # Type (1 byte)
        data.append(int(self.tx_type))
        
        # Nonce (varint, 2 bytes)
        data.extend(encode_varint(self.nonce, 2))
        
        # Fee (1 byte)
        data.append(self.fee)
        
        # Ring size (1 byte)
        data.append(self.ring_size)
        
        # Ring members (8 bytes each)
        for member_id in self.ring_members:
            data.extend(member_id)
        
        # Stealth address (16 bytes)
        data.extend(self.stealth_address)
        
        # Amount encrypted (8 bytes)
        data.extend(self.amount_encrypted)
        
        # Signature (32 bytes)
        data.extend(self.signature)
        
        # Timestamp (varint, 2 bytes)
        data.extend(encode_varint(self.timestamp, 2))
        
        return bytes(data)
    
    @staticmethod
    def deserialize(data: bytes) -> 'Transaction':
        """
        Deserialize transaction from binary format.
        
        Args:
            data: Serialized transaction bytes
        
        Returns:
            Deserialized Transaction object
        
        Raises:
            ValueError: If data is invalid
        """
        if len(data) < 110:
            raise ValueError("Transaction data too short")
        
        offset = 0
        
        # Version
        version = data[offset]
        offset += 1
        
        # Type
        tx_type = TransactionType(data[offset])
        offset += 1
        
        # Nonce
        nonce, bytes_read = decode_varint(data[offset:], 2)
        offset += bytes_read
        
        # Fee
        fee = data[offset]
        offset += 1
        
        # Ring size
        ring_size = data[offset]
        offset += 1
        
        # Ring members
        ring_members = []
        for _ in range(ring_size):
            ring_members.append(data[offset:offset+8])
            offset += 8
        
        # Stealth address
        stealth_address = data[offset:offset+16]
        offset += 16
        
        # Amount encrypted
        amount_encrypted = data[offset:offset+8]
        offset += 8
        
        # Signature
        signature = data[offset:offset+32]
        offset += 32
        
        # Timestamp
        timestamp, bytes_read = decode_varint(data[offset:], 2)
        offset += bytes_read
        
        return Transaction(
            version=version,
            tx_type=tx_type,
            nonce=nonce,
            fee=fee,
            ring_size=ring_size,
            ring_members=ring_members,
            stealth_address=stealth_address,
            amount_encrypted=amount_encrypted,
            signature=signature,
            timestamp=timestamp
        )
    
    def hash(self) -> bytes:
        """
        Calculate transaction hash.
        
        Returns:
            SHA-256 hash of serialized transaction (first 16 bytes)
        """
        serialized = self.serialize()
        full_hash = hashlib.sha256(serialized).digest()
        return full_hash[:16]  # Truncate to 16 bytes
    
    def verify_signature(self) -> bool:
        """
        Verify transaction signature.
        
        Note: This is a placeholder. Actual implementation requires
        ring signature verification library.
        
        Returns:
            True if signature is valid, False otherwise
        """
        # TODO: Implement ring signature verification
        # This requires a cryptographic library like libsodium
        return True
    
    def __repr__(self) -> str:
        """String representation of transaction."""
        return (
            f"Transaction(version={self.version}, "
            f"type={self.tx_type.name}, "
            f"nonce={self.nonce}, "
            f"fee={self.fee}, "
            f"hash={self.hash().hex()[:16]}...)"
        )


def encode_varint(value: int, max_bytes: int = 3) -> bytes:
    """
    Encode integer as variable-length integer.
    
    Args:
        value: Integer to encode
        max_bytes: Maximum number of bytes to use
    
    Returns:
        Encoded bytes
    
    Raises:
        ValueError: If value is too large for max_bytes
    """
    if value < 0:
        raise ValueError("Value must be non-negative")
    
    if value < 128:
        return bytes([value])
    elif value < 16384 and max_bytes >= 2:
        return bytes([
            0x80 | (value & 0x3F),
            (value >> 6) & 0xFF
        ])
    elif value < 2097152 and max_bytes >= 3:
        return bytes([
            0xC0 | (value & 0x1F),
            (value >> 5) & 0xFF,
            (value >> 13) & 0xFF
        ])
    else:
        raise ValueError(f"Value {value} too large for {max_bytes} bytes")


def decode_varint(data: bytes, max_bytes: int = 3) -> tuple[int, int]:
    """
    Decode variable-length integer.
    
    Args:
        data: Bytes to decode
        max_bytes: Maximum number of bytes to read
    
    Returns:
        Tuple of (decoded_value, bytes_read)
    
    Raises:
        ValueError: If data is invalid
    """
    if len(data) < 1:
        raise ValueError("Data too short")
    
    first_byte = data[0]
    
    if first_byte < 128:
        return first_byte, 1
    elif first_byte < 192 and len(data) >= 2 and max_bytes >= 2:
        value = (first_byte & 0x3F) | ((data[1] & 0xFF) << 6)
        return value, 2
    elif first_byte < 224 and len(data) >= 3 and max_bytes >= 3:
        value = (
            (first_byte & 0x1F) |
            ((data[1] & 0xFF) << 5) |
            ((data[2] & 0xFF) << 13)
        )
        return value, 3
    else:
        raise ValueError("Invalid varint encoding")


# Example usage
if __name__ == "__main__":
    # Create a sample transaction
    tx = Transaction(
        version=1,
        tx_type=TransactionType.TRANSFER,
        nonce=42,
        fee=5,
        ring_size=8,
        ring_members=[
            b'\x01\x02\x03\x04\x05\x06\x07\x08',
            b'\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10',
            b'\x11\x12\x13\x14\x15\x16\x17\x18',
            b'\x19\x1a\x1b\x1c\x1d\x1e\x1f\x20',
            b'\x21\x22\x23\x24\x25\x26\x27\x28',
            b'\x29\x2a\x2b\x2c\x2d\x2e\x2f\x30',
            b'\x31\x32\x33\x34\x35\x36\x37\x38',
            b'\x39\x3a\x3b\x3c\x3d\x3e\x3f\x40',
        ],
        stealth_address=b'\x41\x42\x43\x44\x45\x46\x47\x48\x49\x4a\x4b\x4c\x4d\x4e\x4f\x50',
        amount_encrypted=b'\x51\x52\x53\x54\x55\x56\x57\x58',
        signature=b'\x59\x5a\x5b\x5c\x5d\x5e\x5f\x60\x61\x62\x63\x64\x65\x66\x67\x68\x69\x6a\x6b\x6c\x6d\x6e\x6f\x70\x71\x72\x73\x74\x75\x76\x77\x78',
        timestamp=100
    )
    
    print(f"Transaction: {tx}")
    print(f"Hash: {tx.hash().hex()}")
    
    # Serialize and deserialize
    serialized = tx.serialize()
    print(f"Serialized size: {len(serialized)} bytes")
    
    deserialized = Transaction.deserialize(serialized)
    print(f"Deserialized: {deserialized}")
    print(f"Match: {tx.hash() == deserialized.hash()}")
