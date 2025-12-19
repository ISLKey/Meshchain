"""
MeshChain Block Module

Handles block creation, validation, and serialization.
"""

from dataclasses import dataclass, field
from typing import List
import hashlib
import time
from meshchain.transaction import Transaction


@dataclass
class Block:
    """
    Represents a MeshChain block.
    
    Attributes:
        version: Protocol version
        height: Block height (block number)
        timestamp: Block creation timestamp
        previous_hash: Hash of previous block (16 bytes)
        merkle_root: Root of transaction merkle tree (16 bytes)
        proposer_id: Node ID of block proposer (8 bytes)
        validators: List of validator node IDs
        approvals: Bit vector of validator approvals
        transactions: List of transactions in block
    """
    
    version: int
    height: int
    timestamp: int
    previous_hash: bytes
    merkle_root: bytes
    proposer_id: bytes
    validators: List[bytes] = field(default_factory=list)
    approvals: bytes = b''
    transactions: List[Transaction] = field(default_factory=list)
    
    def __post_init__(self) -> None:
        """Validate block fields."""
        if self.version < 0 or self.version > 255:
            raise ValueError("Version must be 0-255")
        
        if self.height < 0:
            raise ValueError("Height must be non-negative")
        
        if len(self.previous_hash) != 16:
            raise ValueError("Previous hash must be 16 bytes")
        
        if len(self.merkle_root) != 16:
            raise ValueError("Merkle root must be 16 bytes")
        
        if len(self.proposer_id) != 8:
            raise ValueError("Proposer ID must be 8 bytes")
        
        if len(self.validators) > 7:
            raise ValueError("Maximum 7 validators per block")
        
        if len(self.transactions) > 5:
            raise ValueError("Maximum 5 transactions per block")
    
    def serialize(self) -> bytes:
        """
        Serialize block to compact binary format.
        
        Returns:
            Serialized block (approximately 500 bytes)
        """
        data = bytearray()
        
        # Version (1 byte)
        data.append(self.version)
        
        # Height (3 bytes, varint)
        data.extend(encode_varint_3(self.height))
        
        # Timestamp (2 bytes)
        data.extend(self.timestamp.to_bytes(2, 'big'))
        
        # Previous hash (16 bytes)
        data.extend(self.previous_hash)
        
        # Merkle root (16 bytes)
        data.extend(self.merkle_root)
        
        # Proposer ID (8 bytes)
        data.extend(self.proposer_id)
        
        # Validator count (1 byte)
        data.append(len(self.validators))
        
        # Validators (8 bytes each)
        for validator_id in self.validators:
            data.extend(validator_id)
        
        # Approval count (1 byte)
        data.append(len(self.approvals))
        
        # Approvals (bit vector)
        data.extend(self.approvals)
        
        # Transaction count (1 byte)
        data.append(len(self.transactions))
        
        # Transactions
        for tx in self.transactions:
            tx_data = tx.serialize()
            data.append(len(tx_data))  # Transaction size
            data.extend(tx_data)
        
        return bytes(data)
    
    @staticmethod
    def deserialize(data: bytes) -> 'Block':
        """
        Deserialize block from binary format.
        
        Args:
            data: Serialized block bytes
        
        Returns:
            Deserialized Block object
        
        Raises:
            ValueError: If data is invalid
        """
        offset = 0
        
        # Version
        version = data[offset]
        offset += 1
        
        # Height
        height, bytes_read = decode_varint_3(data[offset:])
        offset += bytes_read
        
        # Timestamp
        timestamp = int.from_bytes(data[offset:offset+2], 'big')
        offset += 2
        
        # Previous hash
        previous_hash = data[offset:offset+16]
        offset += 16
        
        # Merkle root
        merkle_root = data[offset:offset+16]
        offset += 16
        
        # Proposer ID
        proposer_id = data[offset:offset+8]
        offset += 8
        
        # Validator count
        validator_count = data[offset]
        offset += 1
        
        # Validators
        validators = []
        for _ in range(validator_count):
            validators.append(data[offset:offset+8])
            offset += 8
        
        # Approval count
        approval_count = data[offset]
        offset += 1
        
        # Approvals
        approvals = data[offset:offset+approval_count]
        offset += approval_count
        
        # Transaction count
        tx_count = data[offset]
        offset += 1
        
        # Transactions
        transactions = []
        for _ in range(tx_count):
            tx_size = data[offset]
            offset += 1
            tx_data = data[offset:offset+tx_size]
            transactions.append(Transaction.deserialize(tx_data))
            offset += tx_size
        
        return Block(
            version=version,
            height=height,
            timestamp=timestamp,
            previous_hash=previous_hash,
            merkle_root=merkle_root,
            proposer_id=proposer_id,
            validators=validators,
            approvals=approvals,
            transactions=transactions
        )
    
    def hash(self) -> bytes:
        """
        Calculate block hash.
        
        Returns:
            SHA-256 hash of serialized block (first 16 bytes)
        """
        serialized = self.serialize()
        full_hash = hashlib.sha256(serialized).digest()
        return full_hash[:16]  # Truncate to 16 bytes
    
    def calculate_merkle_root(self) -> bytes:
        """
        Calculate merkle root of transactions.
        
        Returns:
            Merkle root hash (16 bytes)
        """
        if not self.transactions:
            # Empty block
            return hashlib.sha256(b'').digest()[:16]
        
        # Hash all transactions
        tx_hashes = [tx.hash() for tx in self.transactions]
        
        # Build merkle tree
        while len(tx_hashes) > 1:
            if len(tx_hashes) % 2 != 0:
                tx_hashes.append(tx_hashes[-1])  # Duplicate last hash
            
            new_hashes = []
            for i in range(0, len(tx_hashes), 2):
                combined = tx_hashes[i] + tx_hashes[i+1]
                new_hash = hashlib.sha256(combined).digest()[:16]
                new_hashes.append(new_hash)
            
            tx_hashes = new_hashes
        
        return tx_hashes[0]
    
    def is_valid(self) -> bool:
        """
        Validate block structure.
        
        Returns:
            True if block is valid, False otherwise
        """
        # Check merkle root
        calculated_root = self.calculate_merkle_root()
        if calculated_root != self.merkle_root:
            return False
        
        # Check transaction validity
        for tx in self.transactions:
            if not tx.verify_signature():
                return False
        
        # Check approvals
        if len(self.approvals) != len(self.validators):
            return False
        
        return True
    
    def get_approval_count(self) -> int:
        """
        Count number of approvals.
        
        Returns:
            Number of validators who approved
        """
        count = 0
        for byte in self.approvals:
            count += bin(byte).count('1')
        return count
    
    def is_finalized(self) -> bool:
        """
        Check if block is finalized.
        
        Returns:
            True if >66% of validators approved
        """
        if not self.validators:
            return False
        
        approval_count = self.get_approval_count()
        required_approvals = (len(self.validators) * 2 + 2) // 3  # >66%
        
        return approval_count >= required_approvals
    
    def __repr__(self) -> str:
        """String representation of block."""
        return (
            f"Block(height={self.height}, "
            f"hash={self.hash().hex()[:16]}..., "
            f"txs={len(self.transactions)}, "
            f"finalized={self.is_finalized()})"
        )


def encode_varint_3(value: int) -> bytes:
    """
    Encode integer as 3-byte variable-length integer.
    
    Args:
        value: Integer to encode
    
    Returns:
        Encoded bytes (1-3 bytes)
    """
    if value < 128:
        return bytes([value])
    elif value < 16384:
        return bytes([
            0x80 | (value & 0x3F),
            (value >> 6) & 0xFF
        ])
    else:
        return bytes([
            0xC0 | (value & 0x1F),
            (value >> 5) & 0xFF,
            (value >> 13) & 0xFF
        ])


def decode_varint_3(data: bytes) -> tuple[int, int]:
    """
    Decode 3-byte variable-length integer.
    
    Args:
        data: Bytes to decode
    
    Returns:
        Tuple of (decoded_value, bytes_read)
    """
    if len(data) < 1:
        raise ValueError("Data too short")
    
    first_byte = data[0]
    
    if first_byte < 128:
        return first_byte, 1
    elif first_byte < 192 and len(data) >= 2:
        value = (first_byte & 0x3F) | ((data[1] & 0xFF) << 6)
        return value, 2
    elif first_byte < 224 and len(data) >= 3:
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
    # Create a sample block
    block = Block(
        version=1,
        height=1,
        timestamp=int(time.time()),
        previous_hash=b'\x00' * 16,
        merkle_root=b'\x00' * 16,
        proposer_id=b'\x01\x02\x03\x04\x05\x06\x07\x08',
        validators=[
            b'\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10',
            b'\x11\x12\x13\x14\x15\x16\x17\x18',
            b'\x19\x1a\x1b\x1c\x1d\x1e\x1f\x20',
        ],
        approvals=b'\x07',  # All 3 validators approved
        transactions=[]
    )
    
    # Update merkle root
    block.merkle_root = block.calculate_merkle_root()
    
    print(f"Block: {block}")
    print(f"Hash: {block.hash().hex()}")
    print(f"Finalized: {block.is_finalized()}")
    
    # Serialize and deserialize
    serialized = block.serialize()
    print(f"Serialized size: {len(serialized)} bytes")
    
    deserialized = Block.deserialize(serialized)
    print(f"Deserialized: {deserialized}")
    print(f"Match: {block.hash() == deserialized.hash()}")
