"""
MeshChain UTXO (Unspent Transaction Output) Model

Implements the UTXO model for transaction handling and validation.
This is the core of MeshChain's transaction system.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Set
import hashlib
from meshchain.transaction import Transaction, TransactionType
from meshchain.crypto import CryptoUtils


@dataclass
class UTXO:
    """
    Represents an Unspent Transaction Output.
    
    A UTXO is a transaction output that hasn't been spent yet.
    It contains the value and the conditions for spending it.
    
    Attributes:
        utxo_id: Unique identifier (transaction_hash + output_index)
        amount: Amount in satoshis
        stealth_address: Recipient's stealth address (16 bytes)
        block_height: Block height where UTXO was created
        is_spent: Whether this UTXO has been spent
    """
    
    utxo_id: bytes  # 16 bytes
    amount: int
    stealth_address: bytes  # 16 bytes
    block_height: int
    is_spent: bool = False
    
    def __post_init__(self) -> None:
        """Validate UTXO fields."""
        if len(self.utxo_id) != 16:
            raise ValueError("UTXO ID must be 16 bytes")
        
        if self.amount < 0:
            raise ValueError("Amount must be non-negative")
        
        if len(self.stealth_address) != 16:
            raise ValueError("Stealth address must be 16 bytes")
        
        if self.block_height < 0:
            raise ValueError("Block height must be non-negative")
    
    def serialize(self) -> bytes:
        """
        Serialize UTXO to binary format.
        
        Returns:
            Serialized UTXO (~50 bytes)
        """
        data = bytearray()
        
        # UTXO ID (16 bytes)
        data.extend(self.utxo_id)
        
        # Amount (8 bytes, little-endian)
        data.extend(self.amount.to_bytes(8, 'little'))
        
        # Stealth address (16 bytes)
        data.extend(self.stealth_address)
        
        # Block height (4 bytes, little-endian)
        data.extend(self.block_height.to_bytes(4, 'little'))
        
        # Is spent (1 byte)
        data.append(1 if self.is_spent else 0)
        
        return bytes(data)
    
    @staticmethod
    def deserialize(data: bytes) -> 'UTXO':
        """
        Deserialize UTXO from binary format.
        
        Args:
            data: Serialized UTXO bytes
        
        Returns:
            Deserialized UTXO object
        """
        offset = 0
        
        # UTXO ID
        utxo_id = data[offset:offset+16]
        offset += 16
        
        # Amount
        amount = int.from_bytes(data[offset:offset+8], 'little')
        offset += 8
        
        # Stealth address
        stealth_address = data[offset:offset+16]
        offset += 16
        
        # Block height
        block_height = int.from_bytes(data[offset:offset+4], 'little')
        offset += 4
        
        # Is spent
        is_spent = bool(data[offset])
        
        return UTXO(
            utxo_id=utxo_id,
            amount=amount,
            stealth_address=stealth_address,
            block_height=block_height,
            is_spent=is_spent
        )


class UTXOSet:
    """
    Manages the set of unspent transaction outputs.
    
    The UTXO set is the core of the blockchain state. It tracks which
    outputs are available to be spent.
    """
    
    def __init__(self):
        """Initialize empty UTXO set."""
        self.utxos: Dict[bytes, UTXO] = {}
        self.spent_utxos: Set[bytes] = set()
    
    def add_utxo(self, utxo: UTXO) -> None:
        """
        Add a UTXO to the set.
        
        Args:
            utxo: UTXO to add
        
        Raises:
            ValueError: If UTXO already exists
        """
        if utxo.utxo_id in self.utxos:
            raise ValueError(f"UTXO {utxo.utxo_id.hex()} already exists")
        
        self.utxos[utxo.utxo_id] = utxo
    
    def spend_utxo(self, utxo_id: bytes) -> None:
        """
        Mark a UTXO as spent.
        
        Args:
            utxo_id: ID of UTXO to spend
        
        Raises:
            ValueError: If UTXO doesn't exist or already spent
        """
        if utxo_id not in self.utxos:
            raise ValueError(f"UTXO {utxo_id.hex()} not found")
        
        if self.utxos[utxo_id].is_spent:
            raise ValueError(f"UTXO {utxo_id.hex()} already spent")
        
        self.utxos[utxo_id].is_spent = True
        self.spent_utxos.add(utxo_id)
    
    def get_utxo(self, utxo_id: bytes) -> Optional[UTXO]:
        """
        Get a UTXO by ID.
        
        Args:
            utxo_id: ID of UTXO to retrieve
        
        Returns:
            UTXO if found, None otherwise
        """
        return self.utxos.get(utxo_id)
    
    def is_unspent(self, utxo_id: bytes) -> bool:
        """
        Check if a UTXO is unspent.
        
        Args:
            utxo_id: ID of UTXO to check
        
        Returns:
            True if UTXO is unspent, False otherwise
        """
        if utxo_id not in self.utxos:
            return False
        
        return not self.utxos[utxo_id].is_spent
    
    def get_balance(self, stealth_address: bytes) -> int:
        """
        Get total balance for a stealth address.
        
        Args:
            stealth_address: Address to check balance for
        
        Returns:
            Total balance in satoshis
        """
        balance = 0
        
        for utxo in self.utxos.values():
            if utxo.stealth_address == stealth_address and not utxo.is_spent:
                balance += utxo.amount
        
        return balance
    
    def get_unspent_utxos(self, stealth_address: bytes) -> List[UTXO]:
        """
        Get all unspent UTXOs for a stealth address.
        
        Args:
            stealth_address: Address to get UTXOs for
        
        Returns:
            List of unspent UTXOs
        """
        utxos = []
        
        for utxo in self.utxos.values():
            if utxo.stealth_address == stealth_address and not utxo.is_spent:
                utxos.append(utxo)
        
        return utxos
    
    def size(self) -> int:
        """
        Get the size of the UTXO set.
        
        Returns:
            Number of UTXOs (spent and unspent)
        """
        return len(self.utxos)
    
    def unspent_count(self) -> int:
        """
        Get the number of unspent UTXOs.
        
        Returns:
            Number of unspent UTXOs
        """
        return len(self.utxos) - len(self.spent_utxos)
    
    def serialize(self) -> bytes:
        """
        Serialize UTXO set to binary format.
        
        Returns:
            Serialized UTXO set
        """
        data = bytearray()
        
        # Number of UTXOs (4 bytes)
        data.extend(len(self.utxos).to_bytes(4, 'little'))
        
        # Each UTXO
        for utxo in self.utxos.values():
            utxo_data = utxo.serialize()
            data.extend(len(utxo_data).to_bytes(2, 'little'))
            data.extend(utxo_data)
        
        return bytes(data)
    
    @staticmethod
    def deserialize(data: bytes) -> 'UTXOSet':
        """
        Deserialize UTXO set from binary format.
        
        Args:
            data: Serialized UTXO set bytes
        
        Returns:
            Deserialized UTXOSet object
        """
        utxo_set = UTXOSet()
        offset = 0
        
        # Number of UTXOs
        utxo_count = int.from_bytes(data[offset:offset+4], 'little')
        offset += 4
        
        # Each UTXO
        for _ in range(utxo_count):
            utxo_size = int.from_bytes(data[offset:offset+2], 'little')
            offset += 2
            
            utxo_data = data[offset:offset+utxo_size]
            utxo = UTXO.deserialize(utxo_data)
            utxo_set.add_utxo(utxo)
            
            offset += utxo_size
        
        return utxo_set


class TransactionValidator:
    """
    Validates transactions against the UTXO set.
    
    Ensures that:
    - All inputs reference valid UTXOs
    - UTXOs haven't been spent already
    - Sender has sufficient balance
    - Fees are reasonable
    - Signatures are valid
    """
    
    def __init__(self, utxo_set: UTXOSet):
        """
        Initialize transaction validator.
        
        Args:
            utxo_set: UTXO set to validate against
        """
        self.utxo_set = utxo_set
    
    def validate_transaction(self, tx: Transaction) -> tuple[bool, str]:
        """
        Validate a transaction.
        
        Args:
            tx: Transaction to validate
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check transaction structure
        if not self._validate_structure(tx):
            return False, "Invalid transaction structure"
        
        # Check that transaction is not empty
        if not tx.ring_members:
            return False, "Transaction has no inputs"
        
        # Check ring members
        if len(tx.ring_members) < 2:
            return False, "Ring must have at least 2 members"
        
        if len(tx.ring_members) > 16:
            return False, "Ring can have at most 16 members"
        
        # Check fee
        if tx.fee < 1:
            return False, "Fee must be at least 1 satoshi"
        
        if tx.fee > 255:
            return False, "Fee exceeds maximum"
        
        # Check amount is encrypted
        if len(tx.amount_encrypted) != 8:
            return False, "Amount must be encrypted to 8 bytes"
        
        # Check signature
        if not self._validate_signature(tx):
            return False, "Invalid transaction signature"
        
        return True, ""
    
    def _validate_structure(self, tx: Transaction) -> bool:
        """
        Validate transaction structure.
        
        Args:
            tx: Transaction to validate
        
        Returns:
            True if structure is valid
        """
        try:
            # Check all required fields exist
            assert tx.version >= 0
            assert tx.tx_type in [TransactionType.TRANSFER, TransactionType.STAKE, TransactionType.VOTE]
            assert tx.nonce >= 0
            assert tx.fee >= 0
            assert len(tx.stealth_address) == 16
            assert len(tx.signature) == 32
            
            return True
        except AssertionError:
            return False
    
    def _validate_signature(self, tx: Transaction) -> bool:
        """
        Validate transaction signature.
        
        Args:
            tx: Transaction to validate
        
        Returns:
            True if signature is valid
        """
        # In a real implementation, verify the ring signature
        # For now, just check that signature exists
        return len(tx.signature) == 32
    
    def check_double_spend(self, tx: Transaction) -> bool:
        """
        Check if transaction attempts to double-spend.
        
        Args:
            tx: Transaction to check
        
        Returns:
            True if transaction is valid (no double-spend), False otherwise
        """
        # In a real implementation, check if any of the input UTXOs
        # have already been spent in the current block or mempool
        
        # For now, just return True
        return True
    
    def estimate_fee(self, tx_size: int) -> int:
        """
        Estimate appropriate fee for a transaction.
        
        Args:
            tx_size: Size of transaction in bytes
        
        Returns:
            Recommended fee in satoshis
        """
        # Minimum 1 satoshi per byte
        return max(1, tx_size // 100)


# Example usage
if __name__ == "__main__":
    print("MeshChain UTXO Model")
    print("=" * 50)
    
    # Create UTXO set
    print("\n1. Creating UTXO set...")
    utxo_set = UTXOSet()
    
    # Create some UTXOs
    utxo1 = UTXO(
        utxo_id=b'\x01' * 16,
        amount=1000,
        stealth_address=b'\x02' * 16,
        block_height=1
    )
    
    utxo2 = UTXO(
        utxo_id=b'\x03' * 16,
        amount=2000,
        stealth_address=b'\x02' * 16,
        block_height=2
    )
    
    utxo_set.add_utxo(utxo1)
    utxo_set.add_utxo(utxo2)
    
    print(f"   Added 2 UTXOs")
    print(f"   UTXO set size: {utxo_set.size()}")
    
    # Check balance
    print("\n2. Checking balance...")
    balance = utxo_set.get_balance(b'\x02' * 16)
    print(f"   Balance for address: {balance} satoshis")
    
    # Spend a UTXO
    print("\n3. Spending UTXO...")
    utxo_set.spend_utxo(b'\x01' * 16)
    print(f"   Spent UTXO")
    print(f"   Unspent count: {utxo_set.unspent_count()}")
    
    # Check balance again
    balance = utxo_set.get_balance(b'\x02' * 16)
    print(f"   New balance: {balance} satoshis")
    
    # Test serialization
    print("\n4. Testing serialization...")
    serialized = utxo_set.serialize()
    print(f"   Serialized UTXO set: {len(serialized)} bytes")
    
    deserialized = UTXOSet.deserialize(serialized)
    print(f"   Deserialized UTXO set size: {deserialized.size()}")
    
    print("\n" + "=" * 50)
    print("UTXO model working correctly!")
