"""
MeshChain Storage Module

Manages persistent storage of blockchain data using SQLite.
Handles blocks, transactions, UTXO set, and node state.
"""

import sqlite3
import os
from typing import List, Optional, Dict
from pathlib import Path
from block import Block
from transaction import Transaction
from utxo import UTXO, UTXOSet


class BlockchainStorage:
    """
    Manages persistent storage of blockchain data.
    
    Uses SQLite for efficient storage and querying of:
    - Blocks and block headers
    - Transactions
    - UTXO set
    - Node state and metadata
    """
    
    def __init__(self, db_path: str = "meshchain.db"):
        """
        Initialize blockchain storage.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.connection = None
        self.cursor = None
        
        # Create database if it doesn't exist
        self._initialize_database()
    
    def _initialize_database(self) -> None:
        """Initialize database schema."""
        self.connection = sqlite3.connect(self.db_path)
        self.cursor = self.connection.cursor()
        
        # Enable foreign keys
        self.cursor.execute("PRAGMA foreign_keys = ON")
        
        # Create tables
        self._create_tables()
        self.connection.commit()
    
    def _create_tables(self) -> None:
        """Create database tables."""
        
        # Blocks table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS blocks (
                height INTEGER PRIMARY KEY,
                hash BLOB UNIQUE NOT NULL,
                timestamp INTEGER NOT NULL,
                previous_hash BLOB NOT NULL,
                merkle_root BLOB NOT NULL,
                proposer_id BLOB NOT NULL,
                validator_count INTEGER NOT NULL,
                approval_count INTEGER NOT NULL,
                data BLOB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Transactions table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                tx_hash BLOB PRIMARY KEY,
                block_height INTEGER NOT NULL,
                tx_index INTEGER NOT NULL,
                version INTEGER NOT NULL,
                tx_type INTEGER NOT NULL,
                nonce INTEGER NOT NULL,
                fee INTEGER NOT NULL,
                ring_size INTEGER NOT NULL,
                stealth_address BLOB NOT NULL,
                amount_encrypted BLOB NOT NULL,
                signature BLOB NOT NULL,
                timestamp INTEGER NOT NULL,
                data BLOB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (block_height) REFERENCES blocks(height)
            )
        """)
        
        # Create index on transactions
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tx_block 
            ON transactions(block_height)
        """)
        
        # UTXOs table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS utxos (
                utxo_id BLOB PRIMARY KEY,
                amount INTEGER NOT NULL,
                stealth_address BLOB NOT NULL,
                block_height INTEGER NOT NULL,
                is_spent BOOLEAN NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create index on stealth addresses for balance queries
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_utxo_address 
            ON utxos(stealth_address, is_spent)
        """)
        
        # Node state table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS node_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Peers table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS peers (
                node_id BLOB PRIMARY KEY,
                last_seen INTEGER NOT NULL,
                hop_distance INTEGER,
                is_validator BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def add_block(self, block: Block) -> None:
        """
        Add a block to storage.
        
        Args:
            block: Block to add
        
        Raises:
            sqlite3.IntegrityError: If block already exists
        """
        block_hash = block.hash()
        block_data = block.serialize()
        
        self.cursor.execute("""
            INSERT INTO blocks 
            (height, hash, timestamp, previous_hash, merkle_root, 
             proposer_id, validator_count, approval_count, data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            block.height,
            block_hash,
            block.timestamp,
            block.previous_hash,
            block.merkle_root,
            block.proposer_id,
            len(block.validators),
            block.get_approval_count(),
            block_data
        ))
        
        self.connection.commit()
    
    def get_block(self, height: int) -> Optional[Block]:
        """
        Get a block by height.
        
        Args:
            height: Block height
        
        Returns:
            Block if found, None otherwise
        """
        self.cursor.execute(
            "SELECT data FROM blocks WHERE height = ?",
            (height,)
        )
        
        result = self.cursor.fetchone()
        if result is None:
            return None
        
        return Block.deserialize(result[0])
    
    def get_block_by_hash(self, block_hash: bytes) -> Optional[Block]:
        """
        Get a block by hash.
        
        Args:
            block_hash: Block hash (16 bytes)
        
        Returns:
            Block if found, None otherwise
        """
        self.cursor.execute(
            "SELECT data FROM blocks WHERE hash = ?",
            (block_hash,)
        )
        
        result = self.cursor.fetchone()
        if result is None:
            return None
        
        return Block.deserialize(result[0])
    
    def get_latest_block_height(self) -> int:
        """
        Get the height of the latest block.
        
        Returns:
            Latest block height (0 if no blocks)
        """
        self.cursor.execute("SELECT MAX(height) FROM blocks")
        result = self.cursor.fetchone()
        
        if result[0] is None:
            return 0
        
        return result[0]
    
    def add_transaction(self, tx: Transaction, block_height: int, 
                       tx_index: int) -> None:
        """
        Add a transaction to storage.
        
        Args:
            tx: Transaction to add
            block_height: Height of block containing transaction
            tx_index: Index of transaction in block
        """
        tx_hash = tx.hash()
        tx_data = tx.serialize()
        
        self.cursor.execute("""
            INSERT INTO transactions
            (tx_hash, block_height, tx_index, version, tx_type, nonce, fee,
             ring_size, stealth_address, amount_encrypted, signature, 
             timestamp, data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            tx_hash,
            block_height,
            tx_index,
            tx.version,
            int(tx.tx_type),
            tx.nonce,
            tx.fee,
            tx.ring_size,
            tx.stealth_address,
            tx.amount_encrypted,
            tx.signature,
            tx.timestamp,
            tx_data
        ))
        
        self.connection.commit()
    
    def get_transaction(self, tx_hash: bytes) -> Optional[Transaction]:
        """
        Get a transaction by hash.
        
        Args:
            tx_hash: Transaction hash (16 bytes)
        
        Returns:
            Transaction if found, None otherwise
        """
        self.cursor.execute(
            "SELECT data FROM transactions WHERE tx_hash = ?",
            (tx_hash,)
        )
        
        result = self.cursor.fetchone()
        if result is None:
            return None
        
        return Transaction.deserialize(result[0])
    
    def add_utxo(self, utxo: UTXO) -> None:
        """
        Add a UTXO to storage.
        
        Args:
            utxo: UTXO to add
        """
        self.cursor.execute("""
            INSERT INTO utxos
            (utxo_id, amount, stealth_address, block_height, is_spent)
            VALUES (?, ?, ?, ?, ?)
        """, (
            utxo.utxo_id,
            utxo.amount,
            utxo.stealth_address,
            utxo.block_height,
            utxo.is_spent
        ))
        
        self.connection.commit()
    
    def get_utxo(self, utxo_id: bytes) -> Optional[UTXO]:
        """
        Get a UTXO by ID.
        
        Args:
            utxo_id: UTXO ID (16 bytes)
        
        Returns:
            UTXO if found, None otherwise
        """
        self.cursor.execute("""
            SELECT utxo_id, amount, stealth_address, block_height, is_spent
            FROM utxos WHERE utxo_id = ?
        """, (utxo_id,))
        
        result = self.cursor.fetchone()
        if result is None:
            return None
        
        return UTXO(
            utxo_id=result[0],
            amount=result[1],
            stealth_address=result[2],
            block_height=result[3],
            is_spent=bool(result[4])
        )
    
    def spend_utxo(self, utxo_id: bytes) -> None:
        """
        Mark a UTXO as spent.
        
        Args:
            utxo_id: UTXO ID to spend
        """
        self.cursor.execute(
            "UPDATE utxos SET is_spent = 1 WHERE utxo_id = ?",
            (utxo_id,)
        )
        
        self.connection.commit()
    
    def get_balance(self, stealth_address: bytes) -> int:
        """
        Get balance for a stealth address.
        
        Args:
            stealth_address: Address to check balance for
        
        Returns:
            Total balance in satoshis
        """
        self.cursor.execute("""
            SELECT SUM(amount) FROM utxos
            WHERE stealth_address = ? AND is_spent = 0
        """, (stealth_address,))
        
        result = self.cursor.fetchone()
        
        if result[0] is None:
            return 0
        
        return result[0]
    
    def get_unspent_utxos(self, stealth_address: bytes) -> List[UTXO]:
        """
        Get all unspent UTXOs for an address.
        
        Args:
            stealth_address: Address to get UTXOs for
        
        Returns:
            List of unspent UTXOs
        """
        self.cursor.execute("""
            SELECT utxo_id, amount, stealth_address, block_height, is_spent
            FROM utxos
            WHERE stealth_address = ? AND is_spent = 0
            ORDER BY block_height DESC
        """, (stealth_address,))
        
        utxos = []
        for row in self.cursor.fetchall():
            utxos.append(UTXO(
                utxo_id=row[0],
                amount=row[1],
                stealth_address=row[2],
                block_height=row[3],
                is_spent=bool(row[4])
            ))
        
        return utxos
    
    def set_state(self, key: str, value: str) -> None:
        """
        Set a node state value.
        
        Args:
            key: State key
            value: State value
        """
        self.cursor.execute("""
            INSERT OR REPLACE INTO node_state (key, value)
            VALUES (?, ?)
        """, (key, value))
        
        self.connection.commit()
    
    def get_state(self, key: str) -> Optional[str]:
        """
        Get a node state value.
        
        Args:
            key: State key
        
        Returns:
            State value if found, None otherwise
        """
        self.cursor.execute(
            "SELECT value FROM node_state WHERE key = ?",
            (key,)
        )
        
        result = self.cursor.fetchone()
        
        if result is None:
            return None
        
        return result[0]
    
    def add_peer(self, node_id: bytes, last_seen: int, 
                hop_distance: int = None) -> None:
        """
        Add or update a peer.
        
        Args:
            node_id: Peer node ID (8 bytes)
            last_seen: Timestamp when peer was last seen
            hop_distance: Hop distance to peer
        """
        self.cursor.execute("""
            INSERT OR REPLACE INTO peers
            (node_id, last_seen, hop_distance)
            VALUES (?, ?, ?)
        """, (node_id, last_seen, hop_distance))
        
        self.connection.commit()
    
    def get_peers(self) -> List[tuple]:
        """
        Get all known peers.
        
        Returns:
            List of (node_id, last_seen, hop_distance) tuples
        """
        self.cursor.execute("""
            SELECT node_id, last_seen, hop_distance FROM peers
            ORDER BY last_seen DESC
        """)
        
        return self.cursor.fetchall()
    
    def get_statistics(self) -> Dict[str, int]:
        """
        Get blockchain statistics.
        
        Returns:
            Dictionary with statistics
        """
        # Get block count
        self.cursor.execute("SELECT COUNT(*) FROM blocks")
        block_count = self.cursor.fetchone()[0]
        
        # Get transaction count
        self.cursor.execute("SELECT COUNT(*) FROM transactions")
        tx_count = self.cursor.fetchone()[0]
        
        # Get UTXO count
        self.cursor.execute("SELECT COUNT(*) FROM utxos WHERE is_spent = 0")
        utxo_count = self.cursor.fetchone()[0]
        
        # Get total value
        self.cursor.execute("SELECT SUM(amount) FROM utxos WHERE is_spent = 0")
        total_value = self.cursor.fetchone()[0] or 0
        
        return {
            "blocks": block_count,
            "transactions": tx_count,
            "utxos": utxo_count,
            "total_value": total_value
        }
    
    def close(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
    
    def __del__(self):
        """Ensure database is closed on cleanup."""
        self.close()


# Example usage
if __name__ == "__main__":
    print("MeshChain Storage Module")
    print("=" * 50)
    
    # Create storage
    print("\n1. Creating storage...")
    storage = BlockchainStorage(":memory:")  # Use in-memory database for testing
    print("   Storage initialized")
    
    # Add a UTXO
    print("\n2. Adding UTXO...")
    utxo = UTXO(
        utxo_id=b'\x01' * 16,
        amount=1000,
        stealth_address=b'\x02' * 16,
        block_height=1
    )
    storage.add_utxo(utxo)
    print("   UTXO added")
    
    # Check balance
    print("\n3. Checking balance...")
    balance = storage.get_balance(b'\x02' * 16)
    print(f"   Balance: {balance} satoshis")
    
    # Set state
    print("\n4. Setting node state...")
    storage.set_state("last_block_height", "1")
    storage.set_state("network_id", "meshchain-testnet")
    print("   State saved")
    
    # Get state
    print("\n5. Getting node state...")
    height = storage.get_state("last_block_height")
    network = storage.get_state("network_id")
    print(f"   Last block height: {height}")
    print(f"   Network ID: {network}")
    
    # Get statistics
    print("\n6. Getting statistics...")
    stats = storage.get_statistics()
    print(f"   Blocks: {stats['blocks']}")
    print(f"   Transactions: {stats['transactions']}")
    print(f"   UTXOs: {stats['utxos']}")
    print(f"   Total value: {stats['total_value']} satoshis")
    
    storage.close()
    
    print("\n" + "=" * 50)
    print("Storage module working correctly!")
