"""
MeshChain Wallet - Encrypted Wallet Management with microSD Support

This module handles:
1. Wallet creation and management
2. Encrypted key storage on microSD
3. Password-protected access
4. Wallet backup and restore
5. Multi-wallet support
6. BIP39 seed phrase generation

Key Components:
1. WalletConfig: Wallet configuration
2. EncryptedWallet: Encrypted wallet storage
3. WalletManager: High-level wallet interface
4. MicroSDWallet: microSD card integration
"""

import os
import json
import hashlib
import secrets
import time
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict, field
from pathlib import Path
import hmac
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.backends import default_backend
import nacl.utils
import nacl.secret
import nacl.pwhash
from meshchain.crypto import KeyPair


@dataclass
class WalletConfig:
    """Wallet configuration."""
    name: str                           # Wallet name
    address: str                        # Wallet address (public key hex)
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    version: str = "1.0"               # Wallet version
    encrypted: bool = True              # Is encrypted?
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'WalletConfig':
        """Create from dictionary."""
        return cls(**data)


@dataclass
class WalletMetadata:
    """Wallet metadata."""
    wallet_id: str                      # Unique wallet ID
    name: str                           # Wallet name
    created_at: float                   # Creation timestamp
    last_accessed: float                # Last access timestamp
    version: str = field(default="1.0")               # Wallet version
    encrypted: bool = field(default=True)              # Is encrypted?
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict):
        """Create WalletMetadata from dictionary."""
        return cls(
            wallet_id=data.get('wallet_id', ''),
            name=data.get('name', 'Unknown'),
            created_at=data.get('created_at', 0.0),
            last_accessed=data.get('last_accessed', 0.0),
            version=data.get('version', '1.0'),
            encrypted=data.get('encrypted', True)
        )


class EncryptedWallet:
    """
    Handles encrypted wallet storage and access.
    
    Uses:
    1. PBKDF2 for key derivation from password
    2. ChaCha20-Poly1305 for encryption
    3. HMAC for integrity verification
    """
    
    def __init__(self, wallet_id: str, name: str):
        """
        Initialize encrypted wallet.
        
        Args:
            wallet_id: Unique wallet identifier
            name: Wallet name
        """
        self.wallet_id = wallet_id
        self.name = name
        self.created_at = time.time()
        self.last_accessed = time.time()
        
        # Encryption parameters
        self.salt_size = 32  # 256 bits
        self.nonce_size = 12  # 96 bits for ChaCha20Poly1305
        self.iterations = 100000  # PBKDF2 iterations
    
    def encrypt_private_key(self, private_key: bytes, password: str) -> bytes:
        """
        Encrypt private key with password.
        
        Args:
            private_key: Private key bytes (32 bytes)
            password: Password string
            
        Returns:
            Encrypted data (salt + nonce + ciphertext + tag)
        """
        # Generate salt
        salt = secrets.token_bytes(self.salt_size)
        
        # Derive encryption key from password
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,  # 256 bits for ChaCha20
            salt=salt,
            iterations=self.iterations,
            backend=default_backend()
        )
        key = kdf.derive(password.encode())
        
        # Generate nonce
        nonce = secrets.token_bytes(self.nonce_size)
        
        # Encrypt
        cipher = ChaCha20Poly1305(key)
        ciphertext = cipher.encrypt(nonce, private_key, None)
        # ChaCha20Poly1305 returns ciphertext + tag concatenated
        # We'll store them separately for clarity
        tag = ciphertext[-16:]  # Last 16 bytes are the tag
        ciphertext = ciphertext[:-16]  # Everything else is ciphertext
        
        # Return: salt + nonce + ciphertext + tag
        return salt + nonce + ciphertext + tag
    
    def decrypt_private_key(self, encrypted_data: bytes, password: str) -> Optional[bytes]:
        """
        Decrypt private key with password.
        
        Args:
            encrypted_data: Encrypted data (salt + nonce + ciphertext + tag)
            password: Password string
            
        Returns:
            Private key bytes or None if decryption fails
        """
        try:
            # Extract components
            salt = encrypted_data[:self.salt_size]
            nonce = encrypted_data[self.salt_size:self.salt_size + self.nonce_size]
            ciphertext = encrypted_data[self.salt_size + self.nonce_size:-16]  # 16 bytes for tag
            tag = encrypted_data[-16:]
            
            # Derive key from password
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=self.iterations,
                backend=default_backend()
            )
            key = kdf.derive(password.encode())
            
            # Decrypt
            cipher = ChaCha20Poly1305(key)
            # Reconstruct the full ciphertext with tag
            full_ciphertext = ciphertext + tag
            plaintext = cipher.decrypt(nonce, full_ciphertext, None)
            
            return plaintext
            
        except Exception as e:
            print(f"Decryption error: {str(e)}")
            return None
    
    def create_backup(self, private_key: bytes, password: str) -> Dict:
        """
        Create wallet backup.
        
        Args:
            private_key: Private key bytes
            password: Password for encryption
            
        Returns:
            Backup dictionary
        """
        encrypted_key = self.encrypt_private_key(private_key, password)
        
        return {
            'wallet_id': self.wallet_id,
            'name': self.name,
            'created_at': self.created_at,
            'encrypted_key': encrypted_key.hex(),
            'version': '1.0'
        }
    
    def restore_from_backup(self, backup: Dict, password: str) -> Optional[bytes]:
        """
        Restore wallet from backup.
        
        Args:
            backup: Backup dictionary
            password: Password for decryption
            
        Returns:
            Private key bytes or None if restoration fails
        """
        try:
            encrypted_key = bytes.fromhex(backup['encrypted_key'])
            return self.decrypt_private_key(encrypted_key, password)
        except Exception as e:
            print(f"Restoration error: {str(e)}")
            return None


class WalletManager:
    """
    High-level wallet management.
    
    Handles:
    1. Wallet creation
    2. Wallet loading
    3. Multi-wallet support
    4. Wallet switching
    5. Transaction signing
    """
    
    def __init__(self, storage_path: str = "/mnt/microsd/wallets"):
        """
        Initialize wallet manager.
        
        Args:
            storage_path: Path to wallet storage directory
        """
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        self.wallets: Dict[str, Tuple[EncryptedWallet, KeyPair]] = {}
        self.active_wallet: Optional[str] = None
        self.wallet_passwords: Dict[str, str] = {}  # wallet_id -> password (in memory)
    
    def create_wallet(self, name: str, password: str) -> Tuple[str, KeyPair]:
        """
        Create a new wallet.
        
        Args:
            name: Wallet name
            password: Password for encryption
            
        Returns:
            Tuple of (wallet_id, KeyPair)
        """
        # Generate keypair
        keypair = KeyPair()
        
        # Create wallet ID
        wallet_id = hashlib.sha256(
            f"{name}{time.time()}".encode()
        ).hexdigest()[:16]
        
        # Create encrypted wallet
        encrypted_wallet = EncryptedWallet(wallet_id, name)
        
        # Encrypt and save private key
        encrypted_key = encrypted_wallet.encrypt_private_key(
            keypair.private_key, password
        )
        
        # Save wallet metadata
        metadata = WalletMetadata(
            wallet_id=wallet_id,
            name=name,
            created_at=encrypted_wallet.created_at,
            last_accessed=encrypted_wallet.last_accessed
        )
        
        wallet_file = self.storage_path / f"{wallet_id}.json"
        with open(wallet_file, 'w') as f:
            json.dump({
                'metadata': metadata.to_dict(),
                'encrypted_key': encrypted_key.hex(),
                'public_key': keypair.public_key.hex()
            }, f)
        
        # Store in memory
        self.wallets[wallet_id] = (encrypted_wallet, keypair)
        self.wallet_passwords[wallet_id] = password
        self.active_wallet = wallet_id
        
        return wallet_id, keypair
    
    def load_wallet(self, wallet_id: str, password: str) -> Optional[KeyPair]:
        """
        Load wallet from storage.
        
        Args:
            wallet_id: Wallet ID
            password: Password for decryption
            
        Returns:
            KeyPair or None if loading fails
        """
        wallet_file = self.storage_path / f"{wallet_id}.json"
        
        if not wallet_file.exists():
            print(f"Wallet {wallet_id} not found")
            return None
        
        try:
            with open(wallet_file, 'r') as f:
                data = json.load(f)
            
            metadata = WalletMetadata.from_dict(data['metadata'])
            encrypted_key = bytes.fromhex(data['encrypted_key'])
            public_key = bytes.fromhex(data['public_key'])
            
            # Create encrypted wallet
            encrypted_wallet = EncryptedWallet(metadata.wallet_id, metadata.name)
            
            # Decrypt private key
            private_key = encrypted_wallet.decrypt_private_key(encrypted_key, password)
            
            if private_key is None:
                print("Invalid password")
                return None
            
            # Create keypair
            keypair = KeyPair(private_key=private_key)
            
            # Store in memory
            self.wallets[wallet_id] = (encrypted_wallet, keypair)
            self.wallet_passwords[wallet_id] = password
            self.active_wallet = wallet_id
            
            return keypair
            
        except Exception as e:
            print(f"Error loading wallet: {str(e)}")
            return None
    
    def list_wallets(self) -> List[Dict]:
        """
        List all available wallets.
        
        Returns:
            List of wallet metadata dictionaries
        """
        wallets = []
        
        for wallet_file in self.storage_path.glob("*.json"):
            try:
                with open(wallet_file, 'r') as f:
                    data = json.load(f)
                
                metadata = data['metadata']
                metadata['address'] = data['public_key'][:16] + "..."
                wallets.append(metadata)
                
            except Exception as e:
                print(f"Error reading wallet {wallet_file}: {str(e)}")
        
        return wallets
    
    def export_wallet(self, wallet_id: str, export_password: str) -> Optional[Dict]:
        """
        Export wallet for backup.
        
        Args:
            wallet_id: Wallet ID
            export_password: Password for export encryption
            
        Returns:
            Backup dictionary or None
        """
        if wallet_id not in self.wallets:
            print(f"Wallet {wallet_id} not loaded")
            return None
        
        encrypted_wallet, keypair = self.wallets[wallet_id]
        
        # Create backup
        backup = encrypted_wallet.create_backup(
            keypair.private_key,
            export_password
        )
        
        return backup
    
    def import_wallet(self, backup: Dict, import_password: str, 
                     new_password: Optional[str] = None) -> Optional[str]:
        """
        Import wallet from backup.
        
        Args:
            backup: Backup dictionary
            import_password: Password for backup decryption
            new_password: New password for imported wallet (optional)
            
        Returns:
            Wallet ID or None if import fails
        """
        # Restore private key from backup
        encrypted_wallet = EncryptedWallet(
            backup['wallet_id'],
            backup['name']
        )
        
        private_key = encrypted_wallet.restore_from_backup(
            backup,
            import_password
        )
        
        if private_key is None:
            print("Failed to restore wallet")
            return None
        
        # Use new password or original
        password = new_password or import_password
        
        # Re-encrypt with new password if different
        if new_password and new_password != import_password:
            encrypted_key = encrypted_wallet.encrypt_private_key(
                private_key, new_password
            )
        else:
            encrypted_key = bytes.fromhex(backup['encrypted_key'])
        
        # Save wallet
        wallet_id = backup['wallet_id']
        metadata = WalletMetadata(
            wallet_id=wallet_id,
            name=backup['name'],
            created_at=backup['created_at'],
            last_accessed=time.time()
        )
        
        wallet_file = self.storage_path / f"{wallet_id}.json"
        with open(wallet_file, 'w') as f:
            json.dump({
                'metadata': metadata.to_dict(),
                'encrypted_key': encrypted_key.hex() if isinstance(encrypted_key, bytes) else encrypted_key,
                'public_key': KeyPair(private_key=private_key).public_key.hex()
            }, f)
        
        # Store in memory
        keypair = KeyPair(private_key=private_key)
        self.wallets[wallet_id] = (encrypted_wallet, keypair)
        self.wallet_passwords[wallet_id] = password
        self.active_wallet = wallet_id
        
        return wallet_id
    
    def get_active_wallet(self) -> Optional[KeyPair]:
        """Get active wallet keypair."""
        if self.active_wallet and self.active_wallet in self.wallets:
            return self.wallets[self.active_wallet][1]
        return None
    
    def switch_wallet(self, wallet_id: str) -> bool:
        """Switch active wallet."""
        if wallet_id in self.wallets:
            self.active_wallet = wallet_id
            return True
        return False
    
    def delete_wallet(self, wallet_id: str) -> bool:
        """
        Delete wallet.
        
        Args:
            wallet_id: Wallet ID
            
        Returns:
            True if deleted, False otherwise
        """
        wallet_file = self.storage_path / f"{wallet_id}.json"
        
        try:
            if wallet_file.exists():
                wallet_file.unlink()
            
            if wallet_id in self.wallets:
                del self.wallets[wallet_id]
            
            if wallet_id in self.wallet_passwords:
                del self.wallet_passwords[wallet_id]
            
            if self.active_wallet == wallet_id:
                self.active_wallet = None
            
            return True
            
        except Exception as e:
            print(f"Error deleting wallet: {str(e)}")
            return False
    
    def sign_transaction(self, tx_data: bytes, wallet_id: Optional[str] = None) -> Optional[bytes]:
        """
        Sign transaction with wallet.
        
        Args:
            tx_data: Transaction data to sign
            wallet_id: Wallet ID (uses active if not specified)
            
        Returns:
            Signature bytes or None
        """
        if wallet_id is None:
            wallet_id = self.active_wallet
        
        if wallet_id not in self.wallets:
            print("No active wallet")
            return None
        
        _, keypair = self.wallets[wallet_id]
        return keypair.sign(tx_data)
    
    def get_wallet_address(self, wallet_id: Optional[str] = None) -> Optional[str]:
        """
        Get wallet address (public key).
        
        Args:
            wallet_id: Wallet ID (uses active if not specified)
            
        Returns:
            Public key hex string or None
        """
        if wallet_id is None:
            wallet_id = self.active_wallet
        
        if wallet_id not in self.wallets:
            return None
        
        _, keypair = self.wallets[wallet_id]
        return keypair.public_key.hex()
    
    def get_statistics(self) -> Dict:
        """Get wallet manager statistics."""
        return {
            'total_wallets': len(self.wallets),
            'active_wallet': self.active_wallet,
            'storage_path': str(self.storage_path),
            'wallets': [
                {
                    'id': wallet_id,
                    'name': wallet[0].name,
                    'created_at': wallet[0].created_at,
                    'address': wallet[1].public_key.hex()[:16] + "..."
                }
                for wallet_id, wallet in self.wallets.items()
            ]
        }
