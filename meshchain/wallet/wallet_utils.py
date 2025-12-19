"""
MeshChain Wallet Utilities - Backup, Restore, and Key Management

This module provides utility functions for:
1. Generating BIP39 seed phrases
2. Wallet backup to file
3. Wallet restore from file
4. Key export/import
5. Wallet recovery
6. Password strength validation
"""

import json
import hashlib
import secrets
import time
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from datetime import datetime
import qrcode
from io import BytesIO


# BIP39 Word List (first 1000 words for seed phrase generation)
BIP39_WORDLIST = [
    "abandon", "ability", "able", "about", "above", "absent", "absorb", "abstract",
    "abuse", "access", "accident", "account", "accuse", "achieve", "acid", "acoustic",
    "acquire", "across", "act", "action", "actor", "acts", "actual", "acuate",
    # ... (truncated for brevity - full list would be 2048 words)
] * 50  # Repeat for demonstration


class BIP39Generator:
    """Generate BIP39 seed phrases for wallet recovery."""
    
    @staticmethod
    def generate_seed_phrase(word_count: int = 12) -> str:
        """
        Generate BIP39 seed phrase.
        
        Args:
            word_count: Number of words (12, 15, 18, 21, or 24)
            
        Returns:
            Space-separated seed phrase
        """
        if word_count not in [12, 15, 18, 21, 24]:
            raise ValueError("Word count must be 12, 15, 18, 21, or 24")
        
        # Generate random words
        words = []
        for _ in range(word_count):
            word_index = secrets.randbelow(len(BIP39_WORDLIST))
            words.append(BIP39_WORDLIST[word_index])
        
        return " ".join(words)
    
    @staticmethod
    def validate_seed_phrase(seed_phrase: str) -> bool:
        """
        Validate BIP39 seed phrase.
        
        Args:
            seed_phrase: Space-separated seed phrase
            
        Returns:
            True if valid, False otherwise
        """
        words = seed_phrase.split()
        
        # Check word count
        if len(words) not in [12, 15, 18, 21, 24]:
            return False
        
        # Check all words are in wordlist
        for word in words:
            if word not in BIP39_WORDLIST:
                return False
        
        return True


class PasswordValidator:
    """Validate password strength."""
    
    @staticmethod
    def validate_password(password: str) -> Tuple[bool, List[str]]:
        """
        Validate password strength.
        
        Requirements:
        - Minimum 12 characters
        - At least one uppercase letter
        - At least one lowercase letter
        - At least one digit
        - At least one special character
        
        Args:
            password: Password to validate
            
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []
        
        if len(password) < 12:
            issues.append("Password must be at least 12 characters")
        
        if not any(c.isupper() for c in password):
            issues.append("Password must contain at least one uppercase letter")
        
        if not any(c.islower() for c in password):
            issues.append("Password must contain at least one lowercase letter")
        
        if not any(c.isdigit() for c in password):
            issues.append("Password must contain at least one digit")
        
        special_chars = "!@#$%^&*()_+-=[]{}|;:,.<>?"
        if not any(c in special_chars for c in password):
            issues.append("Password must contain at least one special character")
        
        return len(issues) == 0, issues
    
    @staticmethod
    def get_password_strength(password: str) -> str:
        """
        Get password strength rating.
        
        Args:
            password: Password to rate
            
        Returns:
            Strength rating: "WEAK", "FAIR", "GOOD", "STRONG", "EXCELLENT"
        """
        score = 0
        
        # Length
        if len(password) >= 12:
            score += 1
        if len(password) >= 16:
            score += 1
        if len(password) >= 20:
            score += 1
        
        # Character variety
        if any(c.isupper() for c in password):
            score += 1
        if any(c.islower() for c in password):
            score += 1
        if any(c.isdigit() for c in password):
            score += 1
        
        special_chars = "!@#$%^&*()_+-=[]{}|;:,.<>?"
        if any(c in special_chars for c in password):
            score += 1
        
        # Entropy
        if len(set(password)) >= 8:
            score += 1
        
        # Rating
        if score <= 2:
            return "WEAK"
        elif score <= 4:
            return "FAIR"
        elif score <= 6:
            return "GOOD"
        elif score <= 7:
            return "STRONG"
        else:
            return "EXCELLENT"


class WalletBackup:
    """Handle wallet backup operations."""
    
    @staticmethod
    def create_backup_file(wallet_data: Dict, backup_path: str, 
                          include_seed: bool = False) -> bool:
        """
        Create wallet backup file.
        
        Args:
            wallet_data: Wallet data dictionary
            backup_path: Path to save backup
            include_seed: Include seed phrase in backup
            
        Returns:
            True if successful, False otherwise
        """
        try:
            backup = {
                'backup_date': datetime.now().isoformat(),
                'wallet_id': wallet_data.get('wallet_id'),
                'name': wallet_data.get('name'),
                'encrypted_key': wallet_data.get('encrypted_key'),
                'public_key': wallet_data.get('public_key'),
                'version': '1.0'
            }
            
            if include_seed:
                backup['seed_phrase'] = wallet_data.get('seed_phrase')
            
            # Add checksum
            backup_json = json.dumps(backup, sort_keys=True)
            checksum = hashlib.sha256(backup_json.encode()).hexdigest()
            backup['checksum'] = checksum
            
            # Save to file
            backup_path = Path(backup_path)
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(backup_path, 'w') as f:
                json.dump(backup, f, indent=2)
            
            return True
            
        except Exception as e:
            print(f"Error creating backup: {str(e)}")
            return False
    
    @staticmethod
    def restore_backup_file(backup_path: str) -> Optional[Dict]:
        """
        Restore wallet from backup file.
        
        Args:
            backup_path: Path to backup file
            
        Returns:
            Wallet data dictionary or None if restoration fails
        """
        try:
            backup_path = Path(backup_path)
            
            if not backup_path.exists():
                print(f"Backup file not found: {backup_path}")
                return None
            
            with open(backup_path, 'r') as f:
                backup = json.load(f)
            
            # Verify checksum
            checksum = backup.pop('checksum', None)
            backup_json = json.dumps(backup, sort_keys=True)
            calculated_checksum = hashlib.sha256(backup_json.encode()).hexdigest()
            
            if checksum != calculated_checksum:
                print("Backup checksum verification failed - file may be corrupted")
                return None
            
            return backup
            
        except Exception as e:
            print(f"Error restoring backup: {str(e)}")
            return None


class WalletRecovery:
    """Handle wallet recovery operations."""
    
    @staticmethod
    def create_recovery_document(wallet_data: Dict, seed_phrase: str,
                                output_path: str) -> bool:
        """
        Create wallet recovery document (for printing/storage).
        
        Args:
            wallet_data: Wallet data
            seed_phrase: BIP39 seed phrase
            output_path: Path to save document
            
        Returns:
            True if successful, False otherwise
        """
        try:
            recovery_doc = f"""
╔════════════════════════════════════════════════════════════════╗
║          MESHCHAIN WALLET RECOVERY DOCUMENT                    ║
║                   KEEP THIS SAFE!                              ║
╚════════════════════════════════════════════════════════════════╝

WALLET NAME: {wallet_data.get('name')}
WALLET ID: {wallet_data.get('wallet_id')}
CREATED: {datetime.now().isoformat()}

PUBLIC ADDRESS (SAFE TO SHARE):
{wallet_data.get('public_key')}

SEED PHRASE (NEVER SHARE - WRITE DOWN AND STORE SAFELY):
{seed_phrase}

RECOVERY INSTRUCTIONS:
1. Keep this document in a safe place
2. Never share the seed phrase with anyone
3. If you lose access to your device:
   a. Install MeshChain on a new device
   b. Select "Restore from Seed Phrase"
   c. Enter the seed phrase above
   d. Create a new password

SECURITY NOTES:
- This seed phrase grants full access to your wallet
- Anyone with this phrase can steal your funds
- Store multiple copies in secure locations
- Consider using a safe deposit box
- Never store digitally unless encrypted

════════════════════════════════════════════════════════════════
"""
            
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                f.write(recovery_doc)
            
            return True
            
        except Exception as e:
            print(f"Error creating recovery document: {str(e)}")
            return False
    
    @staticmethod
    def create_recovery_qr_code(wallet_data: Dict, output_path: str) -> bool:
        """
        Create QR code for wallet recovery.
        
        Args:
            wallet_data: Wallet data
            output_path: Path to save QR code
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create QR code data
            qr_data = json.dumps({
                'wallet_id': wallet_data.get('wallet_id'),
                'public_key': wallet_data.get('public_key'),
                'name': wallet_data.get('name')
            })
            
            # Generate QR code
            qr = qrcode.QRCode(
                version=1,
                error_correction=qrcode.constants.ERROR_CORRECT_L,
                box_size=10,
                border=4,
            )
            qr.add_data(qr_data)
            qr.make(fit=True)
            
            # Save image
            img = qr.make_image(fill_color="black", back_color="white")
            
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            img.save(output_path)
            
            return True
            
        except Exception as e:
            print(f"Error creating QR code: {str(e)}")
            return False


class KeyExport:
    """Handle key export/import operations."""
    
    @staticmethod
    def export_public_key(public_key: bytes, format: str = "hex") -> str:
        """
        Export public key in various formats.
        
        Args:
            public_key: Public key bytes
            format: Export format ("hex", "base64", "base58")
            
        Returns:
            Exported key string
        """
        if format == "hex":
            return public_key.hex()
        elif format == "base64":
            import base64
            return base64.b64encode(public_key).decode()
        elif format == "base58":
            # Simple base58 encoding
            import base58
            return base58.b58encode(public_key).decode()
        else:
            raise ValueError(f"Unknown format: {format}")
    
    @staticmethod
    def import_public_key(key_string: str, format: str = "hex") -> Optional[bytes]:
        """
        Import public key from various formats.
        
        Args:
            key_string: Key string
            format: Import format ("hex", "base64", "base58")
            
        Returns:
            Public key bytes or None
        """
        try:
            if format == "hex":
                return bytes.fromhex(key_string)
            elif format == "base64":
                import base64
                return base64.b64decode(key_string)
            elif format == "base58":
                import base58
                return base58.b58decode(key_string)
            else:
                raise ValueError(f"Unknown format: {format}")
        except Exception as e:
            print(f"Error importing key: {str(e)}")
            return None


class WalletStatistics:
    """Generate wallet statistics and reports."""
    
    @staticmethod
    def generate_wallet_report(wallet_manager) -> Dict:
        """
        Generate comprehensive wallet report.
        
        Args:
            wallet_manager: WalletManager instance
            
        Returns:
            Report dictionary
        """
        stats = wallet_manager.get_statistics()
        
        report = {
            'generated_at': datetime.now().isoformat(),
            'total_wallets': stats['total_wallets'],
            'active_wallet': stats['active_wallet'],
            'wallets': []
        }
        
        for wallet_info in stats['wallets']:
            report['wallets'].append({
                'name': wallet_info['name'],
                'address': wallet_info['address'],
                'created_at': datetime.fromtimestamp(wallet_info['created_at']).isoformat()
            })
        
        return report
