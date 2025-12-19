"""
MeshChain CLI - Command Line Interface for Wallet and Blockchain Operations

This module provides a command-line interface for:
1. Wallet creation and management
2. Transaction creation and signing
3. Blockchain querying
4. Network status
5. Wallet backup and recovery
"""

import sys
import json
import getpass
from typing import Optional, Dict, List
from pathlib import Path
from datetime import datetime
import argparse

from meshchain.wallet import WalletManager
from meshchain.wallet_utils import (
    PasswordValidator, BIP39Generator, WalletBackup, 
    WalletRecovery, KeyExport, WalletStatistics
)
from meshchain.storage import BlockchainStorage
from meshchain.transaction import Transaction
from meshchain.consensus import ConsensusEngine
from meshchain.network import MeshtasticNetwork


class MeshChainCLI:
    """Command-line interface for MeshChain."""
    
    def __init__(self, storage_path: str = "/mnt/microsd/wallets",
                 db_path: str = "/mnt/microsd/blockchain.db"):
        """
        Initialize CLI.
        
        Args:
            storage_path: Wallet storage path
            db_path: Blockchain database path
        """
        self.wallet_manager = WalletManager(storage_path)
        self.blockchain = BlockchainStorage(db_path)
        self.consensus = ConsensusEngine()
        self.network = None  # Will be initialized when needed
    
    def print_header(self, title: str):
        """Print formatted header."""
        print("\n" + "="*60)
        print(f"  {title}")
        print("="*60 + "\n")
    
    def print_success(self, message: str):
        """Print success message."""
        print(f"✓ {message}")
    
    def print_error(self, message: str):
        """Print error message."""
        print(f"✗ {message}")
    
    def print_info(self, message: str):
        """Print info message."""
        print(f"ℹ {message}")
    
    # Wallet Commands
    
    def cmd_wallet_create(self, args):
        """Create a new wallet."""
        self.print_header("Create New Wallet")
        
        name = input("Wallet name: ").strip()
        if not name:
            self.print_error("Wallet name cannot be empty")
            return
        
        while True:
            password = getpass.getpass("Enter password: ")
            
            # Validate password
            is_valid, issues = PasswordValidator.validate_password(password)
            if not is_valid:
                self.print_error("Password does not meet requirements:")
                for issue in issues:
                    print(f"  - {issue}")
                continue
            
            # Confirm password
            password_confirm = getpass.getpass("Confirm password: ")
            if password != password_confirm:
                self.print_error("Passwords do not match")
                continue
            
            break
        
        # Create wallet
        wallet_id, keypair = self.wallet_manager.create_wallet(name, password)
        
        self.print_success(f"Wallet created: {wallet_id}")
        print(f"\nWallet Name: {name}")
        print(f"Wallet ID: {wallet_id}")
        print(f"Address: {keypair.public_key.hex()}")
        
        # Generate seed phrase
        seed_phrase = BIP39Generator.generate_seed_phrase(12)
        print(f"\nSeed Phrase (12 words):")
        print(f"{seed_phrase}")
        print("\n⚠ IMPORTANT: Write down this seed phrase and store it safely!")
        print("   Anyone with this phrase can access your wallet.")
        
        # Offer to create recovery document
        create_recovery = input("\nCreate recovery document? (y/n): ").lower() == 'y'
        if create_recovery:
            recovery_path = Path("/mnt/microsd/recovery") / f"{wallet_id}_recovery.txt"
            WalletRecovery.create_recovery_document(
                {
                    'name': name,
                    'wallet_id': wallet_id,
                    'public_key': keypair.public_key.hex()
                },
                seed_phrase,
                str(recovery_path)
            )
            self.print_success(f"Recovery document saved: {recovery_path}")
    
    def cmd_wallet_list(self, args):
        """List all wallets."""
        self.print_header("Wallets")
        
        wallets = self.wallet_manager.list_wallets()
        
        if not wallets:
            self.print_info("No wallets found")
            return
        
        print(f"{'Name':<20} {'ID':<16} {'Created':<20} {'Address':<20}")
        print("-" * 80)
        
        for wallet in wallets:
            created = datetime.fromtimestamp(wallet['created_at']).strftime("%Y-%m-%d %H:%M")
            print(f"{wallet['name']:<20} {wallet['wallet_id']:<16} {created:<20} {wallet['address']:<20}")
    
    def cmd_wallet_load(self, args):
        """Load a wallet."""
        self.print_header("Load Wallet")
        
        wallet_id = input("Wallet ID: ").strip()
        password = getpass.getpass("Password: ")
        
        keypair = self.wallet_manager.load_wallet(wallet_id, password)
        
        if keypair:
            self.print_success(f"Wallet loaded: {wallet_id}")
            print(f"Address: {keypair.public_key.hex()}")
        else:
            self.print_error("Failed to load wallet (invalid password?)")
    
    def cmd_wallet_delete(self, args):
        """Delete a wallet."""
        self.print_header("Delete Wallet")
        
        wallet_id = input("Wallet ID: ").strip()
        
        confirm = input(f"Delete wallet {wallet_id}? (y/n): ").lower() == 'y'
        if not confirm:
            self.print_info("Deletion cancelled")
            return
        
        if self.wallet_manager.delete_wallet(wallet_id):
            self.print_success(f"Wallet deleted: {wallet_id}")
        else:
            self.print_error("Failed to delete wallet")
    
    def cmd_wallet_export(self, args):
        """Export wallet for backup."""
        self.print_header("Export Wallet")
        
        wallet_id = input("Wallet ID: ").strip()
        export_password = getpass.getpass("Export password: ")
        
        backup = self.wallet_manager.export_wallet(wallet_id, export_password)
        
        if backup:
            backup_path = Path("/mnt/microsd/backups") / f"{wallet_id}_backup.json"
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(backup_path, 'w') as f:
                json.dump(backup, f, indent=2)
            
            self.print_success(f"Wallet exported: {backup_path}")
        else:
            self.print_error("Failed to export wallet")
    
    def cmd_wallet_import(self, args):
        """Import wallet from backup."""
        self.print_header("Import Wallet")
        
        backup_path = input("Backup file path: ").strip()
        import_password = getpass.getpass("Backup password: ")
        
        backup = WalletBackup.restore_backup_file(backup_path)
        
        if not backup:
            self.print_error("Failed to read backup file")
            return
        
        new_password = getpass.getpass("New password (or press Enter to keep same): ")
        
        wallet_id = self.wallet_manager.import_wallet(
            backup,
            import_password,
            new_password if new_password else None
        )
        
        if wallet_id:
            self.print_success(f"Wallet imported: {wallet_id}")
        else:
            self.print_error("Failed to import wallet")
    
    def cmd_wallet_info(self, args):
        """Show wallet information."""
        self.print_header("Wallet Information")
        
        wallet_id = input("Wallet ID (or press Enter for active): ").strip()
        
        if not wallet_id:
            wallet_id = self.wallet_manager.active_wallet
        
        if not wallet_id:
            self.print_error("No active wallet")
            return
        
        if wallet_id not in self.wallet_manager.wallets:
            self.print_error(f"Wallet not found: {wallet_id}")
            return
        
        encrypted_wallet, keypair = self.wallet_manager.wallets[wallet_id]
        
        print(f"Name: {encrypted_wallet.name}")
        print(f"ID: {wallet_id}")
        print(f"Address: {keypair.public_key.hex()}")
        print(f"Created: {datetime.fromtimestamp(encrypted_wallet.created_at)}")
        print(f"Last Accessed: {datetime.fromtimestamp(encrypted_wallet.last_accessed)}")
    
    # Transaction Commands
    
    def cmd_transaction_create(self, args):
        """Create a new transaction."""
        self.print_header("Create Transaction")
        
        if not self.wallet_manager.active_wallet:
            self.print_error("No active wallet")
            return
        
        recipient = input("Recipient address: ").strip()
        amount = float(input("Amount (MESH): "))
        fee = float(input("Fee (MESH): "))
        
        # Create transaction
        tx = Transaction(
            sender=self.wallet_manager.get_wallet_address(),
            recipient=recipient,
            amount=int(amount * 1e8),  # Convert to satoshis
            fee=int(fee * 1e8),
            timestamp=int(datetime.now().timestamp()),
            ring_size=8,
            signature=b''  # Will be signed
        )
        
        # Sign transaction
        tx_data = tx.serialize_for_signing()
        signature = self.wallet_manager.sign_transaction(tx_data)
        
        if signature:
            tx.signature = signature
            self.print_success("Transaction created and signed")
            print(f"Transaction ID: {tx.transaction_id.hex()}")
            print(f"Amount: {amount} MESH")
            print(f"Fee: {fee} MESH")
        else:
            self.print_error("Failed to sign transaction")
    
    # Blockchain Commands
    
    def cmd_blockchain_info(self, args):
        """Show blockchain information."""
        self.print_header("Blockchain Information")
        
        height = self.blockchain.get_blockchain_height()
        total_txs = len(self.blockchain.get_all_transactions())
        
        print(f"Height: {height}")
        print(f"Total Transactions: {total_txs}")
        print(f"Database: {self.blockchain.db_path}")
    
    def cmd_blockchain_status(self, args):
        """Show blockchain status."""
        self.print_header("Blockchain Status")
        
        stats = self.consensus.get_statistics()
        
        print(f"Validators: {stats['total_validators']}")
        print(f"Total Stake: {stats['total_stake']} MESH")
        print(f"Gini Coefficient: {stats['gini_coefficient']:.4f}")
    
    # Network Commands
    
    def cmd_network_status(self, args):
        """Show network status."""
        self.print_header("Network Status")
        
        if not self.network:
            self.print_error("Network not connected")
            return
        
        stats = self.network.get_statistics()
        
        print(f"Messages Sent: {stats['messages_sent']}")
        print(f"Messages Received: {stats['messages_received']}")
        print(f"Bytes Sent: {stats['bytes_sent']}")
        print(f"Bytes Received: {stats['bytes_received']}")
    
    # Help and Main
    
    def print_help(self):
        """Print help message."""
        self.print_header("MeshChain CLI Help")
        
        commands = {
            "Wallet Commands": [
                ("wallet create", "Create a new wallet"),
                ("wallet list", "List all wallets"),
                ("wallet load", "Load a wallet"),
                ("wallet delete", "Delete a wallet"),
                ("wallet export", "Export wallet for backup"),
                ("wallet import", "Import wallet from backup"),
                ("wallet info", "Show wallet information"),
            ],
            "Transaction Commands": [
                ("tx create", "Create a new transaction"),
            ],
            "Blockchain Commands": [
                ("blockchain info", "Show blockchain information"),
                ("blockchain status", "Show blockchain status"),
            ],
            "Network Commands": [
                ("network status", "Show network status"),
            ],
            "Other": [
                ("help", "Show this help message"),
                ("exit", "Exit the CLI"),
            ]
        }
        
        for category, cmds in commands.items():
            print(f"\n{category}:")
            for cmd, desc in cmds:
                print(f"  {cmd:<25} {desc}")
    
    def run_interactive(self):
        """Run interactive CLI."""
        self.print_header("MeshChain CLI")
        print("Type 'help' for available commands")
        
        while True:
            try:
                command = input("\nmeshchain> ").strip().lower()
                
                if not command:
                    continue
                
                parts = command.split()
                cmd = parts[0]
                
                if cmd == "help":
                    self.print_help()
                elif cmd == "exit":
                    print("Goodbye!")
                    break
                elif cmd == "wallet":
                    if len(parts) < 2:
                        self.print_error("Usage: wallet <create|list|load|delete|export|import|info>")
                        continue
                    
                    subcmd = parts[1]
                    if subcmd == "create":
                        self.cmd_wallet_create(parts[2:])
                    elif subcmd == "list":
                        self.cmd_wallet_list(parts[2:])
                    elif subcmd == "load":
                        self.cmd_wallet_load(parts[2:])
                    elif subcmd == "delete":
                        self.cmd_wallet_delete(parts[2:])
                    elif subcmd == "export":
                        self.cmd_wallet_export(parts[2:])
                    elif subcmd == "import":
                        self.cmd_wallet_import(parts[2:])
                    elif subcmd == "info":
                        self.cmd_wallet_info(parts[2:])
                    else:
                        self.print_error(f"Unknown wallet command: {subcmd}")
                
                elif cmd == "tx":
                    if len(parts) < 2:
                        self.print_error("Usage: tx <create>")
                        continue
                    
                    subcmd = parts[1]
                    if subcmd == "create":
                        self.cmd_transaction_create(parts[2:])
                    else:
                        self.print_error(f"Unknown tx command: {subcmd}")
                
                elif cmd == "blockchain":
                    if len(parts) < 2:
                        self.print_error("Usage: blockchain <info|status>")
                        continue
                    
                    subcmd = parts[1]
                    if subcmd == "info":
                        self.cmd_blockchain_info(parts[2:])
                    elif subcmd == "status":
                        self.cmd_blockchain_status(parts[2:])
                    else:
                        self.print_error(f"Unknown blockchain command: {subcmd}")
                
                elif cmd == "network":
                    if len(parts) < 2:
                        self.print_error("Usage: network <status>")
                        continue
                    
                    subcmd = parts[1]
                    if subcmd == "status":
                        self.cmd_network_status(parts[2:])
                    else:
                        self.print_error(f"Unknown network command: {subcmd}")
                
                else:
                    self.print_error(f"Unknown command: {cmd}")
            
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break
            except Exception as e:
                self.print_error(f"Error: {str(e)}")


def main():
    """Main entry point."""
    cli = MeshChainCLI()
    cli.run_interactive()


if __name__ == "__main__":
    main()
