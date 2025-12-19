"""
Wallet Display and Management UI

Provides UI for:
- Viewing wallet balance
- Displaying wallet address
- Receiving transactions
- Viewing transaction history
- Backing up wallet
- Managing multiple wallets
"""

from typing import Optional, List, Callable
from dataclasses import dataclass
from meshchain.ui_display import Page, Widget, TextWidget, ListWidget, StatusBar, Rect, FontSize, Alignment
from meshchain.ui_menu import Menu, MenuSystem


@dataclass
class WalletInfo:
    """Information about a wallet."""
    name: str
    address: str
    balance: float
    transactions: int
    is_locked: bool = True


class WalletDisplayPage(Page):
    """Page for displaying wallet information."""
    
    def __init__(self, wallet_info: WalletInfo):
        """Initialize wallet display page."""
        super().__init__("Wallet Display")
        self.wallet_info = wallet_info
        
        # Title
        self.add_widget(TextWidget(0, 0, f"Wallet: {wallet_info.name}", FontSize.MEDIUM))
        
        # Status (locked/unlocked)
        status = "LOCKED" if wallet_info.is_locked else "UNLOCKED"
        self.add_widget(TextWidget(0, 16, f"Status: {status}", FontSize.SMALL))
        
        # Balance
        self.add_widget(TextWidget(0, 24, f"Balance: {wallet_info.balance:.2f} MC", FontSize.SMALL))
        
        # Address (truncated)
        addr_short = wallet_info.address[:16] + "..."
        self.add_widget(TextWidget(0, 32, f"Addr: {addr_short}", FontSize.SMALL))
        
        # Transaction count
        self.add_widget(TextWidget(0, 40, f"Txs: {wallet_info.transactions}", FontSize.SMALL))
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Wallet")
        status_bar.set_battery(85)
        status_bar.set_signal(3)
        self.add_widget(status_bar)
    
    def update_wallet(self, wallet_info: WalletInfo):
        """Update wallet information."""
        self.wallet_info = wallet_info
        # In production, would update widgets


class ReceiveAddressPage(Page):
    """Page for displaying receive address."""
    
    def __init__(self, address: str):
        """Initialize receive address page."""
        super().__init__("Receive Address")
        self.address = address
        
        # Title
        self.add_widget(TextWidget(64, 0, "RECEIVE", FontSize.MEDIUM, Alignment.CENTER))
        
        # Address (split into lines)
        addr_line1 = address[:21]
        addr_line2 = address[21:42]
        addr_line3 = address[42:] if len(address) > 42 else ""
        
        self.add_widget(TextWidget(0, 16, addr_line1, FontSize.SMALL))
        self.add_widget(TextWidget(0, 24, addr_line2, FontSize.SMALL))
        if addr_line3:
            self.add_widget(TextWidget(0, 32, addr_line3, FontSize.SMALL))
        
        # Instructions
        self.add_widget(TextWidget(64, 48, "Share this address", FontSize.SMALL, Alignment.CENTER))
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Receive")
        self.add_widget(status_bar)


class TransactionHistoryPage(Page):
    """Page for displaying transaction history."""
    
    def __init__(self, transactions: List[dict]):
        """Initialize transaction history page."""
        super().__init__("Transaction History")
        self.transactions = transactions
        
        # Title
        self.add_widget(TextWidget(64, 0, "TRANSACTIONS", FontSize.MEDIUM, Alignment.CENTER))
        
        # Transaction list
        tx_items = []
        for tx in transactions:
            direction = ">" if tx['direction'] == 'sent' else "<"
            amount = tx['amount']
            tx_items.append(f"{direction} {amount:.2f} MC")
        
        tx_list = ListWidget(0, 16, 128, 32, tx_items)
        self.add_widget(tx_list)
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("History")
        self.add_widget(status_bar)


class WalletBackupPage(Page):
    """Page for wallet backup."""
    
    def __init__(self, seed_phrase: str):
        """Initialize wallet backup page."""
        super().__init__("Wallet Backup")
        self.seed_phrase = seed_phrase
        
        # Title
        self.add_widget(TextWidget(64, 0, "BACKUP SEED", FontSize.MEDIUM, Alignment.CENTER))
        
        # Warning
        self.add_widget(TextWidget(64, 16, "WRITE DOWN THESE WORDS", FontSize.SMALL, Alignment.CENTER))
        self.add_widget(TextWidget(64, 24, "DO NOT SHARE", FontSize.SMALL, Alignment.CENTER))
        
        # Seed words (first 6 shown)
        words = seed_phrase.split()[:6]
        for i, word in enumerate(words):
            y = 32 + i * 4
            self.add_widget(TextWidget(0, y, f"{i+1}. {word}", FontSize.SMALL))
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Backup")
        self.add_widget(status_bar)


class MultiWalletPage(Page):
    """Page for managing multiple wallets."""
    
    def __init__(self, wallets: List[WalletInfo]):
        """Initialize multi-wallet page."""
        super().__init__("Wallets")
        self.wallets = wallets
        
        # Title
        self.add_widget(TextWidget(64, 0, "WALLETS", FontSize.MEDIUM, Alignment.CENTER))
        
        # Wallet list
        wallet_items = [f"{w.name} ({w.balance:.2f})" for w in wallets]
        wallet_list = ListWidget(0, 16, 128, 32, wallet_items)
        self.add_widget(wallet_list)
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Wallets")
        self.add_widget(status_bar)


class WalletMenuBuilder:
    """Builds the wallet menu structure."""
    
    @staticmethod
    def build_wallet_menu(wallet_info: WalletInfo, 
                         on_send: Callable,
                         on_receive: Callable,
                         on_backup: Callable) -> Menu:
        """Build wallet menu."""
        menu = Menu("Wallet")
        
        # View Balance
        menu.add_action("View Balance", 
                       lambda: print(f"Balance: {wallet_info.balance}"))
        
        # Receive
        menu.add_action("Receive", on_receive)
        
        # Send
        menu.add_action("Send", on_send)
        
        # Transaction History
        menu.add_action("History", 
                       lambda: print(f"Transactions: {wallet_info.transactions}"))
        
        # Backup
        menu.add_action("Backup", on_backup)
        
        return menu


class WalletManager:
    """
    Manages wallet display and operations.
    
    Handles:
    - Displaying wallet information
    - Managing multiple wallets
    - Transaction history
    - Backup and restore
    """
    
    def __init__(self):
        """Initialize wallet manager."""
        self.wallets: dict = {}
        self.current_wallet: Optional[str] = None
        self.on_send: Optional[Callable] = None
        self.on_receive: Optional[Callable] = None
        self.on_backup: Optional[Callable] = None
    
    def add_wallet(self, wallet_info: WalletInfo):
        """Add a wallet."""
        self.wallets[wallet_info.name] = wallet_info
        if not self.current_wallet:
            self.current_wallet = wallet_info.name
    
    def get_current_wallet(self) -> Optional[WalletInfo]:
        """Get the current wallet."""
        if self.current_wallet and self.current_wallet in self.wallets:
            return self.wallets[self.current_wallet]
        return None
    
    def set_current_wallet(self, name: str) -> bool:
        """Set the current wallet."""
        if name in self.wallets:
            self.current_wallet = name
            return True
        return False
    
    def get_wallets(self) -> List[WalletInfo]:
        """Get all wallets."""
        return list(self.wallets.values())
    
    def update_balance(self, wallet_name: str, balance: float):
        """Update wallet balance."""
        if wallet_name in self.wallets:
            self.wallets[wallet_name].balance = balance
    
    def add_transaction(self, wallet_name: str, tx: dict):
        """Add a transaction to wallet history."""
        if wallet_name in self.wallets:
            self.wallets[wallet_name].transactions += 1
    
    def lock_wallet(self, wallet_name: str):
        """Lock a wallet."""
        if wallet_name in self.wallets:
            self.wallets[wallet_name].is_locked = True
    
    def unlock_wallet(self, wallet_name: str):
        """Unlock a wallet."""
        if wallet_name in self.wallets:
            self.wallets[wallet_name].is_locked = False
    
    def create_wallet_page(self) -> Optional[Page]:
        """Create a display page for the current wallet."""
        wallet = self.get_current_wallet()
        if wallet:
            return WalletDisplayPage(wallet)
        return None
    
    def create_receive_page(self) -> Optional[Page]:
        """Create a receive address page."""
        wallet = self.get_current_wallet()
        if wallet:
            return ReceiveAddressPage(wallet.address)
        return None
    
    def create_history_page(self, transactions: List[dict]) -> Page:
        """Create a transaction history page."""
        return TransactionHistoryPage(transactions)
    
    def create_backup_page(self, seed_phrase: str) -> Page:
        """Create a backup page."""
        return WalletBackupPage(seed_phrase)
    
    def create_multi_wallet_page(self) -> Page:
        """Create a multi-wallet page."""
        return MultiWalletPage(self.get_wallets())
