"""
Transaction Creation and Signing UI

Provides UI for:
- Entering recipient address
- Entering transaction amount
- Reviewing transaction details
- Signing transactions
- Confirming transaction
- Displaying transaction status
"""

from typing import Optional, Callable, List
from dataclasses import dataclass
from meshchain.ui_display import Page, Widget, TextWidget, ListWidget, ProgressWidget, StatusBar, Rect, FontSize, Alignment


@dataclass
class TransactionDraft:
    """A transaction being created."""
    recipient: str = ""
    amount: float = 0.0
    fee: float = 0.001
    description: str = ""
    
    def get_total(self) -> float:
        """Get total amount including fee."""
        return self.amount + self.fee


class AmountInputPage(Page):
    """Page for entering transaction amount."""
    
    def __init__(self, on_amount_entered: Callable[[float], None]):
        """Initialize amount input page."""
        super().__init__("Enter Amount")
        self.on_amount_entered = on_amount_entered
        self.amount_str = "0.00"
        self.cursor_pos = 0
        
        # Title
        self.add_widget(TextWidget(64, 0, "ENTER AMOUNT", FontSize.MEDIUM, Alignment.CENTER))
        
        # Amount display
        self.amount_widget = TextWidget(64, 24, self.amount_str + " MC", FontSize.LARGE, Alignment.CENTER)
        self.add_widget(self.amount_widget)
        
        # Instructions
        self.add_widget(TextWidget(64, 48, "Up/Down: adjust", FontSize.SMALL, Alignment.CENTER))
        self.add_widget(TextWidget(64, 56, "Select: confirm", FontSize.SMALL, Alignment.CENTER))
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Amount")
        self.add_widget(status_bar)
    
    def increment_amount(self, delta: float = 0.1):
        """Increment the amount."""
        current = float(self.amount_str)
        current += delta
        self.amount_str = f"{current:.2f}"
        self.amount_widget.set_text(self.amount_str + " MC")
    
    def decrement_amount(self, delta: float = 0.1):
        """Decrement the amount."""
        current = float(self.amount_str)
        current = max(0, current - delta)
        self.amount_str = f"{current:.2f}"
        self.amount_widget.set_text(self.amount_str + " MC")
    
    def confirm_amount(self):
        """Confirm the amount."""
        try:
            amount = float(self.amount_str)
            self.on_amount_entered(amount)
        except ValueError:
            pass


class RecipientInputPage(Page):
    """Page for entering recipient address."""
    
    def __init__(self, on_recipient_entered: Callable[[str], None]):
        """Initialize recipient input page."""
        super().__init__("Enter Recipient")
        self.on_recipient_entered = on_recipient_entered
        self.recipient = ""
        
        # Title
        self.add_widget(TextWidget(64, 0, "RECIPIENT", FontSize.MEDIUM, Alignment.CENTER))
        
        # Address display
        self.address_widget = TextWidget(0, 16, self.recipient, FontSize.SMALL)
        self.add_widget(self.address_widget)
        
        # Instructions
        self.add_widget(TextWidget(64, 48, "Enter address", FontSize.SMALL, Alignment.CENTER))
        self.add_widget(TextWidget(64, 56, "Select: confirm", FontSize.SMALL, Alignment.CENTER))
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Recipient")
        self.add_widget(status_bar)
    
    def add_character(self, char: str):
        """Add a character to the address."""
        if len(self.recipient) < 64:
            self.recipient += char
            self.address_widget.set_text(self.recipient)
    
    def backspace(self):
        """Remove the last character."""
        if self.recipient:
            self.recipient = self.recipient[:-1]
            self.address_widget.set_text(self.recipient)
    
    def confirm_recipient(self):
        """Confirm the recipient."""
        if len(self.recipient) > 0:
            self.on_recipient_entered(self.recipient)


class TransactionReviewPage(Page):
    """Page for reviewing transaction details."""
    
    def __init__(self, tx_draft: TransactionDraft,
                 on_confirm: Callable[[], None],
                 on_cancel: Callable[[], None]):
        """Initialize transaction review page."""
        super().__init__("Review Transaction")
        self.tx_draft = tx_draft
        self.on_confirm = on_confirm
        self.on_cancel = on_cancel
        
        # Title
        self.add_widget(TextWidget(64, 0, "REVIEW", FontSize.MEDIUM, Alignment.CENTER))
        
        # Amount
        self.add_widget(TextWidget(0, 12, f"Amount: {tx_draft.amount:.2f} MC", FontSize.SMALL))
        
        # Fee
        self.add_widget(TextWidget(0, 20, f"Fee: {tx_draft.fee:.4f} MC", FontSize.SMALL))
        
        # Total
        total = tx_draft.get_total()
        self.add_widget(TextWidget(0, 28, f"Total: {total:.4f} MC", FontSize.SMALL))
        
        # Recipient (truncated)
        recipient_short = tx_draft.recipient[:20] + "..."
        self.add_widget(TextWidget(0, 36, f"To: {recipient_short}", FontSize.SMALL))
        
        # Instructions
        self.add_widget(TextWidget(64, 48, "Select: confirm", FontSize.SMALL, Alignment.CENTER))
        self.add_widget(TextWidget(64, 56, "Back: cancel", FontSize.SMALL, Alignment.CENTER))
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Review")
        self.add_widget(status_bar)
    
    def confirm(self):
        """Confirm the transaction."""
        self.on_confirm()
    
    def cancel(self):
        """Cancel the transaction."""
        self.on_cancel()


class TransactionSigningPage(Page):
    """Page for signing a transaction."""
    
    def __init__(self, on_signed: Callable[[], None]):
        """Initialize transaction signing page."""
        super().__init__("Signing")
        self.on_signed = on_signed
        self.progress = 0.0
        
        # Title
        self.add_widget(TextWidget(64, 0, "SIGNING", FontSize.MEDIUM, Alignment.CENTER))
        
        # Progress bar
        progress_widget = ProgressWidget(10, 24, 108, 8, 0.0)
        self.add_widget(progress_widget)
        self.progress_widget = progress_widget
        
        # Status
        self.status_widget = TextWidget(64, 40, "Initializing...", FontSize.SMALL, Alignment.CENTER)
        self.add_widget(self.status_widget)
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Sign")
        self.add_widget(status_bar)
    
    def set_progress(self, progress: float):
        """Set signing progress."""
        self.progress = progress
        self.progress_widget.set_progress(progress)
    
    def set_status(self, status: str):
        """Set status message."""
        self.status_widget.set_text(status)
    
    def signing_complete(self):
        """Called when signing is complete."""
        self.on_signed()


class TransactionStatusPage(Page):
    """Page for displaying transaction status."""
    
    def __init__(self, tx_hash: str, status: str = "Pending"):
        """Initialize transaction status page."""
        super().__init__("Transaction Status")
        self.tx_hash = tx_hash
        self.status = status
        
        # Title
        self.add_widget(TextWidget(64, 0, "TRANSACTION", FontSize.MEDIUM, Alignment.CENTER))
        
        # Status
        self.add_widget(TextWidget(64, 16, status.upper(), FontSize.MEDIUM, Alignment.CENTER))
        
        # Transaction hash (truncated)
        tx_short = tx_hash[:16] + "..."
        self.add_widget(TextWidget(0, 32, f"Hash: {tx_short}", FontSize.SMALL))
        
        # Confirmations
        self.confirmations_widget = TextWidget(0, 40, "Confirmations: 0/3", FontSize.SMALL)
        self.add_widget(self.confirmations_widget)
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Status")
        self.add_widget(status_bar)
    
    def update_confirmations(self, confirmations: int, required: int = 3):
        """Update confirmation count."""
        self.confirmations_widget.set_text(f"Confirmations: {confirmations}/{required}")


class TransactionManager:
    """
    Manages transaction creation and signing.
    
    Handles:
    - Transaction creation workflow
    - Amount and recipient input
    - Transaction review
    - Transaction signing
    - Status tracking
    """
    
    def __init__(self):
        """Initialize transaction manager."""
        self.current_tx: Optional[TransactionDraft] = None
        self.on_transaction_created: Optional[Callable] = None
        self.on_transaction_signed: Optional[Callable] = None
        self.on_transaction_failed: Optional[Callable] = None
    
    def start_transaction(self) -> TransactionDraft:
        """Start creating a new transaction."""
        self.current_tx = TransactionDraft()
        return self.current_tx
    
    def set_recipient(self, recipient: str):
        """Set the transaction recipient."""
        if self.current_tx:
            self.current_tx.recipient = recipient
    
    def set_amount(self, amount: float):
        """Set the transaction amount."""
        if self.current_tx:
            self.current_tx.amount = amount
    
    def set_fee(self, fee: float):
        """Set the transaction fee."""
        if self.current_tx:
            self.current_tx.fee = fee
    
    def get_current_tx(self) -> Optional[TransactionDraft]:
        """Get the current transaction draft."""
        return self.current_tx
    
    def cancel_transaction(self):
        """Cancel the current transaction."""
        self.current_tx = None
    
    def create_amount_input_page(self) -> AmountInputPage:
        """Create amount input page."""
        return AmountInputPage(self.set_amount)
    
    def create_recipient_input_page(self) -> RecipientInputPage:
        """Create recipient input page."""
        return RecipientInputPage(self.set_recipient)
    
    def create_review_page(self, on_confirm: Callable, on_cancel: Callable) -> Optional[Page]:
        """Create transaction review page."""
        if self.current_tx:
            return TransactionReviewPage(self.current_tx, on_confirm, on_cancel)
        return None
    
    def create_signing_page(self, on_signed: Callable) -> TransactionSigningPage:
        """Create transaction signing page."""
        return TransactionSigningPage(on_signed)
    
    def create_status_page(self, tx_hash: str, status: str = "Pending") -> TransactionStatusPage:
        """Create transaction status page."""
        return TransactionStatusPage(tx_hash, status)
