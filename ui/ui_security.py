"""
PIN Entry and Security UI

Provides UI for:
- PIN entry (4-6 digits)
- Wallet unlock
- Transaction confirmation
- Seed phrase display
- Security warnings
- Brute-force protection
"""

from typing import Optional, Callable, List
from dataclasses import dataclass
import time
from meshchain.ui_display import Page, Widget, TextWidget, ProgressWidget, StatusBar, FontSize, Alignment


@dataclass
class SecurityState:
    """Tracks security state."""
    is_locked: bool = True
    pin_attempts: int = 0
    max_attempts: int = 3
    lockout_time: int = 0  # seconds
    last_attempt_time: float = 0.0


class PINEntryPage(Page):
    """Page for entering PIN."""
    
    def __init__(self, on_pin_entered: Callable[[str], None], pin_length: int = 4):
        """Initialize PIN entry page."""
        super().__init__("PIN Entry")
        self.on_pin_entered = on_pin_entered
        self.pin_length = pin_length
        self.pin = ""
        self.attempts = 0
        self.max_attempts = 3
        
        # Title
        self.add_widget(TextWidget(64, 0, "ENTER PIN", FontSize.MEDIUM, Alignment.CENTER))
        
        # PIN display (dots)
        self.pin_display = TextWidget(64, 24, "●" * 0, FontSize.LARGE, Alignment.CENTER)
        self.add_widget(self.pin_display)
        
        # Instructions
        self.add_widget(TextWidget(64, 40, "Up/Down: digit", FontSize.SMALL, Alignment.CENTER))
        self.add_widget(TextWidget(64, 48, "Select: next", FontSize.SMALL, Alignment.CENTER))
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("PIN")
        self.add_widget(status_bar)
    
    def add_digit(self, digit: int):
        """Add a digit to the PIN."""
        if len(self.pin) < self.pin_length:
            self.pin += str(digit)
            self.pin_display.set_text("●" * len(self.pin))
            
            # Auto-submit if PIN is complete
            if len(self.pin) == self.pin_length:
                self.submit_pin()
    
    def remove_digit(self):
        """Remove the last digit."""
        if self.pin:
            self.pin = self.pin[:-1]
            self.pin_display.set_text("●" * len(self.pin))
    
    def submit_pin(self):
        """Submit the PIN."""
        if len(self.pin) == self.pin_length:
            self.on_pin_entered(self.pin)
            self.pin = ""
            self.pin_display.set_text("●" * 0)


class PINLockoutPage(Page):
    """Page for PIN lockout."""
    
    def __init__(self, lockout_time: int):
        """Initialize PIN lockout page."""
        super().__init__("Locked")
        self.lockout_time = lockout_time
        self.remaining_time = lockout_time
        
        # Title
        self.add_widget(TextWidget(64, 0, "LOCKED", FontSize.MEDIUM, Alignment.CENTER))
        
        # Warning
        self.add_widget(TextWidget(64, 16, "Too many attempts", FontSize.SMALL, Alignment.CENTER))
        
        # Countdown
        self.countdown_widget = TextWidget(64, 32, f"Wait: {self.remaining_time}s", FontSize.LARGE, Alignment.CENTER)
        self.add_widget(self.countdown_widget)
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Locked")
        self.add_widget(status_bar)
    
    def update_countdown(self, remaining: int):
        """Update countdown."""
        self.remaining_time = remaining
        self.countdown_widget.set_text(f"Wait: {remaining}s")


class WarningPage(Page):
    """Page for displaying security warnings."""
    
    def __init__(self, title: str, message: str, on_acknowledge: Callable[[], None]):
        """Initialize warning page."""
        super().__init__("Warning")
        self.on_acknowledge = on_acknowledge
        
        # Title
        self.add_widget(TextWidget(64, 0, title.upper(), FontSize.MEDIUM, Alignment.CENTER))
        
        # Message (split into lines)
        lines = message.split('\n')
        y = 16
        for line in lines:
            if y < 48:
                self.add_widget(TextWidget(64, y, line, FontSize.SMALL, Alignment.CENTER))
                y += 8
        
        # Instructions
        self.add_widget(TextWidget(64, 48, "Select: acknowledge", FontSize.SMALL, Alignment.CENTER))
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Warning")
        self.add_widget(status_bar)
    
    def acknowledge(self):
        """Acknowledge the warning."""
        self.on_acknowledge()


class SeedPhraseDisplayPage(Page):
    """Page for displaying seed phrase."""
    
    def __init__(self, seed_phrase: str):
        """Initialize seed phrase display page."""
        super().__init__("Seed Phrase")
        self.seed_phrase = seed_phrase
        self.words = seed_phrase.split()
        self.current_page = 0
        self.words_per_page = 6
        
        # Title
        self.add_widget(TextWidget(64, 0, "BACKUP SEED", FontSize.MEDIUM, Alignment.CENTER))
        
        # Warning
        self.add_widget(TextWidget(64, 12, "WRITE DOWN THESE WORDS", FontSize.SMALL, Alignment.CENTER))
        self.add_widget(TextWidget(64, 20, "DO NOT SHARE", FontSize.SMALL, Alignment.CENTER))
        
        # Words display
        self.words_widget = TextWidget(0, 28, "", FontSize.SMALL)
        self.add_widget(self.words_widget)
        
        # Page indicator
        self.page_widget = TextWidget(64, 52, "Page 1/2", FontSize.SMALL, Alignment.CENTER)
        self.add_widget(self.page_widget)
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Backup")
        self.add_widget(status_bar)
        
        self._update_display()
    
    def _update_display(self):
        """Update the displayed words."""
        start = self.current_page * self.words_per_page
        end = min(start + self.words_per_page, len(self.words))
        
        words_text = ""
        for i in range(start, end):
            word_num = i + 1
            word = self.words[i]
            words_text += f"{word_num:2d}. {word}\n"
        
        self.words_widget.set_text(words_text)
        
        total_pages = (len(self.words) + self.words_per_page - 1) // self.words_per_page
        self.page_widget.set_text(f"Page {self.current_page + 1}/{total_pages}")
    
    def next_page(self):
        """Go to next page."""
        total_pages = (len(self.words) + self.words_per_page - 1) // self.words_per_page
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self._update_display()
    
    def previous_page(self):
        """Go to previous page."""
        if self.current_page > 0:
            self.current_page -= 1
            self._update_display()


class TransactionConfirmationPage(Page):
    """Page for confirming sensitive transactions."""
    
    def __init__(self, message: str, on_confirm: Callable[[], None], on_cancel: Callable[[], None]):
        """Initialize transaction confirmation page."""
        super().__init__("Confirm")
        self.on_confirm = on_confirm
        self.on_cancel = on_cancel
        
        # Title
        self.add_widget(TextWidget(64, 0, "CONFIRM", FontSize.MEDIUM, Alignment.CENTER))
        
        # Message
        lines = message.split('\n')
        y = 16
        for line in lines:
            if y < 48:
                self.add_widget(TextWidget(64, y, line, FontSize.SMALL, Alignment.CENTER))
                y += 8
        
        # Instructions
        self.add_widget(TextWidget(32, 48, "Select: YES", FontSize.SMALL, Alignment.CENTER))
        self.add_widget(TextWidget(96, 48, "Back: NO", FontSize.SMALL, Alignment.CENTER))
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Confirm")
        self.add_widget(status_bar)
    
    def confirm(self):
        """Confirm the action."""
        self.on_confirm()
    
    def cancel(self):
        """Cancel the action."""
        self.on_cancel()


class SecurityManager:
    """
    Manages security UI and state.
    
    Handles:
    - PIN entry and verification
    - Brute-force protection
    - Wallet locking/unlocking
    - Security warnings
    - Seed phrase display
    """
    
    def __init__(self):
        """Initialize security manager."""
        self.security_state = SecurityState()
        self.on_pin_verified: Optional[Callable] = None
        self.on_pin_failed: Optional[Callable] = None
        self.correct_pin: str = ""
    
    def set_correct_pin(self, pin: str):
        """Set the correct PIN."""
        self.correct_pin = pin
    
    def verify_pin(self, entered_pin: str) -> bool:
        """Verify the entered PIN."""
        current_time = time.time()
        
        # Check if in lockout
        if self.security_state.lockout_time > 0:
            elapsed = current_time - self.security_state.last_attempt_time
            if elapsed < self.security_state.lockout_time:
                return False
            else:
                # Lockout expired
                self.security_state.lockout_time = 0
                self.security_state.pin_attempts = 0
        
        # Check PIN
        if entered_pin == self.correct_pin:
            self.security_state.is_locked = False
            self.security_state.pin_attempts = 0
            if self.on_pin_verified:
                self.on_pin_verified()
            return True
        else:
            # Wrong PIN
            self.security_state.pin_attempts += 1
            self.security_state.last_attempt_time = current_time
            
            if self.security_state.pin_attempts >= self.security_state.max_attempts:
                # Lock out
                self.security_state.lockout_time = 300  # 5 minutes
            
            if self.on_pin_failed:
                self.on_pin_failed(self.security_state.pin_attempts)
            
            return False
    
    def lock_wallet(self):
        """Lock the wallet."""
        self.security_state.is_locked = True
    
    def is_locked(self) -> bool:
        """Check if wallet is locked."""
        return self.security_state.is_locked
    
    def get_remaining_lockout(self) -> int:
        """Get remaining lockout time."""
        if self.security_state.lockout_time == 0:
            return 0
        
        current_time = time.time()
        elapsed = current_time - self.security_state.last_attempt_time
        remaining = max(0, int(self.security_state.lockout_time - elapsed))
        return remaining
    
    def get_remaining_attempts(self) -> int:
        """Get remaining PIN attempts."""
        return max(0, self.security_state.max_attempts - self.security_state.pin_attempts)
    
    def create_pin_entry_page(self) -> PINEntryPage:
        """Create PIN entry page."""
        return PINEntryPage(self.verify_pin)
    
    def create_lockout_page(self, lockout_time: int) -> PINLockoutPage:
        """Create lockout page."""
        return PINLockoutPage(lockout_time)
    
    def create_warning_page(self, title: str, message: str,
                           on_acknowledge: Callable) -> WarningPage:
        """Create warning page."""
        return WarningPage(title, message, on_acknowledge)
    
    def create_seed_phrase_page(self, seed_phrase: str) -> SeedPhraseDisplayPage:
        """Create seed phrase display page."""
        return SeedPhraseDisplayPage(seed_phrase)
    
    def create_confirmation_page(self, message: str,
                                on_confirm: Callable,
                                on_cancel: Callable) -> TransactionConfirmationPage:
        """Create transaction confirmation page."""
        return TransactionConfirmationPage(message, on_confirm, on_cancel)
