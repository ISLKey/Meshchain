"""
Hierarchical Menu System for Meshtastic Devices

Provides a tree-based menu structure with support for:
- Main menu with sub-menus
- Dynamic menu items
- Menu item callbacks
- Menu state persistence
- Breadcrumb navigation

Menu Structure:
Main Menu
├── Wallet
│   ├── View Balance
│   ├── Receive
│   ├── Send
│   └── Backup
├── Transactions
│   ├── Recent
│   └── History
├── Node Status
│   ├── Blockchain
│   ├── Network
│   └── Validators
└── Settings
    ├── Display
    ├── Network
    └── Security
"""

from typing import List, Dict, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum


class MenuItemType(Enum):
    """Types of menu items."""
    ACTION = 0      # Executes an action
    SUBMENU = 1     # Opens a submenu
    TOGGLE = 2      # Toggles a boolean value
    VALUE = 3       # Displays/edits a value


@dataclass
class MenuItem:
    """A single menu item."""
    name: str
    item_type: MenuItemType
    action: Optional[Callable] = None
    submenu: Optional['Menu'] = None
    value: Optional[str] = None
    enabled: bool = True
    
    def execute(self):
        """Execute the menu item action."""
        if self.action:
            self.action()


class Menu:
    """
    A menu with multiple items.
    
    Supports hierarchical menu structures with navigation.
    """
    
    def __init__(self, name: str):
        """Initialize menu."""
        self.name = name
        self.items: List[MenuItem] = []
        self.selected_index = 0
        self.scroll_offset = 0
        self.parent: Optional['Menu'] = None
    
    def add_item(self, name: str, item_type: MenuItemType = MenuItemType.ACTION,
                action: Optional[Callable] = None, submenu: Optional['Menu'] = None,
                value: Optional[str] = None):
        """Add an item to the menu."""
        item = MenuItem(name, item_type, action, submenu, value)
        self.items.append(item)
        return item
    
    def add_action(self, name: str, action: Callable) -> MenuItem:
        """Add an action item."""
        return self.add_item(name, MenuItemType.ACTION, action=action)
    
    def add_submenu(self, name: str, submenu: 'Menu') -> MenuItem:
        """Add a submenu item."""
        submenu.parent = self
        return self.add_item(name, MenuItemType.SUBMENU, submenu=submenu)
    
    def add_toggle(self, name: str, action: Callable, value: bool = False) -> MenuItem:
        """Add a toggle item."""
        return self.add_item(name, MenuItemType.TOGGLE, action=action,
                           value="ON" if value else "OFF")
    
    def add_value(self, name: str, value: str) -> MenuItem:
        """Add a value display item."""
        return self.add_item(name, MenuItemType.VALUE, value=value)
    
    def get_selected_item(self) -> Optional[MenuItem]:
        """Get the currently selected item."""
        if 0 <= self.selected_index < len(self.items):
            return self.items[self.selected_index]
        return None
    
    def select_next(self):
        """Select the next item."""
        if self.selected_index < len(self.items) - 1:
            self.selected_index += 1
            # Scroll if needed
            if self.selected_index >= self.scroll_offset + 8:
                self.scroll_offset = self.selected_index - 7
    
    def select_previous(self):
        """Select the previous item."""
        if self.selected_index > 0:
            self.selected_index -= 1
            # Scroll if needed
            if self.selected_index < self.scroll_offset:
                self.scroll_offset = self.selected_index
    
    def get_visible_items(self, max_items: int = 8) -> List[MenuItem]:
        """Get visible items for display."""
        end = min(self.scroll_offset + max_items, len(self.items))
        return self.items[self.scroll_offset:end]
    
    def get_breadcrumb(self) -> List[str]:
        """Get breadcrumb trail."""
        breadcrumb = [self.name]
        current = self.parent
        while current:
            breadcrumb.insert(0, current.name)
            current = current.parent
        return breadcrumb


class MenuSystem:
    """
    High-level menu system.
    
    Manages the entire menu hierarchy and navigation.
    """
    
    def __init__(self):
        """Initialize menu system."""
        self.menus: Dict[str, Menu] = {}
        self.current_menu: Optional[Menu] = None
        self.menu_stack: List[Menu] = []
    
    def create_menu(self, name: str) -> Menu:
        """Create a new menu."""
        menu = Menu(name)
        self.menus[name] = menu
        return menu
    
    def set_current_menu(self, menu_name: str):
        """Set the current menu."""
        if menu_name in self.menus:
            self.current_menu = self.menus[menu_name]
            self.menu_stack = [self.current_menu]
    
    def open_submenu(self):
        """Open the selected submenu."""
        if not self.current_menu:
            return
        
        item = self.current_menu.get_selected_item()
        if item and item.item_type == MenuItemType.SUBMENU and item.submenu:
            self.current_menu = item.submenu
            self.menu_stack.append(self.current_menu)
            self.current_menu.selected_index = 0
            self.current_menu.scroll_offset = 0
    
    def close_menu(self) -> bool:
        """Close the current menu and return to parent."""
        if len(self.menu_stack) > 1:
            self.menu_stack.pop()
            self.current_menu = self.menu_stack[-1]
            return True
        return False
    
    def select_item(self):
        """Select the current item."""
        if not self.current_menu:
            return
        
        item = self.current_menu.get_selected_item()
        if not item:
            return
        
        if item.item_type == MenuItemType.ACTION:
            item.execute()
        elif item.item_type == MenuItemType.SUBMENU:
            self.open_submenu()
        elif item.item_type == MenuItemType.TOGGLE:
            # Toggle the value
            if item.value == "ON":
                item.value = "OFF"
            else:
                item.value = "ON"
            item.execute()
    
    def navigate_up(self):
        """Navigate up in the menu."""
        if self.current_menu:
            self.current_menu.select_previous()
    
    def navigate_down(self):
        """Navigate down in the menu."""
        if self.current_menu:
            self.current_menu.select_next()
    
    def get_current_items(self) -> List[MenuItem]:
        """Get current menu items."""
        if self.current_menu:
            return self.current_menu.get_visible_items()
        return []
    
    def get_selected_index(self) -> int:
        """Get the selected item index."""
        if self.current_menu:
            return self.current_menu.selected_index
        return 0
    
    def get_breadcrumb(self) -> List[str]:
        """Get breadcrumb trail."""
        if self.current_menu:
            return self.current_menu.get_breadcrumb()
        return []


class MainMenuBuilder:
    """
    Builds the main menu structure for MeshChain.
    """
    
    @staticmethod
    def build_main_menu() -> MenuSystem:
        """Build the complete main menu."""
        system = MenuSystem()
        
        # Create main menu
        main = system.create_menu("Main")
        
        # Wallet submenu
        wallet = system.create_menu("Wallet")
        wallet.add_action("View Balance", lambda: print("View Balance"))
        wallet.add_action("Receive", lambda: print("Receive"))
        wallet.add_action("Send", lambda: print("Send"))
        wallet.add_action("Backup", lambda: print("Backup"))
        main.add_submenu("Wallet", wallet)
        
        # Transactions submenu
        transactions = system.create_menu("Transactions")
        transactions.add_action("Recent", lambda: print("Recent Transactions"))
        transactions.add_action("History", lambda: print("Transaction History"))
        main.add_submenu("Transactions", transactions)
        
        # Node Status submenu
        node_status = system.create_menu("Node Status")
        node_status.add_action("Blockchain", lambda: print("Blockchain Info"))
        node_status.add_action("Network", lambda: print("Network Info"))
        node_status.add_action("Validators", lambda: print("Validators"))
        main.add_submenu("Node Status", node_status)
        
        # Settings submenu
        settings = system.create_menu("Settings")
        settings.add_action("Display", lambda: print("Display Settings"))
        settings.add_action("Network", lambda: print("Network Settings"))
        settings.add_action("Security", lambda: print("Security Settings"))
        main.add_submenu("Settings", settings)
        
        # Set main menu as current
        system.set_current_menu("Main")
        
        return system


class DynamicMenu(Menu):
    """
    A menu with dynamic items that can be updated.
    
    Useful for displaying lists of items that change.
    """
    
    def __init__(self, name: str, item_provider: Callable):
        """Initialize dynamic menu."""
        super().__init__(name)
        self.item_provider = item_provider
        self.last_update = 0
        self.update_interval = 1.0  # Update every 1 second
    
    def update_items(self):
        """Update menu items from provider."""
        import time
        current_time = time.time()
        
        if current_time - self.last_update >= self.update_interval:
            self.items = self.item_provider()
            self.last_update = current_time
    
    def get_visible_items(self, max_items: int = 8) -> List[MenuItem]:
        """Get visible items, updating first."""
        self.update_items()
        return super().get_visible_items(max_items)
