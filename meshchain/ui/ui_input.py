"""
Button-Based Input System for Meshtastic Devices

Handles button input and navigation with debouncing and long-press detection.

Button Layout (typical Meshtastic device):
- Button 0 (UP): Navigate up/previous
- Button 1 (SELECT): Select/confirm
- Button 2 (DOWN): Navigate down/next

Features:
- Debouncing (50 ms)
- Long-press detection (1 second)
- Button state tracking
- Input event queuing
"""

import time
from enum import Enum
from typing import Callable, Optional, List
from dataclasses import dataclass


class ButtonID(Enum):
    """Button identifiers."""
    UP = 0
    SELECT = 1
    DOWN = 2


class ButtonEvent(Enum):
    """Button event types."""
    PRESSED = 0
    RELEASED = 1
    LONG_PRESSED = 2


@dataclass
class InputEvent:
    """Input event."""
    button: ButtonID
    event_type: ButtonEvent
    timestamp: float


class ButtonState:
    """Tracks state of a single button."""
    
    def __init__(self, button_id: ButtonID):
        """Initialize button state."""
        self.button_id = button_id
        self.is_pressed = False
        self.press_time = 0
        self.last_debounce_time = 0
        self.long_press_threshold = 1.0  # 1 second
        self.debounce_delay = 0.05  # 50 ms
        self.long_press_fired = False
    
    def update(self, is_pressed: bool) -> List[InputEvent]:
        """Update button state and return events."""
        events: List[InputEvent] = []
        current_time = time.time()
        
        # Debounce
        if current_time - self.last_debounce_time < self.debounce_delay:
            return events
        
        # Detect press
        if is_pressed and not self.is_pressed:
            self.is_pressed = True
            self.press_time = current_time
            self.long_press_fired = False
            events.append(InputEvent(self.button_id, ButtonEvent.PRESSED, current_time))
        
        # Detect long press
        elif is_pressed and self.is_pressed and not self.long_press_fired:
            if current_time - self.press_time >= self.long_press_threshold:
                self.long_press_fired = True
                events.append(InputEvent(self.button_id, ButtonEvent.LONG_PRESSED, current_time))
        
        # Detect release
        elif not is_pressed and self.is_pressed:
            self.is_pressed = False
            events.append(InputEvent(self.button_id, ButtonEvent.RELEASED, current_time))
        
        self.last_debounce_time = current_time
        return events


class InputHandler:
    """
    Handles button input and generates navigation events.
    
    Supports:
    - Menu navigation (up/down)
    - Item selection (select)
    - Long-press actions (hold select)
    """
    
    def __init__(self):
        """Initialize input handler."""
        self.button_states = {
            ButtonID.UP: ButtonState(ButtonID.UP),
            ButtonID.SELECT: ButtonState(ButtonID.SELECT),
            ButtonID.DOWN: ButtonState(ButtonID.DOWN),
        }
        self.event_queue: List[InputEvent] = []
        self.input_callbacks: dict = {}
    
    def register_callback(self, event_type: ButtonEvent, button: ButtonID,
                         callback: Callable):
        """Register a callback for a button event."""
        key = (event_type, button)
        if key not in self.input_callbacks:
            self.input_callbacks[key] = []
        self.input_callbacks[key].append(callback)
    
    def update(self, button_states: dict) -> List[InputEvent]:
        """
        Update button states.
        
        Args:
            button_states: Dict with ButtonID keys and boolean values
        
        Returns:
            List of input events
        """
        events: List[InputEvent] = []
        
        for button_id in ButtonID:
            is_pressed = button_states.get(button_id, False)
            button_events = self.button_states[button_id].update(is_pressed)
            events.extend(button_events)
        
        # Queue events and fire callbacks
        for event in events:
            self.event_queue.append(event)
            self._fire_callbacks(event)
        
        return events
    
    def _fire_callbacks(self, event: InputEvent):
        """Fire callbacks for an event."""
        key = (event.event_type, event.button)
        if key in self.input_callbacks:
            for callback in self.input_callbacks[key]:
                try:
                    callback()
                except Exception as e:
                    print(f"Error in input callback: {e}")
    
    def get_events(self) -> List[InputEvent]:
        """Get queued input events."""
        events = self.event_queue
        self.event_queue = []
        return events
    
    def clear_queue(self):
        """Clear the event queue."""
        self.event_queue = []


class NavigationController:
    """
    Controls navigation between pages and menu items.
    
    Supports:
    - Page navigation (main menu, sub-menus)
    - List navigation (up/down through items)
    - Item selection
    - Back button (long press select)
    """
    
    def __init__(self):
        """Initialize navigation controller."""
        self.current_page: Optional[str] = None
        self.page_stack: List[str] = []
        self.on_page_change: Optional[Callable] = None
        self.on_selection: Optional[Callable] = None
        self.on_back: Optional[Callable] = None
    
    def push_page(self, page_name: str):
        """Push a page onto the stack."""
        self.page_stack.append(page_name)
        self.current_page = page_name
        if self.on_page_change:
            self.on_page_change(page_name)
    
    def pop_page(self) -> Optional[str]:
        """Pop a page from the stack."""
        if self.page_stack:
            self.page_stack.pop()
            self.current_page = self.page_stack[-1] if self.page_stack else None
            if self.on_page_change and self.current_page:
                self.on_page_change(self.current_page)
            if self.on_back:
                self.on_back()
            return self.current_page
        return None
    
    def goto_page(self, page_name: str):
        """Go to a specific page."""
        self.current_page = page_name
        if self.on_page_change:
            self.on_page_change(page_name)
    
    def select_item(self, item: str):
        """Handle item selection."""
        if self.on_selection:
            self.on_selection(item)
    
    def get_page_stack(self) -> List[str]:
        """Get the current page stack."""
        return self.page_stack.copy()


class MenuNavigator:
    """
    Manages menu navigation with up/down/select buttons.
    
    Handles:
    - Menu item selection
    - Submenu navigation
    - Back button
    """
    
    def __init__(self, input_handler: InputHandler, nav_controller: NavigationController):
        """Initialize menu navigator."""
        self.input_handler = input_handler
        self.nav_controller = nav_controller
        self.current_selection = 0
        self.menu_items: List[str] = []
        
        # Register input callbacks
        self.input_handler.register_callback(ButtonEvent.PRESSED, ButtonID.UP, self._on_up)
        self.input_handler.register_callback(ButtonEvent.PRESSED, ButtonID.DOWN, self._on_down)
        self.input_handler.register_callback(ButtonEvent.PRESSED, ButtonID.SELECT, self._on_select)
        self.input_handler.register_callback(ButtonEvent.LONG_PRESSED, ButtonID.SELECT, self._on_back)
    
    def set_menu_items(self, items: List[str]):
        """Set the menu items."""
        self.menu_items = items
        self.current_selection = 0
    
    def get_selected_item(self) -> Optional[str]:
        """Get the currently selected item."""
        if 0 <= self.current_selection < len(self.menu_items):
            return self.menu_items[self.current_selection]
        return None
    
    def _on_up(self):
        """Handle up button press."""
        if self.current_selection > 0:
            self.current_selection -= 1
    
    def _on_down(self):
        """Handle down button press."""
        if self.current_selection < len(self.menu_items) - 1:
            self.current_selection += 1
    
    def _on_select(self):
        """Handle select button press."""
        selected = self.get_selected_item()
        if selected:
            self.nav_controller.select_item(selected)
    
    def _on_back(self):
        """Handle back button (long press select)."""
        self.nav_controller.pop_page()


class InputManager:
    """
    High-level input management.
    
    Coordinates input handling, navigation, and menu navigation.
    """
    
    def __init__(self):
        """Initialize input manager."""
        self.input_handler = InputHandler()
        self.nav_controller = NavigationController()
        self.menu_navigator = MenuNavigator(self.input_handler, self.nav_controller)
    
    def update(self, button_states: dict) -> List[InputEvent]:
        """Update input state."""
        return self.input_handler.update(button_states)
    
    def set_menu_items(self, items: List[str]):
        """Set menu items for current page."""
        self.menu_navigator.set_menu_items(items)
    
    def get_selected_item(self) -> Optional[str]:
        """Get the selected menu item."""
        return self.menu_navigator.get_selected_item()
    
    def get_current_selection(self) -> int:
        """Get the current selection index."""
        return self.menu_navigator.current_selection
    
    def push_page(self, page_name: str):
        """Push a page onto the stack."""
        self.nav_controller.push_page(page_name)
    
    def pop_page(self) -> Optional[str]:
        """Pop a page from the stack."""
        return self.nav_controller.pop_page()
    
    def goto_page(self, page_name: str):
        """Go to a specific page."""
        self.nav_controller.goto_page(page_name)
    
    def register_page_change_callback(self, callback: Callable):
        """Register a callback for page changes."""
        self.nav_controller.on_page_change = callback
    
    def register_selection_callback(self, callback: Callable):
        """Register a callback for item selection."""
        self.nav_controller.on_selection = callback
    
    def register_back_callback(self, callback: Callable):
        """Register a callback for back button."""
        self.nav_controller.on_back = callback
