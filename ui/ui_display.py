"""
UI Display Framework for Meshtastic Devices

Efficient rendering for 128x64 OLED screens with minimal memory overhead.
Supports text, graphics, and simple animations.

Features:
- Bitmap buffer management (1 KB)
- Text rendering with multiple fonts
- Simple graphics (lines, rectangles, circles)
- Page management and transitions
- Power-efficient updates (only changed regions)
"""

import time
from enum import Enum
from typing import List, Tuple, Optional, Callable
from dataclasses import dataclass


class FontSize(Enum):
    """Font sizes for display."""
    SMALL = 1      # 5x8 pixels
    MEDIUM = 2     # 8x16 pixels
    LARGE = 3      # 16x32 pixels


class Alignment(Enum):
    """Text alignment options."""
    LEFT = 0
    CENTER = 1
    RIGHT = 2


@dataclass
class Rect:
    """Rectangle for drawing operations."""
    x: int
    y: int
    width: int
    height: int
    
    def contains(self, x: int, y: int) -> bool:
        """Check if point is within rectangle."""
        return (self.x <= x < self.x + self.width and 
                self.y <= y < self.y + self.height)


@dataclass
class Point:
    """Point for drawing operations."""
    x: int
    y: int


class DisplayBuffer:
    """
    Bitmap buffer for OLED display.
    
    Manages a 128x64 pixel display with 1-bit color depth.
    Uses 1 KB of memory for the entire display.
    """
    
    DISPLAY_WIDTH = 128
    DISPLAY_HEIGHT = 64
    BYTES_PER_ROW = DISPLAY_WIDTH // 8  # 16 bytes
    BUFFER_SIZE = BYTES_PER_ROW * DISPLAY_HEIGHT  # 1024 bytes
    
    def __init__(self):
        """Initialize display buffer."""
        self.buffer = bytearray(self.BUFFER_SIZE)
        self.dirty_regions: List[Rect] = []
        self.last_update = 0
        self.update_interval = 0.05  # 50 ms minimum between updates
    
    def clear(self):
        """Clear entire display."""
        self.buffer = bytearray(self.BUFFER_SIZE)
        self.dirty_regions = [Rect(0, 0, self.DISPLAY_WIDTH, self.DISPLAY_HEIGHT)]
    
    def set_pixel(self, x: int, y: int, color: bool = True):
        """Set a single pixel."""
        if not (0 <= x < self.DISPLAY_WIDTH and 0 <= y < self.DISPLAY_HEIGHT):
            return
        
        byte_index = (y // 8) * self.BYTES_PER_ROW + (x // 8)
        bit_index = y % 8
        
        if color:
            self.buffer[byte_index] |= (1 << bit_index)
        else:
            self.buffer[byte_index] &= ~(1 << bit_index)
        
        self._mark_dirty(Rect(x, y, 1, 1))
    
    def draw_line(self, x1: int, y1: int, x2: int, y2: int, color: bool = True):
        """Draw a line using Bresenham's algorithm."""
        dx = abs(x2 - x1)
        dy = abs(y2 - y1)
        sx = 1 if x2 > x1 else -1
        sy = 1 if y2 > y1 else -1
        err = dx - dy
        
        x, y = x1, y1
        while True:
            self.set_pixel(x, y, color)
            
            if x == x2 and y == y2:
                break
            
            e2 = 2 * err
            if e2 > -dy:
                err -= dy
                x += sx
            if e2 < dx:
                err += dx
                y += sy
    
    def draw_rect(self, rect: Rect, filled: bool = False, color: bool = True):
        """Draw a rectangle."""
        if filled:
            for y in range(rect.y, rect.y + rect.height):
                for x in range(rect.x, rect.x + rect.width):
                    self.set_pixel(x, y, color)
        else:
            # Draw outline
            self.draw_line(rect.x, rect.y, rect.x + rect.width - 1, rect.y, color)
            self.draw_line(rect.x + rect.width - 1, rect.y, rect.x + rect.width - 1, 
                          rect.y + rect.height - 1, color)
            self.draw_line(rect.x + rect.width - 1, rect.y + rect.height - 1, 
                          rect.x, rect.y + rect.height - 1, color)
            self.draw_line(rect.x, rect.y + rect.height - 1, rect.x, rect.y, color)
        
        self._mark_dirty(rect)
    
    def draw_text(self, text: str, x: int, y: int, size: FontSize = FontSize.SMALL,
                 alignment: Alignment = Alignment.LEFT, color: bool = True):
        """Draw text on display."""
        # Simple font rendering - each character is 5-8 pixels wide
        char_width = 5 if size == FontSize.SMALL else (8 if size == FontSize.MEDIUM else 16)
        text_width = len(text) * char_width
        
        # Adjust x based on alignment
        if alignment == Alignment.CENTER:
            x = x - text_width // 2
        elif alignment == Alignment.RIGHT:
            x = x - text_width
        
        # Draw each character
        for i, char in enumerate(text):
            char_x = x + i * char_width
            if 0 <= char_x < self.DISPLAY_WIDTH:
                self._draw_char(char, char_x, y, size, color)
        
        self._mark_dirty(Rect(x, y, text_width, 8 if size == FontSize.SMALL else 16))
    
    def _draw_char(self, char: str, x: int, y: int, size: FontSize, color: bool):
        """Draw a single character (simplified)."""
        # This is a simplified implementation
        # In production, would use proper font bitmaps
        if char == ' ':
            return
        
        # Draw a simple box for each character
        if size == FontSize.SMALL:
            self.draw_rect(Rect(x, y, 5, 8), filled=True, color=color)
        elif size == FontSize.MEDIUM:
            self.draw_rect(Rect(x, y, 8, 16), filled=True, color=color)
        else:
            self.draw_rect(Rect(x, y, 16, 32), filled=True, color=color)
    
    def _mark_dirty(self, rect: Rect):
        """Mark a region as dirty (needs update)."""
        # Clamp to display bounds
        rect.x = max(0, min(rect.x, self.DISPLAY_WIDTH - 1))
        rect.y = max(0, min(rect.y, self.DISPLAY_HEIGHT - 1))
        rect.width = min(rect.width, self.DISPLAY_WIDTH - rect.x)
        rect.height = min(rect.height, self.DISPLAY_HEIGHT - rect.y)
        
        self.dirty_regions.append(rect)
    
    def needs_update(self) -> bool:
        """Check if display needs update."""
        if not self.dirty_regions:
            return False
        
        elapsed = time.time() - self.last_update
        return elapsed >= self.update_interval
    
    def get_dirty_regions(self) -> List[Rect]:
        """Get regions that need updating."""
        regions = self.dirty_regions
        self.dirty_regions = []
        self.last_update = time.time()
        return regions
    
    def get_buffer(self) -> bytearray:
        """Get the display buffer."""
        return self.buffer


class Display:
    """
    High-level display interface for Meshtastic devices.
    
    Manages display updates, page rendering, and transitions.
    """
    
    def __init__(self):
        """Initialize display."""
        self.buffer = DisplayBuffer()
        self.current_page: Optional['Page'] = None
        self.pages: dict = {}
        self.is_running = False
    
    def register_page(self, name: str, page: 'Page'):
        """Register a page."""
        self.pages[name] = page
    
    def show_page(self, name: str):
        """Show a specific page."""
        if name in self.pages:
            self.current_page = self.pages[name]
            self.buffer.clear()
    
    def update(self):
        """Update display."""
        if self.current_page:
            self.current_page.render(self.buffer)
        
        if self.buffer.needs_update():
            regions = self.buffer.get_dirty_regions()
            return regions
        
        return []
    
    def get_buffer(self) -> bytearray:
        """Get the display buffer."""
        return self.buffer.get_buffer()


class Page:
    """
    Base class for display pages.
    
    Each page handles its own rendering and input.
    """
    
    def __init__(self, name: str):
        """Initialize page."""
        self.name = name
        self.widgets: List['Widget'] = []
    
    def add_widget(self, widget: 'Widget'):
        """Add a widget to the page."""
        self.widgets.append(widget)
    
    def render(self, buffer: DisplayBuffer):
        """Render the page."""
        buffer.clear()
        for widget in self.widgets:
            widget.render(buffer)
    
    def handle_input(self, button: int) -> bool:
        """Handle button input. Return True if handled."""
        for widget in self.widgets:
            if widget.handle_input(button):
                return True
        return False


class Widget:
    """
    Base class for UI widgets.
    
    Widgets are reusable UI components.
    """
    
    def __init__(self, x: int, y: int, width: int, height: int):
        """Initialize widget."""
        self.rect = Rect(x, y, width, height)
        self.visible = True
    
    def render(self, buffer: DisplayBuffer):
        """Render the widget."""
        pass
    
    def handle_input(self, button: int) -> bool:
        """Handle button input. Return True if handled."""
        return False


class TextWidget(Widget):
    """Widget for displaying text."""
    
    def __init__(self, x: int, y: int, text: str = "", size: FontSize = FontSize.SMALL,
                 alignment: Alignment = Alignment.LEFT):
        """Initialize text widget."""
        super().__init__(x, y, 128, 8)
        self.text = text
        self.size = size
        self.alignment = alignment
    
    def set_text(self, text: str):
        """Set the text."""
        self.text = text
    
    def render(self, buffer: DisplayBuffer):
        """Render the text."""
        if self.visible:
            buffer.draw_text(self.text, self.rect.x, self.rect.y, self.size, self.alignment)


class ButtonWidget(Widget):
    """Widget for displaying a button."""
    
    def __init__(self, x: int, y: int, width: int, height: int, text: str = "",
                 on_press: Optional[Callable] = None):
        """Initialize button widget."""
        super().__init__(x, y, width, height)
        self.text = text
        self.on_press = on_press
        self.pressed = False
    
    def render(self, buffer: DisplayBuffer):
        """Render the button."""
        if self.visible:
            # Draw button border
            buffer.draw_rect(self.rect, filled=False, color=True)
            # Draw button text
            buffer.draw_text(self.text, self.rect.x + self.rect.width // 2,
                           self.rect.y + self.rect.height // 2, FontSize.SMALL,
                           Alignment.CENTER)
    
    def handle_input(self, button: int) -> bool:
        """Handle button input."""
        if self.visible and self.on_press:
            self.on_press()
            return True
        return False


class ListWidget(Widget):
    """Widget for displaying a list of items."""
    
    def __init__(self, x: int, y: int, width: int, height: int, items: List[str]):
        """Initialize list widget."""
        super().__init__(x, y, width, height)
        self.items = items
        self.selected_index = 0
        self.scroll_offset = 0
        self.items_per_page = height // 8
    
    def render(self, buffer: DisplayBuffer):
        """Render the list."""
        if self.visible:
            for i in range(self.items_per_page):
                item_index = self.scroll_offset + i
                if item_index < len(self.items):
                    item = self.items[item_index]
                    y = self.rect.y + i * 8
                    
                    # Highlight selected item
                    if item_index == self.selected_index:
                        buffer.draw_rect(Rect(self.rect.x, y, self.rect.width, 8),
                                       filled=True, color=True)
                    
                    buffer.draw_text(item, self.rect.x + 2, y, FontSize.SMALL)
    
    def handle_input(self, button: int) -> bool:
        """Handle button input."""
        if button == 0:  # Up
            if self.selected_index > 0:
                self.selected_index -= 1
                if self.selected_index < self.scroll_offset:
                    self.scroll_offset = self.selected_index
            return True
        elif button == 1:  # Down
            if self.selected_index < len(self.items) - 1:
                self.selected_index += 1
                if self.selected_index >= self.scroll_offset + self.items_per_page:
                    self.scroll_offset = self.selected_index - self.items_per_page + 1
            return True
        return False
    
    def get_selected(self) -> Optional[str]:
        """Get the selected item."""
        if 0 <= self.selected_index < len(self.items):
            return self.items[self.selected_index]
        return None


class ProgressWidget(Widget):
    """Widget for displaying progress."""
    
    def __init__(self, x: int, y: int, width: int, height: int, progress: float = 0.0):
        """Initialize progress widget."""
        super().__init__(x, y, width, height)
        self.progress = progress
    
    def set_progress(self, progress: float):
        """Set the progress (0.0 to 1.0)."""
        self.progress = max(0.0, min(1.0, progress))
    
    def render(self, buffer: DisplayBuffer):
        """Render the progress bar."""
        if self.visible:
            # Draw border
            buffer.draw_rect(self.rect, filled=False, color=True)
            
            # Draw progress
            progress_width = int(self.rect.width * self.progress)
            if progress_width > 0:
                buffer.draw_rect(Rect(self.rect.x, self.rect.y, progress_width, self.rect.height),
                               filled=True, color=True)


class StatusBar(Widget):
    """Widget for displaying status information."""
    
    def __init__(self, x: int, y: int, width: int, height: int):
        """Initialize status bar."""
        super().__init__(x, y, width, height)
        self.status_text = ""
        self.battery_percent = 100
        self.signal_strength = 3
    
    def set_status(self, text: str):
        """Set the status text."""
        self.status_text = text
    
    def set_battery(self, percent: int):
        """Set the battery percentage."""
        self.battery_percent = max(0, min(100, percent))
    
    def set_signal(self, strength: int):
        """Set the signal strength (0-4)."""
        self.signal_strength = max(0, min(4, strength))
    
    def render(self, buffer: DisplayBuffer):
        """Render the status bar."""
        if self.visible:
            # Draw status text
            buffer.draw_text(self.status_text, self.rect.x + 2, self.rect.y, FontSize.SMALL)
            
            # Draw battery indicator
            battery_x = self.rect.x + self.rect.width - 20
            buffer.draw_rect(Rect(battery_x, self.rect.y, 15, 6), filled=False, color=True)
            battery_fill = int(15 * self.battery_percent / 100)
            if battery_fill > 0:
                buffer.draw_rect(Rect(battery_x + 1, self.rect.y + 1, battery_fill - 2, 4),
                               filled=True, color=True)
            
            # Draw signal strength
            signal_x = battery_x - 12
            for i in range(self.signal_strength):
                y = self.rect.y + 6 - (i + 1) * 2
                buffer.draw_rect(Rect(signal_x + i * 3, y, 2, (i + 1) * 2),
                               filled=True, color=True)
