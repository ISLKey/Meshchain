"""
Settings and Configuration UI

Provides UI for:
- Display settings (brightness, contrast)
- Network settings (frequency, power)
- Security settings (PIN change, timeout)
- Node settings (name, role)
- About information
"""

from typing import Optional, Callable, List, Any
from dataclasses import dataclass
from meshchain.ui_display import Page, Widget, TextWidget, ListWidget, StatusBar, FontSize, Alignment


@dataclass
class DisplaySettings:
    """Display configuration."""
    brightness: int = 100  # 0-100
    contrast: int = 100    # 0-100
    timeout: int = 30      # seconds
    invert: bool = False


@dataclass
class NetworkSettings:
    """Network configuration."""
    frequency: int = 915000000  # Hz
    power: int = 23             # dBm
    bandwidth: int = 125000     # Hz
    spreading_factor: int = 7


@dataclass
class SecuritySettings:
    """Security configuration."""
    pin_enabled: bool = True
    pin_length: int = 4
    lockout_time: int = 300  # seconds
    auto_lock_timeout: int = 60  # seconds


@dataclass
class NodeSettings:
    """Node configuration."""
    name: str = "MeshChain Node"
    role: str = "validator"  # validator, observer
    stake: float = 0.0


class SettingItemPage(Page):
    """Page for editing a single setting."""
    
    def __init__(self, setting_name: str, current_value: Any, 
                 on_value_changed: Callable[[Any], None]):
        """Initialize setting item page."""
        super().__init__(f"Edit {setting_name}")
        self.setting_name = setting_name
        self.current_value = current_value
        self.on_value_changed = on_value_changed
        
        # Title
        self.add_widget(TextWidget(64, 0, setting_name.upper(), FontSize.MEDIUM, Alignment.CENTER))
        
        # Value display
        self.value_widget = TextWidget(64, 24, str(current_value), FontSize.LARGE, Alignment.CENTER)
        self.add_widget(self.value_widget)
        
        # Instructions
        self.add_widget(TextWidget(64, 48, "Up/Down: adjust", FontSize.SMALL, Alignment.CENTER))
        self.add_widget(TextWidget(64, 56, "Select: confirm", FontSize.SMALL, Alignment.CENTER))
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Setting")
        self.add_widget(status_bar)
    
    def increment_value(self, delta: Any = 1):
        """Increment the value."""
        if isinstance(self.current_value, int):
            self.current_value = max(0, self.current_value + delta)
        elif isinstance(self.current_value, float):
            self.current_value = max(0.0, self.current_value + float(delta))
        self.value_widget.set_text(str(self.current_value))
    
    def decrement_value(self, delta: Any = 1):
        """Decrement the value."""
        if isinstance(self.current_value, int):
            self.current_value = max(0, self.current_value - delta)
        elif isinstance(self.current_value, float):
            self.current_value = max(0.0, self.current_value - float(delta))
        self.value_widget.set_text(str(self.current_value))
    
    def toggle_value(self):
        """Toggle boolean value."""
        if isinstance(self.current_value, bool):
            self.current_value = not self.current_value
            self.value_widget.set_text("ON" if self.current_value else "OFF")
    
    def confirm_value(self):
        """Confirm the value change."""
        self.on_value_changed(self.current_value)


class DisplaySettingsPage(Page):
    """Page for display settings."""
    
    def __init__(self, settings: DisplaySettings, on_settings_changed: Callable):
        """Initialize display settings page."""
        super().__init__("Display Settings")
        self.settings = settings
        self.on_settings_changed = on_settings_changed
        
        # Title
        self.add_widget(TextWidget(64, 0, "DISPLAY", FontSize.MEDIUM, Alignment.CENTER))
        
        # Settings list
        items = [
            f"Brightness: {settings.brightness}%",
            f"Contrast: {settings.contrast}%",
            f"Timeout: {settings.timeout}s",
            f"Invert: {'ON' if settings.invert else 'OFF'}"
        ]
        
        settings_list = ListWidget(0, 16, 128, 32, items)
        self.add_widget(settings_list)
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Display")
        self.add_widget(status_bar)


class NetworkSettingsPage(Page):
    """Page for network settings."""
    
    def __init__(self, settings: NetworkSettings, on_settings_changed: Callable):
        """Initialize network settings page."""
        super().__init__("Network Settings")
        self.settings = settings
        self.on_settings_changed = on_settings_changed
        
        # Title
        self.add_widget(TextWidget(64, 0, "NETWORK", FontSize.MEDIUM, Alignment.CENTER))
        
        # Settings list
        items = [
            f"Freq: {settings.frequency/1e6:.1f} MHz",
            f"Power: {settings.power} dBm",
            f"BW: {settings.bandwidth/1000:.0f} kHz",
            f"SF: {settings.spreading_factor}"
        ]
        
        settings_list = ListWidget(0, 16, 128, 32, items)
        self.add_widget(settings_list)
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Network")
        self.add_widget(status_bar)


class SecuritySettingsPage(Page):
    """Page for security settings."""
    
    def __init__(self, settings: SecuritySettings, on_settings_changed: Callable):
        """Initialize security settings page."""
        super().__init__("Security Settings")
        self.settings = settings
        self.on_settings_changed = on_settings_changed
        
        # Title
        self.add_widget(TextWidget(64, 0, "SECURITY", FontSize.MEDIUM, Alignment.CENTER))
        
        # Settings list
        items = [
            f"PIN: {'ON' if settings.pin_enabled else 'OFF'}",
            f"PIN Len: {settings.pin_length}",
            f"Lockout: {settings.lockout_time}s",
            f"Auto-lock: {settings.auto_lock_timeout}s"
        ]
        
        settings_list = ListWidget(0, 16, 128, 32, items)
        self.add_widget(settings_list)
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Security")
        self.add_widget(status_bar)


class NodeSettingsPage(Page):
    """Page for node settings."""
    
    def __init__(self, settings: NodeSettings, on_settings_changed: Callable):
        """Initialize node settings page."""
        super().__init__("Node Settings")
        self.settings = settings
        self.on_settings_changed = on_settings_changed
        
        # Title
        self.add_widget(TextWidget(64, 0, "NODE", FontSize.MEDIUM, Alignment.CENTER))
        
        # Settings list
        items = [
            f"Name: {settings.name[:20]}",
            f"Role: {settings.role}",
            f"Stake: {settings.stake:.2f} MC"
        ]
        
        settings_list = ListWidget(0, 16, 128, 32, items)
        self.add_widget(settings_list)
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Node")
        self.add_widget(status_bar)


class AboutPage(Page):
    """Page for about information."""
    
    def __init__(self, version: str, author: str):
        """Initialize about page."""
        super().__init__("About")
        self.version = version
        self.author = author
        
        # Title
        self.add_widget(TextWidget(64, 0, "MESHCHAIN", FontSize.MEDIUM, Alignment.CENTER))
        
        # Version
        self.add_widget(TextWidget(64, 16, f"v{version}", FontSize.SMALL, Alignment.CENTER))
        
        # Author
        self.add_widget(TextWidget(64, 24, f"by {author}", FontSize.SMALL, Alignment.CENTER))
        
        # Description
        self.add_widget(TextWidget(64, 32, "Blockchain on", FontSize.SMALL, Alignment.CENTER))
        self.add_widget(TextWidget(64, 40, "Meshtastic", FontSize.SMALL, Alignment.CENTER))
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("About")
        self.add_widget(status_bar)


class SettingsManager:
    """
    Manages settings and configuration.
    
    Handles:
    - Display settings
    - Network settings
    - Security settings
    - Node settings
    - Settings persistence
    """
    
    def __init__(self):
        """Initialize settings manager."""
        self.display_settings = DisplaySettings()
        self.network_settings = NetworkSettings()
        self.security_settings = SecuritySettings()
        self.node_settings = NodeSettings()
        self.on_settings_changed: Optional[Callable] = None
    
    def update_display_settings(self, settings: DisplaySettings):
        """Update display settings."""
        self.display_settings = settings
        if self.on_settings_changed:
            self.on_settings_changed("display", settings)
    
    def update_network_settings(self, settings: NetworkSettings):
        """Update network settings."""
        self.network_settings = settings
        if self.on_settings_changed:
            self.on_settings_changed("network", settings)
    
    def update_security_settings(self, settings: SecuritySettings):
        """Update security settings."""
        self.security_settings = settings
        if self.on_settings_changed:
            self.on_settings_changed("security", settings)
    
    def update_node_settings(self, settings: NodeSettings):
        """Update node settings."""
        self.node_settings = settings
        if self.on_settings_changed:
            self.on_settings_changed("node", settings)
    
    def create_display_settings_page(self) -> DisplaySettingsPage:
        """Create display settings page."""
        return DisplaySettingsPage(self.display_settings, self.update_display_settings)
    
    def create_network_settings_page(self) -> NetworkSettingsPage:
        """Create network settings page."""
        return NetworkSettingsPage(self.network_settings, self.update_network_settings)
    
    def create_security_settings_page(self) -> SecuritySettingsPage:
        """Create security settings page."""
        return SecuritySettingsPage(self.security_settings, self.update_security_settings)
    
    def create_node_settings_page(self) -> NodeSettingsPage:
        """Create node settings page."""
        return NodeSettingsPage(self.node_settings, self.update_node_settings)
    
    def create_about_page(self, version: str = "1.0", author: str = "MeshChain") -> AboutPage:
        """Create about page."""
        return AboutPage(version, author)
    
    def save_settings(self):
        """Save settings to persistent storage."""
        # In production, would save to SPIFFS
        pass
    
    def load_settings(self):
        """Load settings from persistent storage."""
        # In production, would load from SPIFFS
        pass
