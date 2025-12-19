"""
Device Configuration System for MeshChain Testnet

Implements:
1. Per-device configuration
2. Multi-device management
3. Configuration validation
4. Configuration serialization
5. Device inventory

This module manages configuration for multiple ESP32 devices
in a testnet deployment.
"""

import json
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DeviceRole(Enum):
    """Device role in testnet."""
    VALIDATOR = "validator"
    FULL_NODE = "full_node"
    LIGHT_NODE = "light_node"


@dataclass
class DeviceHardware:
    """Hardware configuration for device."""
    device_type: str = "ESP32"
    cpu_cores: int = 2
    ram_mb: int = 240
    storage_mb: int = 4096
    radio_type: str = "LoRa"
    radio_frequency: int = 915000000  # 915 MHz


@dataclass
class DeviceNetwork:
    """Network configuration for device."""
    node_id: int
    device_name: str
    serial_port: str
    baudrate: int = 115200
    mesh_channel: int = 0
    hop_limit: int = 3
    position_lat: Optional[float] = None
    position_lon: Optional[float] = None


@dataclass
class DeviceBlockchain:
    """Blockchain configuration for device."""
    role: DeviceRole
    is_validator: bool
    validator_stake: int = 0
    validator_public_key: str = ""
    genesis_block_hash: str = ""
    sync_mode: str = "full"  # full, fast, light
    max_peers: int = 10


@dataclass
class DeviceStorage:
    """Storage configuration for device."""
    storage_path: str = "/spiffs/blockchain"
    max_block_cache: int = 10
    max_utxo_cache: int = 1000
    max_message_cache: int = 1000
    enable_compression: bool = True


@dataclass
class DeviceConfig:
    """Complete device configuration."""
    device_id: str  # Unique device identifier
    hardware: DeviceHardware
    network: DeviceNetwork
    blockchain: DeviceBlockchain
    storage: DeviceStorage
    created_time: int = 0
    version: str = "1.0"
    
    def validate(self) -> bool:
        """
        Validate configuration.
        
        Returns:
            True if valid
        """
        # Check required fields
        if not self.device_id:
            logger.error("device_id is required")
            return False
        
        if not self.network.node_id:
            logger.error("network.node_id is required")
            return False
        
        if not self.network.device_name:
            logger.error("network.device_name is required")
            return False
        
        if not self.network.serial_port:
            logger.error("network.serial_port is required")
            return False
        
        # Check validator configuration
        if self.blockchain.is_validator:
            if self.blockchain.validator_stake <= 0:
                logger.error("validator_stake must be > 0 for validators")
                return False
            
            if not self.blockchain.validator_public_key:
                logger.error("validator_public_key is required for validators")
                return False
        
        # Check hardware
        if self.hardware.ram_mb < 100:
            logger.warning("Device has less than 100 MB RAM, may have issues")
        
        logger.info(f"Configuration for {self.device_id} is valid")
        return True


class DeviceConfigManager:
    """Manages configurations for multiple devices."""
    
    def __init__(self):
        """Initialize device configuration manager."""
        self.devices: Dict[str, DeviceConfig] = {}
        logger.info("Device configuration manager initialized")
    
    def add_device(self, config: DeviceConfig) -> None:
        """
        Add device configuration.
        
        Args:
            config: Device configuration
        """
        if not config.validate():
            raise ValueError(f"Invalid configuration for {config.device_id}")
        
        self.devices[config.device_id] = config
        logger.info(f"Added device configuration: {config.device_id}")
    
    def get_device(self, device_id: str) -> Optional[DeviceConfig]:
        """
        Get device configuration.
        
        Args:
            device_id: Device identifier
        
        Returns:
            Device configuration or None
        """
        return self.devices.get(device_id)
    
    def get_all_devices(self) -> Dict[str, DeviceConfig]:
        """
        Get all device configurations.
        
        Returns:
            Dictionary of device configurations
        """
        return self.devices.copy()
    
    def get_validators(self) -> List[DeviceConfig]:
        """
        Get all validator devices.
        
        Returns:
            List of validator configurations
        """
        return [
            config for config in self.devices.values()
            if config.blockchain.is_validator
        ]
    
    def get_full_nodes(self) -> List[DeviceConfig]:
        """
        Get all full node devices.
        
        Returns:
            List of full node configurations
        """
        return [
            config for config in self.devices.values()
            if config.blockchain.role == DeviceRole.FULL_NODE
        ]
    
    def get_light_nodes(self) -> List[DeviceConfig]:
        """
        Get all light node devices.
        
        Returns:
            List of light node configurations
        """
        return [
            config for config in self.devices.values()
            if config.blockchain.role == DeviceRole.LIGHT_NODE
        ]
    
    def save_to_file(self, filepath: str) -> None:
        """
        Save all configurations to file.
        
        Args:
            filepath: Path to save file
        """
        configs = {
            device_id: {
                'device_id': config.device_id,
                'hardware': asdict(config.hardware),
                'network': asdict(config.network),
                'blockchain': {
                    **asdict(config.blockchain),
                    'role': config.blockchain.role.value
                },
                'storage': asdict(config.storage),
                'version': config.version,
            }
            for device_id, config in self.devices.items()
        }
        
        with open(filepath, 'w') as f:
            json.dump(configs, f, indent=2)
        
        logger.info(f"Saved {len(self.devices)} device configurations to {filepath}")
    
    def load_from_file(self, filepath: str) -> None:
        """
        Load configurations from file.
        
        Args:
            filepath: Path to configuration file
        """
        with open(filepath, 'r') as f:
            configs = json.load(f)
        
        for device_id, config_data in configs.items():
            hardware = DeviceHardware(**config_data['hardware'])
            network = DeviceNetwork(**config_data['network'])
            blockchain_data = config_data['blockchain'].copy()
            blockchain_data['role'] = DeviceRole(blockchain_data['role'])
            blockchain = DeviceBlockchain(**blockchain_data)
            storage = DeviceStorage(**config_data['storage'])
            
            config = DeviceConfig(
                device_id=device_id,
                hardware=hardware,
                network=network,
                blockchain=blockchain,
                storage=storage,
                version=config_data.get('version', '1.0')
            )
            
            self.add_device(config)
        
        logger.info(f"Loaded {len(self.devices)} device configurations from {filepath}")
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary of all devices.
        
        Returns:
            Summary dictionary
        """
        validators = self.get_validators()
        full_nodes = self.get_full_nodes()
        light_nodes = self.get_light_nodes()
        
        return {
            'total_devices': len(self.devices),
            'validators': len(validators),
            'full_nodes': len(full_nodes),
            'light_nodes': len(light_nodes),
            'total_stake': sum(v.blockchain.validator_stake for v in validators),
            'devices': [
                {
                    'device_id': config.device_id,
                    'node_id': hex(config.network.node_id),
                    'device_name': config.network.device_name,
                    'role': config.blockchain.role.value,
                    'is_validator': config.blockchain.is_validator,
                }
                for config in self.devices.values()
            ]
        }


class TestnetConfigBuilder:
    """Helper class to build testnet configurations."""
    
    @staticmethod
    def create_5_node_testnet() -> DeviceConfigManager:
        """
        Create configuration for 5-node testnet.
        
        Returns:
            DeviceConfigManager with 5 devices
        """
        manager = DeviceConfigManager()
        
        devices = [
            {
                'device_id': 'device-1',
                'node_id': 0x11111111,
                'device_name': 'Validator-1',
                'serial_port': '/dev/ttyUSB0',
                'role': DeviceRole.VALIDATOR,
                'is_validator': True,
                'validator_stake': 10000,
                'validator_public_key': 'a' * 64,
            },
            {
                'device_id': 'device-2',
                'node_id': 0x22222222,
                'device_name': 'Validator-2',
                'serial_port': '/dev/ttyUSB1',
                'role': DeviceRole.VALIDATOR,
                'is_validator': True,
                'validator_stake': 10000,
                'validator_public_key': 'b' * 64,
            },
            {
                'device_id': 'device-3',
                'node_id': 0x33333333,
                'device_name': 'Validator-3',
                'serial_port': '/dev/ttyUSB2',
                'role': DeviceRole.VALIDATOR,
                'is_validator': True,
                'validator_stake': 10000,
                'validator_public_key': 'c' * 64,
            },
            {
                'device_id': 'device-4',
                'node_id': 0x44444444,
                'device_name': 'Validator-4',
                'serial_port': '/dev/ttyUSB3',
                'role': DeviceRole.VALIDATOR,
                'is_validator': True,
                'validator_stake': 10000,
                'validator_public_key': 'd' * 64,
            },
            {
                'device_id': 'device-5',
                'node_id': 0x55555555,
                'device_name': 'Validator-5',
                'serial_port': '/dev/ttyUSB4',
                'role': DeviceRole.VALIDATOR,
                'is_validator': True,
                'validator_stake': 10000,
                'validator_public_key': 'e' * 64,
            },
        ]
        
        for device in devices:
            config = DeviceConfig(
                device_id=device['device_id'],
                hardware=DeviceHardware(),
                network=DeviceNetwork(
                    node_id=device['node_id'],
                    device_name=device['device_name'],
                    serial_port=device['serial_port'],
                ),
                blockchain=DeviceBlockchain(
                    role=device['role'],
                    is_validator=device['is_validator'],
                    validator_stake=device['validator_stake'],
                    validator_public_key=device['validator_public_key'],
                ),
                storage=DeviceStorage(),
            )
            manager.add_device(config)
        
        return manager
    
    @staticmethod
    def create_6_node_testnet() -> DeviceConfigManager:
        """
        Create configuration for 6-node testnet.
        
        Returns:
            DeviceConfigManager with 6 devices
        """
        manager = DeviceConfigManager()
        
        devices = [
            {
                'device_id': 'device-1',
                'node_id': 0x11111111,
                'device_name': 'Validator-1',
                'serial_port': '/dev/ttyUSB0',
                'role': DeviceRole.VALIDATOR,
                'is_validator': True,
                'validator_stake': 8000,
                'validator_public_key': 'a' * 64,
            },
            {
                'device_id': 'device-2',
                'node_id': 0x22222222,
                'device_name': 'Validator-2',
                'serial_port': '/dev/ttyUSB1',
                'role': DeviceRole.VALIDATOR,
                'is_validator': True,
                'validator_stake': 8000,
                'validator_public_key': 'b' * 64,
            },
            {
                'device_id': 'device-3',
                'node_id': 0x33333333,
                'device_name': 'Validator-3',
                'serial_port': '/dev/ttyUSB2',
                'role': DeviceRole.VALIDATOR,
                'is_validator': True,
                'validator_stake': 8000,
                'validator_public_key': 'c' * 64,
            },
            {
                'device_id': 'device-4',
                'node_id': 0x44444444,
                'device_name': 'Validator-4',
                'serial_port': '/dev/ttyUSB3',
                'role': DeviceRole.VALIDATOR,
                'is_validator': True,
                'validator_stake': 8000,
                'validator_public_key': 'd' * 64,
            },
            {
                'device_id': 'device-5',
                'node_id': 0x55555555,
                'device_name': 'Validator-5',
                'serial_port': '/dev/ttyUSB4',
                'role': DeviceRole.VALIDATOR,
                'is_validator': True,
                'validator_stake': 8000,
                'validator_public_key': 'e' * 64,
            },
            {
                'device_id': 'device-6',
                'node_id': 0x66666666,
                'device_name': 'Validator-6',
                'serial_port': '/dev/ttyUSB5',
                'role': DeviceRole.VALIDATOR,
                'is_validator': True,
                'validator_stake': 8000,
                'validator_public_key': 'f' * 64,
            },
        ]
        
        for device in devices:
            config = DeviceConfig(
                device_id=device['device_id'],
                hardware=DeviceHardware(),
                network=DeviceNetwork(
                    node_id=device['node_id'],
                    device_name=device['device_name'],
                    serial_port=device['serial_port'],
                ),
                blockchain=DeviceBlockchain(
                    role=device['role'],
                    is_validator=device['is_validator'],
                    validator_stake=device['validator_stake'],
                    validator_public_key=device['validator_public_key'],
                ),
                storage=DeviceStorage(),
            )
            manager.add_device(config)
        
        return manager
