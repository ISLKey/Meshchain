"""
Configuration Validation System for MeshChain ESP32 Node

Provides comprehensive configuration validation for:
1. Node configuration parameters
2. Network settings
3. Consensus parameters
4. Storage configuration
5. Wallet settings
6. Async framework parameters

All parameters are validated against ranges, types, and dependencies.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NetworkType(Enum):
    """Network types."""
    TESTNET = "testnet"
    MAINNET = "mainnet"
    LOCAL = "local"


@dataclass
class ConfigError:
    """Configuration error."""
    parameter: str
    value: Any
    reason: str
    severity: str = "error"  # "error", "warning"


class ConfigValidator:
    """
    Comprehensive configuration validator for MeshChain ESP32 node.
    """
    
    # Default configuration ranges
    VALID_RANGES = {
        'node_id': {'type': str, 'min_len': 1, 'max_len': 64},
        'node_name': {'type': str, 'min_len': 1, 'max_len': 64},
        'network_type': {'type': str, 'values': ['testnet', 'mainnet', 'local']},
        'storage_path': {'type': str, 'min_len': 1, 'max_len': 256},
        'wallet_path': {'type': str, 'min_len': 1, 'max_len': 256},
        'port': {'type': int, 'min': 1024, 'max': 65535},
        'max_peers': {'type': int, 'min': 1, 'max': 100},
        'block_time': {'type': int, 'min': 5, 'max': 300},  # 5-300 seconds
        'max_block_size': {'type': int, 'min': 1024, 'max': 1048576},  # 1KB-1MB
        'max_tx_per_block': {'type': int, 'min': 1, 'max': 10000},
        'consensus_timeout': {'type': int, 'min': 5, 'max': 60},
        'sync_batch_size': {'type': int, 'min': 1, 'max': 1000},
        'cache_size_kb': {'type': int, 'min': 10, 'max': 200},
        'max_memory_mb': {'type': int, 'min': 50, 'max': 240},
        'event_loop_timeout': {'type': float, 'min': 0.01, 'max': 10.0},
        'task_queue_size': {'type': int, 'min': 10, 'max': 1000},
        'pin_length': {'type': int, 'min': 4, 'max': 6},
        'pin_attempts': {'type': int, 'min': 1, 'max': 10},
        'pin_lock_duration': {'type': int, 'min': 60, 'max': 3600},
    }
    
    # Required parameters
    REQUIRED_PARAMETERS = [
        'node_id',
        'node_name',
        'network_type',
        'storage_path',
        'wallet_path',
    ]
    
    # Parameter dependencies
    DEPENDENCIES = {
        'max_block_size': ['max_tx_per_block'],  # max_block_size must be checked with max_tx_per_block
        'consensus_timeout': ['block_time'],     # consensus_timeout should be > block_time
    }
    
    def __init__(self):
        """Initialize validator."""
        self.errors: List[ConfigError] = []
        self.warnings: List[ConfigError] = []
    
    def validate(self, config: Dict[str, Any]) -> Tuple[bool, List[ConfigError]]:
        """
        Validate complete configuration.
        
        Args:
            config: Configuration dictionary
        
        Returns:
            Tuple of (is_valid, error_list)
        """
        self.errors.clear()
        self.warnings.clear()
        
        # Check required parameters
        self._validate_required(config)
        
        # Validate each parameter
        for param, value in config.items():
            self._validate_parameter(param, value)
        
        # Validate dependencies
        self._validate_dependencies(config)
        
        # Check for unknown parameters
        self._check_unknown_parameters(config)
        
        is_valid = len(self.errors) == 0
        all_issues = self.errors + self.warnings
        
        if is_valid:
            logger.info("Configuration validation passed")
        else:
            logger.error(f"Configuration validation failed: {len(self.errors)} errors, {len(self.warnings)} warnings")
        
        return is_valid, all_issues
    
    def _validate_required(self, config: Dict[str, Any]) -> None:
        """Validate required parameters."""
        for param in self.REQUIRED_PARAMETERS:
            if param not in config:
                self.errors.append(ConfigError(
                    parameter=param,
                    value=None,
                    reason=f"Required parameter missing: {param}",
                    severity="error"
                ))
    
    def _validate_parameter(self, param: str, value: Any) -> None:
        """Validate single parameter."""
        if param not in self.VALID_RANGES:
            return  # Unknown parameter, will be caught by _check_unknown_parameters
        
        rules = self.VALID_RANGES[param]
        
        # Check type
        if 'type' in rules:
            expected_type = rules['type']
            if not isinstance(value, expected_type):
                self.errors.append(ConfigError(
                    parameter=param,
                    value=value,
                    reason=f"Invalid type: expected {expected_type.__name__}, got {type(value).__name__}",
                    severity="error"
                ))
                return
        
        # Check string length
        if isinstance(value, str):
            if 'min_len' in rules and len(value) < rules['min_len']:
                self.errors.append(ConfigError(
                    parameter=param,
                    value=value,
                    reason=f"String too short: minimum {rules['min_len']} characters",
                    severity="error"
                ))
            
            if 'max_len' in rules and len(value) > rules['max_len']:
                self.errors.append(ConfigError(
                    parameter=param,
                    value=value,
                    reason=f"String too long: maximum {rules['max_len']} characters",
                    severity="error"
                ))
            
            if 'values' in rules and value not in rules['values']:
                self.errors.append(ConfigError(
                    parameter=param,
                    value=value,
                    reason=f"Invalid value: must be one of {rules['values']}",
                    severity="error"
                ))
        
        # Check numeric range
        if isinstance(value, (int, float)):
            if 'min' in rules and value < rules['min']:
                self.errors.append(ConfigError(
                    parameter=param,
                    value=value,
                    reason=f"Value too small: minimum {rules['min']}",
                    severity="error"
                ))
            
            if 'max' in rules and value > rules['max']:
                self.errors.append(ConfigError(
                    parameter=param,
                    value=value,
                    reason=f"Value too large: maximum {rules['max']}",
                    severity="error"
                ))
    
    def _validate_dependencies(self, config: Dict[str, Any]) -> None:
        """Validate parameter dependencies."""
        # consensus_timeout should be >= block_time
        if 'consensus_timeout' in config and 'block_time' in config:
            if config['consensus_timeout'] < config['block_time']:
                self.warnings.append(ConfigError(
                    parameter='consensus_timeout',
                    value=config['consensus_timeout'],
                    reason=f"consensus_timeout ({config['consensus_timeout']}) should be >= block_time ({config['block_time']})",
                    severity="warning"
                ))
        
        # max_block_size should be reasonable for max_tx_per_block
        if 'max_block_size' in config and 'max_tx_per_block' in config:
            # Assume ~500 bytes per transaction
            min_block_size = config['max_tx_per_block'] * 500
            if config['max_block_size'] < min_block_size:
                self.warnings.append(ConfigError(
                    parameter='max_block_size',
                    value=config['max_block_size'],
                    reason=f"max_block_size may be too small for max_tx_per_block",
                    severity="warning"
                ))
        
        # cache_size_kb should not exceed max_memory_mb
        if 'cache_size_kb' in config and 'max_memory_mb' in config:
            cache_mb = config['cache_size_kb'] / 1024
            if cache_mb > config['max_memory_mb'] * 0.5:
                self.warnings.append(ConfigError(
                    parameter='cache_size_kb',
                    value=config['cache_size_kb'],
                    reason=f"cache_size_kb is more than 50% of max_memory_mb",
                    severity="warning"
                ))
    
    def _check_unknown_parameters(self, config: Dict[str, Any]) -> None:
        """Check for unknown parameters."""
        known_params = set(self.VALID_RANGES.keys())
        config_params = set(config.keys())
        unknown = config_params - known_params
        
        for param in unknown:
            self.warnings.append(ConfigError(
                parameter=param,
                value=config[param],
                reason=f"Unknown parameter: {param}",
                severity="warning"
            ))
    
    def get_default_config(self) -> Dict[str, Any]:
        """
        Get default configuration for ESP32 node.
        
        Returns:
            Default configuration dictionary
        """
        return {
            # Node settings
            'node_id': 'meshchain_node_1',
            'node_name': 'MeshChain Node',
            'network_type': 'testnet',
            
            # Network settings
            'port': 5555,
            'max_peers': 20,
            'sync_batch_size': 10,
            
            # Blockchain settings
            'block_time': 30,  # 30 seconds
            'max_block_size': 65536,  # 64 KB
            'max_tx_per_block': 100,
            'consensus_timeout': 60,  # 60 seconds
            
            # Storage settings
            'storage_path': '/mnt/microsd/blockchain',
            'cache_size_kb': 50,
            'max_memory_mb': 200,
            
            # Wallet settings
            'wallet_path': '/spiffs/meshchain',
            'pin_length': 4,
            'pin_attempts': 3,
            'pin_lock_duration': 300,
            
            # Async framework settings
            'event_loop_timeout': 0.1,
            'task_queue_size': 100,
        }
    
    def validate_with_defaults(self, config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], List[ConfigError]]:
        """
        Validate configuration and merge with defaults.
        
        Args:
            config: Configuration dictionary (may be partial)
        
        Returns:
            Tuple of (is_valid, merged_config, error_list)
        """
        # Get defaults
        merged = self.get_default_config()
        
        # Merge provided config
        merged.update(config)
        
        # Validate
        is_valid, issues = self.validate(merged)
        
        return is_valid, merged, issues


class NodeConfigValidator:
    """
    Validator for MicroNode configuration.
    """
    
    def __init__(self):
        """Initialize node config validator."""
        self.validator = ConfigValidator()
    
    def validate_node_config(self, config: Dict[str, Any]) -> Tuple[bool, List[ConfigError]]:
        """
        Validate node configuration.
        
        Args:
            config: Node configuration
        
        Returns:
            Tuple of (is_valid, error_list)
        """
        return self.validator.validate(config)
    
    def get_default_node_config(self) -> Dict[str, Any]:
        """Get default node configuration."""
        return self.validator.get_default_config()
    
    def validate_and_merge(self, config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], List[ConfigError]]:
        """Validate and merge with defaults."""
        return self.validator.validate_with_defaults(config)
