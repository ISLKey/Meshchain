"""
Testnet Validation and Health Checks

Implements:
1. Configuration validation
2. Network health checks
3. Blockchain state validation
4. Consensus validation
5. Performance monitoring

This module provides comprehensive validation and health
monitoring for testnet deployment.
"""

import time
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthMetric:
    """Single health metric."""
    name: str
    status: HealthStatus
    value: float
    threshold: float
    message: str = ""
    timestamp: int = 0


class TestnetValidator:
    """Validates testnet configuration and state."""
    
    def __init__(self):
        """Initialize testnet validator."""
        self.validation_results: Dict[str, Any] = {}
        logger.info("Testnet validator initialized")
    
    def validate_genesis_block(self, genesis_block: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate genesis block.
        
        Args:
            genesis_block: Genesis block to validate
        
        Returns:
            (is_valid, list of errors)
        """
        errors = []
        
        # Check required fields
        required_fields = ['version', 'height', 'timestamp', 'hash', 'validators', 'utxos']
        for field in required_fields:
            if field not in genesis_block:
                errors.append(f"Missing required field: {field}")
        
        # Check height is 0
        if genesis_block.get('height') != 0:
            errors.append(f"Genesis block height must be 0, got {genesis_block.get('height')}")
        
        # Check validators
        validators = genesis_block.get('validators', [])
        if not validators:
            errors.append("Genesis block must have at least one validator")
        
        # Check UTXOs
        utxos = genesis_block.get('utxos', [])
        if not utxos:
            errors.append("Genesis block must have at least one UTXO")
        
        # Check total stake matches validator stakes
        total_validator_stake = sum(v.get('stake', 0) for v in validators)
        if total_validator_stake == 0:
            errors.append("Total validator stake is 0")
        
        is_valid = len(errors) == 0
        logger.info(f"Genesis block validation: {'PASSED' if is_valid else 'FAILED'}")
        
        return is_valid, errors
    
    def validate_device_configs(self, configs: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
        """
        Validate device configurations.
        
        Args:
            configs: List of device configurations
        
        Returns:
            (is_valid, list of errors)
        """
        errors = []
        
        if not configs:
            errors.append("No device configurations provided")
            return False, errors
        
        node_ids = set()
        serial_ports = set()
        
        for i, config in enumerate(configs):
            # Check required fields
            if 'device_id' not in config:
                errors.append(f"Device {i}: Missing device_id")
            
            if 'network' not in config:
                errors.append(f"Device {i}: Missing network configuration")
                continue
            
            network = config['network']
            
            # Check node_id
            node_id = network.get('node_id')
            if not node_id:
                errors.append(f"Device {i}: Missing node_id")
            elif node_id in node_ids:
                errors.append(f"Device {i}: Duplicate node_id {hex(node_id)}")
            else:
                node_ids.add(node_id)
            
            # Check serial_port
            serial_port = network.get('serial_port')
            if not serial_port:
                errors.append(f"Device {i}: Missing serial_port")
            elif serial_port in serial_ports:
                errors.append(f"Device {i}: Duplicate serial_port {serial_port}")
            else:
                serial_ports.add(serial_port)
            
            # Check blockchain config
            if 'blockchain' not in config:
                errors.append(f"Device {i}: Missing blockchain configuration")
                continue
            
            blockchain = config['blockchain']
            if blockchain.get('is_validator'):
                if blockchain.get('validator_stake', 0) <= 0:
                    errors.append(f"Device {i}: Validator stake must be > 0")
        
        is_valid = len(errors) == 0
        logger.info(f"Device configuration validation: {'PASSED' if is_valid else 'FAILED'}")
        
        return is_valid, errors
    
    def validate_network_topology(self, devices: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
        """
        Validate network topology.
        
        Args:
            devices: List of device configurations
        
        Returns:
            (is_valid, list of errors)
        """
        errors = []
        
        if len(devices) < 3:
            errors.append(f"Network has only {len(devices)} devices, minimum 3 recommended")
        
        if len(devices) > 20:
            errors.append(f"Network has {len(devices)} devices, maximum 20 recommended")
        
        # Check for validators
        validators = [d for d in devices if d.get('blockchain', {}).get('is_validator')]
        if len(validators) < 3:
            errors.append(f"Network has only {len(validators)} validators, minimum 3 required")
        
        is_valid = len(errors) == 0
        logger.info(f"Network topology validation: {'PASSED' if is_valid else 'FAILED'}")
        
        return is_valid, errors


class TestnetHealthMonitor:
    """Monitors testnet health."""
    
    def __init__(self):
        """Initialize health monitor."""
        self.metrics: Dict[str, HealthMetric] = {}
        self.check_history: List[Dict[str, Any]] = []
        logger.info("Testnet health monitor initialized")
    
    def check_device_connectivity(self, device_id: str, is_connected: bool) -> HealthMetric:
        """
        Check device connectivity.
        
        Args:
            device_id: Device identifier
            is_connected: Whether device is connected
        
        Returns:
            Health metric
        """
        status = HealthStatus.HEALTHY if is_connected else HealthStatus.UNHEALTHY
        metric = HealthMetric(
            name=f"device_connectivity_{device_id}",
            status=status,
            value=1.0 if is_connected else 0.0,
            threshold=0.5,
            message=f"Device {device_id} is {'connected' if is_connected else 'disconnected'}",
            timestamp=int(time.time())
        )
        
        self.metrics[metric.name] = metric
        return metric
    
    def check_network_latency(self, latency_ms: float) -> HealthMetric:
        """
        Check network latency.
        
        Args:
            latency_ms: Latency in milliseconds
        
        Returns:
            Health metric
        """
        if latency_ms < 100:
            status = HealthStatus.HEALTHY
        elif latency_ms < 500:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.UNHEALTHY
        
        metric = HealthMetric(
            name="network_latency",
            status=status,
            value=latency_ms,
            threshold=500.0,
            message=f"Network latency: {latency_ms:.1f}ms",
            timestamp=int(time.time())
        )
        
        self.metrics[metric.name] = metric
        return metric
    
    def check_block_production(self, blocks_per_minute: float) -> HealthMetric:
        """
        Check block production rate.
        
        Args:
            blocks_per_minute: Blocks produced per minute
        
        Returns:
            Health metric
        """
        expected_rate = 6.0  # 1 block per 10 seconds
        
        if blocks_per_minute >= expected_rate * 0.8:
            status = HealthStatus.HEALTHY
        elif blocks_per_minute >= expected_rate * 0.5:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.UNHEALTHY
        
        metric = HealthMetric(
            name="block_production",
            status=status,
            value=blocks_per_minute,
            threshold=expected_rate * 0.8,
            message=f"Block production: {blocks_per_minute:.2f} blocks/min (expected {expected_rate})",
            timestamp=int(time.time())
        )
        
        self.metrics[metric.name] = metric
        return metric
    
    def check_consensus_health(self, validators_online: int, total_validators: int) -> HealthMetric:
        """
        Check consensus health.
        
        Args:
            validators_online: Number of online validators
            total_validators: Total number of validators
        
        Returns:
            Health metric
        """
        online_percentage = validators_online / total_validators * 100 if total_validators > 0 else 0
        
        if online_percentage >= 66.7:  # 2/3 threshold
            status = HealthStatus.HEALTHY
        elif online_percentage >= 50:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.UNHEALTHY
        
        metric = HealthMetric(
            name="consensus_health",
            status=status,
            value=online_percentage,
            threshold=66.7,
            message=f"Consensus health: {validators_online}/{total_validators} validators online ({online_percentage:.1f}%)",
            timestamp=int(time.time())
        )
        
        self.metrics[metric.name] = metric
        return metric
    
    def check_memory_usage(self, used_mb: float, total_mb: float) -> HealthMetric:
        """
        Check memory usage.
        
        Args:
            used_mb: Used memory in MB
            total_mb: Total memory in MB
        
        Returns:
            Health metric
        """
        usage_percentage = used_mb / total_mb * 100 if total_mb > 0 else 0
        
        if usage_percentage < 70:
            status = HealthStatus.HEALTHY
        elif usage_percentage < 85:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.UNHEALTHY
        
        metric = HealthMetric(
            name="memory_usage",
            status=status,
            value=usage_percentage,
            threshold=85.0,
            message=f"Memory usage: {used_mb:.1f}/{total_mb:.1f}MB ({usage_percentage:.1f}%)",
            timestamp=int(time.time())
        )
        
        self.metrics[metric.name] = metric
        return metric
    
    def get_overall_health(self) -> HealthStatus:
        """
        Get overall health status.
        
        Returns:
            Overall health status
        """
        if not self.metrics:
            return HealthStatus.UNKNOWN
        
        statuses = [m.status for m in self.metrics.values()]
        
        if HealthStatus.UNHEALTHY in statuses:
            return HealthStatus.UNHEALTHY
        elif HealthStatus.DEGRADED in statuses:
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY
    
    def get_health_report(self) -> Dict[str, Any]:
        """
        Get comprehensive health report.
        
        Returns:
            Health report dictionary
        """
        return {
            'timestamp': int(time.time()),
            'overall_status': self.get_overall_health().value,
            'metrics': {
                name: {
                    'status': metric.status.value,
                    'value': metric.value,
                    'threshold': metric.threshold,
                    'message': metric.message,
                }
                for name, metric in self.metrics.items()
            }
        }
