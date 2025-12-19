"""
Sophisticated Route Metric Calculation for MeshChain

Implements advanced route metric calculation considering:
1. Hop count (primary metric)
2. Link quality (SNR, RSSI)
3. Peer reliability
4. Latency
5. Bandwidth availability
6. Path stability

This module provides intelligent route selection for optimal
performance on LoRa mesh networks.
"""

import time
import logging
from typing import Dict, Optional
from dataclasses import dataclass
import math

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class LinkQuality:
    """Link quality metrics."""
    snr: float = 0.0  # Signal-to-noise ratio (dB)
    rssi: float = -100.0  # Received signal strength (dBm)
    packet_loss: float = 0.0  # 0.0-1.0
    latency_ms: float = 0.0  # Milliseconds
    bandwidth_available: int = 0  # Bytes per second


class RouteMetricCalculator:
    """Calculates sophisticated route metrics for mesh routing."""
    
    # Weights for different factors (sum = 100)
    WEIGHT_HOP_COUNT = 40  # Hop count is primary
    WEIGHT_LINK_QUALITY = 25  # Link quality (SNR/RSSI)
    WEIGHT_RELIABILITY = 20  # Peer reliability
    WEIGHT_LATENCY = 10  # Latency
    WEIGHT_BANDWIDTH = 5  # Bandwidth availability
    
    # Thresholds
    MIN_SNR = -20.0  # dB
    MAX_SNR = 10.0  # dB
    MIN_RSSI = -120.0  # dBm
    MAX_RSSI = -70.0  # dBm
    MAX_LATENCY_MS = 5000.0  # 5 seconds
    MIN_RELIABILITY = 0.1  # 10%
    
    def __init__(self):
        """Initialize route metric calculator."""
        logger.info("Route metric calculator initialized")
    
    def calculate_metric(
        self,
        hop_count: int,
        link_quality: Optional[LinkQuality] = None,
        peer_reliability: float = 1.0,
        path_age_seconds: float = 0.0
    ) -> int:
        """
        Calculate comprehensive route metric.
        
        Args:
            hop_count: Number of hops to destination
            link_quality: Link quality metrics (optional)
            peer_reliability: Peer reliability (0.0-1.0)
            path_age_seconds: Age of path in seconds
        
        Returns:
            Route metric (0-255, lower is better)
        """
        # Normalize hop count (0-255)
        hop_metric = min(255, hop_count * 25)
        
        # Calculate link quality metric
        link_metric = self._calculate_link_quality_metric(link_quality)
        
        # Calculate reliability metric
        reliability_metric = self._calculate_reliability_metric(peer_reliability)
        
        # Calculate latency metric
        latency_metric = self._calculate_latency_metric(link_quality)
        
        # Calculate bandwidth metric
        bandwidth_metric = self._calculate_bandwidth_metric(link_quality)
        
        # Calculate path stability metric (penalize old paths)
        stability_metric = self._calculate_stability_metric(path_age_seconds)
        
        # Weighted combination
        total_metric = (
            (hop_metric * self.WEIGHT_HOP_COUNT / 100) +
            (link_metric * self.WEIGHT_LINK_QUALITY / 100) +
            (reliability_metric * self.WEIGHT_RELIABILITY / 100) +
            (latency_metric * self.WEIGHT_LATENCY / 100) +
            (bandwidth_metric * self.WEIGHT_BANDWIDTH / 100) +
            stability_metric  # Stability is additive penalty
        )
        
        # Clamp to 0-255 range
        return max(0, min(255, int(total_metric)))
    
    def _calculate_link_quality_metric(self, link_quality: Optional[LinkQuality]) -> float:
        """
        Calculate link quality metric (0-255).
        
        Based on SNR and RSSI.
        """
        if not link_quality:
            return 128.0  # Neutral if no data
        
        # Normalize SNR (-20 to 10 dB)
        snr_normalized = max(0.0, min(1.0, (link_quality.snr - self.MIN_SNR) / (self.MAX_SNR - self.MIN_SNR)))
        snr_metric = snr_normalized * 255
        
        # Normalize RSSI (-120 to -70 dBm)
        rssi_normalized = max(0.0, min(1.0, (link_quality.rssi - self.MIN_RSSI) / (self.MAX_RSSI - self.MIN_RSSI)))
        rssi_metric = rssi_normalized * 255
        
        # Average SNR and RSSI metrics (SNR is more important)
        link_metric = (snr_metric * 0.6) + (rssi_metric * 0.4)
        
        # Penalize packet loss
        if link_quality.packet_loss > 0:
            loss_penalty = link_quality.packet_loss * 255
            link_metric = max(0, link_metric - loss_penalty)
        
        return link_metric
    
    def _calculate_reliability_metric(self, reliability: float) -> float:
        """
        Calculate reliability metric (0-255).
        
        Based on peer reliability (0.0-1.0).
        """
        # Clamp reliability to valid range
        reliability = max(self.MIN_RELIABILITY, min(1.0, reliability))
        
        # Inverse: higher reliability = lower metric (better)
        return (1.0 - reliability) * 255
    
    def _calculate_latency_metric(self, link_quality: Optional[LinkQuality]) -> float:
        """
        Calculate latency metric (0-255).
        
        Lower latency is better.
        """
        if not link_quality or link_quality.latency_ms <= 0:
            return 128.0  # Neutral if no data
        
        # Normalize latency (0 to MAX_LATENCY_MS)
        latency_normalized = min(1.0, link_quality.latency_ms / self.MAX_LATENCY_MS)
        
        # Convert to metric (higher latency = higher metric = worse)
        return latency_normalized * 255
    
    def _calculate_bandwidth_metric(self, link_quality: Optional[LinkQuality]) -> float:
        """
        Calculate bandwidth metric (0-255).
        
        Higher bandwidth is better.
        """
        if not link_quality or link_quality.bandwidth_available <= 0:
            return 128.0  # Neutral if no data
        
        # Assume 1 KB/s is minimum acceptable bandwidth
        # and 100 KB/s is excellent
        min_bw = 1024  # 1 KB/s
        max_bw = 102400  # 100 KB/s
        
        bw_normalized = min(1.0, max(0.0, (link_quality.bandwidth_available - min_bw) / (max_bw - min_bw)))
        
        # Inverse: higher bandwidth = lower metric (better)
        return (1.0 - bw_normalized) * 255
    
    def _calculate_stability_metric(self, path_age_seconds: float) -> float:
        """
        Calculate path stability metric (penalty for old paths).
        
        Older paths are less stable and get penalized.
        """
        if path_age_seconds <= 0:
            return 0.0
        
        # Penalize paths older than 60 seconds
        # Penalty increases exponentially
        if path_age_seconds > 60:
            age_factor = min(1.0, (path_age_seconds - 60) / 300)  # Max penalty at 360 seconds
            return age_factor * 50  # Max 50 point penalty
        
        return 0.0
    
    def calculate_hop_cost(self, hop_count: int) -> int:
        """
        Calculate cost of additional hops.
        
        Args:
            hop_count: Number of hops
        
        Returns:
            Cost per hop (0-255)
        """
        # Each hop costs 25 metric points
        # This ensures shorter paths are preferred
        return min(255, hop_count * 25)
    
    def is_metric_better(self, metric1: int, metric2: int, threshold: int = 5) -> bool:
        """
        Check if metric1 is significantly better than metric2.
        
        Uses threshold to avoid flapping between similar metrics.
        
        Args:
            metric1: First metric
            metric2: Second metric
            threshold: Minimum difference to consider better
        
        Returns:
            True if metric1 is better
        """
        return metric1 < (metric2 - threshold)
    
    def get_metric_quality(self, metric: int) -> str:
        """
        Get human-readable quality description for metric.
        
        Args:
            metric: Route metric (0-255)
        
        Returns:
            Quality description
        """
        if metric < 50:
            return "Excellent"
        elif metric < 100:
            return "Good"
        elif metric < 150:
            return "Fair"
        elif metric < 200:
            return "Poor"
        else:
            return "Very Poor"
    
    def explain_metric(self, metric: int) -> str:
        """
        Explain metric calculation.
        
        Args:
            metric: Route metric (0-255)
        
        Returns:
            Explanation string
        """
        quality = self.get_metric_quality(metric)
        
        if metric < 50:
            return f"{quality}: Excellent route quality. Metric={metric}/255"
        elif metric < 100:
            return f"{quality}: Good route quality. Metric={metric}/255"
        elif metric < 150:
            return f"{quality}: Fair route quality. Metric={metric}/255. Consider alternatives."
        elif metric < 200:
            return f"{quality}: Poor route quality. Metric={metric}/255. Alternative routes recommended."
        else:
            return f"{quality}: Very poor route quality. Metric={metric}/255. Route may fail."


# Global instance
_calculator = None


def get_calculator() -> RouteMetricCalculator:
    """Get global route metric calculator instance."""
    global _calculator
    if _calculator is None:
        _calculator = RouteMetricCalculator()
    return _calculator
