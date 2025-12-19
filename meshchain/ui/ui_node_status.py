"""
Node Status and Blockchain Display UI

Provides UI for:
- Displaying blockchain information
- Showing network status
- Displaying validator information
- Showing node health and metrics
- Displaying peer information
- Showing consensus status
"""

from typing import Optional, List
from dataclasses import dataclass
from meshchain.ui_display import Page, Widget, TextWidget, ListWidget, ProgressWidget, StatusBar, FontSize, Alignment


@dataclass
class BlockchainInfo:
    """Information about the blockchain."""
    height: int
    hash: str
    timestamp: int
    transactions: int
    difficulty: float


@dataclass
class NetworkInfo:
    """Information about the network."""
    peers: int
    synced: bool
    sync_progress: float
    bandwidth_up: float
    bandwidth_down: float


@dataclass
class ValidatorInfo:
    """Information about a validator."""
    name: str
    stake: float
    blocks_proposed: int
    blocks_missed: int
    reputation: float


@dataclass
class NodeMetrics:
    """Node performance metrics."""
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    uptime: int
    blocks_synced: int


class BlockchainInfoPage(Page):
    """Page for displaying blockchain information."""
    
    def __init__(self, blockchain_info: BlockchainInfo):
        """Initialize blockchain info page."""
        super().__init__("Blockchain Info")
        self.blockchain_info = blockchain_info
        
        # Title
        self.add_widget(TextWidget(64, 0, "BLOCKCHAIN", FontSize.MEDIUM, Alignment.CENTER))
        
        # Height
        self.add_widget(TextWidget(0, 12, f"Height: {blockchain_info.height}", FontSize.SMALL))
        
        # Transactions
        self.add_widget(TextWidget(0, 20, f"Txs: {blockchain_info.transactions}", FontSize.SMALL))
        
        # Difficulty
        self.add_widget(TextWidget(0, 28, f"Diff: {blockchain_info.difficulty:.2f}", FontSize.SMALL))
        
        # Hash (truncated)
        hash_short = blockchain_info.hash[:16] + "..."
        self.add_widget(TextWidget(0, 36, f"Hash: {hash_short}", FontSize.SMALL))
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Blockchain")
        self.add_widget(status_bar)
    
    def update(self, blockchain_info: BlockchainInfo):
        """Update blockchain information."""
        self.blockchain_info = blockchain_info


class NetworkInfoPage(Page):
    """Page for displaying network information."""
    
    def __init__(self, network_info: NetworkInfo):
        """Initialize network info page."""
        super().__init__("Network Info")
        self.network_info = network_info
        
        # Title
        self.add_widget(TextWidget(64, 0, "NETWORK", FontSize.MEDIUM, Alignment.CENTER))
        
        # Peers
        self.add_widget(TextWidget(0, 12, f"Peers: {network_info.peers}", FontSize.SMALL))
        
        # Sync status
        sync_status = "SYNCED" if network_info.synced else "SYNCING"
        self.add_widget(TextWidget(0, 20, f"Status: {sync_status}", FontSize.SMALL))
        
        # Sync progress
        if not network_info.synced:
            progress_pct = int(network_info.sync_progress * 100)
            self.add_widget(TextWidget(0, 28, f"Progress: {progress_pct}%", FontSize.SMALL))
            
            # Progress bar
            progress_widget = ProgressWidget(10, 36, 108, 6, network_info.sync_progress)
            self.add_widget(progress_widget)
        
        # Bandwidth
        self.add_widget(TextWidget(0, 44, f"Up: {network_info.bandwidth_up:.1f} KB/s", FontSize.SMALL))
        self.add_widget(TextWidget(0, 52, f"Down: {network_info.bandwidth_down:.1f} KB/s", FontSize.SMALL))
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Network")
        self.add_widget(status_bar)
    
    def update(self, network_info: NetworkInfo):
        """Update network information."""
        self.network_info = network_info


class ValidatorListPage(Page):
    """Page for displaying list of validators."""
    
    def __init__(self, validators: List[ValidatorInfo]):
        """Initialize validator list page."""
        super().__init__("Validators")
        self.validators = validators
        
        # Title
        self.add_widget(TextWidget(64, 0, "VALIDATORS", FontSize.MEDIUM, Alignment.CENTER))
        
        # Validator list
        validator_items = []
        for v in validators:
            uptime = (v.blocks_proposed / (v.blocks_proposed + v.blocks_missed) * 100) if (v.blocks_proposed + v.blocks_missed) > 0 else 0
            validator_items.append(f"{v.name} {uptime:.0f}%")
        
        validator_list = ListWidget(0, 16, 128, 32, validator_items)
        self.add_widget(validator_list)
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Validators")
        self.add_widget(status_bar)


class ValidatorDetailPage(Page):
    """Page for displaying validator details."""
    
    def __init__(self, validator: ValidatorInfo):
        """Initialize validator detail page."""
        super().__init__("Validator Details")
        self.validator = validator
        
        # Title
        self.add_widget(TextWidget(64, 0, validator.name.upper(), FontSize.MEDIUM, Alignment.CENTER))
        
        # Stake
        self.add_widget(TextWidget(0, 12, f"Stake: {validator.stake:.2f} MC", FontSize.SMALL))
        
        # Blocks proposed
        self.add_widget(TextWidget(0, 20, f"Proposed: {validator.blocks_proposed}", FontSize.SMALL))
        
        # Blocks missed
        self.add_widget(TextWidget(0, 28, f"Missed: {validator.blocks_missed}", FontSize.SMALL))
        
        # Reputation
        self.add_widget(TextWidget(0, 36, f"Rep: {validator.reputation:.2f}", FontSize.SMALL))
        
        # Uptime
        uptime = (validator.blocks_proposed / (validator.blocks_proposed + validator.blocks_missed) * 100) if (validator.blocks_proposed + validator.blocks_missed) > 0 else 0
        self.add_widget(TextWidget(0, 44, f"Uptime: {uptime:.1f}%", FontSize.SMALL))
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Validator")
        self.add_widget(status_bar)


class NodeMetricsPage(Page):
    """Page for displaying node metrics."""
    
    def __init__(self, metrics: NodeMetrics):
        """Initialize node metrics page."""
        super().__init__("Node Metrics")
        self.metrics = metrics
        
        # Title
        self.add_widget(TextWidget(64, 0, "METRICS", FontSize.MEDIUM, Alignment.CENTER))
        
        # CPU usage
        self.add_widget(TextWidget(0, 12, f"CPU: {metrics.cpu_usage:.1f}%", FontSize.SMALL))
        
        # Memory usage
        self.add_widget(TextWidget(0, 20, f"RAM: {metrics.memory_usage:.1f}%", FontSize.SMALL))
        
        # Disk usage
        self.add_widget(TextWidget(0, 28, f"Disk: {metrics.disk_usage:.1f}%", FontSize.SMALL))
        
        # Uptime (convert to hours)
        uptime_hours = metrics.uptime // 3600
        self.add_widget(TextWidget(0, 36, f"Uptime: {uptime_hours}h", FontSize.SMALL))
        
        # Blocks synced
        self.add_widget(TextWidget(0, 44, f"Synced: {metrics.blocks_synced}", FontSize.SMALL))
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Metrics")
        self.add_widget(status_bar)
    
    def update(self, metrics: NodeMetrics):
        """Update metrics."""
        self.metrics = metrics


class PeerListPage(Page):
    """Page for displaying list of peers."""
    
    def __init__(self, peers: List[dict]):
        """Initialize peer list page."""
        super().__init__("Peers")
        self.peers = peers
        
        # Title
        self.add_widget(TextWidget(64, 0, "PEERS", FontSize.MEDIUM, Alignment.CENTER))
        
        # Peer list
        peer_items = []
        for peer in peers:
            signal = "▓" * peer.get('signal', 0)
            peer_items.append(f"{peer['id'][:8]}... {signal}")
        
        peer_list = ListWidget(0, 16, 128, 32, peer_items)
        self.add_widget(peer_list)
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Peers")
        self.add_widget(status_bar)


class ConsensusStatusPage(Page):
    """Page for displaying consensus status."""
    
    def __init__(self, round_number: int, phase: str, validators_voted: int, total_validators: int):
        """Initialize consensus status page."""
        super().__init__("Consensus Status")
        self.round_number = round_number
        self.phase = phase
        self.validators_voted = validators_voted
        self.total_validators = total_validators
        
        # Title
        self.add_widget(TextWidget(64, 0, "CONSENSUS", FontSize.MEDIUM, Alignment.CENTER))
        
        # Round
        self.add_widget(TextWidget(0, 12, f"Round: {round_number}", FontSize.SMALL))
        
        # Phase
        self.add_widget(TextWidget(0, 20, f"Phase: {phase}", FontSize.SMALL))
        
        # Votes
        self.add_widget(TextWidget(0, 28, f"Votes: {validators_voted}/{total_validators}", FontSize.SMALL))
        
        # Progress
        progress = validators_voted / total_validators if total_validators > 0 else 0
        progress_widget = ProgressWidget(10, 36, 108, 6, progress)
        self.add_widget(progress_widget)
        
        # Status bar
        status_bar = StatusBar(0, 56, 128, 8)
        status_bar.set_status("Consensus")
        self.add_widget(status_bar)


class NodeStatusManager:
    """
    Manages node status display.
    
    Handles:
    - Blockchain information
    - Network status
    - Validator information
    - Node metrics
    - Peer information
    - Consensus status
    """
    
    def __init__(self):
        """Initialize node status manager."""
        self.blockchain_info: Optional[BlockchainInfo] = None
        self.network_info: Optional[NetworkInfo] = None
        self.validators: List[ValidatorInfo] = []
        self.metrics: Optional[NodeMetrics] = None
        self.peers: List[dict] = []
    
    def update_blockchain(self, blockchain_info: BlockchainInfo):
        """Update blockchain information."""
        self.blockchain_info = blockchain_info
    
    def update_network(self, network_info: NetworkInfo):
        """Update network information."""
        self.network_info = network_info
    
    def update_validators(self, validators: List[ValidatorInfo]):
        """Update validator list."""
        self.validators = validators
    
    def update_metrics(self, metrics: NodeMetrics):
        """Update node metrics."""
        self.metrics = metrics
    
    def update_peers(self, peers: List[dict]):
        """Update peer list."""
        self.peers = peers
    
    def create_blockchain_page(self) -> Optional[Page]:
        """Create blockchain info page."""
        if self.blockchain_info:
            return BlockchainInfoPage(self.blockchain_info)
        return None
    
    def create_network_page(self) -> Optional[Page]:
        """Create network info page."""
        if self.network_info:
            return NetworkInfoPage(self.network_info)
        return None
    
    def create_validator_list_page(self) -> Page:
        """Create validator list page."""
        return ValidatorListPage(self.validators)
    
    def create_validator_detail_page(self, index: int) -> Optional[Page]:
        """Create validator detail page."""
        if 0 <= index < len(self.validators):
            return ValidatorDetailPage(self.validators[index])
        return None
    
    def create_metrics_page(self) -> Optional[Page]:
        """Create metrics page."""
        if self.metrics:
            return NodeMetricsPage(self.metrics)
        return None
    
    def create_peer_list_page(self) -> Page:
        """Create peer list page."""
        return PeerListPage(self.peers)
    
    def create_consensus_page(self, round_number: int, phase: str,
                             validators_voted: int, total_validators: int) -> Page:
        """Create consensus status page."""
        return ConsensusStatusPage(round_number, phase, validators_voted, total_validators)
