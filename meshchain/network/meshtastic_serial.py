"""
Meshtastic Serial Communication Layer for ESP32

Provides direct serial communication with Meshtastic radio hardware:
1. Serial port management
2. Meshtastic protocol parsing
3. Message sending and receiving
4. Radio configuration
5. Device management
6. Error handling and recovery

This module enables ESP32 devices to communicate directly with Meshtastic
radios via serial connection, bypassing MQTT for low-latency mesh communication.
"""

import serial
import time
import threading
import logging
from typing import Dict, List, Optional, Callable, Any, Tuple
from dataclasses import dataclass
from enum import IntEnum
from queue import Queue, Empty
import struct

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MeshtasticPortNum(IntEnum):
    """Meshtastic port numbers for different message types."""
    TEXT_MESSAGE_APP = 1
    POSITION_APP = 3
    NODEINFO_APP = 4
    ROUTING_APP = 5
    ADMIN_APP = 6
    TELEMETRY_APP = 67


@dataclass
class MeshtasticPacket:
    """Represents a Meshtastic packet."""
    from_id: int                       # Sender node ID
    to_id: int                         # Recipient node ID
    want_ack: bool = False             # Request acknowledgment
    hop_start: int = 3                 # Initial hop limit
    hop_limit: int = 3                 # Remaining hops
    want_ack_from_id: int = 0          # Who to ack to
    priority: int = 64                 # Message priority
    rx_time: int = 0                   # Receive time
    rx_rssi: int = 0                   # RSSI at receive
    rx_snr: float = 0.0                # SNR at receive
    port_num: int = MeshtasticPortNum.TEXT_MESSAGE_APP
    payload: bytes = b''               # Message payload
    
    def serialize(self) -> bytes:
        """Serialize packet to binary format."""
        data = bytearray()
        
        # Pack header
        data.extend(struct.pack('<I', self.from_id))
        data.extend(struct.pack('<I', self.to_id))
        data.append(self.want_ack)
        data.append(self.hop_start)
        data.append(self.hop_limit)
        data.extend(struct.pack('<I', self.want_ack_from_id))
        data.append(self.priority)
        data.extend(struct.pack('<I', self.rx_time))
        data.extend(struct.pack('<h', self.rx_rssi))
        data.extend(struct.pack('<f', self.rx_snr))
        data.append(self.port_num)
        
        # Add payload length and payload
        data.extend(struct.pack('<H', len(self.payload)))
        data.extend(self.payload)
        
        return bytes(data)
    
    @staticmethod
    def deserialize(data: bytes) -> Optional['MeshtasticPacket']:
        """Deserialize packet from binary format."""
        try:
            if len(data) < 30:
                return None
            
            offset = 0
            from_id = struct.unpack_from('<I', data, offset)[0]
            offset += 4
            
            to_id = struct.unpack_from('<I', data, offset)[0]
            offset += 4
            
            want_ack = data[offset] != 0
            offset += 1
            
            hop_start = data[offset]
            offset += 1
            
            hop_limit = data[offset]
            offset += 1
            
            want_ack_from_id = struct.unpack_from('<I', data, offset)[0]
            offset += 4
            
            priority = data[offset]
            offset += 1
            
            rx_time = struct.unpack_from('<I', data, offset)[0]
            offset += 4
            
            rx_rssi = struct.unpack_from('<h', data, offset)[0]
            offset += 2
            
            rx_snr = struct.unpack_from('<f', data, offset)[0]
            offset += 4
            
            port_num = data[offset]
            offset += 1
            
            payload_len = struct.unpack_from('<H', data, offset)[0]
            offset += 2
            
            if offset + payload_len > len(data):
                return None
            
            payload = data[offset:offset + payload_len]
            
            return MeshtasticPacket(
                from_id=from_id,
                to_id=to_id,
                want_ack=want_ack,
                hop_start=hop_start,
                hop_limit=hop_limit,
                want_ack_from_id=want_ack_from_id,
                priority=priority,
                rx_time=rx_time,
                rx_rssi=rx_rssi,
                rx_snr=rx_snr,
                port_num=port_num,
                payload=payload
            )
        except Exception as e:
            logger.error(f"Error deserializing packet: {e}")
            return None


class MeshtasticSerialConnection:
    """
    Direct serial connection to Meshtastic radio.
    
    Handles:
    - Serial port communication
    - Packet framing
    - Message queuing
    - Error handling
    """
    
    # Frame markers
    FRAME_START = 0x94
    FRAME_END = 0x55
    
    def __init__(self, port: str = '/dev/ttyUSB0', baudrate: int = 115200, timeout: float = 1.0):
        """
        Initialize serial connection.
        
        Args:
            port: Serial port (e.g., '/dev/ttyUSB0' on Linux, 'COM3' on Windows)
            baudrate: Baud rate (default 115200 for Meshtastic)
            timeout: Serial read timeout in seconds
        """
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        
        self.serial: Optional[serial.Serial] = None
        self.is_connected = False
        self.is_running = False
        
        # Message queues
        self.rx_queue: Queue = Queue(maxsize=100)
        self.tx_queue: Queue = Queue(maxsize=100)
        
        # Threads
        self.rx_thread: Optional[threading.Thread] = None
        self.tx_thread: Optional[threading.Thread] = None
        
        # Callbacks
        self.on_packet_received: Optional[Callable[[MeshtasticPacket], None]] = None
        self.on_error: Optional[Callable[[str], None]] = None
        
        # Statistics
        self.stats = {
            'packets_sent': 0,
            'packets_received': 0,
            'bytes_sent': 0,
            'bytes_received': 0,
            'errors': 0,
            'crc_errors': 0,
            'frame_errors': 0,
            'backpressure_events': 0,
            'dropped_packets': 0,
            'requeued_packets': 0
        }
        self.stats_lock = threading.Lock()
        
        # Backpressure callback
        self.on_backpressure: Optional[Callable[[int], None]] = None
        
        logger.info(f"Meshtastic serial connection initialized (port={port}, baudrate={baudrate})")
    
    def connect(self) -> bool:
        """
        Connect to Meshtastic radio.
        
        Returns:
            True if successful
        """
        try:
            self.serial = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=self.timeout,
                write_timeout=self.timeout
            )
            
            self.is_connected = True
            self.is_running = True
            
            # Start RX and TX threads
            self.rx_thread = threading.Thread(target=self._rx_loop, daemon=True)
            self.tx_thread = threading.Thread(target=self._tx_loop, daemon=True)
            
            self.rx_thread.start()
            self.tx_thread.start()
            
            logger.info(f"Connected to Meshtastic radio on {self.port}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to connect to Meshtastic radio: {e}")
            self.is_connected = False
            return False
    
    def disconnect(self) -> None:
        """Disconnect from Meshtastic radio."""
        self.is_running = False
        
        if self.rx_thread:
            self.rx_thread.join(timeout=5.0)
        
        if self.tx_thread:
            self.tx_thread.join(timeout=5.0)
        
        if self.serial:
            try:
                self.serial.close()
            except:
                pass
        
        self.is_connected = False
        logger.info("Disconnected from Meshtastic radio")
    
    def send_packet(self, packet: MeshtasticPacket) -> bool:
        """
        Send packet to radio.
        
        Args:
            packet: Packet to send
        
        Returns:
            True if queued successfully
        """
        try:
            self.tx_queue.put_nowait(packet)
            return True
        except:
            logger.warning("TX queue full, packet dropped")
            return False
    
    def receive_packet(self, timeout: float = 1.0) -> Optional[MeshtasticPacket]:
        """
        Receive packet from radio.
        
        Args:
            timeout: Timeout in seconds
        
        Returns:
            Packet or None if timeout
        """
        try:
            return self.rx_queue.get(timeout=timeout)
        except Empty:
            return None
    
    def _rx_loop(self) -> None:
        """Main receive loop."""
        buffer = bytearray()
        reconnect_attempts = 0
        max_reconnect_attempts = 5
        
        while self.is_running:
            try:
                # Check if serial port is still open
                if not self.serial or not self.serial.is_open:
                    logger.warning("Serial port closed, attempting reconnection")
                    
                    # Attempt reconnection
                    if reconnect_attempts < max_reconnect_attempts:
                        reconnect_attempts += 1
                        logger.info(f"Reconnection attempt {reconnect_attempts}/{max_reconnect_attempts}")
                        
                        try:
                            if self.serial:
                                self.serial.close()
                            
                            time.sleep(1.0)  # Wait before reconnecting
                            
                            self.serial = serial.Serial(
                                self.port,
                                self.baudrate,
                                timeout=self.timeout
                            )
                            reconnect_attempts = 0
                            logger.info("Serial port reconnected successfully")
                        except Exception as e:
                            logger.error(f"Reconnection failed: {e}")
                            time.sleep(2.0)  # Wait longer before next attempt
                            continue
                    else:
                        logger.error(f"Max reconnection attempts ({max_reconnect_attempts}) reached")
                        self.is_connected = False
                        break
                
                # Read data from serial
                if self.serial and self.serial.is_open and self.serial.in_waiting:
                    data = self.serial.read(self.serial.in_waiting)
                    buffer.extend(data)
                    
                    with self.stats_lock:
                        self.stats['bytes_received'] += len(data)
                    
                    # Process frames
                    while True:
                        frame = self._extract_frame(buffer)
                        if frame is None:
                            break
                        
                        # Parse packet
                        packet = MeshtasticPacket.deserialize(frame)
                        if packet:
                            try:
                                self.rx_queue.put_nowait(packet)
                                
                                with self.stats_lock:
                                    self.stats['packets_received'] += 1
                                
                                # Call callback
                                if self.on_packet_received:
                                    self.on_packet_received(packet)
                            except:
                                logger.warning("RX queue full")
                        else:
                            with self.stats_lock:
                                self.stats['frame_errors'] += 1
                else:
                    time.sleep(0.01)
            
            except serial.SerialException as e:
                logger.error(f"Serial port error in RX loop: {e}")
                with self.stats_lock:
                    self.stats['errors'] += 1
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"Error in RX loop: {e}")
                with self.stats_lock:
                    self.stats['errors'] += 1
                time.sleep(0.1)
    
    def _tx_loop(self) -> None:
        """Main transmit loop."""
        backpressure_warned = False
        
        while self.is_running:
            try:
                # Get packet from queue
                packet = self.tx_queue.get(timeout=0.1)
                
                # Check if serial port is open
                if not self.serial or not self.serial.is_open:
                    logger.warning("Serial port not open, requeueing packet")
                    try:
                        self.tx_queue.put_nowait(packet)  # Put packet back
                        with self.stats_lock:
                            self.stats['requeued_packets'] += 1
                    except:
                        logger.warning("TX queue full, packet dropped")
                        with self.stats_lock:
                            self.stats['dropped_packets'] += 1
                    time.sleep(0.5)
                    continue
                
                # Check if queue is getting too full (backpressure)
                queue_size = self.tx_queue.qsize()
                
                if queue_size >= 80:  # 80% of 100
                    if not backpressure_warned:
                        logger.warning(f"TX queue backpressure: {queue_size} packets queued")
                        
                        with self.stats_lock:
                            self.stats['backpressure_events'] += 1
                        
                        # Call backpressure callback
                        if self.on_backpressure:
                            self.on_backpressure(queue_size)
                        
                        # Also call error callback if available
                        if self.on_error:
                            self.on_error(f"TX queue backpressure: {queue_size} packets queued")
                        
                        backpressure_warned = True
                elif queue_size < 50:  # Reset warning below 50% threshold
                    backpressure_warned = False
                
                # Serialize and frame packet
                packet_data = packet.serialize()
                frame = self._create_frame(packet_data)
                
                # Send to radio
                try:
                    self.serial.write(frame)
                    self.serial.flush()  # Ensure data is sent
                    
                    with self.stats_lock:
                        self.stats['packets_sent'] += 1
                        self.stats['bytes_sent'] += len(frame)
                except serial.SerialException as e:
                    logger.error(f"Failed to send packet: {e}")
                    try:
                        self.tx_queue.put_nowait(packet)  # Put packet back
                        with self.stats_lock:
                            self.stats['requeued_packets'] += 1
                    except:
                        logger.warning("TX queue full, packet dropped")
                        with self.stats_lock:
                            self.stats['dropped_packets'] += 1
                    
                    with self.stats_lock:
                        self.stats['errors'] += 1
                    time.sleep(0.5)
            
            except Empty:
                pass
            except serial.SerialException as e:
                logger.error(f"Serial port error in TX loop: {e}")
                with self.stats_lock:
                    self.stats['errors'] += 1
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"Error in TX loop: {e}")
                with self.stats_lock:
                    self.stats['errors'] += 1
    
    def _create_frame(self, data: bytes) -> bytes:
        """
        Create frame with start/end markers and CRC.
        
        Format:
        - Start marker (1 byte): 0x94
        - Length (2 bytes): Big-endian
        - Data (variable)
        - CRC (2 bytes): CRC16-CCITT
        - End marker (1 byte): 0x55
        """
        frame = bytearray()
        
        # Start marker
        frame.append(self.FRAME_START)
        
        # Length
        frame.extend(struct.pack('>H', len(data)))
        
        # Data
        frame.extend(data)
        
        # CRC
        crc = self._crc16_ccitt(data)
        frame.extend(struct.pack('>H', crc))
        
        # End marker
        frame.append(self.FRAME_END)
        
        return bytes(frame)
    
    def _extract_frame(self, buffer: bytearray) -> Optional[bytes]:
        """
        Extract frame from buffer.
        
        Returns:
            Frame data or None if incomplete
        """
        # Find start marker
        start_idx = -1
        for i in range(len(buffer)):
            if buffer[i] == self.FRAME_START:
                start_idx = i
                break
        
        if start_idx == -1:
            return None
        
        # Need at least start + length + end
        if len(buffer) < start_idx + 5:
            return None
        
        # Extract length
        length = struct.unpack_from('>H', buffer, start_idx + 1)[0]
        
        # Check if we have complete frame
        frame_size = 1 + 2 + length + 2 + 1  # start + len + data + crc + end
        if len(buffer) < start_idx + frame_size:
            return None
        
        # Extract frame
        frame_start = start_idx
        frame_end = start_idx + frame_size
        frame = buffer[frame_start:frame_end]
        
        # Verify frame
        if frame[-1] != self.FRAME_END:
            # Remove bad frame
            del buffer[:frame_start + 1]
            return None
        
        # Verify CRC
        data = frame[3:-3]  # Skip start, length, and end marker + CRC
        stored_crc = struct.unpack_from('>H', frame, -3)[0]
        computed_crc = self._crc16_ccitt(data)
        
        if stored_crc != computed_crc:
            logger.warning("CRC error in frame")
            with self.stats_lock:
                self.stats['crc_errors'] += 1
            del buffer[:frame_end]
            return None
        
        # Remove frame from buffer
        del buffer[:frame_end]
        
        return data
    
    # CRC16-CCITT lookup table (generated once)
    _CRC16_CCITT_TABLE = None
    
    @staticmethod
    def _generate_crc16_table():
        """Generate CRC16-CCITT lookup table."""
        if MeshtasticSerialConnection._CRC16_CCITT_TABLE is not None:
            return
        
        table = []
        for i in range(256):
            crc = i << 8
            for _ in range(8):
                crc <<= 1
                if crc & 0x10000:
                    crc ^= 0x1021
                crc &= 0xFFFF
            table.append(crc)
        
        MeshtasticSerialConnection._CRC16_CCITT_TABLE = table
    
    @staticmethod
    def _crc16_ccitt(data: bytes) -> int:
        """
        Calculate CRC16-CCITT checksum using lookup table.
        
        Args:
            data: Data to checksum
        
        Returns:
            CRC16-CCITT value
        """
        # Generate table on first use
        MeshtasticSerialConnection._generate_crc16_table()
        table = MeshtasticSerialConnection._CRC16_CCITT_TABLE
        
        crc = 0xFFFF
        
        for byte in data:
            tbl_idx = ((crc >> 8) ^ byte) & 0xFF
            crc = ((crc << 8) ^ table[tbl_idx]) & 0xFFFF
        
        return crc
    
    def get_stats(self) -> Dict[str, int]:
        """Get connection statistics."""
        with self.stats_lock:
            return self.stats.copy()


class MeshtasticDevice:
    """
    High-level interface to Meshtastic device.
    
    Provides:
    - Device information
    - Configuration
    - Status monitoring
    """
    
    def __init__(self, connection: MeshtasticSerialConnection):
        """
        Initialize device interface.
        
        Args:
            connection: Serial connection to device
        """
        self.connection = connection
        self.device_info: Dict[str, Any] = {}
        self.node_id: Optional[int] = None
        
        logger.info("Meshtastic device interface initialized")
    
    def get_device_info(self) -> Optional[Dict[str, Any]]:
        """
        Get device information.
        
        Returns:
            Device info dictionary
        """
        return self.device_info.copy()
    
    def get_node_id(self) -> Optional[int]:
        """
        Get this device's node ID.
        
        Returns:
            Node ID (4-byte integer)
        """
        return self.node_id
    
    def set_node_id(self, node_id: int) -> None:
        """
        Set this device's node ID.
        
        Args:
            node_id: Node ID to set
        """
        self.node_id = node_id
        logger.info(f"Node ID set to {node_id}")
