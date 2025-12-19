"""
MeshChain Async Core Module

Provides lightweight async/event-driven framework for ESP32.
Designed to work without threading library, using simple event loop.

Key Components:
1. EventLoop: Simple event dispatcher
2. MessageQueue: FIFO message queue
3. TaskScheduler: Schedule periodic tasks
4. StateManager: Track node state
5. EventHandler: Base class for event handlers
"""

import time
from typing import Dict, List, Optional, Callable, Any, Tuple
from dataclasses import dataclass, field
from collections import deque
from enum import IntEnum
import threading


class EventType(IntEnum):
    """Event types for node operation."""
    # Network events
    PEER_DISCOVERED = 1
    PEER_LOST = 2
    MESSAGE_RECEIVED = 3
    MESSAGE_SENT = 4
    SYNC_STARTED = 5
    SYNC_COMPLETED = 6
    
    # Block events
    BLOCK_RECEIVED = 10
    BLOCK_PROPOSED = 11
    BLOCK_VALIDATED = 12
    BLOCK_ADDED = 13
    
    # Transaction events
    TRANSACTION_RECEIVED = 20
    TRANSACTION_VALIDATED = 21
    TRANSACTION_ADDED = 22
    TRANSACTION_CONFIRMED = 23
    
    # Consensus events
    CONSENSUS_ROUND_START = 30
    CONSENSUS_ROUND_END = 31
    VALIDATOR_SELECTED = 32
    BLOCK_APPROVED = 33
    
    # Wallet events
    WALLET_CREATED = 40
    WALLET_UNLOCKED = 41
    WALLET_LOCKED = 42
    
    # Node events
    NODE_STARTED = 50
    NODE_STOPPED = 51
    NODE_ERROR = 52
    NODE_SYNCED = 53
    
    # Custom events
    CUSTOM = 100


class NodeState(IntEnum):
    """Node operational states."""
    INITIALIZING = 0
    WAITING_PEERS = 1
    SYNCING = 2
    SYNCHRONIZED = 3
    VALIDATING = 4
    ERROR = 5
    SHUTTING_DOWN = 6


@dataclass
class Event:
    """
    Represents an event in the node.
    
    Attributes:
        event_type: Type of event
        timestamp: When event occurred
        source: Source of event
        data: Event data
        priority: Priority (higher = process first)
    """
    event_type: EventType
    timestamp: float = field(default_factory=time.time)
    source: str = "system"
    data: Dict[str, Any] = field(default_factory=dict)
    priority: int = 0
    
    def __lt__(self, other: 'Event') -> bool:
        """Compare events by priority (for priority queue)."""
        if self.priority != other.priority:
            return self.priority > other.priority  # Higher priority first
        return self.timestamp < other.timestamp  # Earlier timestamp first


@dataclass
class Message:
    """
    Represents a message in the queue.
    
    Attributes:
        message_type: Type of message
        timestamp: When message was queued
        data: Message payload
        source: Source of message
        retries: Number of retry attempts
    """
    message_type: str
    timestamp: float = field(default_factory=time.time)
    data: Any = None
    source: str = "unknown"
    retries: int = 0
    max_retries: int = 3


@dataclass
class ScheduledTask:
    """
    Represents a scheduled task.
    
    Attributes:
        task_id: Unique task identifier
        callback: Function to call
        interval: Interval in seconds (0 = one-time)
        next_run: When to run next
        enabled: Whether task is enabled
    """
    task_id: str
    callback: Callable
    interval: float
    next_run: float = field(default_factory=time.time)
    enabled: bool = True
    last_run: float = 0.0
    run_count: int = 0


class MessageQueue:
    """
    FIFO message queue for async message processing.
    
    Features:
    - Thread-safe operations
    - Priority support
    - Timeout handling
    - Statistics tracking
    """
    
    def __init__(self, max_size: int = 100):
        """
        Initialize message queue.
        
        Args:
            max_size: Maximum queue size
        """
        self.max_size = max_size
        self.queue: deque[Message] = deque(maxlen=max_size)
        self.lock = threading.Lock()
        self.stats = {
            'enqueued': 0,
            'dequeued': 0,
            'dropped': 0,
            'max_size_reached': 0
        }
    
    def enqueue(self, message: Message) -> bool:
        """
        Add message to queue.
        
        Args:
            message: Message to enqueue
        
        Returns:
            True if successful, False if queue full
        """
        with self.lock:
            if len(self.queue) >= self.max_size:
                self.stats['max_size_reached'] += 1
                self.stats['dropped'] += 1
                return False
            
            self.queue.append(message)
            self.stats['enqueued'] += 1
            return True
    
    def dequeue(self, timeout: float = 0.0) -> Optional[Message]:
        """
        Get message from queue.
        
        Args:
            timeout: Wait timeout in seconds (0 = non-blocking)
        
        Returns:
            Message or None if queue empty
        """
        start_time = time.time()
        
        while True:
            with self.lock:
                if self.queue:
                    msg = self.queue.popleft()
                    self.stats['dequeued'] += 1
                    return msg
            
            # Check timeout
            if timeout > 0 and (time.time() - start_time) > timeout:
                return None
            
            # Small sleep to avoid busy waiting
            time.sleep(0.001)
    
    def peek(self) -> Optional[Message]:
        """Peek at next message without removing."""
        with self.lock:
            if self.queue:
                return self.queue[0]
            return None
    
    def size(self) -> int:
        """Get current queue size."""
        with self.lock:
            return len(self.queue)
    
    def clear(self) -> None:
        """Clear all messages."""
        with self.lock:
            self.queue.clear()
    
    def get_stats(self) -> Dict[str, int]:
        """Get queue statistics."""
        with self.lock:
            return self.stats.copy()


class TaskScheduler:
    """
    Schedules and manages periodic tasks.
    
    Features:
    - One-time and periodic tasks
    - Task enabling/disabling
    - Execution statistics
    """
    
    def __init__(self):
        """Initialize task scheduler."""
        self.tasks: Dict[str, ScheduledTask] = {}
        self.lock = threading.Lock()
        self.stats = {
            'scheduled': 0,
            'executed': 0,
            'failed': 0
        }
    
    def schedule(self, task_id: str, callback: Callable, 
                 interval: float = 0.0) -> bool:
        """
        Schedule a task.
        
        Args:
            task_id: Unique task identifier
            callback: Function to call
            interval: Interval in seconds (0 = one-time)
        
        Returns:
            True if scheduled, False if already exists
        """
        with self.lock:
            if task_id in self.tasks:
                return False
            
            task = ScheduledTask(
                task_id=task_id,
                callback=callback,
                interval=interval,
                next_run=time.time()
            )
            self.tasks[task_id] = task
            self.stats['scheduled'] += 1
            
            return True
    
    def unschedule(self, task_id: str) -> bool:
        """Remove a scheduled task."""
        with self.lock:
            if task_id in self.tasks:
                del self.tasks[task_id]
                return True
            return False
    
    def enable_task(self, task_id: str) -> bool:
        """Enable a task."""
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id].enabled = True
                return True
            return False
    
    def disable_task(self, task_id: str) -> bool:
        """Disable a task."""
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id].enabled = False
                return True
            return False
    
    def get_ready_tasks(self) -> List[ScheduledTask]:
        """Get tasks that are ready to run."""
        ready = []
        current_time = time.time()
        
        with self.lock:
            for task in self.tasks.values():
                if task.enabled and current_time >= task.next_run:
                    ready.append(task)
        
        return ready
    
    def execute_task(self, task: ScheduledTask) -> bool:
        """
        Execute a task.
        
        Args:
            task: Task to execute
        
        Returns:
            True if successful, False if failed
        """
        try:
            task.callback()
            
            with self.lock:
                task.last_run = time.time()
                task.run_count += 1
                
                # Schedule next run
                if task.interval > 0:
                    task.next_run = time.time() + task.interval
                else:
                    # One-time task, disable it
                    task.enabled = False
            
            self.stats['executed'] += 1
            return True
        except Exception as e:
            print(f"Task {task.task_id} failed: {e}")
            self.stats['failed'] += 1
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics."""
        with self.lock:
            return {
                'scheduled': self.stats['scheduled'],
                'executed': self.stats['executed'],
                'failed': self.stats['failed'],
                'active_tasks': len([t for t in self.tasks.values() if t.enabled])
            }


class StateManager:
    """
    Manages node state and state transitions.
    
    Features:
    - State tracking
    - State change callbacks
    - State history
    """
    
    def __init__(self, initial_state: NodeState = NodeState.INITIALIZING):
        """
        Initialize state manager.
        
        Args:
            initial_state: Initial node state
        """
        self.current_state = initial_state
        self.previous_state = initial_state
        self.state_changed_at = time.time()
        self.state_history: List[Tuple[NodeState, float]] = [(initial_state, time.time())]
        self.state_callbacks: Dict[NodeState, List[Callable]] = {}
        self.lock = threading.Lock()
    
    def set_state(self, new_state: NodeState) -> bool:
        """
        Set node state.
        
        Args:
            new_state: New state
        
        Returns:
            True if state changed, False if already in that state
        """
        with self.lock:
            if new_state == self.current_state:
                return False
            
            self.previous_state = self.current_state
            self.current_state = new_state
            self.state_changed_at = time.time()
            
            # Add to history
            self.state_history.append((new_state, time.time()))
            
            # Keep only last 100 state changes
            if len(self.state_history) > 100:
                self.state_history = self.state_history[-100:]
        
        # Call callbacks (outside lock to avoid deadlock)
        self._call_callbacks(new_state)
        
        return True
    
    def get_state(self) -> NodeState:
        """Get current node state."""
        with self.lock:
            return self.current_state
    
    def get_state_duration(self) -> float:
        """Get how long node has been in current state."""
        with self.lock:
            return time.time() - self.state_changed_at
    
    def register_state_callback(self, state: NodeState, callback: Callable) -> None:
        """
        Register callback for state change.
        
        Args:
            state: State to watch
            callback: Function to call when entering state
        """
        with self.lock:
            if state not in self.state_callbacks:
                self.state_callbacks[state] = []
            self.state_callbacks[state].append(callback)
    
    def _call_callbacks(self, state: NodeState) -> None:
        """Call registered callbacks for state."""
        callbacks = self.state_callbacks.get(state, [])
        for callback in callbacks:
            try:
                callback()
            except Exception as e:
                print(f"State callback failed: {e}")
    
    def get_history(self, limit: int = 10) -> List[Tuple[str, float]]:
        """Get state change history."""
        with self.lock:
            history = self.state_history[-limit:]
            return [(NodeState(s).name, t) for s, t in history]


class EventLoop:
    """
    Simple event loop for processing events and messages.
    
    Features:
    - Event dispatching
    - Message processing
    - Task scheduling
    - State management
    """
    
    def __init__(self):
        """Initialize event loop."""
        self.running = False
        self.message_queue = MessageQueue(max_size=100)
        self.task_scheduler = TaskScheduler()
        self.state_manager = StateManager()
        
        # Event handlers: event_type -> list of callbacks
        self.event_handlers: Dict[EventType, List[Callable]] = {}
        
        self.lock = threading.Lock()
        self.stats = {
            'events_processed': 0,
            'messages_processed': 0,
            'tasks_executed': 0,
            'errors': 0
        }
    
    def register_handler(self, event_type: EventType, handler: Callable) -> None:
        """
        Register event handler.
        
        Args:
            event_type: Type of event to handle
            handler: Callback function
        """
        with self.lock:
            if event_type not in self.event_handlers:
                self.event_handlers[event_type] = []
            self.event_handlers[event_type].append(handler)
    
    def unregister_handler(self, event_type: EventType, handler: Callable) -> bool:
        """Unregister event handler."""
        with self.lock:
            if event_type in self.event_handlers:
                try:
                    self.event_handlers[event_type].remove(handler)
                    return True
                except ValueError:
                    return False
            return False
    
    def emit_event(self, event: Event) -> None:
        """
        Emit an event.
        
        Args:
            event: Event to emit
        """
        handlers = self.event_handlers.get(event.event_type, [])
        
        for handler in handlers:
            try:
                handler(event)
            except Exception as e:
                print(f"Event handler failed: {e}")
                self.stats['errors'] += 1
        
        self.stats['events_processed'] += 1
    
    def enqueue_message(self, message: Message) -> bool:
        """Enqueue a message for processing."""
        return self.message_queue.enqueue(message)
    
    def process_message(self, message: Message) -> None:
        """
        Process a message.
        
        Args:
            message: Message to process
        """
        # Override in subclass to handle specific message types
        self.stats['messages_processed'] += 1
    
    def run_once(self, timeout: float = 0.1) -> None:
        """
        Run one iteration of event loop.
        
        Args:
            timeout: Timeout for waiting on messages
        """
        # Process scheduled tasks
        ready_tasks = self.task_scheduler.get_ready_tasks()
        for task in ready_tasks:
            self.task_scheduler.execute_task(task)
            self.stats['tasks_executed'] += 1
        
        # Process one message
        message = self.message_queue.dequeue(timeout=timeout)
        if message:
            self.process_message(message)
    
    def run(self, duration: float = 0.0) -> None:
        """
        Run event loop.
        
        Args:
            duration: How long to run (0 = until stopped)
        """
        self.running = True
        start_time = time.time()
        
        while self.running:
            self.run_once(timeout=0.1)
            
            # Check duration
            if duration > 0 and (time.time() - start_time) > duration:
                break
    
    def stop(self) -> None:
        """Stop event loop."""
        self.running = False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get event loop statistics."""
        return {
            'events_processed': self.stats['events_processed'],
            'messages_processed': self.stats['messages_processed'],
            'tasks_executed': self.stats['tasks_executed'],
            'errors': self.stats['errors'],
            'queue_size': self.message_queue.size(),
            'queue_stats': self.message_queue.get_stats(),
            'scheduler_stats': self.task_scheduler.get_stats(),
            'current_state': self.state_manager.get_state().name
        }


class EventHandler:
    """
    Base class for event handlers.
    
    Subclass this to create custom event handlers.
    """
    
    def __init__(self, event_loop: EventLoop):
        """
        Initialize event handler.
        
        Args:
            event_loop: Event loop to register with
        """
        self.event_loop = event_loop
    
    def register(self, event_type: EventType) -> None:
        """Register to handle an event type."""
        self.event_loop.register_handler(event_type, self.handle)
    
    def unregister(self, event_type: EventType) -> None:
        """Unregister from an event type."""
        self.event_loop.unregister_handler(event_type, self.handle)
    
    def handle(self, event: Event) -> None:
        """
        Handle an event.
        
        Override this method in subclass.
        
        Args:
            event: Event to handle
        """
        raise NotImplementedError("Subclass must implement handle()")
