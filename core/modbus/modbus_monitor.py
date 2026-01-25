"""Runtime Monitor for Modbus polling of all tags in a project.

This module provides a `RuntimeMonitor` that:
1. Extracts all tags from the project tree (left window)
2. Groups them by Channel driver type and Device parameters
3. Creates separate ModbusClient + ModbusWorker for each unique configuration
4. Manages polling lifecycle (start/stop)
5. Emits signals when tag values are updated
"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any, Dict, List, Optional, Callable
from collections import defaultdict
import time

from PyQt6.QtCore import Qt, pyqtSignal, QObject

from core.config import GROUP_SEPARATOR

from .modbus_client import ModbusClient
from .modbus_worker import ModbusWorker, Signal
from .modbus_mapping import map_tag_to_pymodbus


logger = logging.getLogger(__name__)

# Global registry to keep signal objects alive (prevent garbage collection)
_active_signals = []


class RuntimeMonitorSignals(QObject):
    """Qt Signal emitter for RuntimeMonitor - ensures thread-safe signal emission."""
    tag_updated = pyqtSignal(str, object, float, str, int)  # tag_name, value, timestamp, quality, update_count
    error_occurred = pyqtSignal(str)  # error_msg
    started = pyqtSignal()
    stopped = pyqtSignal()


class RuntimeMonitor:
    """Manages polling of all project tags via Modbus according to their channel/device configs."""

    def __init__(self, tree_root_item=None, signals_instance=None):
        """
        Args:
            tree_root_item: The root Connectivity tree item (typically conn_node from app.py)
            signals_instance: RuntimeMonitorSignals instance (must be created in main thread)
        """
        self.tree_root = tree_root_item
        self.is_running = False
        
        # Use provided signals instance (created in main thread by app.py)
        # This avoids thread affinity issues with PyQt6 QObject
        # IMPORTANT: Keep a strong reference to prevent garbage collection
        if signals_instance:
            self.signals = signals_instance
            self._signals_ref = signals_instance  # Extra reference to prevent GC
            # Register in global list to prevent garbage collection
            if signals_instance not in _active_signals:
                _active_signals.append(signals_instance)
                logger.debug(f"Registered signals instance: {id(signals_instance)}, total active: {len(_active_signals)}")
        else:
            # Fallback: create signals locally (may cause thread affinity issues)
            # But better than having None
            self.signals = RuntimeMonitorSignals()
            self._signals_ref = self.signals
            _active_signals.append(self.signals)
            logger.debug(f"Created fallback signals instance: {id(self.signals)}, total active: {len(_active_signals)}")
        
        # Expose Qt signals directly (for external connections)
        # These are the actual PyQt6 signal objects from RuntimeMonitorSignals
        if self.signals:
            self.signal_tag_updated = self.signals.tag_updated
            self.signal_error = self.signals.error_occurred
            self.signal_started = self.signals.started
            self.signal_stopped = self.signals.stopped
        else:
            self.signal_tag_updated = None
            self.signal_error = None
            self.signal_started = None
            self.signal_stopped = None
        
        # Track if callbacks are connected
        self._callbacks_connected = False
        
        # Internal state
        self._clients: Dict[str, ModbusClient] = {}  # keyed by config_id
        self._workers: Dict[str, ModbusWorker] = {}  # keyed by config_id
        self._config_id_map: Dict[str, str] = {}  # maps tag_name -> config_id (for error tracking)
        self._update_counts: Dict[str, int] = defaultdict(int)  # tag_name -> count
        self._last_timestamps: Dict[str, float] = {}  # tag_name -> timestamp
        
    def mark_callbacks_connected(self):
        """Mark that callbacks have been connected in the main thread."""
        self._callbacks_connected = True
        logger.info("[CALLBACKS_READY] All callbacks have been connected")
        # print(f"[CALLBACKS_READY] Callback connection marked ready")
    
    def start(self) -> bool:
        """Start polling all tags in background thread. Returns immediately."""
        if self.is_running:
            logger.warning("RuntimeMonitor already running")
            return False
        
        # Mark as running immediately to prevent multiple starts
        self.is_running = True
        
        # Start initialization in background thread to avoid UI blocking
        init_thread = threading.Thread(target=self._initialize_in_background, daemon=True)
        init_thread.start()
        
        return True
    
    def _initialize_in_background(self):
        """Initialize workers in background thread."""
        try:
            # Debug: Check tree root
            # print(f"\n=== RuntimeMonitor.start() ===")
            # print(f"Tree root item: {self.tree_root}")
            if self.tree_root:
                try:
                    root_type = self.tree_root.data(0, Qt.ItemDataRole.UserRole)
                    root_text = self.tree_root.text(0)
                    root_children = self.tree_root.childCount()
                    # print(f"Tree root type: {root_type}")
                    # print(f"Tree root text: {root_text}")
                    # print(f"Tree root children: {root_children}")
                except Exception as e:
                    # print(f"Error reading tree root: {e}")
                    pass
            else:
                # print("ERROR: Tree root is None!")
                pass
            
            # Collect and start polling (can be slow)
            self._collect_and_start_polling()
            
            # Emit started signal
            logger.info("[SIGNAL_STARTED] Emitting started signal")
            # print(f"[SIGNAL_STARTED] self.signals={self.signals}, signal={self.signals.started}")
            self.signals.started.emit()
            logger.info(f"[SIGNAL_STARTED] RuntimeMonitor started with {len(self._workers)} worker(s)")
            
        except Exception as e:
            logger.error(f"Error in _initialize_in_background: {e}", exc_info=True)
            self.is_running = False
            # Emit error signal through Qt bridge
            self.signals.error_occurred.emit(f"Failed to start monitoring: {e}")
            self.stop()
    
    def stop(self):
        """Stop all polling and cleanup."""
        if not self.is_running:
            return
        
        # Run cleanup in background thread to avoid blocking UI
        def _cleanup():
            try:
                for config_id, worker in list(self._workers.items()):
                    try:
                        worker.stop()
                    except Exception as e:
                        logger.warning(f"Error stopping worker {config_id}: {e}")
                
                for config_id, client in list(self._clients.items()):
                    try:
                        # Try to close async client
                        try:
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            loop.run_until_complete(client.close_async())
                        except Exception as e:
                            logger.warning(f"Error closing client {config_id}: {e}")
                    except Exception as e:
                        logger.warning(f"Error with client {config_id}: {e}")
                
                self._clients.clear()
                self._workers.clear()
                self._config_id_map.clear()
                self._update_counts.clear()
                self._last_timestamps.clear()
                
                self.is_running = False
                # Emit stop signal through Qt bridge
                self.signals.stopped.emit()
                logger.info("RuntimeMonitor stopped")
            except Exception as e:
                logger.error(f"Error during cleanup: {e}", exc_info=True)
        
        # Start cleanup in background thread
        cleanup_thread = threading.Thread(target=_cleanup, daemon=True)
        cleanup_thread.start()
    
    def _collect_and_start_polling(self):
        """Extract all tags from tree and start workers."""
        if not self.tree_root:
            raise ValueError("No tree root provided to RuntimeMonitor")
        
        # Collect all (tag, device, channel) tuples
        all_tags = self._extract_all_tags()
        if not all_tags:
            raise ValueError("No tags found in project tree")
        
        logger.info(f"Collected {len(all_tags)} tag(s)")
        
        # DEBUG: Summary of collected tags - check by parent group name
        data_tags_count = 0
        array_tags_count = 0
        for tag_item, _, _ in all_tags:
            tag_name = tag_item.text(0)
            parent = tag_item.parent()
            parent_text = parent.text(0) if parent else ""
            if parent_text == 'Data':
                data_tags_count += 1
            elif parent_text == 'TOUData':
                pass  # Count separately if needed
            elif 'Array' in tag_name:  # Array tags have 'Array' in their name, not their parent
                array_tags_count += 1
                logger.info(f"[ARRAY_TAG_FOUND] {tag_name} (parent={parent_text})")
        logger.info(f"[TAG_SUMMARY] Data={data_tags_count}, Array={array_tags_count}, Total={len(all_tags)}")
        
        # Group tags by (channel_config, device_config)
        grouped = self._group_tags_by_config(all_tags)
        
        # Create client + worker for each group
        for config_id, (channel_item, device_item, tags) in grouped.items():
            try:
                self._create_worker_for_group(config_id, channel_item, device_item, tags)
            except Exception as e:
                logger.error(f"Failed to create worker for config {config_id}: {e}", exc_info=True)
                # Emit error signal through Qt bridge
                self.signals.error_occurred.emit(f"Failed to create worker for {config_id}: {e}")
    
    def _extract_all_tags(self) -> List[tuple]:
        """Extract all (tag_item, device_item, channel_item) tuples from tree.
        
        Returns list of (tag_item, device_item, channel_item) tuples.
        Handles nested structure: Channel → Device → [Group →] Tag
        """
        result = []
        
        def walk_tree(item, parent_device=None, parent_channel=None, depth=0):
            """Recursively walk tree collecting tags and their parent context."""
            if not item:
                return
            
            try:
                # Use Qt.ItemDataRole.UserRole for correct role value
                item_type = item.data(0, Qt.ItemDataRole.UserRole)
                item_text = item.text(0) or ""
            except Exception as e:
                item_type = None
                item_text = "???"
            
            if item_type == "Channel":
                parent_channel = item
            elif item_type == "Device":
                parent_device = item
            elif item_type == "Tag":
                if parent_device and parent_channel:
                    result.append((item, parent_device, parent_channel))
                    # DEBUG: log Data and Array tags by checking parent group
                    tag_name = item.text(0)
                    parent = item.parent()
                    parent_text = parent.text(0) if parent else "?"
                    if parent_text in ['Data', 'TOU_Array'] or 'Array' in parent_text:
                        logger.debug(f"[EXTRACT_TAG] {parent_text}.{tag_name}")
            # Note: Group items are skipped but their children are still processed
            
            # Recurse to children (including Group children)
            for i in range(item.childCount()):
                walk_tree(item.child(i), parent_device, parent_channel, depth + 1)
        
        walk_tree(self.tree_root)
        return result
    
    def _get_tag_tree_path(self, tag_item) -> str:
        """Get complete tree path for a tag item (e.g., 'Channel1.Device1.Data.Freq')."""
        if not tag_item:
            return ""
        path_parts = []
        current = tag_item
        while current and current != self.tree_root:
            try:
                text = current.text(0)
                if text and text != "Connectivity":
                    path_parts.insert(0, text)
            except Exception:
                pass
            try:
                current = current.parent()
            except Exception:
                break
        return GROUP_SEPARATOR.join(path_parts)
    
    def _group_tags_by_config(self, all_tags: List[tuple]) -> Dict[str, tuple]:
        """Group tags by their unique (channel_driver, device_params) configuration.
        
        Returns dict: config_id -> (channel_item, device_item, [tag_items])
        """
        from collections import defaultdict
        
        groups = defaultdict(lambda: (None, None, []))
        
        for tag_item, device_item, channel_item in all_tags:
            # Extract configuration identifiers
            try:
                channel_driver = channel_item.data(2, 257)  # col 2: Driver
                if isinstance(channel_driver, dict):
                    driver_type = channel_driver.get("type", "unknown")
                else:
                    driver_type = str(channel_driver or "unknown")
            except Exception:
                driver_type = "unknown"
            
            try:
                device_id = device_item.data(2, 257)  # col 2: Device ID
            except Exception:
                device_id = 1
            
            # Create unique config id: driver_type + device_id
            config_id = f"{driver_type}_{device_id}"
            
            # Store in group (keep latest channel/device items)
            _, _, tags_list = groups[config_id]
            tags_list.append(tag_item)
            groups[config_id] = (channel_item, device_item, tags_list)
        
        return dict(groups)
    
    def _create_worker_for_group(self, config_id: str, channel_item, device_item, tag_items: List):
        """Create a ModbusClient and ModbusWorker for a group of tags."""
        
        # Extract channel configuration
        channel_config = self._extract_channel_config(channel_item)
        device_config = self._extract_device_config(device_item)
        
        # Create ModbusClient based on driver type
        client = self._create_modbus_client(config_id, channel_config, device_config)
        
        # Map all tags to canonical format
        canonical_tags = []
        for tag_item in tag_items:
            try:
                tag_data = self._extract_tag_data(tag_item)
                
                # DEBUG: Log extracted tag data for Data group tags
                if 'Data' in tag_data.get('name', ''):
                    logger.debug(f"[TAG_DATA] name={tag_data.get('name')} addr_raw={tag_data.get('address')}")
                
                canonical = map_tag_to_pymodbus(tag_data, device_config, channel_config)
                
                # DEBUG: Log encoding in canonical
                logger.debug(f"[CANONICAL] {tag_data.get('name')} byte_order={canonical.get('byte_order')} word_order={canonical.get('word_order')}")
                
                # ADD: Include tree path for buffer key matching
                # The tree path is what the UI uses to identify tags
                tree_path = self._get_tag_tree_path(tag_item)
                canonical["tree_path"] = tree_path
                
                canonical_tags.append(canonical)
                
                # DEBUG: Log Data and Array tags with full info
                if "Data" in tree_path or "Array" in tree_path:
                    logger.info(f"[MAP_TAG] tree_path={tree_path} addr={canonical.get('address')} count={canonical.get('count')} is_array={canonical.get('is_array')}")
                
                # Track mapping for error reporting
                self._config_id_map[canonical.get("name", "")] = config_id
            except Exception as e:
                logger.warning(f"Failed to map tag: {e}")
        
        if not canonical_tags:
            raise ValueError(f"No valid tags for config {config_id}")
        
        # Create worker with device-configured parameters
        # Extract inter_request_delay from Timing config (in milliseconds)
        timing_dict = device_config.get("Timing", {})
        inter_request_delay_ms = RuntimeMonitor._parse_int(timing_dict.get("inter_request_delay", 0), 0)
        
        # Extract block sizes from Block Sizes config
        block_dict = device_config.get("Block Sizes", {})
        out_coils = RuntimeMonitor._parse_int(block_dict.get("out_coils", 2000), 2000)
        in_coils = RuntimeMonitor._parse_int(block_dict.get("in_coils", 2000), 2000)
        
        # Get duty cycle configuration (Read:Write ratio, default 1:1 for faster writes)
        duty_cycle_ratio = int(device_config.get("duty_cycle_ratio", 1)) or 1
        max_pending_writes = int(device_config.get("max_pending_writes", 100)) or 100
        
        worker = ModbusWorker(
            client=client,
            default_scan_ms=device_config.get("default_scan_ms", 1000),
            max_regs=int(device_config.get("max_regs", 120)),
            inter_request_delay_ms=inter_request_delay_ms,
            out_coils=out_coils,
            in_coils=in_coils,
            duty_cycle_ratio=duty_cycle_ratio,
            max_pending_writes=max_pending_writes
        )
        
        # Connect worker signal
        def tag_polled_callback(tag_dict, value):
            self._on_tag_polled(config_id, tag_dict, value)
        
        worker.tag_polled.connect(tag_polled_callback)
        
        # Add all tags to worker
        for tag in canonical_tags:
            worker.add_tag(tag)
        
        # DEBUG: Log batch composition after adding tags
        from core.modbus.modbus_scheduler import group_reads
        batches = group_reads(canonical_tags, max_regs=int(device_config.get("max_regs", 120)))
        logger.info(f"[BATCH_SUMMARY] config_id={config_id} has {len(batches)} batches for {len(canonical_tags)} tags")
        for batch_idx, batch in enumerate(batches):
            batch_tags = batch.get('tags', [])
            data_count = len([t for t in batch_tags if 'Data' in t.get('tree_path', '')])
            array_count = len([t for t in batch_tags if 'Array' in t.get('tree_path', '')])
            logger.debug(f"[BATCH_{batch_idx}] addr={batch.get('start')}-{batch.get('start') + batch.get('count') - 1} tags={len(batch_tags)} Data={data_count} Array={array_count}")
        
        # Store references
        self._clients[config_id] = client
        self._workers[config_id] = worker
        
        # Start worker
        worker.start()
        logger.info(f"Created worker {config_id} with {len(canonical_tags)} tag(s)")
    
    def _extract_channel_config(self, channel_item) -> Dict[str, Any]:
        """Extract channel configuration from tree item."""
        config = {}
        
        try:
            driver = channel_item.data(2, Qt.ItemDataRole.UserRole)  # col 2: Driver
            if isinstance(driver, dict):
                config["driver_type"] = driver.get("type", "Modbus TCP/IP Ethernet")
                config["driver_params"] = driver.get("params", {})
            else:
                config["driver_type"] = str(driver or "Modbus TCP/IP Ethernet")
                config["driver_params"] = {}
        except Exception:
            config["driver_type"] = "Modbus TCP/IP Ethernet"
            config["driver_params"] = {}
        
        try:
            comm = channel_item.data(3, Qt.ItemDataRole.UserRole)  # col 3: Communication
            if isinstance(comm, dict):
                config["communication"] = comm
            else:
                config["communication"] = {}
        except Exception:
            config["communication"] = {}
        
        return config
    
    def _extract_device_config(self, device_item) -> Dict[str, Any]:
        """Extract device configuration from tree item."""
        config = {}
        
        try:
            device_id = device_item.data(2, Qt.ItemDataRole.UserRole)  # col 2: Device ID
            config["Device ID"] = int(device_id) if device_id else 1
        except Exception:
            config["Device ID"] = 1
        
        try:
            timing = device_item.data(3, Qt.ItemDataRole.UserRole)  # col 3: Timing
            if isinstance(timing, dict):
                config["Timing"] = timing
            else:
                config["Timing"] = {}
        except Exception:
            config["Timing"] = {}
        
        try:
            data_access = device_item.data(4, Qt.ItemDataRole.UserRole)  # col 4: Data Access
            if isinstance(data_access, dict):
                config["Data Access"] = data_access
            else:
                config["Data Access"] = {}
        except Exception:
            config["Data Access"] = {}
        
        try:
            encoding = device_item.data(5, Qt.ItemDataRole.UserRole)  # col 5: Encoding
            if isinstance(encoding, dict):
                config["Encoding"] = encoding
                logger.debug(f"[DEVICE_CONFIG] Found encoding in tree: {encoding}")
            else:
                config["Encoding"] = {}
                logger.debug(f"[DEVICE_CONFIG] Encoding not a dict: {type(encoding)} = {encoding}")
        except Exception as e:
            config["Encoding"] = {}
            logger.debug(f"[DEVICE_CONFIG] Error reading encoding: {e}")
        
        try:
            block_sizes = device_item.data(6, Qt.ItemDataRole.UserRole)  # col 6: Block Sizes
            if isinstance(block_sizes, dict):
                config["Block Sizes"] = block_sizes
            else:
                config["Block Sizes"] = {}
        except Exception:
            config["Block Sizes"] = {}
        
        # Extract numeric parameters for worker
        timing_dict = config.get("Timing", {})
        config["default_scan_ms"] = self._parse_int(timing_dict.get("inter_request_delay", 0), 0)
        
        block_dict = config.get("Block Sizes", {})
        config["max_regs"] = self._parse_int(
            block_dict.get("hold_regs") or block_dict.get("int_regs", 120),
            120
        )
        
        return config
    
    def _extract_tag_data(self, tag_item) -> Dict[str, Any]:
        """Extract tag data from tree item."""
        data = {}
        
        try:
            # Build full path from tree hierarchy (Channel.Device.Group...Tag)
            # Exclude Connectivity and any root-level items that aren't Channel/Device
            path_parts = []
            current = tag_item
            while current:
                try:
                    text = current.text(0)
                    item_type = current.data(0, Qt.ItemDataRole.UserRole)
                    
                    # Skip Connectivity, root project nodes, etc.
                    # Only include known structural nodes
                    if text and text not in ("Connectivity", "Project", "Projects"):
                        path_parts.insert(0, text)
                except Exception:
                    pass
                current = current.parent()
            
            full_path = GROUP_SEPARATOR.join(path_parts) if path_parts else tag_item.text(0) or "Unknown"
            
            data["name"] = full_path  # Use full path as name
            data["short_name"] = tag_item.text(0) or "Unknown"  # Keep short name for reference
            data["description"] = tag_item.data(1, Qt.ItemDataRole.UserRole) or ""  # col 1: Description
            data["data_type"] = tag_item.data(2, Qt.ItemDataRole.UserRole) or "Word"  # col 2: Data Type
            data["access"] = tag_item.data(3, Qt.ItemDataRole.UserRole) or "Read Only"  # col 3: Access
            
            # Address is mandatory - must not be empty
            addr = tag_item.data(4, Qt.ItemDataRole.UserRole)
            if not addr:
                raise ValueError(f"Tag '{tag_item.text(0)}' must have an address")
            data["address"] = addr  # col 4: Address
            
            data["scan_rate"] = self._parse_int(tag_item.data(5, Qt.ItemDataRole.UserRole), 1000)  # col 5: Scan Rate (ms)
            data["scaling"] = tag_item.data(6, Qt.ItemDataRole.UserRole) or {}  # col 6: Scaling
            
            # Extract array element count from address if present (e.g., "428672 [58]")
            # This is needed for Array tags where the count is in the address field
            import re
            if data.get("address"):
                match = re.search(r'\[(\d+)\]', str(data["address"]))
                if match:
                    data["array_element_count"] = int(match.group(1))
                    logger.debug(f"[EXTRACT_ARRAY_COUNT] tag={full_path} count={data['array_element_count']}")
        except Exception as e:
            logger.warning(f"Error extracting tag data: {e}")
        
        return data
    
    def _create_modbus_client(self, config_id: str, channel_config: Dict, device_config: Dict) -> ModbusClient:
        """Create a ModbusClient based on channel driver type."""
        
        driver_type = channel_config.get("driver_type", "Modbus TCP/IP Ethernet").lower()
        driver_params = channel_config.get("driver_params", {})
        comm_params = channel_config.get("communication", {})
        timing = device_config.get("Timing", {})
        
        # Determine mode and extract connection parameters
        mode = "tcp"
        host = "127.0.0.1"
        port = 502
        unit = device_config.get("device_id", 1)
        
        if "rtu serial" in driver_type:
            mode = "rtu"
            # Serial parameters from communication
            serial_port = comm_params.get("com") or comm_params.get("port", "COM1")
            baudrate = self._parse_int(comm_params.get("baud", 9600), 9600)
            kwargs = {"serial_port": serial_port, "baudrate": baudrate}
        elif "rtu over tcp" in driver_type:
            mode = "overtcp"
            host = driver_params.get("ip") or comm_params.get("ip", "127.0.0.1")
            port = self._parse_int(driver_params.get("port") or comm_params.get("port", 502), 502)
            kwargs = {}
        else:  # TCP Ethernet (default)
            mode = "tcp"
            host = driver_params.get("ip") or comm_params.get("ip", "127.0.0.1")
            port = self._parse_int(driver_params.get("port") or comm_params.get("port", 502), 502)
            kwargs = {}
        
        # Extract timing parameters (respect config values, no enforced minimums)
        connect_timeout = self._parse_float(timing.get("connect_timeout", 3), 3.0)
        connect_attempts = self._parse_int(timing.get("connect_attempts", 1), 1)
        # Convert ms to s
        request_timeout_ms = self._parse_int(timing.get("request_timeout", 3000), 3000)
        request_timeout = request_timeout_ms / 1000.0  # Direct conversion, respect config
        attempts_before_timeout = self._parse_int(timing.get("attempts_before_timeout", 1), 1)
        
        # Extract data access parameters
        data_access = device_config.get("Data Access", {})
        
        # Extract encoding parameters
        encoding = device_config.get("Encoding", {})
        
        # Extract block sizes parameters  
        block_sizes = device_config.get("Block Sizes", {})
        
        # Enable diagnostics callback to track method calls
        DEBUG_MODE = False
        
        # Add connect_attempts to kwargs for ModbusClient
        kwargs["connect_attempts"] = connect_attempts
        kwargs["max_attempts"] = attempts_before_timeout  # Add request retry attempts
        
        # Create client
        logger.debug(f"Creating ModbusClient {config_id}: mode={mode} host={host} port={port} unit={unit}")
        
        client = ModbusClient(
            mode=mode,
            host=host,
            port=port,
            unit=unit,
            connect_timeout=connect_timeout,
            request_timeout=request_timeout,
            data_access=data_access,
            encoding=encoding,
            diag_callback=None,
            **kwargs
        )
        
        return client
    
    def _on_tag_polled(self, config_id: str, tag_dict: Dict, value: Any):
        """Callback when a tag is polled.
        
        For array tags, unpacks the array value into individual element updates.
        E.g., if TOU_Array has 50 elements, emits 50 separate updates with [0], [1], etc.
        """
        try:
            # Use tree_path as the key for consistency with UI tree structure
            # tree_path is set in _create_worker_for_group()
            tag_name = tag_dict.get("tree_path") or tag_dict.get("name", "Unknown")
            timestamp = time.time()
            
            # Skip emitting if callbacks aren't connected yet
            if not self._callbacks_connected:
                return
            
            # Check if this is an array tag
            is_array = tag_dict.get("is_array", False)
            
            if is_array and isinstance(value, (list, tuple)):
                # For array tags: emit one update per element with [idx] suffix
                for idx, elem_value in enumerate(value):
                    array_tag_name = f"{tag_name} [{idx}]"
                    self._last_timestamps[array_tag_name] = timestamp
                    self._update_counts[array_tag_name] += 1
                    
                    quality = "Good" if elem_value is not None else "Bad"
                    update_count = self._update_counts[array_tag_name]
                    
                    # Emit signal for this array element
                    if self.signals and hasattr(self.signals, 'tag_updated'):
                        self.signals.tag_updated.emit(array_tag_name, elem_value, timestamp, quality, update_count)
            else:
                # For scalar tags: emit single update
                self._last_timestamps[tag_name] = timestamp
                self._update_counts[tag_name] += 1
                
                # Determine quality (simple: "Good" if value is not None, "Bad" otherwise)
                quality = "Good" if value is not None else "Bad"
                update_count = self._update_counts[tag_name]
                
                # Emit signal through Qt bridge (thread-safe)
                # Use try-except to handle case where signal object was deleted
                try:
                    if self.signals and hasattr(self.signals, 'tag_updated'):
                        self.signals.tag_updated.emit(tag_name, value, timestamp, quality, update_count)
                except RuntimeError as e:
                    # Signals object was deleted by PyQt6
                    if "wrapped C/C++ object" in str(e):
                        logger.warning(f"Signals object was deleted, stopping polling")
                        self.is_running = False
                    else:
                        raise
            
        except Exception as e:
            logger.error(f"Error in _on_tag_polled: {e}", exc_info=True)
    
    @staticmethod
    def _parse_int(value, default=0) -> int:
        """Safe integer parsing."""
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
    
    @staticmethod
    def _parse_float(value, default=0.0) -> float:
        """Safe float parsing."""
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
