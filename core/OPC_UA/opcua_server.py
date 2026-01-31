"""OPC UA Server for ModUA

Dynamic OPC UA server that maps project tags to OPC UA nodes.
All configuration comes from tree widget via data_manager, no hardcoding.
Supports bidirectional read/write operations.
"""

import asyncio
import logging
import threading
import time
from typing import Optional, Any, Dict, List

try:
    from asyncua import Server, ua
except ImportError:
    try:
        from opcua import Server, ua
    except ImportError:
        Server = None
        ua = None
        raise ImportError(
            "Neither asyncua nor opcua is installed. Please install one of them."
        )

try:
    from PyQt6.QtCore import Qt
except ImportError:
    try:
        from PyQt5.QtCore import Qt
    except ImportError:
        Qt = None
        raise ImportError("Neither PyQt6 nor PyQt5 is installed.")

logger = logging.getLogger(__name__)


def get_opcua_datatype(data_type_str: str) -> ua.NodeId:
    """Map Modbus/UI data type to OPC UA DataType NodeId.

    Args:
        data_type_str: Data type string (e.g., "Float", "Boolean", "Int32")

    Returns:
        OPC UA NodeId for the data type
    """
    if not data_type_str:
        return ua.NodeId(ua.ObjectIds.Double)

    s = data_type_str.lower()

    # Boolean types
    if "boolean" in s or "bool" in s:
        return ua.NodeId(ua.ObjectIds.Boolean)

    # Integer types (8-bit)
    if "byte" in s or "uint8" in s or "char" in s:
        return ua.NodeId(ua.ObjectIds.Byte)

    # Integer types (16-bit)
    if "short" in s or "int16" in s:
        return ua.NodeId(ua.ObjectIds.Int16)
    if "word" in s or "uint16" in s or "int" in s:
        return ua.NodeId(ua.ObjectIds.UInt16)

    # Integer types (32-bit)
    if "long" in s or "int32" in s or "dword" in s or "uint32" in s:
        return ua.NodeId(ua.ObjectIds.Int32)

    # Integer types (64-bit)
    if "llong" in s or "int64" in s or "qword" in s or "uint64" in s:
        return ua.NodeId(ua.ObjectIds.Int64)

    # Float types
    if "float" in s or "real" in s:
        return ua.NodeId(ua.ObjectIds.Float)
    if "double" in s:
        return ua.NodeId(ua.ObjectIds.Double)

    # String type
    if "string" in s:
        return ua.NodeId(ua.ObjectIds.String)

    # BCD types (store as Int16/Int32)
    if "lbcd" in s:
        return ua.NodeId(ua.ObjectIds.Int32)
    if "bcd" in s:
        return ua.NodeId(ua.ObjectIds.Int16)

    # Default to Double
    return ua.NodeId(ua.ObjectIds.Double)


def get_access_level(access_str: str) -> int:
    """Convert access string to OPC UA AccessLevel.

    Args:
        access_str: Access string (e.g., "Read/Write", "Read Only", "R/W", "RO")

    Returns:
        OPC UA AccessLevel value:
        - 0x01: CurrentRead (read-only)
        - 0x02: CurrentWrite (write-only)
        - 0x03: CurrentRead | CurrentWrite (read/write)
    """
    if not access_str:
        logger.debug(f"get_access_level: access_str is None or empty, returning 0x01")
        return 0x01

    s = str(access_str).lower().strip()
    logger.debug(f"get_access_level: input='{access_str}' -> normalized='{s}'")

    # Check for write permission - handle various formats
    # Try different common formats
    is_writable = (
        "read/write" in s  # "Read/Write"
        or "read/write" in s  # "Read/Write" (with space)
        or "r/w" in s  # "R/W"
        or "rw" in s  # "RW"
        or ("read" in s and "write" in s)  # Both words present
    )

    if is_writable:
        result = 0x03  # ReadWrite
        logger.debug(f"get_access_level: is_writable=True, returning 0x{result:02x}")
        return result
    elif "write" in s:
        result = 0x02  # WriteOnly
        logger.debug(f"get_access_level: write=True, returning 0x{result:02x}")
        return result
    else:
        result = 0x01  # ReadOnly
        logger.debug(f"get_access_level: readonly=True, returning 0x{result:02x}")
        return result


def get_default_value(data_type_str: str) -> Any:
    """Get default value for a data type.

    Args:
        data_type_str: Data type string

    Returns:
        Default value for the type
    """
    s = data_type_str.lower() if data_type_str else ""

    if "boolean" in s or "bool" in s:
        return False
    elif "float" in s or "real" in s or "double" in s:
        return 0.0
    elif "string" in s:
        return ""
    else:
        return 0


def is_array_type(
    data_type_str: str, address: str = None, metadata: dict = None
) -> bool:
    """Determine if a tag is an array type.

    Checks multiple sources like monitor does:
    1. From data type string (contains "(Array)" or "[]")
    2. From address (contains "[n]" pattern)
    3. From metadata is_array flag

    Args:
        data_type_str: Data type string
        address: Address string (optional)
        metadata: Metadata dict (optional)

    Returns:
        True if array type, False otherwise
    """
    if data_type_str:
        s = data_type_str.lower()
        if "array" in s or "[]" in s or "(array)" in s:
            return True

    if address:
        if "[" in address and "]" in address:
            return True

    if metadata and isinstance(metadata, dict):
        if metadata.get("is_array", False):
            return True

    return False


def get_scaled_datatype(scaling: dict) -> Optional[str]:
    """Get the scaled data type from scaling config.

    If scaling is enabled, OPC UA should use the scaled data type.

    Args:
        scaling: Scaling configuration dict

    Returns:
        Scaled data type string, or None if no scaling
    """
    if not scaling or not isinstance(scaling, dict):
        return None

    scale_type = scaling.get("type", "").lower()
    if scale_type == "none" or not scale_type:
        return None

    # Return the scaled data type if scaling is enabled
    return scaling.get("scaled_type")


class OPCUAServer:
    """Dynamic OPC UA Server for ModUA

    Features:
    - All tags from tree widget mapped to OPC UA nodes
    - No hardcoded values - all from tree/data_manager
    - Bidirectional read/write support
    - Proper data type, access level handling
    - Scaled data type support
    """

    def __init__(self, settings: dict = None):
        """Initialize OPC UA server.

        Args:
            settings: OPC UA settings dict (from app.opcua_settings)
        """
        self.server = None
        self.is_running = False
        self.server_thread = None
        self.loop = None
        self._stop_event = None
        self.settings = settings or {}

        # Data sources (to be set externally)
        self.tree_widget = None
        self.data_buffer = None

        # Runtime monitor reference for write operations
        self.runtime_monitor = None

        # Tag cache for fast lookup
        self._tag_nodes = {}  # {tag_path: (node, tag_info)}
        self._tag_info = {}  # {tag_path: tag_info_dict}

    def set_data_sources(
        self, tree_widget=None, data_buffer=None, runtime_monitor=None
    ):
        """Set data sources for OPC UA server.

        Args:
            tree_widget: ConnectivityTree widget
            data_buffer: ModbusDataBuffer for live values
            runtime_monitor: RuntimeMonitor for write operations
        """
        if tree_widget is not None:
            self.tree_widget = tree_widget
        if data_buffer is not None:
            self.data_buffer = data_buffer
        if runtime_monitor is not None:
            self.runtime_monitor = runtime_monitor

    def _get_server_config(self) -> dict:
        """Extract server configuration from settings.

        Returns:
            Dict with host, port, app_name, namespace
        """
        try:
            gen = (
                self.settings.get("general", {})
                if isinstance(self.settings.get("general"), dict)
                else {}
            )
        except Exception:
            gen = {}

        return {
            "host": gen.get("network_adapter_ip")
            or self.settings.get("network_adapter_ip", "0.0.0.0"),
            "port": int(gen.get("port", self.settings.get("port", 4848))),
            "app_name": gen.get(
                "application_Name", self.settings.get("application_Name", "ModUA")
            ),
            "namespace": gen.get("namespace", self.settings.get("namespace", "ModUA")),
        }

    def _get_security_policies(self) -> List[ua.SecurityPolicyType]:
        """Get security policies from settings.

        Returns:
            List of security policy types (e.g., [ua.SecurityPolicyType.NoSecurity])
        """
        try:
            sec = (
                self.settings.get("security_policies", {})
                if isinstance(self.settings.get("security_policies"), dict)
                else {}
            )
        except Exception:
            sec = {}

        # Map enabled policies to SecurityPolicyType
        policies = []
        for policy_name, enabled in sec.items():
            if enabled:
                try:
                    if hasattr(ua.SecurityPolicyType, policy_name):
                        policies.append(getattr(ua.SecurityPolicyType, policy_name))
                except Exception:
                    logger.warning(f"Unknown security policy: {policy_name}")

        # Default to NoSecurity if no policies enabled
        if not policies:
            policies = [ua.SecurityPolicyType.NoSecurity]

        return policies

    def _run_server_in_thread(self, host: str, port: int):
        """Run the server in a dedicated thread with its own event loop."""
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self._stop_event = asyncio.Event()

            # Run the async server start
            self.loop.run_until_complete(self._start_server_async(host, port))

            # Keep server running until stop is requested
            self.loop.run_until_complete(self._stop_event.wait())

        except Exception as e:
            logger.error(f"Server thread error: {e}")
        finally:
            # Cleanup
            if self.loop and not self.loop.is_closed():
                try:
                    if self.server:
                        self.loop.run_until_complete(self.server.stop())

                    # Cancel all pending tasks
                    pending = asyncio.all_tasks(self.loop)
                    for task in pending:
                        task.cancel()

                    if pending:
                        self.loop.run_until_complete(
                            asyncio.gather(*pending, return_exceptions=True)
                        )

                    self.loop.close()
                    logger.debug("Event loop closed")
                except Exception as e:
                    logger.debug(f"Cleanup error: {e}")
                    try:
                        self.loop.close()
                    except Exception:
                        pass

    async def _start_server_async(self, host: str, port: int):
        """Async server startup."""
        try:
            config = self._get_server_config()

            self.server = Server()

            # Set endpoint
            endpoint = f"opc.tcp://{host}:{port}/"
            self.server.set_endpoint(endpoint)
            logger.info(f"OPC UA Endpoint: {endpoint}")

            # Set server name
            self.server.set_server_name(config["app_name"])
            logger.info(f"OPC UA Server Name: {config['app_name']}")

            # Initialize server
            await self.server.init()

            # Set security policies
            policies = self._get_security_policies()
            self.server.set_security_policy(policies)
            policy_names = [str(p).split(".")[-1] for p in policies]
            logger.info(f"OPC UA Security Policies: {policy_names}")

            # Set authentication from settings
            auth_config = self.settings.get("authentication", {})
            auth_type = auth_config.get("authentication", "Anonymous")
            logger.info(f"OPC UA Authentication: {auth_type}")

            # Start server
            await self.server.start()
            self.is_running = True

            logger.info("OPC UA Server started successfully")

        except OSError as e:
            # Handle port in use error specifically
            if (
                "address already in use" in str(e).lower()
                or "only one usage" in str(e).lower()
            ):
                logger.error(
                    f"OPC UA Port {port} is already in use. Please stop other OPC UA server or change the port."
                )
            logger.error(f"Error details: {e}")
            self.is_running = False
            raise
        except Exception as e:
            logger.error(f"Failed to start OPC UA server: {e}")
            self.is_running = False
            raise

    def start_server(self, host: str = None, port: int = None):
        """Start OPC UA server in background thread.

        Args:
            host: Override host from settings
            port: Override port from settings
        """
        # Early exit if already running
        if self.is_running:
            logger.warning("OPC UA server already running")
            return False

        try:
            # Get config
            config = self._get_server_config()
            host = host or config["host"]
            port = port or config["port"]

            # Stop existing server if there's a server object but not running
            # This handles the case where server was stopped but object still exists
            if not self.is_running and self.server is not None:
                logger.info("Stopping stale OPC UA server...")
                self.stop_server()
                # Wait for cleanup
                import time

                time.sleep(1)

            # Clear any existing tags/nodes
            # Always clear on fresh start to ensure clean state
            self._tag_nodes.clear()
            self._tag_info.clear()

            # Start in background thread
            self.server_thread = threading.Thread(
                target=self._run_server_in_thread, args=(host, port), daemon=True
            )
            self.server_thread.start()

            logger.info(f"OPC UA server starting on {host}:{port}")
            return True

        except Exception as e:
            logger.error(f"Failed to start OPC UA server thread: {e}")
            return False

        try:
            # Get config
            config = self._get_server_config()
            host = host or config["host"]
            port = port or config["port"]

            # Stop existing server and wait for it to fully stop
            if self.is_running or self.server is not None:
                logger.info("Stopping existing OPC UA server...")
                self.stop_server()
                # Wait a bit for cleanup
                import time

                time.sleep(2)  # Increased wait time to ensure full cleanup

            # Clear any existing tags/nodes ONLY if server was running before
            # For fresh start, don't clear (server will be empty anyway)
            if self.is_running or self.server is not None:
                self._tag_nodes.clear()
                self._tag_info.clear()

            # Start in background thread
            self.server_thread = threading.Thread(
                target=self._run_server_in_thread, args=(host, port), daemon=True
            )
            self.server_thread.start()

            logger.info(f"OPC UA server starting on {host}:{port}")
            return True

        except Exception as e:
            logger.error(f"Failed to start OPC UA server thread: {e}")
            return False

    def reload_tags(self) -> bool:
        """Reload all tags - clears old nodes and creates new ones.

        Use this when:
        - A new project is opened
        - Project tags have changed
        - User requests a refresh from OPC UA settings

        Returns:
            True on success, False on failure
        """
        if not self.is_running or not self.server:
            logger.warning("OPC UA server not running, cannot reload tags")
            return False

        if not self.tree_widget:
            logger.warning("Tree widget not set")
            return False

        try:
            # Clear old nodes first
            logger.info("Reloading OPC UA tags - clearing old nodes...")
            self._tag_nodes.clear()
            self._tag_info.clear()

            # Load all tags from scratch
            return self.load_all_tags()

        except Exception as e:
            logger.error(f"Error reloading tags: {e}", exc_info=True)
            return False

        try:
            # Get config
            config = self._get_server_config()
            host = host or config["host"]
            port = port or config["port"]

            # Stop existing server and wait for it to fully stop
            if self.is_running or self.server is not None:
                logger.info("Stopping existing OPC UA server...")
                self.stop_server()
                # Wait a bit for cleanup
                import time

                time.sleep(1)

            # Clear any existing tags/nodes
            self._tag_nodes.clear()
            self._tag_info.clear()

            # Start in background thread
            self.server_thread = threading.Thread(
                target=self._run_server_in_thread, args=(host, port), daemon=True
            )
            self.server_thread.start()

            logger.info(f"OPC UA server starting on {host}:{port}")
            return True

        except Exception as e:
            logger.error(f"Failed to start OPC UA server thread: {e}")
            return False

        try:
            # Get config
            config = self._get_server_config()
            host = host or config["host"]
            port = port or config["port"]

            # Stop existing server
            self.stop_server()

            # Start in background thread
            self.server_thread = threading.Thread(
                target=self._run_server_in_thread, args=(host, port), daemon=True
            )
            self.server_thread.start()

            logger.info(f"OPC UA server starting on {host}:{port}")
            return True

        except Exception as e:
            logger.error(f"Failed to start OPC UA server thread: {e}")
            return False

    def stop_server(self):
        """Stop OPC UA server gracefully."""
        if not self.is_running:
            return

        try:
            # Signal server to stop
            if self._stop_event and self.loop:
                asyncio.run_coroutine_threadsafe(self._stop_event.set(), self.loop)

            # Wait for thread to finish
            if self.server_thread and self.server_thread.is_alive():
                self.server_thread.join(timeout=5)

            self.is_running = False
            self._tag_nodes.clear()
            self._tag_info.clear()

            logger.info("OPC UA server stopped")
        except Exception as e:
            logger.error(f"Error stopping OPC UA server: {e}")

    def load_all_tags(self) -> bool:
        """Load all tags from tree widget to OPC UA server.

        Similar to how monitor extracts all tags using tree_root.
        Note: Does NOT clear old nodes - use reload_tags() for that.

        Returns:
            True on success, False on failure
        """
        if not self.is_running or not self.server:
            logger.error("OPC UA server not running, cannot load tags")
            return False

        if not self.tree_widget:
            logger.warning("Tree widget not set")
            return False

        try:
            # Use tree_root like monitor does - this walks ALL items including hidden tags
            tree_root = getattr(self.tree_widget, "root_node", None)
            if not tree_root:
                logger.warning("No root node in tree")
                return False

            tag_count = [0]  # Use list for mutable reference

            # Walk tree: Project -> Connectivity -> Channel -> Device -> [Group] -> Tag
            # Similar to monitor's _extract_all_tags method
            def walk_tree(item, parent_channel=None, parent_device=None):
                """Recursively walk tree collecting tags."""
                if not item:
                    return

                try:
                    item_type = item.data(0, Qt.ItemDataRole.UserRole)
                except Exception as e:
                    logger.debug(f"Could not get item type: {e}")
                    item_type = None

                # Update parent context
                if item_type == "Channel":
                    parent_channel = item
                elif item_type == "Device":
                    parent_device = item
                elif item_type == "Tag":
                    # Add tag to OPC UA
                    try:
                        if self._add_tag_to_opcua(item):
                            tag_count[0] += 1
                    except Exception as e:
                        logger.error(
                            f"Error adding tag '{item.text(0)}' to OPC UA: {e}"
                        )
                    # Don't recurse further for tags
                    return

                # Recurse to children (including Group children)
                for i in range(item.childCount()):
                    walk_tree(item.child(i), parent_channel, parent_device)

            # Start from root node (like monitor)
            walk_tree(tree_root)

            logger.info(f"Loaded {tag_count[0]} tags to OPC UA server")
            return True

        except Exception as e:
            logger.error(f"Error loading tags to OPC UA: {e}", exc_info=True)
            return False

        if not self.tree_widget:
            logger.warning("Tree widget not set")
            return False

        try:
            # Clear old nodes first
            self._clear_all_nodes()

            # Use tree_root like monitor does - this walks ALL items including hidden tags
            tree_root = getattr(self.tree_widget, "root_node", None)
            if not tree_root:
                logger.warning("No root node in tree")
                return False

            tag_count = [0]  # Use list for mutable reference

            # Walk tree: Project -> Connectivity -> Channel -> Device -> [Group] -> Tag
            # Similar to monitor's _extract_all_tags method
            def walk_tree(item, parent_channel=None, parent_device=None):
                """Recursively walk tree collecting tags."""
                if not item:
                    return

                try:
                    item_type = item.data(0, Qt.ItemDataRole.UserRole)
                except Exception as e:
                    logger.debug(f"Could not get item type: {e}")
                    item_type = None

                # Update parent context
                if item_type == "Channel":
                    parent_channel = item
                elif item_type == "Device":
                    parent_device = item
                elif item_type == "Tag":
                    # Add tag to OPC UA
                    try:
                        if self._add_tag_to_opcua(item):
                            tag_count[0] += 1
                    except Exception as e:
                        logger.error(
                            f"Error adding tag '{item.text(0)}' to OPC UA: {e}"
                        )
                    # Don't recurse further for tags
                    return

                # Recurse to children (including Group children)
                for i in range(item.childCount()):
                    walk_tree(item.child(i), parent_channel, parent_device)

            # Start from root node (like monitor)
            walk_tree(tree_root)

            logger.info(f"Loaded {tag_count[0]} tags to OPC UA server")
            return True

        except Exception as e:
            logger.error(f"Error loading tags to OPC UA: {e}", exc_info=True)
            return False

    def _add_tag_to_opcua(self, tag_item) -> bool:
        """Add a single tag from tree item to OPC UA server.

        Extracts all tag properties from tree item and creates OPC UA node.
        Uses same extraction logic as monitor and modbus_mapping.

        Args:
            tag_item: QTreeWidgetItem for the tag

        Returns:
            True on success, False on failure
        """
        try:
            # Extract tag properties (same as monitor/modbus_monitor)
            tag_name = tag_item.text(0)
            tag_path = self._get_tag_path(tag_item)

            # Get parent info for full path
            description = tag_item.data(1, Qt.ItemDataRole.UserRole) or ""
            data_type = tag_item.data(2, Qt.ItemDataRole.UserRole) or "Float"
            access = tag_item.data(3, Qt.ItemDataRole.UserRole) or "Read Only"
            address = tag_item.data(4, Qt.ItemDataRole.UserRole) or ""
            scan_rate = tag_item.data(5, Qt.ItemDataRole.UserRole) or "1000"
            scaling = tag_item.data(6, Qt.ItemDataRole.UserRole) or {}
            metadata = tag_item.data(7, Qt.ItemDataRole.UserRole) or {}

            # Debug: log access value from tree
            logger.debug(
                f"Tag '{tag_name}' access from tree: '{access}' (type: {type(access)})"
            )

            # Build tag info dict
            tag_info = {
                "path": tag_path,
                "name": tag_name,
                "description": description,
                "data_type": data_type,
                "access": access,
                "address": address,
                "scan_rate": scan_rate,
                "scaling": scaling,
                "metadata": metadata,
                "is_array": is_array_type(data_type, address, metadata),
            }

            # Get scaled data type if scaling is enabled
            scaled_type = get_scaled_datatype(scaling)
            if scaled_type:
                tag_info["opcua_datatype"] = scaled_type
            else:
                tag_info["opcua_datatype"] = data_type

            # Add node asynchronously
            if self.loop and not self.loop.is_closed():
                future = asyncio.run_coroutine_threadsafe(
                    self._add_opcua_node_async(tag_info), self.loop
                )
                node = future.result(timeout=5)

                if node:
                    # Store node and info for later use
                    self._tag_nodes[tag_path] = (node, tag_info)
                    self._tag_info[tag_path] = tag_info

                    # Update data_buffer with tag info
                    if self.data_buffer:
                        access_code = "RW" if "Write" in access else "R"
                        self.data_buffer.set_tag_info(tag_path, data_type, access_code)

                    logger.info(
                        f"Added OPC UA node: {tag_path} (type={data_type}, access={access}, opcua_level={get_access_level(access):04x})"
                    )
                    return True

            return False

        except Exception as e:
            logger.error(f"Error adding tag '{tag_item.text(0)}' to OPC UA: {e}")
            return False

        except Exception as e:
            logger.error(f"Error adding tag '{tag_item.text(0)}' to OPC UA: {e}")
            return False

        except Exception as e:
            logger.error(f"Error adding tag '{tag_item.text(0)}' to OPC UA: {e}")
            return False

    async def _add_opcua_node_async(self, tag_info: dict):
        """Async method to add OPC UA variable node.

        Args:
            tag_info: Tag information dict

        Returns:
            OPC UA variable node
        """
        try:
            objects = self.server.get_objects_node()

            # Build node ID
            node_id = f"ns=2;s={tag_info['path']}"

            # Get OPC UA data type
            opcua_datatype = get_opcua_datatype(tag_info["opcua_datatype"])

            # Get default value
            default_value = get_default_value(tag_info["opcua_datatype"])

            # Get access level first
            access_level = get_access_level(tag_info["access"])
            logger.debug(
                f"Creating node '{tag_info['path']}': access_str='{tag_info['access']}' -> level=0x{access_level:02x}"
            )

            # Check if node already exists
            try:
                existing = await self.server.get_node(ua.NodeId.from_string(node_id))
                # Delete existing node
                await existing.delete()
                logger.debug(f"Deleted existing node: {tag_info['path']}")
            except Exception:
                # Node doesn't exist, that's fine
                pass

            # Create variable node
            var_node = await objects.add_variable(
                ua.NodeId.from_string(node_id),
                tag_info["name"],
                default_value,
                datatype=opcua_datatype,
            )

            # Set node properties
            if tag_info.get("description"):
                try:
                    desc = ua.LocalizedText(tag_info["description"])
                    await var_node.set_attribute(ua.AttributeIds.Description, desc)
                except Exception:
                    pass

            # Set access level - try multiple approaches for asyncua compatibility
            if access_level == 0x03:  # Read/Write
                try:
                    # Method 1: set_writable() (asyncua method)
                    await var_node.set_writable()
                    logger.debug(
                        f"Node '{tag_info['path']}' set as writable via set_writable()"
                    )
                except Exception as e1:
                    logger.debug(f"set_writable() failed: {e1}")
                    # Method 2: Set AccessLevel attribute directly
                    try:
                        await var_node.set_attribute(
                            ua.AttributeIds.AccessLevel, access_level
                        )
                        logger.debug(
                            f"AccessLevel set via set_attribute: 0x{access_level:02x}"
                        )
                    except Exception as e2:
                        logger.debug(f"set_attribute(AccessLevel) failed: {e2}")
            # For read-only nodes (0x01), no action needed - nodes are read-only by default

            # Verify the access level was set
            try:
                current_level = await var_node.read_attribute(
                    ua.AttributeIds.AccessLevel
                )
                logger.debug(
                    f"Verified AccessLevel for '{tag_info['path']}': 0x{current_level.Value.Value:02x}"
                )
            except Exception as e:
                logger.debug(f"Failed to verify AccessLevel: {e}")

            # Set access level
            access_level = get_access_level(tag_info["access"])
            logger.debug(
                f"Setting AccessLevel for '{tag_info['path']}': access_str='{tag_info['access']}' -> level=0x{access_level:02x}"
            )

            # Try multiple ways to set access level (asyncua compatibility)
            try:
                # Method 1: set_attribute with variant
                from asyncua.ua.uatypes import Variant

                await var_node.set_attribute(
                    ua.AttributeIds.AccessLevel,
                    Variant(access_level, ua.VariantType.Byte),
                )
                logger.debug(f"AccessLevel set using set_attribute with Variant")
            except Exception as e1:
                logger.debug(f"Method 1 failed: {e1}")
                try:
                    # Method 2: direct set_writable/readable
                    if access_level == 0x03:  # Read/Write
                        var_node.set_writable()
                        logger.debug(f"AccessLevel set using set_writable()")
                    elif access_level == 0x02:  # Write only
                        var_node.set_writable()
                        # Note: No set_readable() exists, so read-only is the default
                except Exception as e2:
                    logger.debug(f"Method 2 failed: {e2}")
                    try:
                        # Method 3: traditional set_attribute without Variant
                        await var_node.set_attribute(
                            ua.AttributeIds.AccessLevel, access_level
                        )
                        logger.debug(
                            f"AccessLevel set using set_attribute without Variant"
                        )
                    except Exception as e3:
                        logger.debug(f"Method 3 failed: {e3}")
                        logger.warning(
                            f"Failed to set AccessLevel for '{tag_info['path']} after all attempts"
                        )
                        pass

            # Set access level
            access_level = get_access_level(tag_info["access"])
            logger.debug(
                f"Setting AccessLevel for '{tag_info['path']}': access_str='{tag_info['access']}' -> level=0x{access_level:02x}"
            )
            try:
                await var_node.set_attribute(ua.AttributeIds.AccessLevel, access_level)
            except Exception as e:
                logger.debug(f"Error setting AccessLevel: {e}")
                pass

            # Set access level
            access_level = get_access_level(tag_info["access"])
            try:
                from opcua import ua as ua_module

                await var_node.set_attribute(
                    ua_module.AttributeIds.AccessLevel, access_level
                )
            except Exception:
                pass

            # Set array type if needed
            if tag_info.get("is_array"):
                try:
                    from opcua import ua as ua_module

                    await var_node.set_attribute(ua_module.AttributeIds.ValueRank, 1)
                except Exception:
                    pass

            return var_node

        except Exception as e:
            logger.error(f"Error creating OPC UA node for '{tag_info['path']}': {e}")
            return None

    def _clear_all_nodes(self):
        """Clear all variable nodes from OPC UA server."""
        if not self.is_running or not self.server:
            return

        try:
            if self.loop and not self.loop.is_closed():
                future = asyncio.run_coroutine_threadsafe(
                    self._clear_all_nodes_async(), self.loop
                )
                future.result(timeout=10)
        except Exception as e:
            logger.warning(f"Error clearing OPC UA nodes: {e}")

    async def _clear_all_nodes_async(self):
        """Async method to clear all variable nodes in namespace 2."""
        try:
            objects = self.server.get_objects_node()

            try:
                children = await objects.get_children()
            except Exception:
                return

            deleted = 0
            for child in children:
                try:
                    node_id = child.nodeid
                    if (
                        hasattr(node_id, "NamespaceIndex")
                        and node_id.NamespaceIndex == 2
                    ):
                        try:
                            node_class = await child.read_node_class()
                            from opcua import ua as ua_module

                            if node_class == ua_module.NodeClass.Variable:
                                await objects.delete(child)
                                deleted += 1
                        except Exception:
                            pass
                except Exception:
                    continue

            logger.info(f"Cleared {deleted} OPC UA nodes")
            self._tag_nodes.clear()
            self._tag_info.clear()

        except Exception as e:
            logger.error(f"Error in _clear_all_nodes_async: {e}")

    def _get_tag_path(self, tag_item) -> str:
        """Get full tag path from tree item (e.g., "Channel1.Device1.Data.Tag1").

        Exactly like RuntimeMonitor._get_tag_tree_path
        """
        path_parts = []
        current = tag_item

        # Get tree root to know when to stop
        tree_root = getattr(self.tree_widget, "root_node", None)

        while current and current != tree_root:
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

        from core.config import GROUP_SEPARATOR

        return (
            GROUP_SEPARATOR.join(path_parts)
            if path_parts
            else tag_item.text(0) or "Unknown"
        )

    def sync_values(self):
        """Synchronize tag values from data_buffer to OPC UA.

        Called periodically (e.g., every 200ms) to push latest values.
        """
        if not self.is_running or not self.data_buffer or not self._tag_nodes:
            return

        try:
            for tag_path, (node, tag_info) in self._tag_nodes.items():
                try:
                    # Get value from buffer
                    value = self.data_buffer.get_tag_value(tag_path)

                    if value is not None:
                        # Update OPC UA node value
                        if self.loop and not self.loop.is_closed():
                            asyncio.run_coroutine_threadsafe(
                                self._update_node_value_async(node, value), self.loop
                            )
                except Exception as e:
                    logger.debug(f"Error syncing tag '{tag_path}': {e}")
        except Exception as e:
            logger.error(f"Error in sync_values: {e}")

    async def _update_node_value_async(self, node, value):
        """Async method to update node value."""
        try:
            await node.write_value(value)
        except Exception as e:
            logger.debug(f"Error writing value to node: {e}")

    def write_tag_from_opcua(self, tag_path: str, value: Any) -> bool:
        """Handle write from OPC UA client to tag.

        Called when OPC UA client writes to a node.
        Updates data_buffer and triggers Modbus write.

        Args:
            tag_path: Full tag path
            value: Value to write

        Returns:
            True on success, False on failure
        """
        try:
            # Update data_buffer
            if self.data_buffer:
                self.data_buffer.write_tag_value(tag_path, value)

            # TODO: Trigger Modbus write through RuntimeMonitor
            # This would require integrating with the write queue system

            logger.debug(f"OPC UA write: {tag_path} = {value}")
            return True
        except Exception as e:
            logger.error(f"Error handling OPC UA write: {e}")
            return False

    def __del__(self):
        """Cleanup on destruction."""
        try:
            self.stop_server()
        except Exception:
            pass


# For backwards compatibility
OPCServer = OPCUAServer
