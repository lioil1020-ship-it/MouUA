"""Core AppController for managing application configuration and data persistence.

This module provides the main AppController class which handles:
- Channel/Device/Tag configuration management
- Configuration persistence (import/export)
- Data validation and normalization
"""

import os
import json
from copy import deepcopy
from collections import OrderedDict
from typing import Any, Dict, Optional

from PyQt6.QtCore import Qt

from core.config import GROUP_SEPARATOR
from .validators import (
    to_numeric_flag, 
    normalize_dict_flags, 
    is_tcp_like_driver,
)
from .config_builder import (
    normalize_communication_params,
    normalize_opcua_network_adapter,
    build_device_timing_for_driver,
)
from .serializers import export_tags_to_csv


class AppController:
    """Main application controller for configuration management.
    
    Manages the application's tree structure and provides methods for:
    - Normalizing channel, device, and tag configurations
    - Importing/exporting project structures
    - Validating and persisting settings
    """

    def __init__(self, app=None):
        """Initialize controller with optional app reference.
        
        Args:
            app: The main application instance (optional)
        """
        self.app = app

    def normalize_channel(self, data: Dict[str, Any], item=None) -> Dict[str, Any]:
        """Normalize channel configuration data.
        
        Ensures communication params use canonical keys (network_adapter/network_adapter_ip)
        for TCP-like drivers.
        
        Args:
            data: Channel configuration dict
            item: Optional QTreeWidgetItem to inspect existing driver info
            
        Returns:
            Normalized copy of channel data
        """
        try:
            nd = deepcopy(data) if data else {}
        except Exception:
            nd = dict(data or {})
        
        try:
            params = nd.get('params') if isinstance(nd.get('params'), dict) else None
            if params is None:
                params = nd.get('communication') if isinstance(nd.get('communication'), dict) else None
            if params is None:
                params = {}
            
            # Detect driver type from data or existing item
            driver_type = nd.get('driver')
            if item is not None:
                try:
                    old_drv = item.data(2, Qt.ItemDataRole.UserRole)
                    if driver_type is None or driver_type == '':
                        driver_type = old_drv
                except Exception:
                    pass
            
            # Extract driver type string
            if isinstance(driver_type, dict):
                driver_type = driver_type.get('type')
            
            # Normalize params for TCP-like drivers
            params = normalize_communication_params(params, driver_type)
            nd['params'] = params
            nd['communication'] = params
            
        except Exception:
            pass
        
        return nd

    def save_channel(self, item, data: Dict[str, Any]):
        """Save channel configuration to tree item.
        
        Stores normalized channel data in appropriate QTreeWidgetItem roles.
        
        Args:
            item: QTreeWidgetItem to save into
            data: Channel configuration dict
        """
        try:
            data = self.normalize_channel(data, item)
        except Exception:
            pass
        
        # Extract general info
        general = data.get('general', {}) if isinstance(data.get('general'), dict) else {}
        name = general.get('channel_name') or general.get('name') or data.get('name') or 'Channel'
        desc = general.get('description') or data.get('description')
        
        try:
            item.setText(0, name)
        except Exception:
            pass
        
        # Role 1: description
        try:
            if desc is not None:
                item.setData(1, Qt.ItemDataRole.UserRole, desc)
        except Exception:
            pass
        
        # Role 2: driver
        try:
            driver = data.get('driver')
            item.setData(2, Qt.ItemDataRole.UserRole, driver)
            if isinstance(driver, dict):
                item.setData(9, Qt.ItemDataRole.UserRole, 
                           OrderedDict([('type', driver.get('type')), ('params', driver.get('params', {}))]))
        except Exception:
            pass
        
        # Role 3: communication params
        try:
            params = data.get('params') or data.get('communication')
            if params:
                item.setData(3, Qt.ItemDataRole.UserRole, params)
        except Exception:
            pass

    def save_device(self, item, data: Dict[str, Any]):
        """Save device configuration to tree item.
        
        Stores normalized device data including timing, encoding, and block sizes.
        
        Args:
            item: QTreeWidgetItem to save into
            data: Device configuration dict
        """
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            general = data.get('general', {}) if isinstance(data.get('general'), dict) else {}
        except Exception:
            general = {}
        
        # Name
        name = general.get('name') or data.get('name')
        if name:
            try:
                item.setText(0, name)
            except Exception as e:
                logger.error(f"Error setting device name: {e}")
        
        # Device ID (role 2)
        device_id = general.get('device_id') or data.get('device_id')
        try:
            if device_id is not None:
                item.setData(2, Qt.ItemDataRole.UserRole, device_id)
        except Exception as e:
            logger.error(f"Error setting device_id: {e}")
        
        # Description (role 1)
        desc = general.get('description') or data.get('description')
        try:
            if desc is not None:
                item.setData(1, Qt.ItemDataRole.UserRole, desc)
        except Exception as e:
            logger.error(f"Error setting description: {e}")
        
        # Timing (role 3) - normalize keys
        timing = general.get('timing') or data.get('timing')
        if timing and isinstance(timing, dict):
            try:
                # Normalize timing keys to canonical names but preserve any
                # additional timing fields (e.g. 'attempts', 'inter_req_delay').
                nt = dict(timing)  # start with a shallow copy to preserve extras
                # mapping of known legacy keys -> canonical keys
                mapping = {
                    'connect_timeout': 'connect_timeout',
                    'connect_timeout_ms': 'connect_timeout',
                    'conn_timeout': 'connect_timeout',
                    'request_timeout': 'request_timeout',
                    'req_timeout': 'request_timeout',
                    'request_timeout_ms': 'request_timeout',
                }
                # Additional mappings for runtime names expected by ModbusMonitor
                extra_map = {
                    'attempts': 'attempts_before_timeout',
                    'attempts_before_timeout': 'attempts_before_timeout',
                    'inter_req_delay': 'inter_request_delay',
                    'inter_request_delay': 'inter_request_delay',
                }
                # Apply mapping: copy value to canonical name, remove legacy key when renamed
                for old_key, new_key in mapping.items():
                    if old_key in timing:
                        nt[new_key] = timing[old_key]
                        # keep the original key as well so UI form fields
                        # that expect legacy keys (e.g. 'req_timeout') can still
                        # load values via FormBuilder.set_values().
                for old_key, new_key in extra_map.items():
                    if old_key in timing:
                        nt[new_key] = timing[old_key]

                item.setData(3, Qt.ItemDataRole.UserRole, nt)
            except Exception as e:
                logger.error(f"Error setting timing: {e}")
        
        # Data access (role 4)
        access = general.get('data_access') or data.get('data_access')
        try:
            if access:
                logger.debug(f"Saving data_access: {access}")
                item.setData(4, Qt.ItemDataRole.UserRole, normalize_dict_flags(access))
            else:
                logger.debug("No data_access provided")
        except Exception as e:
            logger.error(f"Error setting data_access: {e}")
        
        # Encoding (role 5)
        encoding = general.get('encoding') or data.get('encoding')
        try:
            if encoding:
                logger.debug(f"Saving encoding raw: {encoding}")
                normalized = normalize_dict_flags(encoding)
                logger.debug(f"Saving encoding normalized: {normalized}")
                item.setData(5, Qt.ItemDataRole.UserRole, normalized)
            else:
                logger.debug(f"No encoding provided. general.encoding={general.get('encoding')}, data.encoding={data.get('encoding')}")
        except Exception as e:
            logger.error(f"Error setting encoding: {e}", exc_info=True)
        
        # Block sizes (role 6)
        blocks = general.get('block_sizes') or data.get('block_sizes')
        try:
            if blocks:
                item.setData(6, Qt.ItemDataRole.UserRole, blocks)
        except Exception as e:
            logger.error(f"Error setting block_sizes: {e}")

    def save_tag(self, item, data: Dict[str, Any]):
        """Save tag configuration to tree item.
        
        Stores tag properties in appropriate roles: name, description, data type, 
        address, scan rate, and scaling info.
        
        Args:
            item: QTreeWidgetItem to save into
            data: Tag configuration dict
        """
        try:
            general = data.get('general', {})
            if not isinstance(general, dict):
                general = data
            
            name = general.get('name') or data.get('name') or 'Tag'
            item.setText(0, name)
            
            # Role 1: description
            item.setData(1, Qt.ItemDataRole.UserRole, general.get('description'))
            
            # Role 2: data type
            item.setData(2, Qt.ItemDataRole.UserRole, general.get('data_type'))
            
            # Role 3: access
            access = general.get('access') or data.get('access')
            if access:
                item.setData(3, Qt.ItemDataRole.UserRole, access)
            
            # Role 4: address
            addr = general.get('address') or data.get('address')
            if addr:
                # Store address as-is; it should already be formatted correctly
                # from calculate_next_address (e.g., "400000", "000000", etc.)
                item.setData(4, Qt.ItemDataRole.UserRole, str(addr).strip())
            
            # Role 5: scan rate
            scan = general.get('scan_rate') or data.get('scan_rate')
            if scan:
                item.setData(5, Qt.ItemDataRole.UserRole, scan)
            
            # Role 6: scaling
            scaling = data.get('scaling')
            if isinstance(scaling, dict):
                item.setData(6, Qt.ItemDataRole.UserRole, scaling)
            
            # Role 7: metadata (addrnum, is_array, array_size)
            try:
                import re
                addr_val = item.data(4, Qt.ItemDataRole.UserRole)
                dt_val = item.data(2, Qt.ItemDataRole.UserRole)
                nm = item.text(0) or ''
                
                addrnum = None
                if addr_val:
                    m = re.search(r'(\d+)', str(addr_val))
                    if m:
                        addrnum = int(m.group(1))
                
                is_array = False
                array_size = 1  # 預設佔用 1 個地址
                if isinstance(dt_val, str) and 'array' in dt_val.lower():
                    is_array = True
                elif isinstance(addr_val, str) and re.search(r'\[\d+\]', addr_val):
                    is_array = True
                elif 'array' in nm.lower():
                    is_array = True
                
                # 如果是 Array 型別，提取陣列大小
                if is_array and isinstance(addr_val, str):
                    match = re.search(r'\[(\d+)\]', str(addr_val))
                    if match:
                        array_size = int(match.group(1))
                
                item.setData(7, Qt.ItemDataRole.UserRole, {'addrnum': addrnum, 'is_array': is_array, 'array_size': array_size})
            except Exception:
                pass
        except Exception:
            pass

    def normalize_all_channels(self) -> int:
        """Re-normalize all channels in the current tree.
        
        Walks the tree and applies normalization to every channel,
        updating stored role data to use canonical communication parameter keys.
        
        Returns:
            Number of channels processed
        """
        if not self.app:
            return 0
        
        tree = getattr(self.app, 'tree', None)
        if not tree:
            return 0
        
        conn = getattr(tree, 'conn_node', None)
        if not conn:
            return 0
        
        count = 0
        for i in range(conn.childCount()):
            try:
                ch = conn.child(i)
                # Reconstruct data dict from roles
                data = {}
                try:
                    name = ch.text(0)
                    desc = ch.data(1, Qt.ItemDataRole.UserRole)
                    data['general'] = {'channel_name': name, 'description': desc}
                except Exception:
                    pass
                
                try:
                    drv = ch.data(2, Qt.ItemDataRole.UserRole)
                    data['driver'] = drv
                except Exception:
                    pass
                
                try:
                    comm = ch.data(3, Qt.ItemDataRole.UserRole)
                    if isinstance(comm, dict):
                        data['communication'] = dict(comm)
                except Exception:
                    pass
                
                self.save_channel(ch, data)
                count += 1
            except Exception:
                continue
        
        # Also normalize OPC UA settings if present
        try:
            opc = getattr(self.app, 'opcua_settings', None)
            if isinstance(opc, dict):
                opc = normalize_opcua_network_adapter(opc)
                self.app.opcua_settings = opc
                if hasattr(self.app, 'apply_opcua_settings'):
                    try:
                        self.app.apply_opcua_settings(opc)
                    except Exception:
                        pass
        except Exception:
            pass
        
        return count

    def normalize_opcua_settings(self, opc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize OPC UA settings for network adapter configuration.
        
        Args:
            opc: OPC UA settings dict
            
        Returns:
            Normalized OPC UA settings
        """
        return normalize_opcua_network_adapter(opc)

    def export_device_to_csv(self, device_item, filepath: str) -> None:
        """Export tags from device to CSV file.
        
        Args:
            device_item: QTreeWidgetItem for the device
            filepath: Path where CSV will be written
        """
        export_tags_to_csv(device_item, filepath)

    def import_device_from_csv(self, device_item, filepath: str) -> None:
        """Import tags from CSV file into device.
        
        Args:
            device_item: QTreeWidgetItem for the device
            filepath: Path to CSV file to import
        """
        import csv
        from PyQt6.QtWidgets import QTreeWidgetItem
        
        if not filepath or not os.path.exists(filepath):
            return
        
        try:
            with open(filepath, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                if reader.fieldnames is None:
                    return
                
                for row in reader:
                    # Skip empty rows
                    full_tag_name = row.get('Tag Name', '').strip()
                    if not full_tag_name:
                        continue
                    
                    # Parse tag name: if it contains GROUP_SEPARATOR, split into group path and tag name
                    # e.g., "Set.WIRE" -> groups=["Set"], tag_name="WIRE"
                    # e.g., "Set.Group2.WIRE" -> groups=["Set", "Group2"], tag_name="WIRE"
                    parts = full_tag_name.split(GROUP_SEPARATOR)
                    if len(parts) > 1:
                        groups = parts[:-1]
                        tag_name = parts[-1]
                    else:
                        groups = []
                        tag_name = full_tag_name
                    
                    # Navigate/create group structure
                    current_parent = device_item
                    for group_name in groups:
                        # Find existing group
                        group_item = None
                        for i in range(current_parent.childCount()):
                            child = current_parent.child(i)
                            if (child.text(0) == group_name and 
                                child.data(0, Qt.ItemDataRole.UserRole) == 'Group'):
                                group_item = child
                                break
                        
                        # Create group if not found
                        if group_item is None:
                            group_item = QTreeWidgetItem(current_parent)
                            group_item.setText(0, group_name)
                            group_item.setData(0, Qt.ItemDataRole.UserRole, 'Group')
                            group_item.setData(1, Qt.ItemDataRole.UserRole, '')  # description
                        
                        current_parent = group_item
                    
                    # Get tag data from CSV
                    address = row.get('Address', '').strip()
                    data_type = row.get('Data Type', '').strip()
                    access = row.get('Client Access', 'R/W').strip()
                    scan_rate = row.get('Scan Rate', '').strip()
                    description = row.get('Description', '').strip()
                    
                    # Normalize address format: 103 -> 000103, 400095 [25] -> 400095 [25], etc.
                    if address:
                        if '[' in address:
                            # Handle array format like "103 [5]"
                            parts = address.split(' [')
                            addr_part = parts[0].strip()
                            # Pad with zeros to 6 digits
                            addr_part = addr_part.zfill(6)
                            address = f"{addr_part} [{parts[1]}"
                        else:
                            # Pad with zeros to 6 digits
                            address = address.zfill(6)
                    
                    # Convert format back: R/W -> Read/Write, RO -> Read Only
                    if access == 'R/W':
                        access = 'Read/Write'
                    elif access == 'RO':
                        access = 'Read Only'
                    
                    # Convert data type back: Word Array -> Word(Array), etc.
                    data_type = data_type.replace(' Array', '(Array)')
                    
                    # Build tag data
                    tag_data = {
                        'name': tag_name,
                        'description': description,
                        'data_type': data_type,
                        'access': access,
                        'address': address,
                        'scan_rate': scan_rate,
                        'general': {
                            'name': tag_name,
                            'description': description,
                            'data_type': data_type,
                            'access': access,
                            'address': address,
                            'scan_rate': scan_rate,
                        },
                        'scaling': {}
                    }
                    
                    # Get scaling data if present
                    scaling_type = row.get('Scaling', '').strip() if row.get('Scaling') else ''
                    
                    # If Scaling field is empty or 'None', treat as no scaling
                    if not scaling_type or scaling_type == 'None':
                        tag_data['scaling'] = {
                            'type': 'None',
                            'raw_low': '0',
                            'raw_high': '1000',
                            'scaled_type': 'Float',
                            'scaled_low': '0.0',
                            'scaled_high': '100.0',
                            'clamp_low': 'No',
                            'clamp_high': 'No',
                            'negate': 'No',
                            'units': '',
                        }
                    else:
                        # Extract scaling values from CSV rows
                        tag_data['scaling'] = {
                            'type': scaling_type,
                            'raw_low': row.get('Raw Low', '0').strip() if row.get('Raw Low') else '0',
                            'raw_high': row.get('Raw High', '1000').strip() if row.get('Raw High') else '1000',
                            'scaled_type': row.get('Scaled Data Type', 'Float').strip() if row.get('Scaled Data Type') else 'Float',
                            'scaled_low': row.get('Scaled Low', '0.0').strip() if row.get('Scaled Low') else '0.0',
                            'scaled_high': row.get('Scaled High', '100.0').strip() if row.get('Scaled High') else '100.0',
                            'clamp_low': row.get('Clamp Low', 'No').strip() if row.get('Clamp Low') else 'No',
                            'clamp_high': row.get('Clamp High', 'No').strip() if row.get('Clamp High') else 'No',
                            'negate': row.get('Negate Value', 'No').strip() if row.get('Negate Value') else 'No',
                            'units': row.get('Eng Units', '').strip() if row.get('Eng Units') else '',
                        }
                    
                    # Find or create tag item under current parent (group or device)
                    tag_item = None
                    for i in range(current_parent.childCount()):
                        child = current_parent.child(i)
                        if child.text(0) == tag_name:
                            tag_item = child
                            break
                    
                    if tag_item is None:
                        # Create new tag
                        tag_item = QTreeWidgetItem(current_parent)
                        tag_item.setText(0, tag_name)
                        tag_item.setData(0, Qt.ItemDataRole.UserRole, 'Tag')
                        tag_item.setHidden(True)
                    
                    # Save tag data
                    self.save_tag(tag_item, tag_data)
                    
                    print(f"Imported tag: {full_tag_name} -> Address: {address}, Data Type: {data_type}")
        except Exception as e:
            import traceback
            print(f"Import error: {e}")
            traceback.print_exc()

    def import_project_from_json(self, filepath):
        # Load a project JSON previously created by `export_project_to_json`.
        if not filepath or not os.path.exists(filepath):
            return
        from PyQt6.QtWidgets import QTreeWidgetItem
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                doc = json.load(f)
        except Exception:
            return

        root = getattr(self.app, 'tree', None)
        if root is None:
            return
        conn = getattr(root, 'conn_node', None)
        if conn is None:
            return

        # clear existing children
        while conn.childCount() > 0:
            try:
                conn.removeChild(conn.child(0))
            except Exception:
                break

        def _normalize_driver(drv):
            try:
                if isinstance(drv, dict):
                    dtype = drv.get('type')
                    dparams = drv.get('params') if isinstance(drv.get('params'), dict) else (drv.get('params') or {})
                    if isinstance(dtype, dict):
                        inner = dtype
                        if isinstance(inner.get('type'), str):
                            return {'type': inner.get('type'), 'params': inner.get('params') or dparams or {}}
                    return {'type': dtype, 'params': dparams if isinstance(dparams, dict) else {}}
                else:
                    return {'type': drv, 'params': {}}
            except Exception:
                return {'type': drv, 'params': {}}

        def build(parent, node):
            t = node.get('type')
            txt = node.get('text') or (node.get('general') or {}).get('name') or ''
            item = QTreeWidgetItem(parent)
            try:
                item.setText(0, txt)
            except Exception:
                pass
            try:
                item.setData(0, Qt.ItemDataRole.UserRole, t)
            except Exception:
                pass

            if t == 'Tag':
                try:
                    item.setHidden(True)
                except Exception:
                    pass

            if t == 'Channel':
                try:
                    general = node.get('general') if isinstance(node.get('general'), dict) else {}
                    desc = general.get('description') if general.get('description') is not None else node.get('description') or ''
                    try:
                        item.setData(1, Qt.ItemDataRole.UserRole, desc)
                    except Exception:
                        pass

                    drv_raw = node.get('driver') if 'driver' in node else node.get('params') or {}
                    drv = _normalize_driver(drv_raw)
                    try:
                        item.setData(2, Qt.ItemDataRole.UserRole, drv if drv is not None else drv_raw)
                    except Exception:
                        try:
                            item.setData(2, Qt.ItemDataRole.UserRole, drv_raw)
                        except Exception:
                            pass
                    try:
                        item.setData(9, Qt.ItemDataRole.UserRole, OrderedDict([('type', drv.get('type')), ('params', drv.get('params') or {})]))
                    except Exception:
                        pass

                    comm = node.get('communication') if isinstance(node.get('communication'), dict) else (node.get('params') if isinstance(node.get('params'), dict) else {})
                    if not comm:
                        try:
                            dp = drv.get('params') if isinstance(drv.get('params'), dict) else {}
                            comm_keys = set(['com', 'baud', 'data_bits', 'parity', 'stop', 'flow', 'ip', 'port'])
                            comm = {k: v for k, v in dp.items() if k in comm_keys}
                        except Exception:
                            comm = {}

                    # Normalize import: for TCP-like channels, convert ip/port to network_adapter if no adapter specified
                    try:
                        drv_type = drv.get('type') if isinstance(drv, dict) else ''
                        drv_low = str(drv_type or '').lower()
                        tcp_like = any(x in drv_low for x in ("over tcp", "ethernet", "tcp"))
                    except Exception:
                        tcp_like = False

                    if tcp_like:
                        try:
                            comm_src = node.get('communication') if isinstance(node.get('communication'), dict) else None
                            has_ip_port = isinstance(comm_src, dict) and (('ip' in comm_src) or ('port' in comm_src))
                            has_adapter_key = isinstance(comm_src, dict) and any(k in comm_src for k in ('network_adapter', 'adapter', 'adapter_name'))
                            if has_ip_port and not has_adapter_key:
                                comm = {'network_adapter': 'Default'}
                            else:
                                # if driver params contain adapter info, prefer that
                                try:
                                    dp = drv.get('params') if isinstance(drv.get('params'), dict) else {}
                                    a_raw = dp.get('adapter') or dp.get('adapter_name') or dp.get('adapter_ip')
                                    if a_raw:
                                        if isinstance(a_raw, str) and ' - ' in a_raw:
                                            _, name_part = a_raw.split(' - ', 1)
                                            comm = {'network_adapter': name_part.strip()}
                                        else:
                                            comm = {'network_adapter': a_raw}
                                except Exception:
                                    pass
                        except Exception:
                            pass
                    try:
                        item.setData(3, Qt.ItemDataRole.UserRole, comm or {})
                    except Exception:
                        pass
                except Exception:
                    pass

            if t == 'Device':
                try:
                    general = node.get('general') if isinstance(node.get('general'), dict) else {}
                    name = general.get('name') or node.get('name') or item.text(0)
                    desc = general.get('description') if general.get('description') is not None else node.get('description')
                    device_id = general.get('device_id') if general.get('device_id') is not None else node.get('device_id')
                    try:
                        if name is not None:
                            item.setText(0, name)
                    except Exception:
                        pass
                    try:
                        if desc is not None:
                            item.setData(1, Qt.ItemDataRole.UserRole, desc)
                    except Exception:
                        pass
                    try:
                        if device_id is not None:
                            item.setData(2, Qt.ItemDataRole.UserRole, device_id)
                    except Exception:
                        pass
                    try:
                        if node.get('timing') is not None:
                            item.setData(3, Qt.ItemDataRole.UserRole, node.get('timing'))
                    except Exception:
                        pass
                    try:
                        if node.get('data_access') is not None:
                            item.setData(4, Qt.ItemDataRole.UserRole, node.get('data_access'))
                    except Exception:
                        pass
                    try:
                        if node.get('encoding') is not None:
                            enc = node.get('encoding')
                            # Backward compatibility: map old field names to new ones
                            if isinstance(enc, dict):
                                if 'word_low' in enc and 'word_order' not in enc:
                                    enc['word_order'] = enc.pop('word_low')
                                if 'dword_low' in enc and 'dword_order' not in enc:
                                    enc['dword_order'] = enc.pop('dword_low')
                                if 'treat_long' in enc and 'treat_longs_as_decimals' not in enc:
                                    enc['treat_longs_as_decimals'] = enc.pop('treat_long')
                            item.setData(5, Qt.ItemDataRole.UserRole, enc)
                    except Exception:
                        pass
                    try:
                        if node.get('block_sizes') is not None:
                            item.setData(6, Qt.ItemDataRole.UserRole, node.get('block_sizes'))
                    except Exception:
                        pass
                except Exception:
                    pass

            if t == 'Tag':
                try:
                    general = node.get('general') if isinstance(node.get('general'), dict) else {}
                    name = general.get('name') or node.get('name') or item.text(0)
                    desc = general.get('description') if general.get('description') is not None else node.get('description')
                    dtype = general.get('data_type') if general.get('data_type') is not None else node.get('data_type')
                    access = general.get('access') if general.get('access') is not None else node.get('access')
                    addr = general.get('address') if general.get('address') is not None else node.get('address')
                    scan = general.get('scan_rate') if general.get('scan_rate') is not None else node.get('scan_rate')
                    try:
                        if name is not None:
                            item.setText(0, name)
                    except Exception:
                        pass
                    try:
                        if desc is not None:
                            item.setData(1, Qt.ItemDataRole.UserRole, desc)
                    except Exception:
                        pass
                    try:
                        if dtype is not None:
                            item.setData(2, Qt.ItemDataRole.UserRole, dtype)
                    except Exception:
                        pass
                    try:
                        if access is not None:
                            item.setData(3, Qt.ItemDataRole.UserRole, access)
                    except Exception:
                        pass
                    try:
                        if addr is not None:
                            item.setData(4, Qt.ItemDataRole.UserRole, addr)
                    except Exception:
                        pass
                    try:
                        if scan is not None:
                            item.setData(5, Qt.ItemDataRole.UserRole, scan)
                    except Exception:
                        pass
                    try:
                        scaling = node.get('scaling') if node.get('scaling') is not None else None
                        if scaling is not None:
                            item.setData(6, Qt.ItemDataRole.UserRole, scaling)
                    except Exception:
                        pass
                    try:
                        import re
                        addr_val = item.data(4, Qt.ItemDataRole.UserRole)
                        dt_val = item.data(2, Qt.ItemDataRole.UserRole)
                        nm = (item.text(0) or '')
                        addrnum = None
                        if addr_val is not None:
                            m = re.search(r"(\d+)", str(addr_val))
                            if m:
                                addrnum = int(m.group(1))
                        is_array = False
                        try:
                            if isinstance(dt_val, str) and 'array' in dt_val.lower():
                                is_array = True
                            if isinstance(addr_val, str) and re.search(r"\[\d+\]", addr_val):
                                is_array = True
                            if 'array' in nm.lower():
                                is_array = True
                        except Exception:
                            is_array = False
                        item.setData(7, Qt.ItemDataRole.UserRole, {'addrnum': addrnum, 'is_array': is_array})
                    except Exception:
                        pass
                except Exception:
                    pass

            if t == 'Group':
                try:
                    desc = node.get('description') if node.get('description') is not None else ''
                    if desc is not None:
                        item.setData(1, Qt.ItemDataRole.UserRole, desc)
                except Exception:
                    pass

            for c in node.get('children', []) or []:
                try:
                    build(item, c)
                except Exception:
                    pass

        for ch in doc.get('channels', []) or []:
            try:
                build(conn, ch)
            except Exception:
                pass

        try:
            opc = doc.get('opcua_settings')
            if isinstance(opc, dict) and hasattr(self, 'normalize_opcua_settings'):
                try:
                    opc = self.normalize_opcua_settings(opc)
                except Exception:
                    pass
            if opc is not None and hasattr(self, 'app') and self.app is not None:
                try:
                    # apply opcua_settings from import (no terminal output)
                    self.app.opcua_settings = opc
                    if hasattr(self.app, 'apply_opcua_settings'):
                        try:
                            self.app.apply_opcua_settings(opc)
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass

    def save_group(self, item, data):
        """Save group data to a tree item.
        
        Args:
            item: QTreeWidgetItem representing the group
            data: Dictionary with group information (name, description, etc.)
        """
        try:
            # Extract group name and description from data
            name = (data.get("general") or {}).get("name") or data.get("name") or item.text(0)
            desc = (data.get("general") or {}).get("description") or data.get("description") or ""
            
            # Set the item data
            if name:
                item.setText(0, name)
            if desc is not None:
                item.setData(1, Qt.ItemDataRole.UserRole, desc)
        except Exception:
            pass

    def calculate_next_id(self, channel_item):
        """Return next available device id for devices under `channel_item`.
        Scans child items of the channel for role 2 (device id) and returns max+1,
        clamped to [1, 65535]. If none found returns 1.
        """
        try:
            if channel_item is None:
                return 1
            max_id = 0
            for i in range(channel_item.childCount()):
                try:
                    c = channel_item.child(i)
                    if c is None:
                        continue
                    try:
                        t = c.data(0, Qt.ItemDataRole.UserRole)
                    except Exception:
                        t = None
                    if t != 'Device':
                        continue
                    # device id stored in role 2
                    try:
                        did = c.data(2, Qt.ItemDataRole.UserRole)
                    except Exception:
                        did = None
                    if did is None:
                        # try nested general
                        try:
                            gen = c.data(0, Qt.ItemDataRole.UserRole)
                        except Exception:
                            gen = None
                        did = None
                    # coerce to int if possible
                    try:
                        if isinstance(did, str) and did.strip():
                            val = int(float(did.strip()))
                        elif isinstance(did, (int, float)):
                            val = int(did)
                        else:
                            val = None
                    except Exception:
                        val = None
                    if val is not None and val > max_id:
                        max_id = val
                except Exception:
                    continue
            next_id = max(1, max_id + 1)
            if next_id > 65535:
                next_id = 65535
            return next_id
        except Exception:
            return 1

    def calculate_next_address(self, parent_node, prefix=None, new_type=None):
        """Calculate the next available address for a tag under parent_node.
        
        Args:
            parent_node: QTreeWidgetItem representing the parent (Device or Group)
            prefix: Optional address prefix (e.g., "0", "1", "3", "4")
            new_type: Optional data type to determine address step size
            
        Returns:
            str: Suggested next address
        """
        try:
            if parent_node is None:
                return "0"
            
            # Get size for this data type
            size_map = {
                "Boolean": 1,
                "Char": 1,
                "Short": 1,
                "Word": 1,
                "Int": 1,           # 16-bit signed integer (1 register)
                "DInt": 2,
                "Long": 2,
                "DWord": 2,
                "Real": 2,
                "Float": 2,
                "Double": 4,
                "String": 6,
            }
            
            # Determine step size based on type
            # Sort by key length (descending) to match more specific types first
            # e.g., "DWord" before "Word"
            step = 1
            if new_type:
                type_name = str(new_type).strip()
                sorted_keys = sorted(size_map.keys(), key=len, reverse=True)
                for key in sorted_keys:
                    if key.lower() in type_name.lower():
                        step = size_map[key]
                        break
            
            # Scan all child items for used addresses with the same prefix
            # Track the maximum ending address (not just starting address)
            max_end_addr = -1
            
            for i in range(parent_node.childCount()):
                try:
                    child = parent_node.child(i)
                    if child is None:
                        continue
                    
                    # Skip non-Tag items
                    try:
                        child_type = child.data(0, __import__('PyQt6.QtCore', fromlist=['Qt']).Qt.ItemDataRole.UserRole)
                        if child_type != 'Tag':
                            continue
                    except Exception:
                        continue
                    
                    # Get address and data_type from the child
                    try:
                        addr = child.data(4, __import__('PyQt6.QtCore', fromlist=['Qt']).Qt.ItemDataRole.UserRole)
                        if addr is None:
                            continue
                        
                        child_dtype = child.data(2, __import__('PyQt6.QtCore', fromlist=['Qt']).Qt.ItemDataRole.UserRole)
                        
                        # Extract numeric part from address
                        import re
                        addr_str = str(addr)
                        
                        # Remove array notation [n] if present (for Array types)
                        addr_str = re.sub(r'\s*\[\d+\]\s*$', '', addr_str)
                        
                        # When prefix is specified, only count addresses with that prefix
                        if prefix is not None:
                            # Check if address starts with this prefix
                            if not addr_str.startswith(prefix):
                                # Skip addresses with different prefixes
                                continue
                            # Remove prefix to get numeric part
                            addr_str = addr_str[len(prefix):]
                        
                        # Extract digits (starting address)
                        match = re.search(r'(\d+)', addr_str)
                        if match:
                            start_num = int(match.group(1))
                            
                            # Determine size of this tag (in registers)
                            # Sort by key length (descending) to match more specific types first
                            register_size = 1  # default registers per element
                            if child_dtype:
                                dtype_name = str(child_dtype).strip()
                                sorted_keys = sorted(size_map.keys(), key=len, reverse=True)
                                for key in sorted_keys:
                                    if key.lower() in dtype_name.lower():
                                        register_size = size_map[key]
                                        break
                            
                            # If this tag is an Array, calculate total size
                            # Array occupies: array_size × register_size addresses
                            metadata = child.data(7, __import__('PyQt6.QtCore', fromlist=['Qt']).Qt.ItemDataRole.UserRole)
                            if isinstance(metadata, dict) and metadata.get('is_array'):
                                array_size = metadata.get('array_size', 1)
                                child_size = array_size * register_size
                            else:
                                child_size = register_size
                            
                            # Calculate ending address
                            end_num = start_num + child_size - 1
                            
                            # Track maximum ending address
                            if end_num > max_end_addr:
                                max_end_addr = end_num
                    except Exception:
                        continue
                except Exception:
                    continue
            
            # Calculate next address: 
            # If no tags exist with this prefix, start from 0
            # Otherwise, start from the maximum ending address + 1
            if max_end_addr == -1:
                next_addr_num = 0
            else:
                next_addr_num = max_end_addr + 1
            
            # Build result address with proper formatting (6 digits total: prefix + 5-digit number)
            if prefix is not None:
                return f"{prefix}{next_addr_num:05d}"
            else:
                return f"{next_addr_num:06d}"
            
        except Exception:
            return "0"

    def export_project_to_json(self, filepath):
        # Export tree under the project's connection node to a JSON file.
        root = getattr(self.app, 'tree', None)
        if root is None:
            return
        conn = getattr(root, 'conn_node', None)
        if conn is None:
            return

        def serialize(item):
            t = item.data(0, Qt.ItemDataRole.UserRole)
            # Build node with deterministic key order matching UI tab/field ordering
            from collections import OrderedDict as _OD
            node = _OD()
            node["type"] = t
            node["text"] = item.text(0)
            if t == "Channel":
                # Follow project configuration tree order and avoid duplicate flat keys.
                # new mapping: description->role1, driver->role2, communication->role3
                params = item.data(3, Qt.ItemDataRole.UserRole) or {}
                driver_val = item.data(2, Qt.ItemDataRole.UserRole)
                desc = item.data(1, Qt.ItemDataRole.UserRole) or ""

                # general (identity + description) - ordered: Channel Name, Description
                node["general"] = _OD()
                node["general"]["channel_name"] = item.text(0) or ""
                node["general"]["description"] = desc

                # communication/params — canonical order matching UI: COM ID, Baud, Data Bits, Parity, Stop Bits, Flow Control, (IP, Port)
                comm_keys = ["com", "baud", "data_bits", "parity", "stop", "flow", "ip", "port"]
                communication = _OD()
                for k in comm_keys:
                    if isinstance(params, dict) and k in params:
                        communication[k] = params[k]

                # driver-specific params: normalize nested forms so that
                # node['driver']['type'] is a string and node['driver']['params'] is a dict
                driver_params = _OD()
                drv_type_val = ""
                try:
                    if isinstance(driver_val, dict):
                        # driver_val may be {'type': <str|dict>, 'params': {...}}
                        raw_type = driver_val.get('type')
                        outer_params = driver_val.get('params') if isinstance(driver_val.get('params'), dict) else {}
                        if isinstance(raw_type, dict):
                            # nested form: {'type': {'type': 'Modbus RTU Serial', 'params': {...}}, 'params': {...}}
                            inner = raw_type
                            drv_type_val = inner.get('type') or ''
                            inner_params = inner.get('params') if isinstance(inner.get('params'), dict) else {}
                            # merge inner and outer params (outer overrides)
                            for k, v in (inner_params or {}).items():
                                driver_params[k] = v
                            for k, v in (outer_params or {}).items():
                                driver_params[k] = v
                        else:
                            drv_type_val = raw_type or ''
                            for k, v in (outer_params or {}).items():
                                driver_params[k] = v
                    else:
                        drv_type_val = str(driver_val or '')
                except Exception:
                    try:
                        drv_type_val = str(driver_val or '')
                    except Exception:
                        drv_type_val = ''

                node["driver"] = _OD([("type", drv_type_val), ("params", driver_params)])

                # communication: prefer explicit communication (role3).
                # For TCP-like drivers prefer adapter/network_adapter keys; otherwise keep ip/port.
                comm = communication or {}
                try:
                    drv_low = str(drv_type_val or '').lower()
                    tcp_like = any(x in drv_low for x in ("over tcp", "ethernet", "tcp"))
                except Exception:
                    tcp_like = False

                if not comm:
                    try:
                        if tcp_like:
                            # prefer adapter fields if present
                            if isinstance(driver_params, dict) and (driver_params.get('adapter') or driver_params.get('adapter_name') or driver_params.get('adapter_ip')):
                                a_raw = driver_params.get('adapter') or driver_params.get('adapter_name') or driver_params.get('adapter_ip')
                                # If adapter string looks like "IP - Name", split into ip and name
                                try:
                                    if isinstance(a_raw, str) and ' - ' in a_raw:
                                        ip_part, name_part = a_raw.split(' - ', 1)
                                        comm = _OD([('network_adapter', name_part.strip()), ('network_adapter_ip', ip_part.strip())])
                                    else:
                                        comm = _OD([('network_adapter', a_raw)])
                                except Exception:
                                    comm = _OD([('adapter', a_raw)])
                                # keep backwards-compatible keys too
                                try:
                                    comm['adapter'] = a_raw
                                except Exception:
                                    pass
                                # also expose adapter ip if available (explicit param)
                                if driver_params.get('adapter_ip'):
                                    comm['adapter_ip'] = driver_params.get('adapter_ip')
                                    # mirror to network_adapter_ip if not already set
                                    if 'network_adapter_ip' not in comm:
                                        comm['network_adapter_ip'] = driver_params.get('adapter_ip')
                            elif isinstance(driver_params, dict) and driver_params.get('ip') and driver_params.get('port'):
                                # If no adapter info present, prefer explicit network adapter selection
                                # rather than exporting raw ip/port; set to Default by convention
                                comm = _OD([('network_adapter', 'Default')])
                            else:
                                comm = _OD()
                        else:
                            # serial-like: keep com/baud etc from params
                            if isinstance(driver_params, dict):
                                for k in ("com", "baud", "data_bits", "parity", "stop", "flow"):
                                    if k in driver_params:
                                        comm[k] = driver_params.get(k)
                    except Exception:
                        comm = communication or _OD()

                node["communication"] = comm
                # Ensure exported communication is simplified for TCP-like channels:
                # only include a single `network_adapter` entry formatted as
                # 'Interface Name (IP)' when possible. Remove other adapter/ip keys.
                try:
                    if isinstance(params, dict) and tcp_like:
                        na = params.get('network_adapter') or params.get('adapter') or params.get('adapter_name')
                        nip = params.get('network_adapter_ip') or params.get('adapter_ip') or params.get('ip')
                        if na:
                            # If na already contains an ip in parentheses, keep it as-is.
                            if isinstance(na, str) and '(' in na and na.endswith(')'):
                                node['communication'] = _OD([('network_adapter', na)])
                            else:
                                if nip:
                                    node['communication'] = _OD([('network_adapter', f"{na} ({nip})")])
                                else:
                                    node['communication'] = _OD([('network_adapter', na)])
                        else:
                            # no adapter name known: leave whatever communication we already built
                            pass
                    else:
                        # non-tcp: preserve any additional adapter-ish fields from params
                        if isinstance(params, dict):
                            if 'network_adapter' in params:
                                node['communication']['network_adapter'] = params.get('network_adapter')
                            if 'network_adapter_ip' in params:
                                node['communication']['network_adapter_ip'] = params.get('network_adapter_ip')
                            if 'adapter' in params and 'adapter' not in node['communication']:
                                node['communication']['adapter'] = params.get('adapter')
                except Exception:
                    pass
            elif t == "Device":
                # Export Device using OrderedDict in configuration-tree order and avoid duplicate flat keys
                try:
                    name_val = item.text(0) or ""
                except Exception:
                    name_val = ""
                try:
                    device_id_val = item.data(2, Qt.ItemDataRole.UserRole)
                except Exception:
                    device_id_val = None
                try:
                    desc_val = item.data(1, Qt.ItemDataRole.UserRole) or ""
                except Exception:
                    desc_val = ""

                # Build ordered node: type,text,general,timing,data_access,encoding,block_sizes,ethernet,children
                node = _OD()
                node["type"] = "Device"
                node["text"] = item.text(0)
                # general ordered: Device Name, Description, Device ID
                node["general"] = _OD()
                node["general"]["name"] = name_val
                node["general"]["description"] = desc_val
                node["general"]["device_id"] = device_id_val

                # timing - shape depends on channel driver type (serial vs over-tcp vs ethernet)
                try:
                    timing_src = item.data(3, Qt.ItemDataRole.UserRole)
                except Exception:
                    timing_src = None

                # determine driver type from the ancestor Channel node (Device may not store driver)
                try:
                    # find ancestor channel
                    anc = item.parent()
                    while anc is not None and anc.data(0, Qt.ItemDataRole.UserRole) != 'Channel':
                        anc = anc.parent()
                    drv_type = ''
                    if anc is not None:
                        try:
                            pdrv = anc.data(2, Qt.ItemDataRole.UserRole)
                        except Exception:
                            pdrv = None
                        # fallback to role9 if role2 is not dict/string
                        if not pdrv:
                            try:
                                pdrv = anc.data(9, Qt.ItemDataRole.UserRole)
                            except Exception:
                                pdrv = None
                        if isinstance(pdrv, dict):
                            dt = pdrv.get('type')
                            if isinstance(dt, dict):
                                drv_type = str(dt.get('type') or '')
                            else:
                                drv_type = str(dt or '')
                        else:
                            drv_type = str(pdrv or '')
                    drv_type = (drv_type or '').lower()
                except Exception:
                    drv_type = ''

                # helpers to read fields with fallbacks
                def _g(k, alt=None, default=''):
                    try:
                        if isinstance(timing_src, dict) and timing_src.get(k) is not None:
                            return timing_src.get(k)
                        if alt and isinstance(timing_src, dict) and timing_src.get(alt) is not None:
                            return timing_src.get(alt)
                    except Exception:
                        pass
                    return default

                timing_od = OrderedDict()
                # RTU over TCP: include connect_timeout and connect_attempts
                if 'rtu over tcp' in drv_type:
                    timing_od['connect_timeout'] = _g('connect_timeout', 'req_timeout', '1000')
                    timing_od['connect_attempts'] = _g('connect_attempts', 'attempts', '1')
                    timing_od['request_timeout'] = _g('request_timeout', 'req_timeout', '1000')
                    timing_od['attempts_before_timeout'] = _g('attempts_before_timeout', 'attempts', '1')
                    timing_od['inter_request_delay'] = _g('inter_request_delay', 'inter_req_delay', '0')
                # TCP/IP Ethernet: include connect_timeout but not connect_attempts
                elif 'tcp' in drv_type and 'ethernet' in drv_type or 'modbus tcp' in drv_type:
                    timing_od['connect_timeout'] = _g('connect_timeout', 'req_timeout', '3')
                    timing_od['request_timeout'] = _g('request_timeout', 'req_timeout', '1000')
                    timing_od['attempts_before_timeout'] = _g('attempts_before_timeout', 'attempts', '1')
                    timing_od['inter_request_delay'] = _g('inter_request_delay', 'inter_req_delay', '0')
                else:
                    # default/serial: only include request_timeout, attempts_before_timeout, inter_request_delay
                    timing_od['request_timeout'] = _g('request_timeout', 'req_timeout', '1000')
                    timing_od['attempts_before_timeout'] = _g('attempts_before_timeout', 'attempts', '1')
                    timing_od['inter_request_delay'] = _g('inter_request_delay', 'inter_req_delay', '0')

                node['timing'] = timing_od

                # data_access
                try:
                    access = item.data(4, Qt.ItemDataRole.UserRole)
                except Exception:
                    access = None
                # data_access - ordered per config: zero_based, zero_based_bit, bit_writes, func_06, func_05
                if access is None:
                    access = {}
                da_od = OrderedDict()
                da_od["zero_based"] = to_numeric_flag(access.get("zero_based") if isinstance(access, dict) else access)
                da_od["zero_based_bit"] = to_numeric_flag(access.get("zero_based_bit") if isinstance(access, dict) else access)
                da_od["bit_writes"] = to_numeric_flag(access.get("bit_writes") if isinstance(access, dict) else access)
                da_od["func_06"] = to_numeric_flag(access.get("func_06") if isinstance(access, dict) else access)
                da_od["func_05"] = to_numeric_flag(access.get("func_05") if isinstance(access, dict) else access)
                node["data_access"] = da_od

                # encoding
                try:
                    enc = item.data(5, Qt.ItemDataRole.UserRole)
                except Exception:
                    enc = None
                # encoding - ordered: byte_order, word_order, dword_order, bit_order, treat_longs_as_decimals
                if enc is None:
                    enc = {}
                enc_od = OrderedDict()
                enc_od["byte_order"] = to_numeric_flag(enc.get("byte_order") if isinstance(enc, dict) else enc)
                enc_od["word_order"] = to_numeric_flag(enc.get("word_order") if isinstance(enc, dict) else enc)
                enc_od["dword_order"] = to_numeric_flag(enc.get("dword_order") if isinstance(enc, dict) else enc)
                enc_od["bit_order"] = to_numeric_flag(enc.get("bit_order") if isinstance(enc, dict) else enc)
                enc_od["treat_longs_as_decimals"] = to_numeric_flag(enc.get("treat_longs_as_decimals") if isinstance(enc, dict) else enc)
                node["encoding"] = enc_od

                # block_sizes
                try:
                    blocks = item.data(6, Qt.ItemDataRole.UserRole)
                except Exception:
                    blocks = None
                # block_sizes - ordered: out_coils, in_coils, int_regs, hold_regs
                if blocks is None:
                    blocks = {}
                blocks_od = OrderedDict()
                blocks_od["out_coils"] = blocks.get("out_coils") if isinstance(blocks, dict) and blocks.get("out_coils") is not None else 2000
                blocks_od["in_coils"] = blocks.get("in_coils") if isinstance(blocks, dict) and blocks.get("in_coils") is not None else 2000
                blocks_od["int_regs"] = blocks.get("int_regs") if isinstance(blocks, dict) and blocks.get("int_regs") is not None else 120
                blocks_od["hold_regs"] = blocks.get("hold_regs") if isinstance(blocks, dict) and blocks.get("hold_regs") is not None else 120
                node["block_sizes"] = blocks_od

                # ethernet
                # ethernet role removed in new project mapping
            elif t == "Tag":
                # Export Tag using OrderedDict and configuration-tree order
                try:
                    desc = item.data(1, Qt.ItemDataRole.UserRole) or ""
                except Exception:
                    desc = ""
                try:
                    dtype = item.data(2, Qt.ItemDataRole.UserRole)
                except Exception:
                    dtype = None
                try:
                    access = item.data(3, Qt.ItemDataRole.UserRole)
                except Exception:
                    access = None
                try:
                    addr = item.data(4, Qt.ItemDataRole.UserRole)
                except Exception:
                    addr = None
                try:
                    scan_rate = item.data(5, Qt.ItemDataRole.UserRole)
                except Exception:
                    scan_rate = None

                # Build ordered node: type,text,general,scaling
                node = _OD()
                node["type"] = "Tag"
                node["text"] = item.text(0) or ""
                # general ordered: Tag Name, Description, Data Type, Access, Address, Scan Rate
                gen_od = _OD()
                gen_od["name"] = item.text(0) or ""
                gen_od["description"] = desc
                gen_od["data_type"] = dtype
                if access is not None:
                    gen_od["access"] = access
                gen_od["address"] = addr
                if scan_rate is not None:
                    gen_od["scan_rate"] = scan_rate
                node["general"] = gen_od

                # include scaling if present (ordered) and type is not "None"
                try:
                    scaling = item.data(6, Qt.ItemDataRole.UserRole)
                    if isinstance(scaling, dict) and scaling.get("type") != "None":
                        # desired scaling order: type, raw_low, raw_high, scaled_type, scaled_low, scaled_high, clamp_low, clamp_high, negate, units
                        s_od = _OD()
                        s_od["type"] = scaling.get("type")
                        s_od["raw_low"] = scaling.get("raw_low")
                        s_od["raw_high"] = scaling.get("raw_high")
                        s_od["scaled_type"] = scaling.get("scaled_type")
                        s_od["scaled_low"] = scaling.get("scaled_low")
                        s_od["scaled_high"] = scaling.get("scaled_high")
                        s_od["clamp_low"] = scaling.get("clamp_low")
                        s_od["clamp_high"] = scaling.get("clamp_high")
                        s_od["negate"] = scaling.get("negate")
                        s_od["units"] = scaling.get("units")
                        node["scaling"] = s_od
                except Exception:
                    pass
            elif t == "Group":
                # Export Group with ordered keys: type,text,description,children
                node = _OD()
                node["type"] = "Group"
                node["text"] = item.text(0) or ""
                try:
                    # Group description now stored in role 1
                    node["description"] = item.data(1, Qt.ItemDataRole.UserRole) or ""
                except Exception:
                    node["description"] = ""
            children = []
            for i in range(item.childCount()):
                children.append(serialize(item.child(i)))
            if children:
                node['children'] = children
            return node

        doc = {"type": "Project", "channels": []}
        for i in range(conn.childCount()):
            ch = conn.child(i)
            if ch.data(0, Qt.ItemDataRole.UserRole) == "Channel":
                doc['channels'].append(serialize(ch))

        # include opcua settings if the app has them; if missing, try to obtain defaults
        try:
            opc = getattr(self.app, 'opcua_settings', None)
        except Exception:
            opc = None
        # normalize any existing opc settings so network_adapter fields are canonical
        try:
            if isinstance(opc, dict):
                try:
                    opc = self.normalize_opcua_settings(opc)
                except Exception:
                    pass
        except Exception:
            pass

        if opc is None:
            # attempt to instantiate the dialog to retrieve canonical defaults
            try:
                from ui.dialogs.opcua_dialog import OPCUADialog
                try:
                    dlg = OPCUADialog(getattr(self, 'app', None))
                    opc = dlg.get_data() if dlg is not None else None
                except Exception:
                    opc = None
            except Exception:
                opc = None
        try:
            if opc is not None:
                from collections import OrderedDict as _OD
                def pick(src, *keys, default=None):
                    if not isinstance(src, dict):
                        return default
                    for k in keys:
                        if k in src:
                            return src.get(k)
                    for sec in ('general', 'authentication', 'certificate', 'security_policies'):
                        sub = src.get(sec) if isinstance(src.get(sec), dict) else {}
                        for k in keys:
                            if k in sub:
                                return sub.get(k)
                    return default

                opc_od = _OD()
                # Build general section: prefer network_adapter string and omit separate network_adapter_ip
                gen_keys = ['application_name', 'host_name', 'namespace', 'port', 'product_uri', 'max_sessions', 'publish_interval']
                gen_od = _OD()
                for k in gen_keys:
                    v = pick(opc, k)
                    if v is not None:
                        gen_od[k] = v

                # handle network adapter specially: merge name + ip into single 'network_adapter' field
                na = pick(opc, 'network_adapter')
                nip = pick(opc, 'network_adapter_ip')
                if isinstance(na, str) and '(' in na and na.endswith(')'):
                    gen_od['network_adapter'] = na
                else:
                    if na and nip:
                        gen_od['network_adapter'] = f"{na} ({nip})"
                    elif na:
                        gen_od['network_adapter'] = na
                    elif nip:
                        gen_od['network_adapter'] = f"Auto ({nip})"

                opc_od['general'] = gen_od

                # Build authentication: ensure we don't emit duplicated nested username/password
                auth_type = None
                auth_user = None
                auth_pass = None
                # prefer nested authentication dict if present
                try:
                    asec = opc.get('authentication') if isinstance(opc, dict) else None
                    if isinstance(asec, dict):
                        # asec may contain either {'authentication': 'Anonymous', 'username': '', 'password': ''}
                        auth_type = asec.get('authentication') or asec.get('type')
                        auth_user = asec.get('username') if 'username' in asec else None
                        auth_pass = asec.get('password') if 'password' in asec else None
                    # fallback to top-level keys
                    if auth_type is None:
                        auth_type = pick(opc, 'authentication')
                    if auth_user is None:
                        auth_user = pick(opc, 'username')
                    if auth_pass is None:
                        auth_pass = pick(opc, 'password')
                except Exception:
                    pass
                # If authentication is Anonymous, do not export username/password
                try:
                    if isinstance(auth_type, str) and auth_type.strip().lower() == 'anonymous':
                        auth_user = None
                        auth_pass = None
                except Exception:
                    pass

                # Always emit explicit authentication structure so import/load
                # round-trips the user's choice. Use empty strings for missing
                # username/password to avoid losing fields during import.
                auth_od = _OD()
                try:
                    auth_od['authentication'] = str(auth_type) if auth_type is not None else 'Anonymous'
                except Exception:
                    auth_od['authentication'] = 'Anonymous'
                try:
                    auth_od['username'] = '' if auth_user is None else str(auth_user)
                except Exception:
                    auth_od['username'] = ''
                try:
                    auth_od['password'] = '' if auth_pass is None else str(auth_pass)
                except Exception:
                    auth_od['password'] = ''
                opc_od['authentication'] = auth_od

                sp_keys = ['policy_none', 'policy_sign_aes128', 'policy_sign_aes256', 'policy_sign_basic256sha256', 'policy_encrypt_aes128', 'policy_encrypt_aes256', 'policy_encrypt_basic256sha256']
                sp_od = _OD()
                for k in sp_keys:
                    v = None
                    try:
                        if isinstance(opc, dict) and k in opc:
                            v = opc.get(k)
                        else:
                            sp = opc.get('security_policies') if isinstance(opc.get('security_policies'), dict) else {}
                            if k in sp:
                                v = sp.get(k)
                    except Exception:
                        v = None
                    if v is not None:
                        # normalize booleans to numeric flags like Modbus handling
                        try:
                            sp_od[k] = to_numeric_flag(v)
                        except Exception:
                            sp_od[k] = v
                opc_od['security_policies'] = sp_od

                cert_keys = ['auto_generate', 'common_name', 'organization', 'organization_unit', 'locality', 'state', 'country', 'cert_validity']
                cert_od = _OD()
                for k in cert_keys:
                    v = None
                    try:
                        if isinstance(opc, dict) and k in opc:
                            v = opc.get(k)
                        else:
                            c = opc.get('certificate') if isinstance(opc.get('certificate'), dict) else {}
                            if k in c:
                                v = c.get(k)
                    except Exception:
                        v = None
                    if v is not None:
                        # normalize auto_generate boolean to numeric flag
                        try:
                            if k == 'auto_generate':
                                cert_od[k] = to_numeric_flag(v)
                            else:
                                cert_od[k] = v
                        except Exception:
                            cert_od[k] = v
                opc_od['certificate'] = cert_od

                doc['opcua_settings'] = opc_od
        except Exception:
            pass

        try:
            d = os.path.dirname(filepath)
            if d and not os.path.exists(d):
                os.makedirs(d, exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)
        except Exception:
            return


__all__ = ["AppController"]
