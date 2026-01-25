"""
Modbus Data Buffer - Thread-safe storage for real-time tag values.

Stores all tag data (value, timestamp, quality, update_count) independently
of UI state. Serves as central hub for:
- Modbus Worker → writes real-time data
- Monitor UI → reads & displays data
- OPC UA Server → syncs with this buffer
"""

import threading
from typing import Dict, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class ModbusDataBuffer:
    """Thread-safe buffer for Modbus tag data."""
    
    def __init__(self):
        """Initialize buffer with thread lock."""
        self._lock = threading.RLock()
        self._data = {}  # {tag_name: {'value': ..., 'timestamp': ..., 'quality': ..., 'update_count': ...}}
        self._tag_info = {}  # {tag_name: {'data_type': ..., 'access': ...}}  (static info)
    
    def update_tag_value(self, tag_name: str, value: Any, timestamp: float, quality: str, update_count: int):
        """Update tag's dynamic values (called by Modbus Worker)."""
        try:
            with self._lock:
                if tag_name not in self._data:
                    self._data[tag_name] = {}
                
                self._data[tag_name].update({
                    'value': value,
                    'timestamp': timestamp,
                    'quality': quality,
                    'update_count': update_count,
                    'last_update': datetime.now()
                })
        except Exception as e:
            logger.error(f"Error updating tag {tag_name}: {e}")
    
    def set_tag_info(self, tag_name: str, data_type: str = "", access: str = "R"):
        """Set tag's static info (called during initialization)."""
        try:
            with self._lock:
                if tag_name not in self._tag_info:
                    self._tag_info[tag_name] = {}
                
                self._tag_info[tag_name].update({
                    'data_type': data_type,
                    'access': access
                })
        except Exception as e:
            logger.error(f"Error setting tag info {tag_name}: {e}")
    
    def get_tag_data(self, tag_name: str) -> Optional[Dict[str, Any]]:
        """Get complete tag data (value + static info)."""
        try:
            with self._lock:
                data = self._data.get(tag_name, {}).copy()
                info = self._tag_info.get(tag_name, {}).copy()
                return {**info, **data}
        except Exception as e:
            logger.error(f"Error getting tag data {tag_name}: {e}")
            return None
    
    def get_tag_value(self, tag_name: str) -> Any:
        """Get only tag's current value."""
        try:
            with self._lock:
                return self._data.get(tag_name, {}).get('value')
        except Exception as e:
            logger.error(f"Error getting tag value {tag_name}: {e}")
            return None
    
    def write_tag_value(self, tag_name: str, value: Any):
        """Write tag value back to buffer (for bidirectional support)."""
        try:
            with self._lock:
                if tag_name not in self._data:
                    self._data[tag_name] = {}
                
                self._data[tag_name]['value'] = value
                self._data[tag_name]['last_write'] = datetime.now()
                return True
        except Exception as e:
            logger.error(f"Error writing tag {tag_name}: {e}")
            return False
    
    def get_all_tags(self) -> Dict[str, Dict[str, Any]]:
        """Get all tag data."""
        try:
            with self._lock:
                result = {}
                for tag_name in self._data.keys():
                    data = self._data[tag_name].copy()
                    info = self._tag_info.get(tag_name, {}).copy()
                    result[tag_name] = {**info, **data}
                return result
        except Exception as e:
            logger.error(f"Error getting all tags: {e}")
            return {}
    
    def clear(self):
        """Clear all data."""
        try:
            with self._lock:
                self._data.clear()
                self._tag_info.clear()
        except Exception as e:
            logger.error(f"Error clearing buffer: {e}")
