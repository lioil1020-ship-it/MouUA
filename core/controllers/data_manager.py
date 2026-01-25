"""Data management and caching for polled tag values.

This module provides the DataBroker class which stores the latest polled
values from Modbus devices in a thread-safe manner, available for UI
components to query.
"""

import threading
from typing import Any, Dict, Optional

from core.config import GROUP_SEPARATOR
class DataBroker:
    """Thread-safe cache for polled tag values.
    
    Provides a snapshot of the most recent polled values from all connected
    devices. UI components can query the latest tag values without blocking
    the polling loop.
    """

    def __init__(self, *args, **kwargs):
        """Initialize the DataBroker with an empty value cache."""
        self._lock = threading.RLock()
        self._latest: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _make_key_from_tag_item(tag_item) -> str:
        """Convert a QTreeWidgetItem to a dot-separated key path.
        
        Walks up the tree hierarchy and builds a key like "Channel.Device.Group.Tag"
        """
        try:
            parts = []
            it = tag_item
            while it is not None:
                try:
                    t = it.text(0)
                except Exception:
                    t = None
                if t:
                    parts.insert(0, str(t))
                try:
                    it = it.parent()
                except Exception:
                    it = None
            return GROUP_SEPARATOR.join(parts) if parts else f"tag_{id(tag_item)}"
        except Exception:
            return f"tag_{id(tag_item)}"

    def handle_polled(self, tag_item, value, timestamp=None, quality=None):
        """Record a newly polled tag value.
        
        Args:
            tag_item: QTreeWidgetItem or tag identifier
            value: The polled value
            timestamp: Optional timestamp of the poll
            quality: Optional quality indicator
        """
        key = self._make_key_from_tag_item(tag_item)
        with self._lock:
            self._latest[key] = {"value": value, "timestamp": timestamp, "quality": quality}

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        """Get a copy of all current cached values.
        
        Returns a dict mapping tag keys to {value, timestamp, quality} dicts.
        """
        with self._lock:
            return dict(self._latest)

    def get(self, key: str, default=None) -> Optional[Dict[str, Any]]:
        """Get a single tag's cached value by key.
        
        Args:
            key: The tag key (dot-separated path)
            default: Default value if key not found
            
        Returns:
            Dict with 'value', 'timestamp', 'quality' keys, or default if not found
        """
        with self._lock:
            return self._latest.get(key, default)


__all__ = ["DataBroker"]
