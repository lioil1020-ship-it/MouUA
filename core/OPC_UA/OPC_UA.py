# Lightweight OPC UA shim for UI compatibility.
# Archived original lives in archived_py/OPC_UA.bak_20260121.py
from typing import Dict, Any


class OPCServer:
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self._nodes = {}
        self._is_running = False

    def start(self):
        self._is_running = True

    def stop(self):
        self._is_running = False

    def add_tag(self, tag_meta: Dict[str, Any]):
        tid = tag_meta.get('id') or tag_meta.get('name')
        self._nodes[tid] = {"value": None, "meta": tag_meta}
        return None

    def setup_tags_from_config(self, devices_config):
        return

    def setup_tags_from_tree(self, conn_root_item):
        return

    def get_endpoints(self):
        return []

    def is_tag_writable(self, key):
        info = self._nodes.get(key)
        return bool(info and info.get('meta', {}).get('writable', False))

    def read_tag_value(self, key):
        info = self._nodes.get(key)
        return info.get('value') if info else None

    def update_tag(self, key, value):
        if key in self._nodes:
            self._nodes[key]['value'] = value
            return True
        return False
