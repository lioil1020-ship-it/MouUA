import threading
from typing import Any, Dict
try:
    from PyQt6.QtCore import Qt
except Exception:
    Qt = None


class DataBroker:
    """Thread-safe broker that stores latest tag values.

    - `update_from_polled(tag_item, value, timestamp, quality)` is intended to be
      connected to `AsyncPoller.tag_polled` and will write a canonical key.
    - `snapshot()` returns a shallow copy of the latest values for consumers.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._latest: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _make_key_from_tag_item(tag_item) -> str:
        try:
            # Build a canonical key: DeviceName.TagName or DeviceName.Group.TagName
            parts = []
            t = tag_item
            name = getattr(t, 'text', None)
            try:
                tn = t.text(0)
            except Exception:
                tn = None
            if tn:
                parts.insert(0, tn)

            # climb ancestors to collect Group, Device and Channel names
            parent = tag_item.parent()
            group_parts = []
            dev_name = None
            channel_name = None
            while parent is not None:
                try:
                    role = parent.data(0, Qt.ItemDataRole.UserRole) if Qt is not None else None
                except Exception:
                    role = None
                try:
                    pt = parent.text(0)
                except Exception:
                    pt = None
                if role == 'Device' or (pt and str(pt).lower().startswith('device')):
                    dev_name = pt
                    # channel may be parent of device
                    try:
                        p2 = parent.parent()
                        if p2 is not None:
                            try:
                                r2 = p2.data(0, Qt.ItemDataRole.UserRole) if Qt is not None else None
                            except Exception:
                                r2 = None
                            if r2 == 'Channel' or (p2.text(0) and str(p2.text(0)).lower().startswith('channel')):
                                channel_name = p2.text(0)
                    except Exception:
                        channel_name = None
                    break
                # collect group names (between device and tag)
                if pt:
                    group_parts.insert(0, pt)
                parent = parent.parent()

            # build canonical key: Channel.Device(.Group)*.Tag
            key_parts = []
            if channel_name:
                key_parts.append(channel_name)
            if dev_name:
                key_parts.append(dev_name)
            for g in group_parts:
                key_parts.append(g)
            key_parts.append(tn or str(id(tag_item)))
            return ".".join(key_parts)
        except Exception:
            try:
                return tag_item.text(0)
            except Exception:
                return str(id(tag_item))

    def handle_polled(self, tag_item, value, timestamp, quality):
        key = self._make_key_from_tag_item(tag_item)
        with self._lock:
            self._latest[key] = {"value": value, "timestamp": timestamp, "quality": quality}

    def snapshot(self):
        with self._lock:
            return dict(self._latest)

    def get(self, key, default=None):
        with self._lock:
            return self._latest.get(key, default)
