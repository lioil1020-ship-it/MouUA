"""
OPC_UA.py

簡單的 OPC UA Server 封裝，會根據 UI 中 `opcuadialog` 的設定建立伺服器，
並提供從應用程式傳入 Tag 列表以建立對應的 Variable 節點的 API。

依賴: python-opcua (`pip install opcua`)

介面:
  - OPCServer(config_dict)
    - start()
    - stop()
    - update_tag(tag_id, value)  # 更新已建立的節點值
    - add_tag(tag_meta)  # 以 tag metadata 建立節點

設計要點:
  - `config_dict` 參考 `dialogs/opcua_dialog.OPCUADialog.values()` 回傳的 dict。
  - Tag metadata 格式示例：{
        'id': <unique id>,
        'name': 'Device.Tag',
        'data_type': 'Float'|'Int'|...,
        'address': '400001'  # optional
    }

此檔為簡易整合版本，重點在於快速啟動一個可被外部程式更新的 OPC UA server。
"""
import threading
import time
import socket
from typing import Dict, Any

try:
    from opcua import ua, Server
except Exception:
    Server = None
try:
    from PyQt6.QtCore import Qt
except Exception:
    Qt = None


class OPCServer:
    def __init__(self, config: Dict[str, Any] = None):
        """config 預期為 opcua_dialog.values() 的字典。"""
        self.config = config or {}
        self._server = None
        self._is_running = False
        self._thread = None
        self._nodes = {}  # mapping from tag id -> {node, type}
        # helper mapping cache for dtype -> ua.VariantType
        self._dtype_map_cache = {}

    def _map_dtype_to_variant(self, dtype: str):
        """Map a textual dtype (e.g. 'Int', 'Float', 'Double') to a ua.VariantType.
        Defaults to Int16 on unknown.
        """
        try:
            key = (dtype or '').strip().lower()
            if key in self._dtype_map_cache:
                return self._dtype_map_cache[key]
            if 'double' in key:
                vt = ua.VariantType.Double
            elif 'float' in key:
                vt = ua.VariantType.Float
            elif 'bool' in key or 'boolean' in key or 'bit' in key or 'coil' in key:
                vt = ua.VariantType.Boolean
            elif 'qword' in key or 'uint64' in key or 'ulong' in key:
                vt = ua.VariantType.UInt64
            elif 'llong' in key or 'int64' in key or 'long long' in key:
                vt = ua.VariantType.Int64
            elif 'dword' in key or 'uint32' in key or 'unsigned long' in key:
                vt = ua.VariantType.UInt32
            elif 'long' in key and 'llong' not in key:
                vt = ua.VariantType.Int32
            elif 'word' in key or 'uint16' in key:
                vt = ua.VariantType.UInt16
            elif 'short' in key or 'int16' in key:
                vt = ua.VariantType.Int16
            elif 'byte' in key:
                vt = ua.VariantType.Byte
            elif 'bcd' in key:
                vt = ua.VariantType.UInt32
            else:
                vt = ua.VariantType.Int16
            self._dtype_map_cache[key] = vt
            return vt
        except Exception:
            return ua.VariantType.Int16

    def _map_dtype_to_nodeid(self, dtype: str):
        """Map textual dtype to an appropriate UA built-in DataType NodeId."""
        try:
            k = (dtype or '').strip().lower()
            if 'bool' in k or 'boolean' in k or 'bit' in k or 'coil' in k:
                return ua.NodeId(ua.ObjectIds.Boolean)
            if 'string' in k or 'char' in k:
                return ua.NodeId(ua.ObjectIds.String)
            if 'byte' in k and 'word' not in k:
                return ua.NodeId(ua.ObjectIds.Byte)
            if 'double' in k:
                return ua.NodeId(ua.ObjectIds.Double)
            if 'float' in k:
                return ua.NodeId(ua.ObjectIds.Float)
            if 'qword' in k or 'uint64' in k or 'ulong' in k:
                return ua.NodeId(ua.ObjectIds.UInt64)
            if 'llong' in k or 'int64' in k or 'long long' in k:
                return ua.NodeId(ua.ObjectIds.Int64)
            if 'dword' in k or 'uint32' in k or 'unsigned long' in k:
                return ua.NodeId(ua.ObjectIds.UInt32)
            if 'long' in k and 'llong' not in k:
                return ua.NodeId(ua.ObjectIds.Int32)
            if 'lbcd' in k or 'l_bcd' in k or ('l' in k and 'bcd' in k):
                return ua.NodeId(ua.ObjectIds.UInt64)
            if 'bcd' in k:
                return ua.NodeId(ua.ObjectIds.UInt32)
            if 'word' in k or 'uint16' in k:
                return ua.NodeId(ua.ObjectIds.UInt16)
            if 'short' in k or 'int16' in k:
                return ua.NodeId(ua.ObjectIds.Int16)
            if 'int' in k:
                return ua.NodeId(ua.ObjectIds.Int32)
            return ua.NodeId(ua.ObjectIds.Int16)
        except Exception:
            return ua.NodeId(ua.ObjectIds.Int16)

    def _prepare_server(self):
        if Server is None:
            raise RuntimeError("python-opcua not installed. install with 'pip install opcua'")

        self._server = Server()
        # 應用設定
        app_name = self.config.get('application_Name') or self.config.get('server_name') or 'ModUA'
        # `host_name` 在 UI 中通常是應用名稱，而非用於 socket bind 的 IP。
        # 這裡我們讓伺服器實際綁定到所有介面（0.0.0.0），
        # 但偵測一個可用的 LAN IP 作為顯示/Discovery 用的 host（不做為實際 bind）。
        raw_host = str(self.config.get('host_name', '') or '').strip()
        display_host = None
        try:
            import ipaddress

            if raw_host:
                try:
                    ipaddress.ip_address(raw_host)
                    display_host = raw_host
                except Exception:
                    display_host = None
        except Exception:
            display_host = None

        if not display_host:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(("8.8.8.8", 80))
                display_host = s.getsockname()[0]
                s.close()
            except Exception:
                display_host = '127.0.0.1'

        # determine bind/display host
        bind_host = '0.0.0.0'
        port = int(self.config.get('port', 48480))
        namespace = self.config.get('namespace', app_name)

        # If UI provided a specific network adapter IP, prefer that for binding/display
        try:
            na_ip = (self.config.get('network_adapter_ip') or '').strip()
            import ipaddress
            if na_ip:
                try:
                    ipaddress.ip_address(na_ip)
                    bind_host = na_ip
                    display_host = na_ip
                except Exception:
                    pass
        except Exception:
            pass

        # 記錄顯示用 host
        self._display_host = display_host
        self._server.set_endpoint(f"opc.tcp://{bind_host}:{port}")
        self._server.set_server_name(app_name)
        idx = self._server.register_namespace(namespace)

        # 建立 Objects folder
        self._objects = self._server.get_objects_node()
        self._nsidx = idx

    def start(self):
        if self._is_running:
            return
        # prepare if needed
        if self._server is None:
            self._prepare_server()

        # try to start synchronously so callers can immediately observe success/failure
        try:
            self._server.start()
        except Exception:
            # If synchronous start fails, ensure we don't leave inconsistent state
            try:
                self._server.stop()
            except Exception:
                pass
            raise

        # if start() succeeded, mark running and spawn a background thread to keep server alive
        self._is_running = True

        def run_monitor():
            try:
                while self._is_running:
                    time.sleep(0.5)
            finally:
                try:
                    self._server.stop()
                except Exception:
                    pass

        self._thread = threading.Thread(target=run_monitor, daemon=True)
        self._thread.start()

    def get_endpoints(self):
        """Return list of endpoint URL strings for diagnostics."""
        out = []
        try:
            eps = self._server.get_endpoints()
            for e in eps:
                try:
                    url = getattr(e, 'EndpointUrl', str(e))
                    try:
                        if isinstance(self._display_host, str) and '0.0.0.0' in url:
                            url = url.replace('0.0.0.0', self._display_host)
                    except Exception:
                        pass
                    out.append(str(url))
                except Exception:
                    try:
                        out.append(str(e))
                    except Exception:
                        pass
        except Exception:
            try:
                out.append(str(getattr(self._server, 'application_uri', 'N/A')))
            except Exception:
                pass
        return out

    def stop(self):
        self._is_running = False
        if self._server:
            try:
                self._server.stop()
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=1.0)

    def add_tag(self, tag_meta: Dict[str, Any]):
        """根據 tag_meta 建立變數節點，tag_meta 需包含 'id' 與 'name'。
        回傳節點物件。
        """
        if not self._server:
            raise RuntimeError("Server not started")
        tid = tag_meta.get('id') or tag_meta.get('name')
        name = tag_meta.get('name', f"Tag_{tid}")
        dtype = str(tag_meta.get('data_type', 'Int')).lower()

        # 創建一個物件節點以 group tags（以 namespace index 為參考）
        folder = self._objects
        # Use a simple flat node: Objects -> name
        try:
            vt = self._map_dtype_to_variant(dtype)
            init_val = ua.Variant(0, vt)
            var = folder.add_variable(self._nsidx, name, init_val)
        except Exception:
            var = folder.add_variable(self._nsidx, name, 0)
        # set writable according to provided meta (client access)
        writable = True
        try:
            access = tag_meta.get('access') or tag_meta.get('client_access') or ''
            if isinstance(access, str) and 'read' in access.lower() and 'write' not in access.lower():
                writable = False
        except Exception:
            writable = True
        try:
            var.set_writable(bool(writable))
        except Exception:
            try:
                var.set_writable(True)
            except Exception:
                pass

        # 嘗試根據 dtype 設定典型 UA 型別（簡易對應）
        try:
            kd = str(dtype).lower()
            try:
                var.set_data_type(self._map_dtype_to_nodeid(dtype))
            except Exception:
                pass
        except Exception:
            pass

        # store node, type and writable flag for later coercion
        self._nodes[tid] = {"node": var, "type": dtype, "writable": bool(writable)}
        return var

    def setup_tags_from_config(self, devices_config):
        """Create OPC UA tree from a configuration list.

        devices_config: iterable of devices, each device is dict with keys:
            'name' and 'tags' (list of tag dicts with 'name','id','data_type')

        Resulting structure: Objects -> DeviceName -> (optional GroupName) -> Tag

        注意：此為 API 輔助函式，可能由外部配置/部署流程動態呼叫以建立節點，
        因此請勿移除。
        """
        if not self._server:
            raise RuntimeError("Server not started")
        # remove any previously created nodes/children to avoid duplicates
        try:
            try:
                children = self._objects.get_children()
                for c in children:
                    try:
                        c.delete()
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                self._nodes.clear()
            except Exception:
                self._nodes = {}
        except Exception:
            pass
        for dev in devices_config:
            dev_name = dev.get('name') or 'Device'
            dev_node = self._objects.add_object(self._nsidx, dev_name)
            tags = dev.get('tags') or []
            for tag in tags:
                tag_id = tag.get('id') or f"{dev_name}.{tag.get('name')}"
                tag_name = tag.get('name') or tag_id
                dtype = tag.get('data_type') or 'Int'
                try:
                    vt = self._map_dtype_to_variant(dtype)
                    node = dev_node.add_variable(self._nsidx, tag_name, ua.Variant(0, vt))
                except Exception:
                    node = dev_node.add_variable(self._nsidx, tag_name, 0)
                # determine writable
                writable = True
                try:
                    access = tag.get('access') or tag.get('client_access') or ''
                    if isinstance(access, str) and 'read' in access.lower() and 'write' not in access.lower():
                        writable = False
                except Exception:
                    writable = True
                try:
                    node.set_writable(bool(writable))
                except Exception:
                    try:
                        node.set_writable(True)
                    except Exception:
                        pass
                try:
                    kd = str(dtype).lower()
                    try:
                        node.set_data_type(self._map_dtype_to_nodeid(dtype))
                    except Exception:
                        pass
                except Exception:
                    pass
                self._nodes[tag_id] = {"node": node, "type": dtype, "writable": bool(writable)}

    def setup_tags_from_tree(self, conn_root_item):
        """Walk a PyQt tree `conn_root_item` (Connectivity root) and create nodes.

        Expects tree items to use the same UserRole convention as the UI: Tag
        items have role 'Tag', parent Device items have role 'Device'. The
        canonical id used is 'DeviceName[.GroupName].TagName'.
        """
        if not self._server:
            raise RuntimeError("Server not started")
        try:
            # clear previously-created nodes/objects to avoid accumulating duplicates
            try:
                # remove child objects under Objects node if possible
                try:
                    children = self._objects.get_children()
                    for c in children:
                        try:
                            c.delete()
                        except Exception:
                            pass
                except Exception:
                    pass
                # clear internal mapping
                try:
                    self._nodes.clear()
                except Exception:
                    self._nodes = {}
            except Exception:
                pass
            # iterate channels under root
            for ch_idx in range(conn_root_item.childCount()):
                ch = conn_root_item.child(ch_idx)
                ch_name = ch.text(0) or f"Channel{ch_idx}"
                ch_node = self._objects.add_object(self._nsidx, ch_name)
                # devices under channel
                for d_idx in range(ch.childCount()):
                    dev = ch.child(d_idx)
                    try:
                        role = dev.data(0, Qt.ItemDataRole.UserRole) if Qt is not None else None
                    except Exception:
                        role = None
                    if role != 'Device' and (dev.text(0) is None or 'device' not in str(dev.text(0)).lower()):
                        continue
                    dev_name = dev.text(0) or f"Device{d_idx}"
                    dev_node = ch_node.add_object(self._nsidx, dev_name)
                    # direct tag children
                    for ti in range(dev.childCount()):
                        tag_item = dev.child(ti)
                        try:
                            trole = tag_item.data(0, Qt.ItemDataRole.UserRole) if Qt is not None else None
                        except Exception:
                            trole = None
                        if trole == 'Tag' or (tag_item.text(0) is not None and tag_item.text(0) != ''):
                            tag_name = tag_item.text(0) or f"Tag{ti}"
                            # detect array length from tag address or name
                            try:
                                addr = tag_item.data(1, Qt.ItemDataRole.UserRole)
                            except Exception:
                                addr = None
                            import re
                            array_len = None
                            try:
                                if addr:
                                    m = re.search(r"\[\s*(\d+)\s*\]", str(addr))
                                    if m:
                                        array_len = int(m.group(1))
                                # also detect in tag_name if user encoded size there
                                if array_len is None:
                                    m2 = re.search(r"\[\s*(\d+)\s*\]", str(tag_name))
                                    if m2:
                                        array_len = int(m2.group(1))
                            except Exception:
                                array_len = None

                            tag_id = f"{ch_name}.{dev_name}.{tag_name}"
                            try:
                                dtype = tag_item.data(2, Qt.ItemDataRole.UserRole) if Qt is not None else None
                            except Exception:
                                dtype = None
                            dtype = dtype or 'Int'

                            # If we've previously created this tag, reuse the node instead of creating a new one.
                            if tag_id in self._nodes:
                                node = self._nodes[tag_id].get('node')
                                # update stored type
                                self._nodes[tag_id]['type'] = dtype
                            else:
                                # if array_len present, create an array initial value
                                try:
                                    if array_len and int(array_len) > 0:
                                        base = dtype or 'Int'
                                        dt = str(base).lower()
                                        try:
                                            vt = self._map_dtype_to_variant(base)
                                            if 'float' in dt or 'double' in dt:
                                                init_list = [0.0] * int(array_len)
                                            else:
                                                init_list = [0] * int(array_len)
                                            node = dev_node.add_variable(self._nsidx, tag_name, ua.Variant(init_list, vt))
                                        except Exception:
                                            if 'float' in dt or 'double' in dt:
                                                init_val = [0.0] * int(array_len)
                                            else:
                                                init_val = [0] * int(array_len)
                                            node = dev_node.add_variable(self._nsidx, tag_name, init_val)
                                    else:
                                        try:
                                            vt = self._map_dtype_to_variant(dtype)
                                            node = dev_node.add_variable(self._nsidx, tag_name, ua.Variant(0, vt))
                                        except Exception:
                                            node = dev_node.add_variable(self._nsidx, tag_name, 0)
                                except Exception:
                                    node = dev_node.add_variable(self._nsidx, tag_name, 0)
                                self._nodes[tag_id] = {"node": node, "type": dtype, "writable": True}

                            # set writable based on Tag's client access (slot 9) if available
                            try:
                                access_val = tag_item.data(9, Qt.ItemDataRole.UserRole) if Qt is not None else None
                            except Exception:
                                access_val = None
                            try:
                                writable = True
                                if isinstance(access_val, str) and 'read' in access_val.lower() and 'write' not in access_val.lower():
                                    writable = False
                                node.set_writable(bool(writable))
                                # store writable flag if node known in mapping
                                if tag_id in self._nodes:
                                    self._nodes[tag_id]['writable'] = bool(writable)
                            except Exception:
                                pass

                                try:
                                    node.set_data_type(self._map_dtype_to_nodeid(dtype))
                                except Exception:
                                    pass
                    # groups inside device
                    for g in range(dev.childCount()):
                        grp = dev.child(g)
                        try:
                            groe = grp.data(0, Qt.ItemDataRole.UserRole) if Qt is not None else None
                        except Exception:
                            groe = None
                        if groe != 'Group' and (grp.text(0) is None or 'group' not in str(grp.text(0)).lower()):
                            continue
                        grp_node = dev_node.add_object(self._nsidx, grp.text(0) or f"Group{g}")
                        for ti in range(grp.childCount()):
                            tag_item = grp.child(ti)
                            try:
                                trole2 = tag_item.data(0, Qt.ItemDataRole.UserRole) if Qt is not None else None
                            except Exception:
                                trole2 = None
                            if trole2 == 'Tag' or (tag_item.text(0) is not None and tag_item.text(0) != ''):
                                tag_name = tag_item.text(0) or f"Tag{ti}"
                                tag_id = f"{ch_name}.{dev_name}.{grp.text(0)}.{tag_name}"
                                try:
                                    dtype = tag_item.data(2, Qt.ItemDataRole.UserRole) if Qt is not None else None
                                except Exception:
                                    dtype = None
                                dtype = dtype or 'Int'
                                # detect array length in address or tag name for group tags
                                try:
                                    addr = tag_item.data(1, Qt.ItemDataRole.UserRole)
                                except Exception:
                                    addr = None
                                import re
                                array_len = None
                                try:
                                    if addr:
                                        m = re.search(r"\[\s*(\d+)\s*\]", str(addr))
                                        if m:
                                            array_len = int(m.group(1))
                                    if array_len is None:
                                        m2 = re.search(r"\[\s*(\d+)\s*\]", str(tag_name))
                                        if m2:
                                            array_len = int(m2.group(1))
                                except Exception:
                                    array_len = None

                                # reuse existing node if present
                                if tag_id in self._nodes:
                                    node = self._nodes[tag_id].get('node')
                                    self._nodes[tag_id]['type'] = dtype
                                else:
                                    if array_len and int(array_len) > 0:
                                        try:
                                            vt = self._map_dtype_to_variant(dtype)
                                            if 'float' in str(dtype).lower() or 'double' in str(dtype).lower():
                                                init_list = [0.0] * int(array_len)
                                            else:
                                                init_list = [0] * int(array_len)
                                            node = grp_node.add_variable(self._nsidx, tag_name, ua.Variant(init_list, vt))
                                        except Exception:
                                            if 'float' in str(dtype).lower() or 'double' in str(dtype).lower():
                                                init_val = [0.0] * int(array_len)
                                            else:
                                                init_val = [0] * int(array_len)
                                            node = grp_node.add_variable(self._nsidx, tag_name, init_val)
                                    else:
                                        try:
                                            vt = self._map_dtype_to_variant(dtype)
                                            node = grp_node.add_variable(self._nsidx, tag_name, ua.Variant(0, vt))
                                        except Exception:
                                            node = grp_node.add_variable(self._nsidx, tag_name, 0)
                                        self._nodes[tag_id] = {"node": node, "type": dtype, "writable": True}

                                try:
                                    access_val2 = tag_item.data(9, Qt.ItemDataRole.UserRole) if Qt is not None else None
                                except Exception:
                                    access_val2 = None
                                try:
                                    writable = True
                                    if isinstance(access_val2, str) and 'read' in access_val2.lower() and 'write' not in access_val2.lower():
                                        writable = False
                                    node.set_writable(bool(writable))
                                    if tag_id in self._nodes:
                                        self._nodes[tag_id]['writable'] = bool(writable)
                                except Exception:
                                    pass
                                try:
                                    kd = str(dtype).lower()
                                    if 'bool' in kd or 'boolean' in kd or 'bit' in kd or 'coil' in kd:
                                        node.set_data_type(ua.NodeId(ua.ObjectIds.Boolean))
                                    elif 'float' in kd:
                                        node.set_data_type(ua.NodeId(ua.ObjectIds.Float))
                                    elif 'double' in kd:
                                        node.set_data_type(ua.NodeId(ua.ObjectIds.Double))
                                    else:
                                        node.set_data_type(ua.NodeId(ua.ObjectIds.Int16))
                                except Exception:
                                    pass
        except Exception:
            pass

    def update_tag(self, tag_id, value):
        entry = self._nodes.get(tag_id)
        if not entry:
            return False
        node = entry.get("node")
        dtype = entry.get("type")
        try:
            # lazy import to avoid circulars
            from utils.typeconv import coerce_value_for_dtype

            val = coerce_value_for_dtype(value, dtype)
            node.set_value(val)
            return True
        except Exception:
            try:
                node.set_value(value)
                return True
            except Exception:
                return False

    def read_tag_value(self, tag_id):
        """Return the current OPC UA value for a stored tag id, or None."""
        entry = self._nodes.get(tag_id)
        if not entry:
            return None
        node = entry.get('node')
        try:
            return node.get_value()
        except Exception:
            return None

    def is_tag_writable(self, tag_id):
        """Return True if the server-side node was marked writable when created."""
        entry = self._nodes.get(tag_id)
        if not entry:
            return False
        try:
            return bool(entry.get('writable', True))
        except Exception:
            return True

    def get_tag_dtype(self, tag_id):
        """Return stored data type for tag_id.

        Kept as a small public helper used when coercing values or mapping types; may be used dynamically.
        """
        entry = self._nodes.get(tag_id)
        if not entry:
            return None
        return entry.get('type')


if __name__ == '__main__':
    # 簡單 demo
    cfg = {'application_Name': 'ModUA', 'host_name': '127.0.0.1', 'port': '48480', 'namespace': 'ModUA'}
    s = OPCServer(cfg)
    try:
        s.start()
        print('OPC UA server started. ctrl-c to stop')
        # demo 建立測試節點
        time.sleep(0.5)
        s.add_tag({'id': 't1', 'name': 'Demo.Tag1', 'data_type': 'Float'})
        i = 0
        while True:
            s.update_tag('t1', float(i))
            i += 1
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        s.stop()
