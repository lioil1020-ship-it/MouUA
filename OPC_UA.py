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

        # 實際 bind 到所有介面
        bind_host = '0.0.0.0'
        port = int(self.config.get('port', 48480))
        namespace = self.config.get('namespace', app_name)

        # 記錄顯示用 host，啟動後列出 endpoints 時會用此 host 替代 0.0.0.0
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
        self._prepare_server()

        def run():
            try:
                try:
                    self._server.start()
                    # 印出已註冊的 endpoints 與應用資訊，方便診斷客戶端連線問題
                    try:
                        eps = self._server.get_endpoints()
                        print("OPC UA endpoints:")
                        for e in eps:
                            try:
                                url = getattr(e, 'EndpointUrl', str(e))
                                # if server bound to 0.0.0.0, present detected LAN IP for discovery readability
                                try:
                                    if isinstance(self._display_host, str) and '0.0.0.0' in url:
                                        url = url.replace('0.0.0.0', self._display_host)
                                except Exception:
                                    pass
                                policy = getattr(e, 'SecurityPolicyUri', getattr(e, 'securityPolicyUri', ''))
                                mode = getattr(e, 'securityMode', '')
                                print(f"  URL: {url}  Policy: {policy}  Mode: {mode}")
                            except Exception:
                                print(f"  endpoint: {e}")
                    except Exception:
                        # 若 server.get_endpoints 不可用，嘗試印出 product/application uri
                        try:
                            print(f"OPC UA application uri: {getattr(self._server, 'application_uri', 'N/A')}")
                        except Exception:
                            pass
                except socket.gaierror as e:
                    # host 名稱解析錯誤等網路層問題，記錄並安全地停止此執行緒
                    print(f"OPC UA 無法啟動伺服器: {e}")
                    return
                self._is_running = True
                while self._is_running:
                    time.sleep(0.5)
            finally:
                try:
                    self._server.stop()
                except Exception:
                    pass

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

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
        var = folder.add_variable(self._nsidx, name, 0)
        var.set_writable(True)

        # 嘗試根據 dtype 設定典型 UA 型別（簡易對應）
        try:
            if 'float' in dtype:
                var.set_data_type(ua.NodeId(ua.ObjectIds.Float))
            elif 'double' in dtype:
                var.set_data_type(ua.NodeId(ua.ObjectIds.Double))
            else:
                var.set_data_type(ua.NodeId(ua.ObjectIds.Int16))
        except Exception:
            pass

        # store node and type for later coercion
        self._nodes[tid] = {"node": var, "type": dtype}
        return var

    def setup_tags_from_config(self, devices_config):
        """Create OPC UA tree from a configuration list.

        devices_config: iterable of devices, each device is dict with keys:
            'name' and 'tags' (list of tag dicts with 'name','id','data_type')

        Resulting structure: Objects -> DeviceName -> (optional GroupName) -> Tag
        """
        if not self._server:
            raise RuntimeError("Server not started")
        for dev in devices_config:
            dev_name = dev.get('name') or 'Device'
            dev_node = self._objects.add_object(self._nsidx, dev_name)
            tags = dev.get('tags') or []
            for tag in tags:
                tag_id = tag.get('id') or f"{dev_name}.{tag.get('name')}"
                tag_name = tag.get('name') or tag_id
                dtype = tag.get('data_type') or 'Int'
                node = dev_node.add_variable(self._nsidx, tag_name, 0)
                node.set_writable(True)
                try:
                    if 'float' in str(dtype).lower():
                        node.set_data_type(ua.NodeId(ua.ObjectIds.Float))
                    elif 'double' in str(dtype).lower():
                        node.set_data_type(ua.NodeId(ua.ObjectIds.Double))
                    else:
                        node.set_data_type(ua.NodeId(ua.ObjectIds.Int16))
                except Exception:
                    pass
                self._nodes[tag_id] = {"node": node, "type": dtype}

    def setup_tags_from_tree(self, conn_root_item):
        """Walk a PyQt tree `conn_root_item` (Connectivity root) and create nodes.

        Expects tree items to use the same UserRole convention as the UI: Tag
        items have role 'Tag', parent Device items have role 'Device'. The
        canonical id used is 'DeviceName[.GroupName].TagName'.
        """
        if not self._server:
            raise RuntimeError("Server not started")
        try:
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

                            # if array_len present, create an array initial value
                            try:
                                if array_len and int(array_len) > 0:
                                    # create list of zeros with appropriate type
                                    base = dtype or 'Int'
                                    dt = str(base).lower()
                                    if 'float' in dt or 'double' in dt:
                                        init_val = [0.0] * int(array_len)
                                    else:
                                        init_val = [0] * int(array_len)
                                    node = dev_node.add_variable(self._nsidx, tag_name, init_val)
                                else:
                                    node = dev_node.add_variable(self._nsidx, tag_name, 0)
                                node.set_writable(True)
                                try:
                                    if 'float' in str(dtype).lower():
                                        node.set_data_type(ua.NodeId(ua.ObjectIds.Float))
                                    elif 'double' in str(dtype).lower():
                                        node.set_data_type(ua.NodeId(ua.ObjectIds.Double))
                                    else:
                                        node.set_data_type(ua.NodeId(ua.ObjectIds.Int16))
                                except Exception:
                                    pass
                            except Exception:
                                node = dev_node.add_variable(self._nsidx, tag_name, 0)
                                node.set_writable(True)

                            self._nodes[tag_id] = {"node": node, "type": dtype}
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
                                if array_len and int(array_len) > 0:
                                    if 'float' in str(dtype).lower() or 'double' in str(dtype).lower():
                                        init_val = [0.0] * int(array_len)
                                    else:
                                        init_val = [0] * int(array_len)
                                    node = grp_node.add_variable(self._nsidx, tag_name, init_val)
                                else:
                                    node = grp_node.add_variable(self._nsidx, tag_name, 0)
                                node.set_writable(True)
                                try:
                                    if 'float' in str(dtype).lower():
                                        node.set_data_type(ua.NodeId(ua.ObjectIds.Float))
                                    elif 'double' in str(dtype).lower():
                                        node.set_data_type(ua.NodeId(ua.ObjectIds.Double))
                                    else:
                                        node.set_data_type(ua.NodeId(ua.ObjectIds.Int16))
                                except Exception:
                                    pass
                                self._nodes[tag_id] = {"node": node, "type": dtype}
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
