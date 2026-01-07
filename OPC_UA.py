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
from typing import Dict, Any

try:
    from opcua import ua, Server
except Exception:
    Server = None


class OPCServer:
    def __init__(self, config: Dict[str, Any] = None):
        """config 預期為 opcua_dialog.values() 的字典。"""
        self.config = config or {}
        self._server = None
        self._is_running = False
        self._thread = None
        self._nodes = {}  # mapping from tag id -> node

    def _prepare_server(self):
        if Server is None:
            raise RuntimeError("python-opcua not installed. install with 'pip install opcua'")

        self._server = Server()
        # 應用設定
        app_name = self.config.get('application_Name') or self.config.get('server_name') or 'ModUA'
        host = self.config.get('host_name', '0.0.0.0')
        port = int(self.config.get('port', 4848))
        namespace = self.config.get('namespace', app_name)

        self._server.set_endpoint(f"opc.tcp://{host}:{port}")
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
                self._server.start()
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

        self._nodes[tid] = var
        return var

    def update_tag(self, tag_id, value):
        node = self._nodes.get(tag_id)
        if not node:
            return False
        try:
            node.set_value(value)
            return True
        except Exception:
            return False


if __name__ == '__main__':
    # 簡單 demo
    cfg = {'application_Name': 'ModUA', 'host_name': '127.0.0.1', 'port': '4848', 'namespace': 'ModUA'}
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
