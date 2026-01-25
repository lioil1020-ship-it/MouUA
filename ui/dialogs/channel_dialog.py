import sys, os
# Allow running this dialog file directly from the project folder.
# When executed directly, ensure the project root is on sys.path so
# imports like `ui.widgets.form_builder` resolve correctly.
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import serial.tools.list_ports
import psutil
import socket
from PyQt6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QTabWidget,
    QWidget,
    QLineEdit,
    QComboBox,
    QLabel,
    QPushButton,
)
from ui.components import FormBuilder
from ..theme import FORM_FIELD_STYLE
from collections import OrderedDict


class ChannelDialog(QDialog):
    def __init__(self, parent=None, suggested_name="Channel1"):
        super().__init__(parent)
        self.setWindowTitle("Channel Properties")
        self.setMinimumSize(600, 500)
        self.setStyleSheet(FORM_FIELD_STYLE)

        main_layout = QVBoxLayout(self)
        self.tabs = QTabWidget()

        # 1. General 頁面 (包含 Name 與 Description)
        self.tab_ident = QWidget()
        id_lay = QVBoxLayout(self.tab_ident)

        self.name_edit = QLineEdit(suggested_name)
        id_lay.addWidget(QLabel("Channel Name:"))
        id_lay.addWidget(self.name_edit)

        # ✨ 新增單行描述欄位（放在 General 頁面，依配置樹順序）
        self.desc_edit = QLineEdit()
        id_lay.addWidget(QLabel("Description:"))
        id_lay.addWidget(self.desc_edit)

        # 2. Driver 頁面
        self.tab_driver = QWidget()
        drv_lay = QVBoxLayout(self.tab_driver)
        self.driver_combo = QComboBox()
        self.driver_combo.addItems(
            ["Modbus RTU Serial", "Modbus RTU over TCP", "Modbus TCP/IP Ethernet"]
        )
        drv_lay.addWidget(QLabel("Select Driver:"))
        drv_lay.addWidget(self.driver_combo)
        # Driver-specific settings builder (e.g. IP, Port, Protocol)
        self.driver_builder = FormBuilder()
        drv_lay.addWidget(self.driver_builder)
        drv_lay.addStretch()


        id_lay.addStretch()
        
        # 3. Communication 頁面
        self.tab_comm = QWidget()
        comm_lay = QVBoxLayout(self.tab_comm)
        self.builder = FormBuilder()
        comm_lay.addWidget(self.builder)

        # 調整分頁順序：General -> Driver -> Communication
        self.tabs.addTab(self.tab_ident, "General")
        self.tabs.addTab(self.tab_driver, "Driver")
        self.tabs.addTab(self.tab_comm, "Communication")
        main_layout.addWidget(self.tabs)

        # 按鈕佈局
        btns = QHBoxLayout()
        self.btn_finish = QPushButton("Finish")
        self.btn_cancel = QPushButton("Cancel")
        btns.addStretch()
        btns.addWidget(self.btn_finish)
        btns.addWidget(self.btn_cancel)
        main_layout.addLayout(btns)

        # 事件連結
        self.driver_combo.currentIndexChanged.connect(self._update_comm_fields)
        self.btn_finish.clicked.connect(self.accept)
        self.btn_cancel.clicked.connect(self.reject)

        # 初始化欄位
        self._update_comm_fields()

    def _get_available_ports(self):
        # 🔍 自動搜尋目前電腦上的 COM Ports
        ports = [p.device for p in serial.tools.list_ports.comports()]
        return ports if ports else ["COM1"]

    def _get_network_adapters(self):
        # 🌐 自動搜尋目前電腦上的 Network Adapters
        adapters = []
        try:
            interfaces = psutil.net_if_addrs()
            for name, snics in interfaces.items():
                for snic in snics:
                    if snic.family == socket.AF_INET:
                        adapters.append(f"{name} ({snic.address})")
        except Exception:
            pass
        # 如果沒找到任何實體網卡，至少提供一個自動偵測項（Auto - <ip>）
        if not adapters:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.settimeout(0.2); s.connect(('8.8.8.8', 80)); ip = s.getsockname()[0]; s.close()
            except Exception:
                ip = '127.0.0.1'
            adapters = [f"Auto - {ip}"]
        return adapters

    def _update_comm_fields(self):
        # 根據選擇的 Driver 動態更新 Communication 頁面
        self.builder.clear_form()
        # driver-specific settings
        self.driver_builder.clear_form()
        self.driver_builder.setVisible(False)
        driver = self.driver_combo.currentText()

        if driver == "Modbus RTU Serial":
            # ✨ 使用自動偵測的 Ports 清單
            ports = self._get_available_ports()
            self.builder.add_field(
                "com", "COM ID:", "combo", options=ports, default=ports[0]
            )

            self.builder.add_field(
                "baud",
                "Baud Rate:",
                "combo",
                options=["4800", "9600", "19200", "38400", "57600", "115200"],
                default="9600",
            )
            self.builder.add_field(
                "data_bits",
                "Data Bits:",
                "combo",
                options=["5", "6", "7", "8"],
                default="8",
            )
            self.builder.add_field(
                "parity",
                "Parity:",
                "combo",
                options=["None", "Odd", "Even"],
                default="None",
            )
            self.builder.add_field(
                "stop",
                "Stop Bits:",
                "combo",
                options=["1", "2"],
                default="1",
            )
            self.builder.add_field(
                "flow",
                "Flow Control:",
                "combo",
                options=[
                    "None",
                    "DTR",
                    "RTS",
                    "RTS/DTR",
                    "RTS Always",
                    "RTS Manual",
                ],
                default="None",
            )
        else:
            # ✨ 使用自動偵測的網卡清單
            adapters = self._get_network_adapters()
            # default to first detected adapter
            default_choice = adapters[0] if adapters else ''
            # display as 'Interface (IP)' for clarity
            self.builder.add_field(
                "adapter", "Network Adapter:", "combo", options=adapters, default=default_choice
            )
            # show driver-level ethernet settings for TCP drivers
            if driver in ("Modbus RTU over TCP", "Modbus TCP/IP Ethernet"):
                # 將 IP, Port, Protocol 移到 Driver 分頁下
                self.driver_builder.add_field("ip", "IP Address:", "text", default="127.0.0.1")
                self.driver_builder.add_field("port", "Port:", "text", default="502")
                self.driver_builder.add_field(
                    "protocol", "Protocol:", "combo", options=["TCP/IP", "UDP"], default="TCP/IP"
                )
                self.driver_builder.setVisible(True)

    def load_data(self, data):
        # 載入舊資料，包含描述內容
        if not data:
            return
        # Accept nested structure: {'general': {...}, 'driver': {...}, 'communication': {...}}
        general = data.get("general") if isinstance(data.get("general"), dict) else None
        if general:
            name = general.get("channel_name") or general.get("name") or ""
            desc = general.get("description") or ""
            self.name_edit.setText(str(name))
            self.desc_edit.setText(str(desc))
        else:
            self.name_edit.setText(data.get("name", ""))
            self.desc_edit.setText(data.get("description", ""))

        # Driver: support flat string or nested dict {'type':..., 'params': {...}}
        driver_section = data.get("driver")
        if isinstance(driver_section, dict):
            dtype = driver_section.get("type")
            if dtype:
                idx = self.driver_combo.findText(str(dtype))
                if idx >= 0:
                    self.driver_combo.setCurrentIndex(idx)
            # driver-level params
            try:
                dp = driver_section.get("params") or {}
                if isinstance(dp, dict):
                    self.driver_builder.set_values(dp)
            except Exception:
                pass
        else:
            idx = self.driver_combo.findText(data.get("driver", ""))
            if idx >= 0:
                self.driver_combo.setCurrentIndex(idx)

        # Communication params: prefer explicit 'communication', then driver.params, then top-level 'params'
        comm = data.get("communication") if isinstance(data.get("communication"), dict) else None
        if comm is not None:
            try:
                self.builder.set_values(comm)
            except Exception:
                pass
        else:
            # fallback to driver.params if present
            if isinstance(driver_section, dict) and isinstance(driver_section.get("params"), dict):
                try:
                    self.builder.set_values(driver_section.get("params") or {})
                except Exception:
                    pass
            else:
                if "params" in data:
                    try:
                        self.builder.set_values(data["params"])
                    except Exception:
                        pass

    def get_data(self):
        # 回傳當前設定
        params = {}
        try:
            params.update(self.driver_builder.get_values())
        except Exception:
            pass
        try:
            params.update(self.builder.get_values())
        except Exception:
            pass
        # For compatibility: return flat structure as before, and also
        # provide nested keys matching the configuration tree.
        # split params into driver-level and comm-level where possible
        driver_params = {}
        comm_params = {}
        try:
            driver_params = self.driver_builder.get_values()
        except Exception:
            driver_params = {}
        try:
            comm_params = self.builder.get_values()
        except Exception:
            comm_params = {}

        flat = {
            "name": self.name_edit.text(),
            "description": self.desc_edit.text(),
            "driver": self.driver_combo.currentText(),
            "params": {**driver_params, **comm_params},
        }

        nested = OrderedDict([
            ("general", {"channel_name": self.name_edit.text(), "description": self.desc_edit.text()}),
            ("driver", OrderedDict([("type", self.driver_combo.currentText()), ("params", driver_params)])),
            ("communication", comm_params),
        ])

        # merge flat and nested for backward compatibility
        out = {**flat, **nested}
        return out
