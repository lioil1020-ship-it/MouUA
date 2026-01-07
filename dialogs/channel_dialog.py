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
from ui.widgets.form_builder import FormBuilder


class ChannelDialog(QDialog):
    def __init__(self, parent=None, suggested_name="Channel1"):
        super().__init__(parent)
        self.setWindowTitle("Channel Properties")
        self.setMinimumSize(600, 500)

        # 使表單欄位高度與主介面表格行高一致
        try:
            row_h = 22
            self.setStyleSheet(f"QLineEdit, QComboBox, QSpinBox {{ min-height: {row_h}px; max-height: {row_h}px; }} QLabel {{ min-height: {row_h}px; max-height: {row_h}px; }}")
        except Exception:
            pass

        main_layout = QVBoxLayout(self)
        self.tabs = QTabWidget()

        # 1. Driver 頁面
        self.tab_driver = QWidget()
        drv_lay = QVBoxLayout(self.tab_driver)
        self.driver_combo = QComboBox()
        self.driver_combo.addItems(
            ["Modbus RTU Serial", "Modbus RTU over TCP", "Modbus TCP/IP Ethernet"]
        )
        drv_lay.addWidget(QLabel("Select Driver:"))
        drv_lay.addWidget(self.driver_combo)
        drv_lay.addStretch()

        # 2. Identification 頁面 (包含 Name 與 Description)
        self.tab_ident = QWidget()
        id_lay = QVBoxLayout(self.tab_ident)

        self.name_edit = QLineEdit(suggested_name)
        id_lay.addWidget(QLabel("Channel Name:"))
        id_lay.addWidget(self.name_edit)

        # ✨ 新增單行描述欄位
        self.desc_edit = QLineEdit()
        id_lay.addWidget(QLabel("Description:"))
        id_lay.addWidget(self.desc_edit)

        id_lay.addStretch()

        # 3. Communication 頁面
        self.tab_comm = QWidget()
        comm_lay = QVBoxLayout(self.tab_comm)
        self.builder = FormBuilder()
        comm_lay.addWidget(self.builder)

        self.tabs.addTab(self.tab_driver, "Driver")
        self.tabs.addTab(self.tab_ident, "Identification")
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
        """🔍 自動搜尋目前電腦上的 COM Ports"""
        ports = [p.device for p in serial.tools.list_ports.comports()]
        return ports if ports else ["COM1"]

    def _get_network_adapters(self):
        """🌐 自動搜尋目前電腦上的 Network Adapters"""
        adapters = ["Default"]
        try:
            interfaces = psutil.net_if_addrs()
            for name, snics in interfaces.items():
                for snic in snics:
                    if snic.family == socket.AF_INET:
                        adapters.append(f"{snic.address} - {name}")
        except Exception:
            pass
        return adapters

    def _update_comm_fields(self):
        """根據選擇的 Driver 動態更新 Communication 頁面"""
        self.builder.clear_form()
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
            self.builder.add_field(
                "adapter", "Network Adapter:", "combo", options=adapters, default="Default"
            )

    def load_data(self, data):
        """載入舊資料，包含描述內容"""
        if not data:
            return
        self.name_edit.setText(data.get("name", ""))
        self.desc_edit.setText(data.get("description", ""))
        idx = self.driver_combo.findText(data.get("driver", ""))
        if idx >= 0:
            self.driver_combo.setCurrentIndex(idx)
        if "params" in data:
            self.builder.set_values(data["params"])

    def get_data(self):
        """回傳當前設定"""
        return {
            "name": self.name_edit.text(),
            "description": self.desc_edit.text(),
            "driver": self.driver_combo.currentText(),
            "params": self.builder.get_values(),
        }