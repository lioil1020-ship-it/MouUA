from PyQt6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QTabWidget,
    QWidget,
    QLineEdit,
    QSpinBox,
    QLabel,
    QPushButton,
)
from PyQt6.QtCore import Qt
from ui.widgets.form_builder import FormBuilder


class DeviceDialog(QDialog):
    def __init__(self, parent=None, suggested_name="", driver_type="Modbus RTU Serial"):
        super().__init__(parent)
        self.driver_type = str(driver_type)
        self.setWindowTitle("Device Properties")
        self.setMinimumSize(600, 550)

        # 使表單欄位高度與主介面表格行高一致
        try:
            row_h = 22
            self.setStyleSheet(f"QLineEdit, QComboBox, QSpinBox {{ min-height: {row_h}px; max-height: {row_h}px; }} QLabel {{ min-height: {row_h}px; max-height: {row_h}px; }}")
        except Exception:
            pass

        # --- 根據 Driver 字串判斷顯示邏輯 ---
        self.is_serial = self.driver_type == "Modbus RTU Serial"
        self.is_over_tcp = self.driver_type == "Modbus RTU over TCP"
        self.is_ethernet = self.driver_type == "Modbus TCP/IP Ethernet"

        main_layout = QVBoxLayout(self)
        self.tabs = QTabWidget()

        # 1. Identification
        self._setup_identification_tab(suggested_name)

        # 2. Ethernet Settings (僅 TCP 相關驅動顯示)
        if self.is_over_tcp or self.is_ethernet:
            self._setup_ethernet_settings_tab()

        # 3. Timing (預設值參考邏輯圖)
        self._setup_timing_tab()

        # 4. DataAccess (預設值參考邏輯圖)
        self._setup_access_tab()

        # 5. Data Encoding (預設值參考邏輯圖)
        self._setup_encoding_tab()

        # 6. Block Sizes (預設值參考邏輯圖)
        self._setup_blocks_tab()

        main_layout.addWidget(self.tabs)

        # 按鈕列
        btns = QHBoxLayout()
        self.btn_finish = QPushButton("Finish")
        self.btn_cancel = QPushButton("Cancel")
        btns.addStretch()
        btns.addWidget(self.btn_finish)
        btns.addWidget(self.btn_cancel)
        main_layout.addLayout(btns)

        self.btn_finish.clicked.connect(self.accept)
        self.btn_cancel.clicked.connect(self.reject)

    def _setup_identification_tab(self, suggested_name):
        """對應邏輯圖：Device Name, Description, Device ID"""
        self.tab_ident = QWidget()
        lay = QVBoxLayout(self.tab_ident)
        self.name_edit = QLineEdit(suggested_name)
        self.desc_edit = QLineEdit("")
        self.id_spin = QSpinBox()
        self.id_spin.setRange(1, 65535)
        self.id_spin.setValue(1)

        lay.addWidget(QLabel("Device Name:"))
        lay.addWidget(self.name_edit)
        lay.addWidget(QLabel("Description:"))
        lay.addWidget(self.desc_edit)
        lay.addWidget(QLabel("Device ID:"))
        lay.addWidget(self.id_spin)
        lay.addStretch()
        self.tabs.addTab(self.tab_ident, "Identification")

    def _setup_ethernet_settings_tab(self):
        """對應邏輯圖：IP Address, Port, Protocol (TCP/IP, UDP)"""
        self.tab_eth = QWidget()
        lay = QVBoxLayout(self.tab_eth)
        self.eth_builder = FormBuilder()
        self.eth_builder.add_field("ip", "IP Address:", "text", default="127.0.0.1")
        self.eth_builder.add_field("port", "Port:", "text", default="502")
        self.eth_builder.add_field(
            "protocol", "Protocol:", "combo", options=["TCP/IP", "UDP"], default="TCP/IP"
        )
        lay.addWidget(self.eth_builder)
        self.tabs.addTab(self.tab_eth, "Ethernet Settings")

    def _setup_timing_tab(self):
        """對應邏輯圖：Request Timeout, Attempts, Inter-Request Delay"""
        self.tab_timing = QWidget()
        lay = QVBoxLayout(self.tab_timing)
        self.timing_builder = FormBuilder()

        if self.is_over_tcp or self.is_ethernet:
            self.timing_builder.add_field(
                "connect_timeout", "Connect Timeout (s):", "text", default="3"
            )

        if self.is_over_tcp:
            self.timing_builder.add_field("connect_attempts", "Connect Attempts:", "text", default="1")

        # 預設值修正：Attempts 改為 3
        self.timing_builder.add_field(
            "req_timeout", "Request Timeout (ms):", "text", default="1000"
        )
        self.timing_builder.add_field(
            "attempts", "Attempts Before Timeout:", "text", default="1"
        )
        self.timing_builder.add_field(
            "inter_req_delay", "Inter-Request Delay (ms):", "text", default="0"
        )

        lay.addWidget(self.timing_builder)
        self.tabs.addTab(self.tab_timing, "Timing")

    def _setup_access_tab(self):
        """對應邏輯圖 DataAccess 節點"""
        self.tab_access = QWidget()
        lay = QVBoxLayout(self.tab_access)
        self.access_builder = FormBuilder()

        # 預設值修正：Addressing 改為 Disable
        self.access_builder.add_field(
            "zero_based",
            "Zero-Based Addressing:",
            "combo",
            options=["Enable", "Disable"],
            default="Disable",
        )
        self.access_builder.add_field(
            "zero_based_bit",
            "Zero-Based Bit Addressing:",
            "combo",
            options=["Enable", "Disable"],
            default="Enable",
        )
        self.access_builder.add_field(
            "bit_writes",
            "Holding Register Bit Writes:",
            "combo",
            options=["Enable", "Disable"],
            default="Disable",
        )
        self.access_builder.add_field(
            "func_06",
            "Modbus Function 06:",
            "combo",
            options=["Enable", "Disable"],
            default="Enable",
        )
        self.access_builder.add_field(
            "func_05",
            "Modbus Function 05:",
            "combo",
            options=["Enable", "Disable"],
            default="Enable",
        )

        lay.addWidget(self.access_builder)
        self.tabs.addTab(self.tab_access, "DataAccess")

    def _setup_encoding_tab(self):
        """對應邏輯圖 Data Encoding 節點"""
        self.tab_encoding = QWidget()
        lay = QVBoxLayout(self.tab_encoding)
        self.encoding_builder = FormBuilder()

        # 預設值修正：Bit Order 與 Treat Longs 改為 Disable
        self.encoding_builder.add_field(
            "byte_order",
            "Modbus Byte Order:",
            "combo",
            options=["Enable", "Disable"],
            default="Enable",
        )
        self.encoding_builder.add_field(
            "word_low",
            "First Word Low:",
            "combo",
            options=["Enable", "Disable"],
            default="Enable",
        )
        self.encoding_builder.add_field(
            "dword_low",
            "First Dword Low:",
            "combo",
            options=["Enable", "Disable"],
            default="Enable",
        )
        self.encoding_builder.add_field(
            "bit_order",
            "Modicon Bit Order:",
            "combo",
            options=["Enable", "Disable"],
            default="Disable",
        )
        self.encoding_builder.add_field(
            "treat_long",
            "Treat Longs as Decimals:",
            "combo",
            options=["Enable", "Disable"],
            default="Disable",
        )

        lay.addWidget(self.encoding_builder)
        self.tabs.addTab(self.tab_encoding, "DataEncoding")

    def _setup_blocks_tab(self):
        """對應邏輯圖 Block Sizes 節點"""
        self.tab_blocks = QWidget()
        lay = QVBoxLayout(self.tab_blocks)
        self.block_builder = FormBuilder()

        # 預設值修正：Coils=2000, Registers=120
        self.block_builder.add_field("out_coils", "Output Coils:", "text", default="2000")
        self.block_builder.add_field("in_coils", "Input Coils:", "text", default="2000")
        self.block_builder.add_field("int_regs", "Internal Registers:", "text", default="120")
        self.block_builder.add_field("hold_regs", "Holding Registers:", "text", default="120")

        lay.addWidget(self.block_builder)
        self.tabs.addTab(self.tab_blocks, "Block Sizes")

    def load_data(self, data):
        if not data:
            return
        self.name_edit.setText(data.get("name", ""))
        self.desc_edit.setText(data.get("description", ""))
        # If caller didn't provide a device_id, ask parent.controller for a suggestion
        device_id = data.get("device_id")
        if (
            not device_id
            and self.parent()
            and hasattr(self.parent(), "controller")
            and hasattr(self.parent(), "tree")
        ):
            current = self.parent().tree.currentItem()
            if current and current.data(0, Qt.ItemDataRole.UserRole) == "Channel":
                try:
                    device_id = self.parent().controller.calculate_next_id(current)
                except Exception:
                    device_id = 1

        self.id_spin.setValue(int(device_id or 1))

        if "timing" in data:
            self.timing_builder.set_values(data["timing"])
        if "data_access" in data:
            self.access_builder.set_values(data["data_access"])
        if "encoding" in data:
            self.encoding_builder.set_values(data["encoding"])
        if "block_sizes" in data:
            self.block_builder.set_values(data["block_sizes"])
        if "ethernet" in data and hasattr(self, "eth_builder"):
            self.eth_builder.set_values(data["ethernet"])

    def get_data(self):
        data = {
            "name": self.name_edit.text(),
            "description": self.desc_edit.text(),
            "device_id": self.id_spin.value(),
            "timing": self.timing_builder.get_values(),
            "data_access": self.access_builder.get_values(),
            "encoding": self.encoding_builder.get_values(),
            # normalize block sizes to integers and canonical keys
            "block_sizes": self._normalize_block_sizes(self.block_builder.get_values()),
        }
        if hasattr(self, "eth_builder"):
            data["ethernet"] = self.eth_builder.get_values()
        return data

    def _normalize_block_sizes(self, raw):
        """Coerce the block sizes dict values to ints and keep canonical keys.

        Accepts dicts with keys like 'out_coils','in_coils','int_regs','hold_regs'
        and will return a dict with those keys and integer values where possible.
        """
        out = {}
        try:
            if not raw:
                return out
            if isinstance(raw, dict):
                for k, v in raw.items():
                    lk = str(k).strip()
                    try:
                        if v is None or str(v).strip() == "":
                            continue
                        vi = int(float(str(v).strip()))
                    except Exception:
                        continue
                    # keep only expected keys
                    if lk in ("out_coils", "in_coils", "int_regs", "hold_regs"):
                        out[lk] = vi
                    else:
                        # try to map common alternative names
                        lk2 = lk.lower()
                        if "hold" in lk2:
                            out.setdefault("hold_regs", vi)
                        elif "int" in lk2 or "internal" in lk2 or "input" in lk2 and "reg" in lk2:
                            out.setdefault("int_regs", vi)
                        elif "out" in lk2 and "coil" in lk2:
                            out.setdefault("out_coils", vi)
                        elif "in" in lk2 and "coil" in lk2:
                            out.setdefault("in_coils", vi)
            # if caller passed a single numeric value, apply to registers
            elif isinstance(raw, (int, float)):
                v = int(raw)
                out = {"hold_regs": v, "int_regs": v}
            elif isinstance(raw, str) and raw.strip().isdigit():
                v = int(raw.strip())
                out = {"hold_regs": v, "int_regs": v}
        except Exception:
            return {}
        return out