from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QTabWidget, QWidget,
    QLineEdit, QSpinBox, QLabel, QPushButton,
)
from PyQt6.QtCore import Qt
from ui.components import FormBuilder
from ..theme import FORM_FIELD_STYLE


class DeviceDialog(QDialog):
    def __init__(self, parent=None, suggested_name="", driver_type="Modbus RTU Serial"):
        super().__init__(parent)
        self.driver_type = str(driver_type)
        self.setWindowTitle("Device Properties")
        self.setMinimumSize(600, 550)
        self.setStyleSheet(FORM_FIELD_STYLE)

        # --- 根據 Driver 字串判斷顯示邏輯 ---
        self.is_serial = self.driver_type == "Modbus RTU Serial"
        self.is_over_tcp = self.driver_type == "Modbus RTU over TCP"
        self.is_ethernet = self.driver_type == "Modbus TCP/IP Ethernet"

        main_layout = QVBoxLayout(self)
        self.tabs = QTabWidget()

        # 1. General
        self._setup_general_tab(suggested_name)

        # 2. Timing (預設值參考邏輯圖)
        self._setup_timing_tab()

        # 3. DataAccess (預設值參考邏輯圖)
        self._setup_access_tab()

        # 4. Data Encoding (預設值參考邏輯圖)
        self._setup_encoding_tab()

        # 5. Block Sizes (預設值參考圖)
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

    def _setup_general_tab(self, suggested_name):
        # 對應邏輯圖：Device Name, Description, Device ID
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
        self.tabs.addTab(self.tab_ident, "General")

    # Ethernet settings are now handled at Channel/Driver level

    def _setup_timing_tab(self):
        # 對應邏輯圖：Request Timeout, Attempts, Inter-Request Delay
        self.tab_timing = QWidget()
        lay = QVBoxLayout(self.tab_timing)
        self.timing_builder = FormBuilder()

        if self.is_over_tcp or self.is_ethernet:
            self.timing_builder.add_field(
                "connect_timeout", "Connect Timeout (s):", "text", default="3"
            )

        if self.is_over_tcp:
            self.timing_builder.add_field("connect_attempts", "Connect Attempts:", "text", default="1")

        # 預設值修正：Attempts 改為 1
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
        # 對應邏輯圖 DataAccess 節點
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
        # 對應邏輯圖 Data Encoding 節點
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
            "word_order",
            "First Word Low:",
            "combo",
            options=["Enable", "Disable"],
            default="Enable",
        )
        self.encoding_builder.add_field(
            "dword_order",
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
            "treat_longs_as_decimals",
            "Treat Longs as Decimals:",
            "combo",
            options=["Enable", "Disable"],
            default="Disable",
        )

        lay.addWidget(self.encoding_builder)
        self.tabs.addTab(self.tab_encoding, "DataEncoding")

    def _setup_blocks_tab(self):
        # 對應邏輯圖 Block Sizes 節點
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
        # Support both flat structure and nested {'general': {...}} for compatibility.
        general = None
        if isinstance(data.get("general"), dict):
            general = data.get("general")
        else:
            # fallback to flat keys
            general = {"name": data.get("name", ""), "description": data.get("description", ""), "device_id": data.get("device_id")}

        self.name_edit.setText(general.get("name", ""))
        self.desc_edit.setText(general.get("description", ""))
        # If caller didn't provide a device_id, ask parent.controller for a suggestion
        # device id may be provided under general or top-level
        device_id = general.get("device_id") if general is not None else data.get("device_id")
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

        # Load per-tab data: prefer top-level keys, fall back to values nested under `general`.
        timing = data.get("timing") if data.get("timing") is not None else (general.get("timing") if isinstance(general, dict) and general.get("timing") is not None else None)
        if timing is not None:
            self.timing_builder.set_values(timing)

        access = data.get("data_access") if data.get("data_access") is not None else (general.get("data_access") if isinstance(general, dict) and general.get("data_access") is not None else None)
        if access is not None:
            # Convert numeric values to string representation for combo boxes
            access_display = {}
            if isinstance(access, dict):
                for k, v in access.items():
                    # Convert 1/0 to "Enable"/"Disable"
                    if k in ['zero_based', 'zero_based_bit', 'bit_writes', 'func_06', 'func_05']:
                        if v in (1, '1', 'enable', 'Enable', 'true', 'True'):
                            access_display[k] = 'Enable'
                        elif v in (0, '0', 'disable', 'Disable', 'false', 'False'):
                            access_display[k] = 'Disable'
                        else:
                            access_display[k] = str(v)
                    else:
                        access_display[k] = str(v)
            else:
                access_display = access
            self.access_builder.set_values(access_display)

        enc = data.get("encoding") if data.get("encoding") is not None else (general.get("encoding") if isinstance(general, dict) and general.get("encoding") is not None else None)
        if enc is not None:
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Loading encoding data (raw): {enc}")
            
            # Convert numeric values to string representation for combo boxes
            enc_display = {}
            if isinstance(enc, dict):
                for k, v in enc.items():
                    # Convert 1/0 to "Enable"/"Disable"
                    if k in ['byte_order', 'word_order', 'dword_order', 'bit_order', 'treat_longs_as_decimals']:
                        if v in (1, '1', 'enable', 'Enable', 'true', 'True'):
                            enc_display[k] = 'Enable'
                        elif v in (0, '0', 'disable', 'Disable', 'false', 'False'):
                            enc_display[k] = 'Disable'
                        else:
                            enc_display[k] = str(v)
                    else:
                        enc_display[k] = str(v)
            
            logger.debug(f"Loading encoding data (display): {enc_display}")
            self.encoding_builder.set_values(enc_display)

        blocks = data.get("block_sizes") if data.get("block_sizes") is not None else (general.get("block_sizes") if isinstance(general, dict) and general.get("block_sizes") is not None else None)
        if blocks is not None:
            self.block_builder.set_values(blocks)

        # Ethernet params moved to Channel; Device no longer manages them

    def get_data(self):
        # Return both flat and nested structures for compatibility
        nested = {
            "general": {
                "name": self.name_edit.text(),
                "description": self.desc_edit.text(),
                "device_id": self.id_spin.value(),
            },
            "timing": self.timing_builder.get_values(),
            "data_access": self.access_builder.get_values(),
            "encoding": self.encoding_builder.get_values(),
            # normalize block sizes to integers and canonical keys
            "block_sizes": self._normalize_block_sizes(self.block_builder.get_values()),
        }
        flat = {
            "name": self.name_edit.text(),
            "description": self.desc_edit.text(),
            "device_id": self.id_spin.value(),
            "timing": nested["timing"],
            "data_access": nested["data_access"],
            "encoding": nested["encoding"],
            "block_sizes": nested["block_sizes"],
        }
        # ethernet moved to Channel/Driver; Device no longer returns ethernet settings
        result = {**flat, **nested}
        import logging
        logger = logging.getLogger(__name__)
        logger.debug(f"DeviceDialog.get_data() returning encoding: {result.get('encoding')}")
        return result

    def _normalize_block_sizes(self, raw):
        # Coerce the block sizes dict values to ints and keep canonical keys.
        # Accepts dicts with keys like 'out_coils','in_coils','int_regs','hold_regs'
        # and will return a dict with those keys and integer values where possible.
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
