from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLineEdit, QComboBox,
    QPushButton, QFormLayout, QTabWidget, QWidget, QFrame,
)
from PyQt6.QtGui import QIntValidator
from ..theme import FORM_FIELD_STYLE
from core.config import (
    MODBUS_ADDRESS_OFFSET,
    MODBUS_SEQUENCE_WIDTH,
    MODBUS_HOLDING_REG_PREFIX,
    MODBUS_INPUT_REG_PREFIX,
    MODBUS_COIL_PREFIX,
    MODBUS_DISCRETE_PREFIX,
)


class TagDialog(QDialog):
    def __init__(self, parent=None, suggested_name="Tag1", suggested_addr="400000", target_item=None, is_new=True):
        super().__init__(parent)
        self.setWindowTitle("Tag Properties")
        self.setMinimumSize(480, 580)
        self.setStyleSheet(FORM_FIELD_STYLE)
        
        # 儲存目標 item（用於計算下一個地址）
        self.target_item = target_item
        
        # 是否為新增標籤（不只是根據 suggested_addr 是否為 None）
        self._is_new = is_new

        # 暫存 Register 類型清單，用於 UI 邏輯判斷
        self.register_types = [
            "Word",
            "Short",
            "Long",
            "DWord",
            "Float",
            "Double",
            "BCD",
            "LBCD",
            "LLong",
            "QWord",
            "Char",
            "Byte",
            "String",
        ]

        main_layout = QVBoxLayout(self)
        self.tabs = QTabWidget()

        # 1. General 分頁
        self.tab_general = QWidget()
        gen_lay = QFormLayout(self.tab_general)

        # Ensure suggested values are strings (QLineEdit expects str)
        self.name_edit = QLineEdit(str(suggested_name) if suggested_name is not None else "")
        self.desc_edit = QLineEdit("")

        # 1-1. Data Type 下拉選單
        self.type_combo = QComboBox()
        all_types = []
        # 加入 Boolean
        all_types.extend(["Boolean", "Boolean(Array)"])
        # 加入所有 Register 類型及其 Array 版本
        for t in self.register_types:
            all_types.append(t)
            all_types.append(f"{t}(Array)")
        self.type_combo.addItems(all_types)
        self.type_combo.setCurrentText("Word")

        # 1-2. Client Access 下拉選單
        self.access_combo = QComboBox()
        self.access_combo.addItems(["Read/Write", "Read Only"])

        # 1-3. Address 欄位
        # Address may be provided as int by callers; coerce to string
        self.addr_edit = QLineEdit(str(suggested_addr) if suggested_addr is not None else "")

        # 1-4. Scan Rate (default centralized here)
        self.scan_rate = QLineEdit("10")
        self.scan_rate.setValidator(QIntValidator(1, 600000))

        gen_lay.addRow("Tag Name:", self.name_edit)
        gen_lay.addRow("Description:", self.desc_edit)
        gen_lay.addRow("Data Type:", self.type_combo)
        gen_lay.addRow("Client Access:", self.access_combo)
        gen_lay.addRow("Address:", self.addr_edit)
        gen_lay.addRow("Scan Rate (ms):", self.scan_rate)

        # 2. Scaling 分頁
        self.tab_scaling = QWidget()
        scaling_layout_container = QVBoxLayout(self.tab_scaling)

        type_form = QFormLayout()
        self.scale_type = QComboBox()
        self.scale_type.addItems(["None", "Linear", "Square Root"])
        type_form.addRow("Scaling Type:", self.scale_type)
        scaling_layout_container.addLayout(type_form)

        # Scaling 參數容器 (可隱藏)
        self.scaling_params_frame = QFrame()
        self.params_layout = QFormLayout(self.scaling_params_frame)

        self.raw_low = QLineEdit("0")
        self.raw_high = QLineEdit("1000")
        self.scaled_low = QLineEdit("0.0")
        self.scaled_high = QLineEdit("100.0")
        self.scaled_type = QComboBox()
        self.scaled_type.addItems(
            ["Char", "Byte", "Short", "Word", "Long", "DWord", "Float", "Double"]
        )
        self.scaled_type.setCurrentText("Float")
        self.clamp_low = QComboBox()
        self.clamp_low.addItems(["No", "Yes"])
        self.clamp_low.setCurrentText("No")
        self.clamp_high = QComboBox()
        self.clamp_high.addItems(["No", "Yes"])
        self.clamp_high.setCurrentText("No")
        self.negate = QComboBox()
        self.negate.addItems(["No", "Yes"])
        self.negate.setCurrentText("No")
        self.units = QLineEdit("")

        self.params_layout.addRow("Raw Low:", self.raw_low)
        self.params_layout.addRow("Raw High:", self.raw_high)
        self.params_layout.addRow("Scaled Data Type:", self.scaled_type)
        self.params_layout.addRow("Scaled Low:", self.scaled_low)
        self.params_layout.addRow("Scaled High:", self.scaled_high)
        self.params_layout.addRow("Clamp Low:", self.clamp_low)
        self.params_layout.addRow("Clamp High:", self.clamp_high)
        self.params_layout.addRow("Negate Value:", self.negate)
        self.params_layout.addRow("Units:", self.units)

        scaling_layout_container.addWidget(self.scaling_params_frame)
        scaling_layout_container.addStretch()

        # 加入 Tabs
        self.tabs.addTab(self.tab_general, "General")
        self.tabs.addTab(self.tab_scaling, "Scaling")
        main_layout.addWidget(self.tabs)

        # --- 按鈕列 ---
        btns = QHBoxLayout()
        self.btn_ok = QPushButton("OK")
        self.btn_cancel = QPushButton("Cancel")
        btns.addStretch()
        btns.addWidget(self.btn_ok)
        btns.addWidget(self.btn_cancel)
        main_layout.addLayout(btns)

        # --- 信號與槽 ---
        self.btn_ok.clicked.connect(self.accept)
        self.btn_cancel.clicked.connect(self.reject)

        # 關鍵 UI 邏輯：Data Type 或 Access 改變時自動修正位址與 Scaling 權限
        self.type_combo.currentTextChanged.connect(self._update_modbus_logic)
        self.access_combo.currentTextChanged.connect(self._update_modbus_logic)

        # Scaling 顯示/隱藏切換
        self.scale_type.currentTextChanged.connect(self._toggle_scaling_visibility)

        # 初始化執行一次
        self._update_modbus_logic()
        # 根據目前 scaling 值設定初始可見性
        self._toggle_scaling_visibility(self.scale_type.currentText())

    def _update_modbus_logic(self):
        # 核心防錯邏輯：自動根據 Modbus 標準更換位址首位
        data_type = self.type_combo.currentText()
        access = self.access_combo.currentText()
        addr_text = self.addr_edit.text()
        
        # 檢查是否為 Array 型別
        is_array = "Array" in data_type
        
        # 從地址中提取陣列大小（如果存在）
        array_size = 1  # 預設
        try:
            import re
            match = re.search(r'\[(\d+)\]', addr_text)
            if match:
                array_size = int(match.group(1))
        except Exception:
            pass
        
        # 提取目前的流水號（取末 5 位，去掉陣列部分）
        try:
            # 移除 [n] 部分
            import re
            addr_without_array = re.sub(r'\[\d+\]', '', addr_text)
            # 只濾出數字部分
            nums = "".join(filter(str.isdigit, addr_without_array))
            # 取得數值後取 MODBUS_ADDRESS_OFFSET 的餘數，保留原有的序號
            offset = int(nums) % MODBUS_ADDRESS_OFFSET if nums else 0
        except ValueError:
            offset = 0

        # --- 判斷位址首位與 Scaling 權限 ---
        if "Boolean" in data_type:
            # 1. Boolean 邏輯 (Coils/Inputs)
            prefix = MODBUS_COIL_PREFIX if access == "Read/Write" else MODBUS_DISCRETE_PREFIX
            # If creating a new tag, ask controller for per-prefix next address
            if self._is_new and self.parent() and hasattr(self.parent(), "controller"):
                try:
                    target = self.target_item if hasattr(self, 'target_item') and self.target_item else None
                    if target:
                        nxt = self.parent().controller.calculate_next_address(target, prefix=prefix, new_type=data_type)
                        if isinstance(nxt, str):
                            base_addr = nxt
                        else:
                            base_addr = f"{prefix}{int(nxt):0{MODBUS_SEQUENCE_WIDTH}d}"
                        # 如果是 Array，添加 [size]
                        if is_array:
                            base_addr = f"{base_addr} [{array_size}]"
                        self.addr_edit.setText(base_addr)
                    else:
                        base_addr = f"{prefix}{offset:0{MODBUS_SEQUENCE_WIDTH}d}"
                        if is_array:
                            base_addr = f"{base_addr} [{array_size}]"
                        self.addr_edit.setText(base_addr)
                except Exception:
                    base_addr = f"{prefix}{offset:0{MODBUS_SEQUENCE_WIDTH}d}"
                    if is_array:
                        base_addr = f"{base_addr} [{array_size}]"
                    self.addr_edit.setText(base_addr)
            else:
                base_addr = f"{prefix}{offset:0{MODBUS_SEQUENCE_WIDTH}d}"
                if is_array:
                    base_addr = f"{base_addr} [{array_size}]"
                self.addr_edit.setText(base_addr)

            # 2. Boolean 禁用 Scaling
            self.scale_type.setCurrentText("None")
            self.scale_type.setEnabled(False)
        else:
            # 3. Register 邏輯 (Holding/Input Registers)
            prefix = MODBUS_HOLDING_REG_PREFIX if access == "Read/Write" else MODBUS_INPUT_REG_PREFIX
            if self._is_new and self.parent() and hasattr(self.parent(), "controller"):
                try:
                    target = self.target_item if hasattr(self, 'target_item') and self.target_item else None
                    if target:
                        nxt = self.parent().controller.calculate_next_address(target, prefix=prefix, new_type=data_type)
                        if isinstance(nxt, str):
                            base_addr = nxt
                        else:
                            base_addr = f"{prefix}{int(nxt):0{MODBUS_SEQUENCE_WIDTH}d}"
                        # 如果是 Array，添加 [size]
                        if is_array:
                            base_addr = f"{base_addr} [{array_size}]"
                        self.addr_edit.setText(base_addr)
                    else:
                        base_addr = f"{prefix}{offset:0{MODBUS_SEQUENCE_WIDTH}d}"
                        if is_array:
                            base_addr = f"{base_addr} [{array_size}]"
                        self.addr_edit.setText(base_addr)
                except Exception:
                    base_addr = f"{prefix}{offset:0{MODBUS_SEQUENCE_WIDTH}d}"
                    if is_array:
                        base_addr = f"{base_addr} [{array_size}]"
                    self.addr_edit.setText(base_addr)
            else:
                base_addr = f"{prefix}{offset:0{MODBUS_SEQUENCE_WIDTH}d}"
                if is_array:
                    base_addr = f"{base_addr} [{array_size}]"
                self.addr_edit.setText(base_addr)

            # 4. 解鎖 Scaling
            self.scale_type.setEnabled(True)

    def _toggle_scaling_visibility(self, text):
        # 切換 Scaling 參數區塊的顯示
        self.scaling_params_frame.setVisible(text != "None")

    def get_data(self):
        # 回傳雙層結構字典給 IoTApp 使用
        nested = {
            "general": {
                "name": self.name_edit.text(),
                "description": self.desc_edit.text(),
                "address": self.addr_edit.text(),
                "data_type": self.type_combo.currentText(),
                "access": self.access_combo.currentText(),
                "scan_rate": self.scan_rate.text(),
            },
            "scaling": {
                "type": self.scale_type.currentText(),
                "raw_low": self.raw_low.text(),
                "raw_high": self.raw_high.text(),
                "scaled_type": self.scaled_type.currentText(),
                "scaled_low": self.scaled_low.text(),
                "scaled_high": self.scaled_high.text(),
                "clamp_low": self.clamp_low.currentText(),
                "clamp_high": self.clamp_high.currentText(),
                "negate": self.negate.currentText(),
                "units": self.units.text(),
            },
        }
        flat = {
            "name": self.name_edit.text(),
            "description": self.desc_edit.text(),
            "address": self.addr_edit.text(),
            "data_type": self.type_combo.currentText(),
            "access": self.access_combo.currentText(),
            "scan_rate": self.scan_rate.text(),
            "scaling": nested["scaling"],
        }
        out = {**flat, **nested}
        return out

    def load_data(self, data):
        # 載入現有 Tag 資料進入 Dialog
        if not data:
            return

        # 暫時封鎖訊號，避免載入資料時觸發 _update_modbus_logic 導致位址被覆蓋
        self.type_combo.blockSignals(True)
        self.access_combo.blockSignals(True)

        gen = data.get("general", {})
        # load general values, falling back to dialog defaults or controller suggestions
        self.name_edit.setText(gen.get("name", self.name_edit.text()))
        self.desc_edit.setText(gen.get("description", self.desc_edit.text()))
        # Address: prefer provided, otherwise use suggested_addr set in constructor or controller-suggested
        addr = gen.get("address")
        if not addr:
            # try controller suggestion if available; pass prefix computed from current type/access
            if (
                self.parent()
                and hasattr(self.parent(), "controller")
                and hasattr(self.parent(), "tree")
            ):
                current = self.parent().tree.currentItem()
                if current:
                    try:
                        # determine prefix consistent with _update_modbus_logic
                        data_type = self.type_combo.currentText()
                        access = self.access_combo.currentText()
                        if "Boolean" in data_type:
                            prefix = "0" if access == "Read/Write" else "1"
                        else:
                            prefix = "4" if access == "Read/Write" else "3"
                        # pass current selected data type as fallback for step calculation
                        addr = self.parent().controller.calculate_next_address(current, prefix=prefix, new_type=self.type_combo.currentText())
                    except Exception:
                        addr = self.addr_edit.text()
            else:
                addr = self.addr_edit.text()

        self.addr_edit.setText(addr)
        self.type_combo.setCurrentText(gen.get("data_type", self.type_combo.currentText()))
        self.access_combo.setCurrentText(gen.get("access", self.access_combo.currentText()))
        self.scan_rate.setText(gen.get("scan_rate", self.scan_rate.text()))

        sc = data.get("scaling", {})
        stype = sc.get("type", self.scale_type.currentText())
        self.scale_type.setCurrentText(stype)
        self.raw_low.setText(sc.get("raw_low", self.raw_low.text()))
        self.raw_high.setText(sc.get("raw_high", self.raw_high.text()))
        self.scaled_type.setCurrentText(sc.get("scaled_type", self.scaled_type.currentText()))
        self.scaled_low.setText(sc.get("scaled_low", self.scaled_low.text()))
        self.scaled_high.setText(sc.get("scaled_high", self.scaled_high.text()))
        self.clamp_low.setCurrentText(sc.get("clamp_low", self.clamp_low.currentText()))
        self.clamp_high.setCurrentText(sc.get("clamp_high", self.clamp_high.currentText()))
        self.negate.setCurrentText(sc.get("negate", self.negate.currentText()))
        self.units.setText(sc.get("units", self.units.text()))

        # 恢復訊號
        self.type_combo.blockSignals(False)
        self.access_combo.blockSignals(False)

        # 載入完畢後手動整理一次 UI 狀態
        self._toggle_scaling_visibility(stype)
        # 根據載入的型態檢查 Scaling 是否該禁用
        if "Boolean" in self.type_combo.currentText():
            self.scale_type.setEnabled(False)
        else:
            self.scale_type.setEnabled(True)
