"""
Write Value Dialog for Modbus Operations
"""

from PyQt6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QMessageBox,
    QSpinBox,
    QDoubleSpinBox,
    QCheckBox,
)
from PyQt6.QtCore import Qt, pyqtSignal
from typing import Any, Optional, Dict


class WriteValueDialog(QDialog):
    """
    寫入值對話框
    
    功能:
    - 顯示當前點位資訊 (地址、函數碼、當前值)
    - 允許用戶輸入新值
    - 驗證值的類型和範圍
    - 返回使用者確認的值
    """
    
    # 信號: (address, fc, value)
    write_requested = pyqtSignal(int, int, object)
    
    def __init__(self, tag_info: Dict[str, Any], current_value: Any = None, parent=None):
        """
        初始化寫值對話框
        
        Args:
            tag_info: 標籤資訊字典
                - address: Modbus 地址
                - name: 標籤名稱
                - function_code: FC (5, 6, 15, 16)
                - data_type: 數據類型 (int, float, bool, etc)
                - read_write: 讀寫權限 ('Read/Write', 'Read Only', etc)
            current_value: 當前值
            parent: 父窗口
        """
        super().__init__(parent)
        self.tag_info = tag_info or {}
        self.current_value = current_value
        self.new_value = None
        
        self._setup_ui()
        self._populate_info()
    
    def _setup_ui(self):
        """設置 UI"""
        self.setWindowTitle("寫入值")
        self.setModal(True)
        self.setMinimumWidth(400)
        
        layout = QVBoxLayout()
        
        # 標籤名稱和地址
        name_layout = QHBoxLayout()
        name_layout.addWidget(QLabel("標籤名稱:"))
        self.name_label = QLabel()
        name_layout.addWidget(self.name_label)
        name_layout.addStretch()
        layout.addLayout(name_layout)
        
        # 地址和 FC
        addr_layout = QHBoxLayout()
        addr_layout.addWidget(QLabel("地址:"))
        self.addr_label = QLabel()
        addr_layout.addWidget(self.addr_label)
        addr_layout.addWidget(QLabel("函數碼:"))
        self.fc_label = QLabel()
        addr_layout.addWidget(self.fc_label)
        addr_layout.addStretch()
        layout.addLayout(addr_layout)
        
        # 當前值
        curr_layout = QHBoxLayout()
        curr_layout.addWidget(QLabel("當前值:"))
        self.curr_value_label = QLabel()
        curr_layout.addWidget(self.curr_value_label)
        curr_layout.addStretch()
        layout.addLayout(curr_layout)
        
        # 新值輸入
        new_layout = QHBoxLayout()
        new_layout.addWidget(QLabel("新值:"))
        self.new_value_input = QLineEdit()
        self.new_value_input.setPlaceholderText("輸入新值...")
        new_layout.addWidget(self.new_value_input)
        layout.addLayout(new_layout)
        
        # 數據類型提示
        type_layout = QHBoxLayout()
        type_layout.addWidget(QLabel("數據類型:"))
        self.type_label = QLabel()
        type_layout.addWidget(self.type_label)
        type_layout.addStretch()
        layout.addLayout(type_layout)
        
        # 權限提示
        perm_layout = QHBoxLayout()
        perm_layout.addWidget(QLabel("權限:"))
        self.perm_label = QLabel()
        perm_layout.addWidget(self.perm_label)
        perm_layout.addStretch()
        layout.addLayout(perm_layout)
        
        # 按鈕
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        
        ok_btn = QPushButton("確定")
        ok_btn.clicked.connect(self._on_ok)
        btn_layout.addWidget(ok_btn)
        
        cancel_btn = QPushButton("取消")
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(cancel_btn)
        
        layout.addLayout(btn_layout)
        
        self.setLayout(layout)
    
    def _populate_info(self):
        """填充標籤資訊"""
        name = self.tag_info.get('name', 'Unknown')
        address = self.tag_info.get('address', 'N/A')
        fc = self.tag_info.get('function_code', 'N/A')
        data_type = self.tag_info.get('data_type', 'Unknown')
        read_write = self.tag_info.get('read_write', 'Unknown')
        
        self.name_label.setText(str(name))
        self.addr_label.setText(str(address))
        self.fc_label.setText(str(fc))
        self.curr_value_label.setText(str(self.current_value if self.current_value is not None else 'N/A'))
        self.type_label.setText(str(data_type))
        self.perm_label.setText(str(read_write))
        
        # 驗證權限
        if 'Read Only' in str(read_write):
            QMessageBox.warning(
                self,
                "警告",
                "此標籤為只讀，無法寫入！",
                QMessageBox.StandardButton.Ok
            )
            self.new_value_input.setEnabled(False)
    
    def _on_ok(self):
        """處理確定按鈕"""
        value_str = self.new_value_input.text().strip()
        
        if not value_str:
            QMessageBox.warning(
                self,
                "錯誤",
                "請輸入新值！",
                QMessageBox.StandardButton.Ok
            )
            return
        
        # 嘗試解析值
        try:
            self.new_value = self._parse_value(value_str)
            
            # 驗證值
            if not self._validate_value(self.new_value):
                return
            
            # 發出信號
            address = self.tag_info.get('address')
            fc = self.tag_info.get('function_code')
            
            # 轉換 address 為 int
            try:
                address = int(address) if address is not None else 0
            except (ValueError, TypeError):
                address = 0
            
            try:
                fc = int(fc) if fc is not None else 16
            except (ValueError, TypeError):
                fc = 16
            
            self.write_requested.emit(address, fc, self.new_value)
            
            # 不立即關閉，讓信號槽完成後才自動關閉
            # self.accept() 會由信號槽完成後調用
            pass
        
        except ValueError as e:
            QMessageBox.critical(
                self,
                "錯誤",
                f"無法解析值: {str(e)}",
                QMessageBox.StandardButton.Ok
            )
    
    def _parse_value(self, value_str: str) -> Any:
        """
        解析用戶輸入的值
        
        Args:
            value_str: 字符串值
        
        Returns:
            解析後的值
        
        Raises:
            ValueError: 如果無法解析
        """
        data_type = self.tag_info.get('data_type', 'int').lower()
        
        # 布爾值
        if 'bool' in data_type:
            if value_str.lower() in ('1', 'true', 'yes', 'on'):
                return True
            elif value_str.lower() in ('0', 'false', 'no', 'off'):
                return False
            else:
                raise ValueError(f"布爾值必須是 0/1 或 True/False")
        
        # 浮點值
        elif 'float' in data_type or 'double' in data_type:
            return float(value_str)
        
        # 整數值
        elif 'int' in data_type:
            return int(value_str)
        
        # 字符串值
        else:
            return value_str
    
    def _validate_value(self, value: Any) -> bool:
        """
        驗證值的有效性
        
        Args:
            value: 要驗證的值
        
        Returns:
            True 如果有效
        """
        # TODO: 添加範圍驗證，基於標籤配置
        # 例如: min, max, enum_values 等
        return True
    
    def get_new_value(self) -> Optional[Any]:
        """獲取新值"""
        return self.new_value
