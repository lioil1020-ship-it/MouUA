from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QFormLayout, QLineEdit, 
    QPushButton, QTabWidget, QWidget, QHBoxLayout,
)
from ..theme import FORM_FIELD_STYLE, DIALOG_MIN_WIDTH, DIALOG_MIN_HEIGHT, ROW_HEIGHT


class GroupDialog(QDialog):
    """组属性对话框 - 简化版本"""
    
    def __init__(self, parent=None, suggested_name="Group1"):
        super().__init__(parent)
        self.setWindowTitle("Group Properties")
        self.setMinimumSize(DIALOG_MIN_WIDTH, DIALOG_MIN_HEIGHT)
        self.setStyleSheet(FORM_FIELD_STYLE)

        main_layout = QVBoxLayout(self)
        
        # General tab
        tab = QWidget()
        form = QFormLayout(tab)
        self.name_edit = QLineEdit(str(suggested_name) if suggested_name else "")
        self.desc_edit = QLineEdit("")
        form.addRow("Name:", self.name_edit)
        form.addRow("Description:", self.desc_edit)

        tabs = QTabWidget()
        tabs.addTab(tab, "General")
        main_layout.addWidget(tabs)

        # Buttons
        btns = QHBoxLayout()
        self.btn_ok = QPushButton("Finish")
        self.btn_cancel = QPushButton("Cancel")
        btns.addStretch()
        btns.addWidget(self.btn_ok)
        btns.addWidget(self.btn_cancel)
        main_layout.addLayout(btns)

        self.btn_ok.clicked.connect(self.accept)
        self.btn_cancel.clicked.connect(self.reject)

    def load_data(self, data):
        """加载数据"""
        if not data:
            return
        # 支持嵌套结构或平铺结构
        if isinstance(data.get("general"), dict):
            data = data["general"]
        
        self.name_edit.setText(str(data.get("name", "")))
        self.desc_edit.setText(str(data.get("description", "")))

    def get_data(self):
        """获取数据"""
        return {
            "name": self.name_edit.text(),
            "description": self.desc_edit.text(),
            "general": {
                "name": self.name_edit.text(),
                "description": self.desc_edit.text(),
            }
        }
