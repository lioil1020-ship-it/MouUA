"""
UI 組件工具模塊 - 統合所有 UI 相關的輔助工具和組件
包含：表格設置、表單生成器、數據轉換、樹項工具等
"""

from PyQt6.QtWidgets import (
    QTableWidget, QAbstractButton, QWidget, QFormLayout,
    QLineEdit, QComboBox, QLabel
)
from PyQt6.QtCore import Qt
from .theme import (
    TABLE_STYLE, HEADER_V_STYLE, HEADER_H_STYLE, 
    CORNER_BUTTON_STYLE, FORM_FIELD_STYLE, ROW_HEIGHT, FORM_ROW_SPACING
)


# ===== 表格相關工具 =====

def setup_table(table: QTableWidget, show_vertical_header=True):
    """
    統一設置表格樣式和屬性
    
    Args:
        table: QTableWidget 實例
        show_vertical_header: 是否顯示行號列
    """
    if not table:
        return
    
    # 應用全局樣式表
    table.setStyleSheet(TABLE_STYLE)
    
    # 配置垂直表頭（行號）
    vh = table.verticalHeader()
    if vh:
        vh.setStyleSheet(HEADER_V_STYLE)
    
    # 配置水平表頭（列標題）
    hh = table.horizontalHeader()
    if hh:
        hh.setStyleSheet(HEADER_H_STYLE)
    
    # 處理角落按鈕
    corner_button = table.findChild(QAbstractButton)
    if corner_button:
        corner_button.setStyleSheet(CORNER_BUTTON_STYLE)


# ===== 數據轉換工具 =====

def to_numeric_flag(v):
    """
    將值轉換為布林標誌（1/0）
    
    支持的輸入類型：
    - bool: True→1, False→0
    - int/float: 轉為 int
    - str: 'enable', 'true', '1', 'on'→1; 'disable', 'false', '0', 'off'→0
    - None: 保持 None
    
    Args:
        v: 任何類型的值
        
    Returns:
        布林標誌（0、1）或原始值
    """
    try:
        if v is None:
            return v
        if isinstance(v, bool):
            return 1 if v else 0
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if not s:
            return s
        low = s.lower()
        if low in ("enable", "enabled", "true", "1", "on"):
            return 1
        if low in ("disable", "disabled", "false", "0", "off"):
            return 0
        try:
            return int(float(s))
        except Exception:
            return s
    except Exception:
        return v


# ===== 樹項工具 =====

def safe_data(item, role, default=None):
    """
    安全地從 QTreeWidgetItem 讀取 UserRole 數據，失敗時返回默認值
    
    Args:
        item: QTreeWidgetItem 對象
        role: 要讀取的數據角色（通常是 0-9）
        default: 如果讀取失敗或 item 為 None，返回此值
        
    Returns:
        讀取的數據或默認值
    """
    try:
        if item is None:
            return default
        # QTreeWidgetItem.data expects (column:int, role:int)
        # QTableWidgetItem.data expects (role:int)
        # Try QTreeWidgetItem signature first; if it fails, fall back to QTableWidgetItem signature.
        try:
            return item.data(role, Qt.ItemDataRole.UserRole)
        except TypeError:
            try:
                return item.data(role)
            except Exception:
                return default
    except Exception:
        return default


# ===== 控制器工具 =====

def call_controller(app, method_name, *args, **kwargs):
    """
    安全地呼叫應用控制器方法，若不可用則返回 None
    
    Args:
        app: IoTApp 應用實例
        method_name: 要呼叫的控制器方法名
        *args: 位置參數
        **kwargs: 關鍵字參數
        
    Returns:
        方法返回值或 None
    """
    try:
        ctrl = getattr(app, 'controller', None)
        if ctrl is None:
            return None
        fn = getattr(ctrl, method_name, None)
        if fn is None:
            return None
        return fn(*args, **kwargs)
    except Exception:
        return None


def schedule_temp_export(app, delay=50):
    """
    排程短延遲匯出以儲存暫存專案
    
    Args:
        app: IoTApp 應用實例
        delay: 延遲毫秒數（默認 50ms）
    """
    from PyQt6.QtCore import QTimer
    try:
        if getattr(app, '_temp_json', None) and getattr(app, 'controller', None):
            try:
                QTimer.singleShot(delay, lambda: call_controller(app, 'export_project_to_json', app._temp_json))
            except Exception:
                try:
                    call_controller(app, 'export_project_to_json', app._temp_json)
                except Exception:
                    pass
    except Exception:
        pass


def collect_selected_tree_items(table):
    """
    收集表格中選取的樹項
    
    Args:
        table: QTableWidget 實例
        
    Returns:
        選取的樹項列表
    """
    items = []
    try:
        sel = table.selectionModel().selectedRows()
        for s in sel:
            try:
                row = s.row()
                itm = table.item(row, 0)
                if itm is None:
                    continue
                tree_item = None
                try:
                    tree_item = safe_data(itm, Qt.ItemDataRole.UserRole)
                except Exception:
                    tree_item = None
                if tree_item is not None:
                    items.append(tree_item)
            except Exception:
                pass
    except Exception:
        pass
    return items


# ===== 表單生成器 =====

class FormBuilder(QWidget):
    """
    簡潔的表單生成器 - 用於對話框中動態生成表單
    
    提供：
    - 動態添加/移除表單字段
    - 統一的樣式管理
    - 值的獲取和設置
    """
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.layout = QFormLayout(self)
        self.layout.setVerticalSpacing(FORM_ROW_SPACING)
        self.fields = {}
        self.setStyleSheet(FORM_FIELD_STYLE)

    def add_field(self, field_id, label_text, field_type="text", options=None, default=""):
        """
        添加表單字段
        
        Args:
            field_id: 字段唯一識別碼
            label_text: 標籤文本
            field_type: 字段類型 ('text' 或 'combo')
            options: 組合框的選項列表
            default: 默認值
        """
        label = QLabel(label_text)
        label.setFixedHeight(ROW_HEIGHT)
        
        if field_type == "combo":
            widget = QComboBox()
            if options:
                widget.addItems(options)
            if default:
                widget.setCurrentText(str(default))
        else:
            widget = QLineEdit()
            widget.setText(str(default))
        
        widget.setFixedHeight(ROW_HEIGHT)
        self.layout.addRow(label, widget)
        self.fields[field_id] = widget

    def clear_form(self):
        """清空所有字段"""
        while self.layout.count() > 0:
            self.layout.removeRow(0)
        self.fields = {}

    def get_values(self):
        """
        獲取所有字段值
        
        Returns:
            字段 ID 到值的字典映射
        """
        values = {}
        for fid, widget in self.fields.items():
            if isinstance(widget, QLineEdit):
                values[fid] = widget.text()
            elif isinstance(widget, QComboBox):
                values[fid] = widget.currentText()
        return values

    def set_values(self, data):
        """
        設置表單字段值
        
        Args:
            data: 字段 ID 到值的字典映射
        """
        for fid, value in data.items():
            if fid in self.fields:
                widget = self.fields[fid]
                if isinstance(widget, QLineEdit):
                    widget.setText(str(value))
                elif isinstance(widget, QComboBox):
                    widget.setCurrentText(str(value))


__all__ = [
    # 表格工具
    'setup_table',
    # 數據轉換
    'to_numeric_flag',
    # 樹項工具
    'safe_data',
    # 控制器工具
    'call_controller',
    'schedule_temp_export',
    'collect_selected_tree_items',
    # 表單生成器
    'FormBuilder',
]
