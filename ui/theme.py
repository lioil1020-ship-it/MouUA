"""
UI主题配置与样式定义
集中管理所有PyQt6样式表和UI常量
"""

# ===== 颜色定义 =====
COLOR_DARK_BG = "#2b2b2b"
COLOR_LIGHT_TEXT = "#ffffff"
COLOR_SELECTED = "#0d47a1"
COLOR_HOVER = "#1565c0"
COLOR_BORDER = "none"

# ===== 尺寸定义 =====
ROW_HEIGHT = 22
HEADER_HEIGHT = 24
FORM_ROW_SPACING = 2
DIALOG_MIN_WIDTH = 380
DIALOG_MIN_HEIGHT = 180
FORM_MAX_WIDTH = 600

# ===== 间距定义 =====
MARGIN_H = 20
MARGIN_V = 18
SPACING = 12
FORM_MAX_WIDTH = 600

# ===== 表格样式 =====
TABLE_STYLE = f'''
QTableWidget {{
    outline: none;
    border: none;
    gridline-color: transparent;
    background-color: {COLOR_DARK_BG};
}}
QTableWidget::item {{
    background-color: {COLOR_DARK_BG};
    color: {COLOR_LIGHT_TEXT};
    padding: 2px;
    border: none;
}}
QTableWidget::item:selected {{
    background-color: {COLOR_SELECTED};
    color: {COLOR_LIGHT_TEXT};
    outline: none;
    border: none;
}}
QTableWidget::item:selected:hover {{
    background-color: {COLOR_HOVER};
    outline: none;
    border: none;
}}
QHeaderView {{
    background-color: {COLOR_DARK_BG};
    border: none;
}}
QHeaderView::section {{
    background-color: {COLOR_DARK_BG};
    color: {COLOR_LIGHT_TEXT};
    border: none;
    padding: 2px;
    height: {HEADER_HEIGHT}px;
    margin: 0px;
    padding-left: 2px;
}}
QTableWidget::cornerButton {{
    background-color: {COLOR_DARK_BG} !important;
    border: none !important;
    margin: 0px !important;
    padding: 0px !important;
}}
QAbstractScrollArea {{
    background-color: {COLOR_DARK_BG};
}}
QTableView {{
    background-color: {COLOR_DARK_BG};
}}
'''

# ===== 对话框表单样式 =====
FORM_FIELD_STYLE = f'''
QLineEdit, QComboBox, QSpinBox {{
    min-height: {ROW_HEIGHT}px;
    max-height: {ROW_HEIGHT}px;
}}
QLabel {{
    min-height: {ROW_HEIGHT}px;
    max-height: {ROW_HEIGHT}px;
}}
'''

# ===== 树形控件样式 =====
TREE_ITEM_STYLE = f'''
QTreeWidget::item {{
    padding: 3px;
    height: {ROW_HEIGHT}px;
}}
QHeaderView::section {{
    padding: 2px;
    height: {HEADER_HEIGHT}px;
}}
'''

# ===== 分割条样式 =====
SPLITTER_STYLE = f"QSplitter {{ background-color: {COLOR_DARK_BG}; }} QMainWindow {{ background-color: {COLOR_DARK_BG}; }}"

# ===== 表头样式定义 =====
HEADER_V_STYLE = f"QHeaderView::section {{ background-color: {COLOR_DARK_BG}; color: {COLOR_LIGHT_TEXT}; border: none; padding: 0px; margin: 0px; }}"
HEADER_H_STYLE = f"QHeaderView::section {{ background-color: {COLOR_DARK_BG}; color: {COLOR_LIGHT_TEXT}; border: none; padding: 2px; height: {HEADER_HEIGHT}px; }}"
CORNER_BUTTON_STYLE = f"background-color: {COLOR_DARK_BG}; border: none;"
