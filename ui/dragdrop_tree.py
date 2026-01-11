from PyQt6.QtWidgets import QTreeWidget, QTreeWidgetItem, QMenu
from PyQt6.QtCore import Qt, pyqtSignal

class ConnectivityTree(QTreeWidget):
    # 📡 定義操作訊號
    request_new_channel = pyqtSignal(QTreeWidgetItem)
    request_new_device = pyqtSignal(QTreeWidgetItem)
    request_new_group = pyqtSignal(QTreeWidgetItem)
    request_new_tag = pyqtSignal(QTreeWidgetItem)
    
    request_edit_item = pyqtSignal(QTreeWidgetItem)
    request_delete_item = pyqtSignal(QTreeWidgetItem)
    request_copy_item = pyqtSignal(QTreeWidgetItem)
    request_paste_item = pyqtSignal(QTreeWidgetItem)
    request_cut_item = pyqtSignal(QTreeWidgetItem)
    request_import_csv = pyqtSignal(QTreeWidgetItem)
    request_export_csv = pyqtSignal(QTreeWidgetItem)
    request_device_diagnostics = pyqtSignal(QTreeWidgetItem)

    def __init__(self, parent=None):
        super().__init__(parent)
        
        # 1. 🟢 關鍵修正：隱藏 QTreeWidget 內建的 Header，避免多出一層
        self.setHeaderHidden(True)
        # 強制所有行使用相同高度（與主視窗一致）
        try:
            self.setUniformRowHeights(True)
        except Exception:
            pass
        
        self.setColumnCount(1)
        
        # ➕ 視覺優化：強制顯示展開控制項 (+/-)
        self.setRootIsDecorated(True) 
        
        # 🖱️ 行為優化：雙擊不要自動展開，改為觸發編輯或手動控制展開
        self.setExpandsOnDoubleClick(False)
        self.itemDoubleClicked.connect(self._handle_double_click)
        
        # 2. 🟢 建立符合 Project -> Connectivity 的結構
        self.root_node = QTreeWidgetItem(self)
        self.root_node.setText(0, "Project")
        self.root_node.setData(0, Qt.ItemDataRole.UserRole, "Project")
        self.root_node.setExpanded(True)

        # 在 Project 下新增 Connectivity 節點
        self.conn_node = QTreeWidgetItem(self.root_node)
        self.conn_node.setText(0, "Connectivity")
        self.conn_node.setData(0, Qt.ItemDataRole.UserRole, "Connectivity")
        self.conn_node.setExpanded(True)
        # 確保頂層節點使用與其他節點相同的字型，避免因字型或樣式造成高度差異
        try:
            default_font = self.font()
            self.root_node.setFont(0, default_font)
            self.conn_node.setFont(0, default_font)
        except Exception:
            pass

    def _handle_double_click(self, item, _column):
        """處理雙擊行為：根節點與連線節點切換展開，其餘開啟內容"""
        node_type = item.data(0, Qt.ItemDataRole.UserRole)
        
        # 讓 Project 與 Connectivity 雙擊時是展開/收合，而不是開啟編輯視窗
        if node_type in ["Project", "Connectivity"]:
            item.setExpanded(not item.isExpanded())
        else:
            # 其餘節點（Channel, Device...）雙擊開啟編輯視窗
            self.request_edit_item.emit(item)

    def contextMenuEvent(self, event):
        """處理右鍵選單邏輯"""
        item = self.itemAt(event.pos())
        if not item: return

        node_type = item.data(0, Qt.ItemDataRole.UserRole)
        menu = QMenu(self)

        # 根據節點類型顯示不同選單
        if node_type == "Connectivity":
            menu.addAction("➕ 新增 Channel", lambda: self.request_new_channel.emit(item))
        elif node_type == "Channel":
            menu.addAction("➕ 新增 Device", lambda: self.request_new_device.emit(item))
            menu.addSeparator()
            self._add_common_actions(menu, item)
        elif node_type in ["Device", "Group"]:
            menu.addAction("➕ 新增 Group", lambda: self.request_new_group.emit(item))
            menu.addAction("➕ 新增 Tag", lambda: self.request_new_tag.emit(item))
            menu.addSeparator()
            self._add_common_actions(menu, item)
            # Diagnostics only for Device (show per-device diagnostics window)
            if node_type == 'Device':
                menu.addSeparator()
                menu.addAction("📊 Diagnostics", lambda: self.request_device_diagnostics.emit(item))
            # CSV import/export only on Device nodes
            if node_type == 'Device':
                menu.addSeparator()
                menu.addAction("📥 匯入 CSV", lambda: self.request_import_csv.emit(item))
                menu.addAction("📤 匯出 CSV", lambda: self.request_export_csv.emit(item))
        elif node_type == "Tag":
            self._add_common_actions(menu, item)

        if not menu.isEmpty():
            menu.exec(event.globalPos())

    def _add_common_actions(self, menu, item):
        """通用選單動作：剪切、複製、貼上、刪除、內容"""
        menu.addAction("✂️ 剪下", lambda: self.request_cut_item.emit(item))
        menu.addAction("📋 複製", lambda: self.request_copy_item.emit(item))
        menu.addAction("📥 貼上", lambda: self.request_paste_item.emit(item))
        menu.addSeparator()
        menu.addAction("❌ 刪除", lambda: self.request_delete_item.emit(item))
        menu.addAction("✏️ 內容", lambda: self.request_edit_item.emit(item))