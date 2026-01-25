from PyQt6.QtWidgets import QTreeWidget, QTreeWidgetItem, QMenu
from PyQt6.QtCore import Qt, pyqtSignal, QBuffer, QByteArray, QIODevice
from PyQt6.QtGui import QPixmap, QPainter, QPen, QColor, QIcon
import base64

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
    # New signal: request the main UI show the content page for an item (used for Group double-click)
    request_show_content = pyqtSignal(QTreeWidgetItem)

    def __init__(self, parent=None):
        super().__init__(parent)
        
        # Create minimal root/conn nodes early so tree structure exists
        try:
            self.root_node = QTreeWidgetItem(self)
            self.root_node.setText(0, "Project")
            self.root_node.setData(0, Qt.ItemDataRole.UserRole, "Project")
            try:
                self.root_node.setExpanded(False)
            except Exception:
                pass
            self.conn_node = QTreeWidgetItem(self.root_node)
            self.conn_node.setText(0, "Connectivity")
            self.conn_node.setData(0, Qt.ItemDataRole.UserRole, "Connectivity")
            try:
                self.conn_node.setExpanded(False)
            except Exception:
                pass
        except Exception as e:
            import traceback
            print(f"⚠️ Error initializing tree nodes: {e}")
            traceback.print_exc()
            self.root_node = None
            self.conn_node = None
        
        self.setHeaderHidden(True)
        try:
            self.setUniformRowHeights(True)
        except Exception:
            pass
        
        self.setColumnCount(1)
        
        try:
            self.setRootIsDecorated(False)
            self.setItemsExpandable(True)
        except Exception:
            pass
        
        try:
            self.setIndentation(20)
        except Exception:
            pass

        # 建立 plus/minus 圖示為 DataURL
        try:
            def _make_symbol_pixmap(size, minus=False):
                pix = QPixmap(size, size)
                pix.fill(QColor(0, 0, 0, 0))
                p = QPainter(pix)
                pen = QPen(QColor(200, 200, 200))
                pen.setWidth(max(2, size // 8))
                p.setPen(pen)
                y = size // 2
                p.drawLine(size // 4, y, size * 3 // 4, y)
                if not minus:
                    x = size // 2
                    p.drawLine(x, size // 4, x, size * 3 // 4)
                p.end()
                return pix

            def _pixmap_to_dataurl(pix):
                buf = QBuffer()
                buf.open(QIODevice.OpenModeFlag.WriteOnly)
                pix.save(buf, 'PNG')
                data = bytes(buf.data())
                b64 = base64.b64encode(data).decode('ascii')
                return f"data:image/png;base64,{b64}"

            size = 28
            plus_pix = _make_symbol_pixmap(size, minus=False)
            minus_pix = _make_symbol_pixmap(size, minus=True)
            plus_url = _pixmap_to_dataurl(plus_pix)
            minus_url = _pixmap_to_dataurl(minus_pix)
            
            sheet = f'''
QTreeWidget::branch:closed:has-children {{ image: url({plus_url}); width: {size}px; height: {size}px; margin-left: 0px; margin-right: 5px; }}
QTreeWidget::branch:open:has-children {{ image: url({minus_url}); width: {size}px; height: {size}px; margin-left: 0px; margin-right: 5px; }}
QTreeWidget {{ 
    margin-left: 0px; 
    padding-left: 0px; 
    outline: none; 
    border: none;
    background-color: #2b2b2b;
}}
QTreeWidget::item {{ 
    padding-left: 0px; 
    outline: none; 
    border: none;
    background-color: #2b2b2b;
    color: #ffffff;
}}
QTreeWidget::item:selected {{ 
    background-color: #0d47a1;
    color: #ffffff;
    outline: none;
    border: none;
}}
QTreeWidget::item:selected:hover {{
    background-color: #1565c0;
    outline: none;
    border: none;
}}
'''
            try:
                self.setStyleSheet(sheet)
            except Exception as e:
                print(f"⚠️ Error setting stylesheet: {e}")
        except Exception as e:
            import traceback
            print(f"⚠️ Error creating branch symbols: {e}")
            traceback.print_exc()

        # 圖標管理和 Tag 隱藏
        try:
            self._plus_icon = QIcon(plus_pix)
            self._minus_icon = QIcon(minus_pix)

            def _has_non_tag_child(node):
                try:
                    if node is None:
                        return False
                    for ii in range(node.childCount()):
                        try:
                            ch = node.child(ii)
                            if ch is None:
                                continue
                            if ch.data(0, Qt.ItemDataRole.UserRole) != 'Tag':
                                return True
                        except Exception:
                            return True
                except Exception:
                    pass
                return False

            def _update_item_icon(item):
                try:
                    if item is None:
                        return
                    # Hide Tag-level nodes
                    ntype = item.data(0, Qt.ItemDataRole.UserRole)
                    if ntype == 'Tag':
                        item.setHidden(True)
                        return
                    
                    # Update expand/collapse icon
                    if _has_non_tag_child(item):
                        if item.isExpanded():
                            item.setIcon(0, self._minus_icon)
                        else:
                            item.setIcon(0, self._plus_icon)
                    else:
                        item.setIcon(0, QIcon())
                except Exception:
                    pass

            def _apply_recursive(node):
                try:
                    if node is None:
                        return
                    _update_item_icon(node)
                    for i in range(node.childCount()):
                        _apply_recursive(node.child(i))
                except Exception:
                    pass

            # Connect signals for icon updates
            try:
                self.itemExpanded.connect(lambda it: _update_item_icon(it))
                self.itemCollapsed.connect(lambda it: _update_item_icon(it))
            except Exception:
                pass

            # Handle newly inserted rows
            try:
                model = self.model()
                def _on_rows_inserted(parent_index, start, end):
                    try:
                        if parent_index.isValid():
                            parent_item = self.itemFromIndex(parent_index)
                        else:
                            parent_item = self.invisibleRootItem()
                        for i in range(start, end + 1):
                            child = parent_item.child(i)
                            _apply_recursive(child)
                        _update_item_icon(parent_item)
                    except Exception:
                        pass
                model.rowsInserted.connect(_on_rows_inserted)
            except Exception:
                pass

            # Initial pass to apply icons
            _apply_recursive(self.invisibleRootItem())
        except Exception:
            pass
        
        # 🖱️ 行為優化：雙擊不要自動展開，改為觸發編輯或手動控制展開
        self.setExpandsOnDoubleClick(False)
        self.itemDoubleClicked.connect(self._handle_double_click)

    def drawBranches(self, painter, rect, index):
        # Override to completely skip drawing branch lines
        # This prevents the | symbols from appearing
        pass

    def drawTree(self, painter, region):
        # Skip drawing tree to prevent any branch lines
        # Call parent's item painting but not branch painting
        super().drawTree(painter, region)

    def hide_all_tags(self):
        # Public helper: walk the whole tree and hide any Tag-level nodes.
        try:
            def _walk(node):
                if node is None:
                    return
                try:
                    ntype = node.data(0, Qt.ItemDataRole.UserRole)
                    if ntype == 'Tag':
                        try:
                            node.setHidden(True)
                        except Exception:
                            pass
                except Exception:
                    pass
                for i in range(node.childCount()):
                    _walk(node.child(i))

            _walk(self.invisibleRootItem())
        except Exception:
            pass
        
        # 2. 🟢 建立符合 Project -> Connectivity 的結構（若尚未建立）
        try:
            if not getattr(self, 'root_node', None):
                self.root_node = QTreeWidgetItem(self)
                self.root_node.setText(0, "Project")
                self.root_node.setData(0, Qt.ItemDataRole.UserRole, "Project")
                try:
                    self.root_node.setExpanded(False)
                except Exception:
                    pass
            if not getattr(self, 'conn_node', None):
                self.conn_node = QTreeWidgetItem(self.root_node)
                self.conn_node.setText(0, "Connectivity")
                self.conn_node.setData(0, Qt.ItemDataRole.UserRole, "Connectivity")
                try:
                    self.conn_node.setExpanded(False)
                except Exception:
                    pass
        except Exception:
            pass
        # 確保頂層節點使用與其他節點相同的字型，避免因字型或樣式造成高度差異
        try:
            default_font = self.font()
            self.root_node.setFont(0, default_font)
            self.conn_node.setFont(0, default_font)
        except Exception:
            pass

    def _has_non_tag_child(self, node):
        # Return True if node has any child that is not a Tag (Tags are hidden)
        try:
            if node is None:
                return False
            for ii in range(node.childCount()):
                try:
                    ch = node.child(ii)
                    if ch is None:
                        continue
                    try:
                        if ch.data(0, Qt.ItemDataRole.UserRole) != 'Tag':
                            return True
                    except Exception:
                        return True
                except Exception:
                    continue
        except Exception:
            pass
        return False

    def _handle_double_click(self, item, _column):
        # 處理雙擊行為：根節點與連線節點切換展開，其餘開啟內容
        node_type = item.data(0, Qt.ItemDataRole.UserRole)
        
        # 讓 Project 與 Connectivity 雙擊時是展開/收合，而不是開啟編輯視窗
        if node_type in ["Project", "Connectivity"]:
            item.setExpanded(not item.isExpanded())
        else:
            # Group 雙擊時改為顯示內容頁（切換為當前項目並觸發 itemClicked）
            if node_type == 'Group':
                try:
                    self.setCurrentItem(item)
                    # Debug: log that group double-click was received
                    try:
                        # debug print removed
                        pass
                    except Exception:
                        pass
                    # emit a dedicated signal to request showing content for this item
                    try:
                        self.request_show_content.emit(item)
                    except Exception:
                        pass
                    return
                except Exception:
                    pass
            # 其餘節點（Channel, Device...）雙擊開啟編輯視窗
            self.request_edit_item.emit(item)

    def mousePressEvent(self, event):
        # Override to make clicking the left indentation/icon area toggle expand/collapse
        try:
            pos = event.pos()
            item = self.itemAt(pos)
            if item is not None:
                # get visual rect for item text/icon
                try:
                    rect = self.visualItemRect(item)
                except Exception:
                    rect = None
                if rect is not None:
                    # define a branch-hit zone using our branch icon size (fallback to 28)
                    icon_w = getattr(self, '_branch_icon_size', None)
                    try:
                        if icon_w is None:
                            # attempt to infer from icon available
                            icon_w = self._plus_icon.actualSize(self._plus_icon.availableSizes()[0]).width() if hasattr(self, '_plus_icon') and self._plus_icon.availableSizes() else 28
                    except Exception:
                        icon_w = 28
                    zone_left = rect.left()
                    x, y, w, h = (zone_left, rect.top(), icon_w + 8, rect.height())
                    if x <= pos.x() <= x + w and rect.top() <= pos.y() <= rect.bottom():
                        try:
                            # only toggle expand/collapse for items that have non-Tag children
                            if self._has_non_tag_child(item):
                                item.setExpanded(not item.isExpanded())
                                # update icon immediately
                                try:
                                    item.setIcon(0, self._minus_icon if item.isExpanded() else self._plus_icon)
                                except Exception:
                                    pass
                            return
                        except Exception:
                            pass
        except Exception:
            pass
        super().mousePressEvent(event)

    def contextMenuEvent(self, event):
        # 處理右鍵選單邏輯
        # Qt override - this method is invoked by the framework; keep it even if static analysis flags it.
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
        # 通用選單動作：剪切、複製、貼上、刪除、內容
        menu.addAction("✂️ 剪下", lambda: self.request_cut_item.emit(item))
        menu.addAction("📋 複製", lambda: self.request_copy_item.emit(item))
        menu.addAction("📥 貼上", lambda: self.request_paste_item.emit(item))
        menu.addSeparator()
        menu.addAction("❌ 刪除", lambda: self.request_delete_item.emit(item))
        menu.addAction("✏️ 內容", lambda: self.request_edit_item.emit(item))