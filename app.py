import sys
import os
import time
import threading
import logging
from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QSplitter,
    QTableWidget,
    QHeaderView,
    QTreeWidgetItem,
    QTableWidgetItem,
    QMessageBox,
    QMenu,
    QDialog,
    QFileDialog,
    QTextEdit,
    QInputDialog,
    QPushButton,
    QToolBar,
    QWidget,
    QVBoxLayout,
)
from PyQt6.QtGui import QShortcut, QKeySequence, QFont, QAction
from PyQt6.QtCore import Qt, QTimer
from ui.dragdrop_tree import ConnectivityTree
from dialogs.channel_dialog import ChannelDialog
from dialogs.device_dialog import DeviceDialog
from dialogs.tag_dialog import TagDialog
from dialogs.opcua_dialog import OPCUADialog
try:
    from core.data_manager import DataBroker
except Exception:
    DataBroker = None
try:
    from OPC_UA import OPCServer
except Exception:
    OPCServer = None
from clipboard import ClipboardManager
from controllers import AppController
from modbus_worker import AsyncPoller
from core.diagnostics import DiagnosticsManager

class MonitorWindow(QMainWindow):
    """Deprecated placeholder: full implementation moved to `deprecated/moved_monitor_window.py`.

    Keeping a small stub to avoid name resolution issues if referenced dynamically.
    """
    pass


class TerminalWindow(QMainWindow):
    """終端視窗 - 顯示診斷信息。
    支援以 device_item 為過濾，當 device_item 為 None 時顯示全部訊息（global）。
    """
    def __init__(self, parent=None, device_item=None, diagnostics_manager=None):
        super().__init__(parent)
        self.device_item = device_item
        # keep a reference to the main IoTApp parent for callbacks
        self.parent_window = parent
        self._diag_manager = diagnostics_manager
        self._diag_listener_token = None
        self.setWindowTitle("Diagnostics" if device_item is None else f"Diagnostics - {self._device_path(device_item)}")
        self.resize(1000, 600)

        # 主容器
        main_widget = QWidget()
        # diagnostics table
        layout = QVBoxLayout()
        self.diagnostics_table = QTableWidget()
        self.diagnostics_table.setColumnCount(5)
        self.diagnostics_table.setHorizontalHeaderLabels(["Date", "Time", "Event", "Length", "Data"])
        header = self.diagnostics_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        layout.addWidget(self.diagnostics_table)
        main_widget.setLayout(layout)
        self.setCentralWidget(main_widget)

        self._device_tag_ids = set()
        self._device_path_str = None
        self._device_unit = None
        if device_item is not None:
            try:
                # collect tag ids under device
                for i in range(device_item.childCount()):
                    c = device_item.child(i)
                    if c.data(0, Qt.ItemDataRole.UserRole) == "Tag":
                        try:
                            self._device_tag_ids.add(id(c))
                        except Exception:
                            pass
                # build device path like Channel1.Device1
                self._device_path_str = self._device_path(device_item)
                # try to read configured unit id from device (common stored at role index 2)
                try:
                    u = device_item.data(2, Qt.ItemDataRole.UserRole)
                    if u is not None:
                        try:
                            self._device_unit = int(u)
                        except Exception:
                            self._device_unit = None
                except Exception:
                    self._device_unit = None
                try:
                    self._device_item_id = id(device_item)
                except Exception:
                    self._device_item_id = None
            except Exception:
                pass

        if self._diag_manager:
            try:
                # Use a thread-safe, minimal matcher (only check for TX/RX text)
                # and perform the full Qt-based matching inside the callback on
                # the main thread. This avoids accessing Qt objects from worker
                # threads which can raise and cause messages to be dropped.
                def _cb(ts, txt, ctx=None):
                    try:
                        def _deliver():
                            try:
                                if self.matches_message(txt, ctx):
                                    self.add_message(ts, txt, ctx)
                            except Exception:
                                pass

                        if threading.current_thread() is threading.main_thread():
                            _deliver()
                        else:
                            QTimer.singleShot(0, _deliver)
                    except Exception:
                        pass

                # Lightweight matcher executed in emission thread only checks for TX/RX markers
                # Accept messages like 'TX FC3' or 'RX FC1' as well as 'TX:'/'RX:'.
                import re as _re
                lightweight_matcher = lambda t, c: bool(_re.search(r"\b(TX|RX)\b", str(t or "")))

                # Register without a matcher so the manager forwards all
                # records; perform per-window filtering on the main thread
                # inside `_cb` to avoid missing messages due to any
                # background-thread matcher false-negatives.
                self._diag_listener_token = self._diag_manager.register_listener(
                    name=f"terminal-{id(self)}",
                    callback=_cb,
                    matcher=None,
                )
                try:
                    snap = self._diag_manager.snapshot()
                    for rec in snap:
                        try:
                            # Pass structured context to matcher so replayed records
                            # are evaluated using the same rules as live emissions.
                            ctx = getattr(rec, 'context', None)
                            if self.matches_message(rec.text, ctx):
                                _cb(rec.timestamp, rec.text, ctx)
                        except Exception:
                            pass
                    # track last processed index so periodic poll only handles new records
                    try:
                        self._last_diag_index = len(snap)
                    except Exception:
                        self._last_diag_index = 0
                except Exception:
                    self._last_diag_index = 0
            except Exception:
                self._diag_listener_token = None

        # 建立菜單欄
        self._setup_menu()

        # start a lightweight poll timer to catch any records that might
        # not arrive via callback due to threading or emission timing issues.
        try:
            self._diag_poll_timer = QTimer(self)
            self._diag_poll_timer.setInterval(200)
            self._diag_poll_timer.timeout.connect(self._poll_diagnostics)
            self._diag_poll_timer.start()
        except Exception:
            self._diag_poll_timer = None

    def closeEvent(self, event):
        try:
            if self._diag_manager and self._diag_listener_token:
                try:
                    self._diag_manager.unregister_listener(self._diag_listener_token)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if getattr(self, '_diag_poll_timer', None):
                try:
                    self._diag_poll_timer.stop()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            p = None
            try:
                p = self.parent()
            except Exception:
                p = None
            if p is not None:
                try:
                    setattr(p, '_terminal_auto_open_blocked', True)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            super().closeEvent(event)
        except Exception:
            event.accept()

    def _device_path(self, item):
        parts = []
        it = item
        while it is not None and it.data(0, Qt.ItemDataRole.UserRole) != "Connectivity":
            parts.insert(0, it.text(0))
            it = it.parent()
        return ".".join(parts)

    def matches_message(self, text: str, ctx=None) -> bool:
        """決定訊息是否應顯示在此視窗（較簡潔、安全的實作）。"""
        # Global diagnostics window: show everything
        if self.device_item is None:
            return True

        txt = str(text or "")

        import re
        # Only TX/RX shown in per-device windows; accept 'TX' or 'RX' tokens
        if not re.search(r"\b(TX|RX)\b", txt):
            return False

        # Prefer structured context when available
        try:
            if isinstance(ctx, dict):
                # match explicit device id if provided
                # NOTE: some emitters include an object `id()` which is not
                # stable across tree rebuilds; only accept immediately when
                # it equals the window's known id, otherwise continue and
                # allow other matching heuristics (host/port/unit, path).
                dev_ctx = ctx.get("dev_id") or ctx.get("device_id")
                if dev_ctx is not None and getattr(self, "_device_item_id", None) is not None:
                    try:
                        if int(dev_ctx) == int(self._device_item_id):
                            return True
                    except Exception:
                        pass

                # match unit if present
                if self._device_unit is not None and ctx.get("unit") is not None:
                    try:
                        if int(ctx.get("unit")) != int(self._device_unit):
                            return False
                    except Exception:
                        pass

                # match host/port against channel params when possible
                try:
                    ch = self.device_item.parent()
                    ch_params = ch.data(2, Qt.ItemDataRole.UserRole) if ch else None
                except Exception:
                    ch_params = None
                if isinstance(ch_params, dict):
                    ch_host = ch_params.get('host') or ch_params.get('ip') or ch_params.get('address')
                    ch_port = ch_params.get('port')
                    if ctx.get('host') is not None and ch_host is not None:
                        if str(ctx.get('host')) != str(ch_host):
                            return False
                    if ctx.get('port') is not None and ch_port is not None:
                        try:
                            if int(ctx.get('port')) != int(ch_port):
                                return False
                        except Exception:
                            pass

                # match device_path/device_name when provided
                if ctx.get('device_path') and getattr(self, '_device_path_str', None) is not None:
                    try:
                        if str(ctx.get('device_path')) != str(self._device_path_str):
                            return False
                    except Exception:
                        pass
                if ctx.get('device_name'):
                    try:
                        name = self.device_item.text(0)
                    except Exception:
                        name = None
                    if name and str(ctx.get('device_name')) != str(name):
                        return False

                # enforce FC whitelist when present
                fc = ctx.get('fc')
                if fc is not None:
                    try:
                        if int(fc) not in (1, 2, 3, 4, 5, 6, 15, 16):
                            return False
                    except Exception:
                        pass

                return True
        except Exception:
            # fall through to heuristics
            pass

        # Fallback: look for DEV_ID= in text
        m2 = re.search(r"DEV_ID=(\d+)", txt)
        if m2:
            try:
                if self._device_item_id is not None and int(m2.group(1)) == int(self._device_item_id):
                    return True
            except Exception:
                pass

        # Heuristic: parse hex payload between | ... |
        m = re.search(r"\|\s*([0-9A-Fa-f\s]+)\s*\|", txt)
        if m:
            parts = [p for p in m.group(1).split() if p]
            bytes_list = []
            for p in parts:
                try:
                    if len(p) <= 2:
                        bytes_list.append(int(p, 16))
                except Exception:
                    continue
            candidate_unit = None
            if len(bytes_list) >= 7:
                candidate_unit = bytes_list[6]
            if candidate_unit is None and bytes_list:
                candidate_unit = bytes_list[0]
            if candidate_unit is not None and self._device_unit is not None:
                try:
                    if int(candidate_unit) == int(self._device_unit):
                        return True
                except Exception:
                    pass

        # match id=NUMBER tokens against known tag ids
        for m in re.finditer(r"id=(\d+)", txt):
            try:
                if int(m.group(1)) in self._device_tag_ids:
                    return True
            except Exception:
                pass

        # textual match device path or name
        if getattr(self, '_device_path_str', None) and self._device_path_str in txt:
            return True
        try:
            name = self.device_item.text(0)
        except Exception:
            name = None
        if name and name in txt:
            return True

        return False

    def add_message(self, ts: str, text: str, ctx=None):
        try:
            from datetime import datetime as _dt
            # ts is a time string like HH:MM:SS.mmm; build date string for Date col
            try:
                date_str = _dt.now().strftime("%Y/%m/%d")
            except Exception:
                date_str = ""

            # prevent exact-duplicate entries (same time + same data) appearing
            try:
                last_row = self.diagnostics_table.rowCount() - 1
                if last_row >= 0:
                    last_time_item = self.diagnostics_table.item(last_row, 1)
                    last_data_item = self.diagnostics_table.item(last_row, 4)
                    last_time = last_time_item.text() if last_time_item is not None else None
                    last_data = last_data_item.text() if last_data_item is not None else None
                    if last_time == ts and last_data == str(text or ""):
                        return
            except Exception:
                pass

            row = self.diagnostics_table.rowCount()
            self.diagnostics_table.insertRow(row)

            # Event/Length/Data parsing with optional structured context
            event = ""
            length = ""
            data_text = str(text or "")
            meta_bits = []
            try:
                if isinstance(ctx, dict):
                    direction = str(ctx.get("direction") or "").upper()
                    fc_val = ctx.get("fc")
                    try:
                        fc_val = int(fc_val)
                    except Exception:
                        fc_val = fc_val
                    if direction:
                        event = direction if fc_val is None else f"{direction} FC{fc_val}"
                    if ctx.get("length") is not None:
                        try:
                            length = str(int(ctx.get("length")))
                        except Exception:
                            length = str(ctx.get("length"))
                    hex_text = ctx.get("hex") or ctx.get("hex_str")
                    if hex_text:
                        data_text = str(hex_text)
                    unit_val = ctx.get("unit")
                    host_val = ctx.get("host")
                    port_val = ctx.get("port")
                    if unit_val is not None:
                        meta_bits.append(f"unit={unit_val}")
                    if host_val:
                        meta_bits.append(f"host={host_val}")
                    if port_val is not None:
                        meta_bits.append(f"port={port_val}")
                    if meta_bits and data_text:
                        data_text = f"{data_text}   ({', '.join(meta_bits)})"
            except Exception:
                pass

            # fallback parsing when no structured context or missing pieces
            if not event:
                try:
                    import re
                    txt_str = str(text or "")
                    m = re.search(r"TX:\s*\|\s*([0-9A-Fa-f\s]+)\s*\|", txt_str)
                    if not m:
                        m = re.search(r"RX:\s*\|\s*([0-9A-Fa-f\s]+)\s*\|", txt_str)
                    if m:
                        hex_s = m.group(1)
                        parts = [p for p in hex_s.split() if p]
                        data_text = " ".join(p.upper() for p in parts)
                        try:
                            length = length or str(len(parts))
                        except Exception:
                            length = length or ""
                        if 'TX:' in txt_str:
                            event = 'TX'
                        elif 'RX:' in txt_str:
                            event = 'RX'
                    else:
                        data_text = txt_str
                except Exception:
                    data_text = str(text or "")

            date_item = QTableWidgetItem(date_str)
            date_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            time_item = QTableWidgetItem(ts)
            time_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            event_item = QTableWidgetItem(event)
            event_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            length_item = QTableWidgetItem(length)
            length_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            data_item = QTableWidgetItem(data_text)

            self.diagnostics_table.setItem(row, 0, date_item)
            self.diagnostics_table.setItem(row, 1, time_item)
            self.diagnostics_table.setItem(row, 2, event_item)
            self.diagnostics_table.setItem(row, 3, length_item)
            self.diagnostics_table.setItem(row, 4, data_item)
            self.diagnostics_table.scrollToBottom()
        except Exception:
            pass

    def _poll_diagnostics(self):
        """Periodically poll DiagnosticsManager.snapshot() and process new records."""
        try:
            if not getattr(self, '_diag_manager', None):
                return
            try:
                snap = self._diag_manager.snapshot()
            except Exception:
                return
            try:
                last = getattr(self, '_last_diag_index', 0) or 0
            except Exception:
                last = 0
            total = len(snap)
            if total <= last:
                return
            for rec in snap[last:]:
                try:
                    ctx = getattr(rec, 'context', None)
                    if self.matches_message(rec.text, ctx):
                        # ensure UI update on main thread
                        try:
                            self.add_message(rec.timestamp, rec.text, ctx)
                        except Exception:
                            pass
                except Exception:
                    pass
            try:
                self._last_diag_index = total
            except Exception:
                self._last_diag_index = total
        except Exception:
            pass
    
    def _setup_menu(self):
        """設置菜單欄"""
        # 清除
        clear_action = QAction("🗑️ 清除", self)
        clear_action.triggered.connect(self._clear_diagnostics)
        self.menuBar().addAction(clear_action)
        
        # 匯出txt
        export_action = QAction("💾 匯出.txt", self)
        export_action.triggered.connect(self._export_to_txt)
        self.menuBar().addAction(export_action)
        # (已移除顯示選項：Diagnostics 現只顯示 TX/RX)
    
    def _clear_diagnostics(self):
        """清除診斷信息"""
        self.diagnostics_table.setRowCount(0)
        if self.parent_window:
            self.parent_window.clear_diagnostics()
    
    def _export_to_txt(self):
        """匯出診斷信息到txt文件"""
        import os
        desktop_path = os.path.join(os.path.expanduser("~"), "Desktop", "Diagnostics.txt")
        file_path, _ = QFileDialog.getSaveFileName(
            self, 
            "匯出診斷信息", 
            desktop_path, 
            "文本文件 (*.txt)"
        )
        if file_path:
            if not file_path.lower().endswith(".txt"):
                file_path = file_path + ".txt"
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    # 寫入表頭（所有欄位）
                    headers = []
                    for c in range(self.diagnostics_table.columnCount()):
                        h = self.diagnostics_table.horizontalHeaderItem(c)
                        headers.append(h.text() if h is not None else "")
                    f.write("\t".join(headers) + "\n")
                    f.write("-" * 100 + "\n")
                    # 寫入每一列的所有欄位，Tab 分隔
                    for row in range(self.diagnostics_table.rowCount()):
                        cols = []
                        for c in range(self.diagnostics_table.columnCount()):
                            item = self.diagnostics_table.item(row, c)
                            cols.append(item.text() if item is not None else "")
                        f.write("\t".join(cols) + "\n")
                QMessageBox.information(self, "成功", f"已匯出到：{file_path}")
            except Exception as e:
                QMessageBox.warning(self, "錯誤", f"匯出失敗：{str(e)}")
    
    def _on_diag_context_menu(self, point):
        """診斷視圖上下文菜單

        注意：此方法可能透過 Qt 的信號/槽或動態建立的選單被呼叫，
        因此即使靜態分析顯示未使用，也請勿移除或重新命名。
        """
        # 保留此 handler 以供動態 UI 呼叫
        pass

    def _on_only_txrx_toggled(self, v: bool):
        """UI toggle handler kept for backward compatibility.

        保留為 Terminal/Diagnostics menu 的回呼（可能以動態方式綁定）。
        即使目前不做事，也不要移除以免破壞動態綁定。
        """
        # intentionally a no-op to preserve signal/slot binding compatibility
        return

    def _on_show_raw_toggled(self, v: bool):
        """UI toggle handler kept for backward compatibility.

        保留為 Terminal/Diagnostics menu 的回呼（可能以動態方式綁定）。
        即使目前不做事，也不要移除以免破壞動態綁定。
        """
        # intentionally a no-op to preserve signal/slot binding compatibility
        return

    # TerminalWindow toggles call these IoTApp setters via parent reference
    def _set_diag_show_only_txrx(self, v: bool):
        try:
            self._diag_show_only_txrx = bool(v)
            try:
                if getattr(self, 'diagnostics', None):
                    self.diagnostics.set_only_txrx(bool(v))
            except Exception:
                pass
        except Exception:
            pass

    def _set_diag_show_raw(self, v: bool):
        try:
            self._diag_show_raw = bool(v)
            try:
                if getattr(self, 'diagnostics', None):
                    # raw view shows everything; otherwise respect the TX/RX-only toggle
                    if self._diag_show_raw:
                        self.diagnostics.set_only_txrx(False)
                    else:
                        self.diagnostics.set_only_txrx(bool(self._diag_show_only_txrx))
            except Exception:
                pass
        except Exception:
            pass


class IoTApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Modbus to OPC UA")
        self.resize(1200, 800)
        self.clipboard_manager = ClipboardManager(self)
        self.controller = AppController(self)
        self.current_project_path = None
        # dirty flag: True when there are unsaved changes
        self._dirty = False
        # tx id counter for MBAP ADU generation in UI-synthesized diagnostics
        self._txid = 0
        # diagnostics display flags (can be toggled from TerminalWindow menu)
        self._diag_show_only_txrx = True
        self._diag_show_raw = False
        # When True, auto-opening the Diagnostics window is suppressed (set when user closes it)
        self._terminal_auto_open_blocked = False

        # Centralized diagnostics manager
        try:
            # 建立 DiagnosticsManager：不要在建立時過濾，讓 per-device 視窗自行過濾
            self.diagnostics = DiagnosticsManager(capacity=8000, logger=logging.getLogger("diagnostics"), only_txrx=False)
        except Exception:
            self.diagnostics = None

        # Avoid spamming stdout by default; attach a NullHandler so libraries won't warn.
        try:
            diag_logger = logging.getLogger("diagnostics")
            if not diag_logger.handlers:
                diag_logger.addHandler(logging.NullHandler())
        except Exception:
            pass

        # 檔案路徑：使用 %APPDATA%/ModUA 存放 temp.json 與 last project path
        try:
            appdata_root = os.getenv('APPDATA') or os.path.join(os.path.expanduser('~'), '.modua')
            self._appdata_dir = os.path.join(appdata_root, 'ModUA')
            os.makedirs(self._appdata_dir, exist_ok=True)
            self._temp_json = os.path.join(self._appdata_dir, 'temp.json')
            self._last_project_file = os.path.join(self._appdata_dir, 'last_project.txt')
        except Exception:
            self._appdata_dir = None
            self._temp_json = None
            self._last_project_file = None
        
        # 不在啟動時建立全域 Diagnostics 視窗（僅透過 Device 右鍵開啟 per-device 窗口）
        self.terminal_window = None
        # 管理多個 per-device diagnostics 視窗
        self.terminal_windows = []

        # Note: previously installed a pymodbus log handler that forwarded
        # SEND/RECV messages into Diagnostics; removed to avoid terminal
        # clutter and duplicate emissions. Diagnostics are now produced by
        # pollers/clients directly via `append_diagnostic`.

        self.splitter = QSplitter(Qt.Orientation.Horizontal)
        self.tree = ConnectivityTree()
        
        # 調整樹形視圖的行高，使其與右邊表格保持一致
        self.tree.setUniformRowHeights(True)

        # 統一表頭高度和行高
        if hasattr(self.tree, "setStyleSheet"):
            self.tree.setStyleSheet(
                """
                QTreeWidget::item {
                    padding: 3px;
                    height: 22px;
                }
                QHeaderView::section {
                    padding: 2px;
                    height: 24px;
                }
            """
            )

        # 調整表頭高度
        if hasattr(self.tree, "header"):
            self.tree.header().setDefaultSectionSize(100)

        # Data broker: thread-safe latest-values store
        try:
            self.data_broker = DataBroker()
        except Exception:
            self.data_broker = None

        # OPC server holder and update timer
        self.opc_server = None
        self._opc_update_timer = None

        # 🔗 連結樹狀圖訊號
        self.tree.request_new_channel.connect(self.on_new_channel)
        self.tree.request_new_device.connect(self.on_new_device)
        self.tree.request_new_group.connect(self.on_new_group)
        self.tree.request_new_tag.connect(self.on_new_tag)

        # device diagnostics (right-click on Device node)
        try:
            self.tree.request_device_diagnostics.connect(self.open_device_diagnostics)
        except Exception:
            pass

        self.tree.request_edit_item.connect(self.on_edit_item)
        self.tree.request_delete_item.connect(self.on_delete_item)
        self.tree.request_copy_item.connect(self.on_copy_item)
        self.tree.request_paste_item.connect(self.on_paste_item)
        self.tree.request_cut_item.connect(self.on_cut_item)
        self.tree.request_import_csv.connect(self.on_import_device_csv)
        self.tree.request_export_csv.connect(self.on_export_device_csv)

        # 單擊更新右側表格
        self.tree.itemClicked.connect(self.update_right_table)
        self.splitter.addWidget(self.tree)

        # --- 右側表格初始化 ---
        self.tag_table = QTableWidget()
        self.tag_table.setContextMenuPolicy(
            Qt.ContextMenuPolicy.CustomContextMenu
        )
        self.tag_table.customContextMenuRequested.connect(
            self.on_table_context_menu
        )
        self.tag_table.cellDoubleClicked.connect(self.on_table_cell_double_clicked)

        # 🔑 關鍵修改：支援 Shift 多選
        self.tag_table.setSelectionMode(
            QTableWidget.SelectionMode.ExtendedSelection
        )
        self.tag_table.setSelectionBehavior(
            QTableWidget.SelectionBehavior.SelectRows
        )

        # (已回復) 不強制設定 tag_table 的固定列高與表頭高度，使用預設行為

        # 🔑 關鍵修改：Delete 快捷鍵
        self.del_shortcut = QShortcut(QKeySequence.StandardKey.Delete, self.tag_table)
        self.del_shortcut.activated.connect(self.on_delete_selected_tags)

        self.splitter.addWidget(self.tag_table)
        self.splitter.setStretchFactor(0, 3)
        self.splitter.setStretchFactor(1, 7)
        
        # 創建垂直分割器，上方是 tree+tag_table，下方是 monitor_table
        self.vsplitter = QSplitter(Qt.Orientation.Vertical)
        self.vsplitter.addWidget(self.splitter)
        
        # --- Monitor table（Tag 監看視窗）---
        self.monitor_table = QTableWidget()
        self.monitor_table.setColumnCount(6)
        self.monitor_table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        self.monitor_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.monitor_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.monitor_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.monitor_table.customContextMenuRequested.connect(self._on_monitor_value_context_menu)
        self.monitor_table.setHorizontalHeaderLabels([
            "Item ID",
            "Data Type",
            "Value",
            "Timestamp",
            "Quality",
            "Update Count",
        ])
        header = self.monitor_table.horizontalHeader()
        # 欄位自動根據內容調整寬度；如果還有空間則保留空白（不強制拉伸）
        for c in range(self.monitor_table.columnCount()):
            header.setSectionResizeMode(c, QHeaderView.ResizeMode.ResizeToContents)
        header.setStretchLastSection(False)
        # (已回復) 不強制設定 monitor_table 的固定列高與表頭高度，使用預設行為
        
        self.vsplitter.addWidget(self.monitor_table)

        self.setCentralWidget(self.vsplitter)
        # UI initialization complete

        # polling structures
        # `self.pollers` holds one AsyncPoller instance per unique device connection
        self.pollers = []
        self.monitor_row = {}
        self.monitor_counts = {}
        self.monitor_last_values = {}
        self.poll_settings = {"host": "127.0.0.1", "port": 502, "unit": 1, "interval": 1.0}
        self.monitored_tags = []

        # --- File menu (New/Open/Save/Save As) ---
        file_menu = self.menuBar().addMenu("📁 File")
        new_action = file_menu.addAction("📄 New")
        open_action = file_menu.addAction("📂 Open...")
        save_action = file_menu.addAction("💾 Save")
        save_as_action = file_menu.addAction("💾 Save As...")
        new_action.triggered.connect(self.new_project)
        open_action.triggered.connect(self.open_project)
        save_action.triggered.connect(self.save_project)
        save_as_action.triggered.connect(self.save_project_as)

        # --- Runtime indicator/action (click to toggle start/stop) ---
        runtime_indicator = QAction("🟢 Runtime", self)
        runtime_indicator.triggered.connect(self.toggle_runtime)
        # add as a top-level action (appears on the menu bar like a menu title)
        self.menuBar().addAction(runtime_indicator)
        # keep reference so we can update the indicator text/color emoji
        self.runtime_indicator_action = runtime_indicator
        
        # Diagnostics is now exposed via Device right-click in the tree.

        # --- OPC UA button (open settings) ---
        opcua_action = QAction("🔗 OPC UA", self)
        opcua_action.triggered.connect(self.open_opcua_settings)
        self.menuBar().addAction(opcua_action)

        # 初始化焦點選取在 Connectivity 節點
        self.tree.setCurrentItem(self.tree.conn_node)
        # 嘗試載入上次儲存的專案（若存在）
        try:
            # 如果 temp.json 存在，優先載入 temp（恢復上次的暫存狀態）
            loaded = False
            if self._temp_json and os.path.exists(self._temp_json):
                try:
                    self.controller.import_project_from_json(self._temp_json)
                    self.current_project_path = None
                    loaded = True
                    try:
                        self.tree.expandAll()
                    except Exception:
                        pass
                except Exception:
                    loaded = False

            # 若沒有 temp，嘗試載入上次開啟的專案路徑（若存在）
            if not loaded and self._last_project_file and os.path.exists(self._last_project_file):
                try:
                    with open(self._last_project_file, "r", encoding="utf-8") as _f:
                        last_path = _f.read().strip()
                except Exception:
                    last_path = None
                if last_path and os.path.exists(last_path):
                    try:
                        self.controller.import_project_from_json(last_path)
                        self.current_project_path = last_path
                        try:
                            self.tree.expandAll()
                        except Exception:
                            pass
                    except Exception:
                        pass
                else:
                    try:
                        self.tree.expandAll()
                    except Exception:
                        pass
            else:
                try:
                    self.tree.expandAll()
                except Exception:
                    pass
        except Exception:
            try:
                self.tree.expandAll()
            except Exception:
                pass

        # 更新右側表格（以目前 tree 狀態為準）
        self.update_right_table(self.tree.conn_node, 0)
        try:
            # ensure monitor & opcua reflect loaded project immediately
            if hasattr(self, '_on_project_structure_changed'):
                try:
                    self._on_project_structure_changed()
                except Exception:
                    pass
        except Exception:
            pass
        # On startup, if monitor empty, populate it from the tree
        try:
            if getattr(self, 'monitor_table', None) is not None and self.monitor_table.rowCount() == 0:
                try:
                    self.add_all_tags_to_monitor()
                except Exception:
                    pass
        except Exception:
            pass

        # Diagnostic: report whether opcua settings were loaded and whether OPCServer class is available
        try:
            try:
                s = getattr(self, 'opcua_settings', None)
                ok = OPCServer is not None
                # summarize settings (avoid dumping sensitive data)
                summary = None
                try:
                    if s is None:
                        summary = 'None'
                    elif isinstance(s, dict):
                        summary = f'dict(keys={list(s.keys())})'
                    else:
                        summary = str(type(s))
                except Exception:
                    summary = 'unrepresentable'
                try:
                    self._write_opc_trace(f'Startup: opcua_settings={summary}; OPCServerAvailable={ok}')
                except Exception:
                    pass
            except Exception:
                pass
        except Exception:
            pass
        # 若載入時帶有 opcua_settings，統一以 `apply_opcua_settings()` 處理
        #（該函式會停止舊伺服器、建立並啟動新伺服器，然後建立節點）
        try:
            if getattr(self, 'opcua_settings', None):
                try:
                    self._write_opc_trace('Startup: applying opcua_settings via apply_opcua_settings()')
                except Exception:
                    pass
                try:
                    # use the centralised helper to ensure identical behaviour
                    # to when the user presses OK in the OPC UA settings dialog
                    self.apply_opcua_settings(self.opcua_settings)
                except Exception as e:
                    try:
                        import traceback
                        self._write_opc_trace(f'Startup: apply_opcua_settings failed: {e}\n{traceback.format_exc()}')
                    except Exception:
                        pass
        except Exception:
            pass
    
    def show_terminal_window(self):
        """彈出 Terminal 窗口

        注意：保留此方法以便外部程式或測試可以顯式呼叫；
        它目前不會自動建立或顯示全域視窗，但仍是公開介面的一部份。
        """
        # 全域 Diagnostics 已被禁用：此方法保留以維持公開介面兼容性
        return

    def open_device_diagnostics(self, device_item):
        """Open a diagnostics window filtered to the given device_item."""
        try:
            tw = TerminalWindow(self, device_item=device_item, diagnostics_manager=getattr(self, 'diagnostics', None))
            self.terminal_windows.append(tw)
            tw.show()
            tw.raise_()
            tw.activateWindow()
            # return created window in case caller needs it
            return tw
        except Exception:
            return None

    def _get_or_open_device_terminal(self, device_item):
        """Return an existing per-device TerminalWindow for device_item or open one."""
        try:
            if device_item is None:
                return None
            dev_id = int(id(device_item))
            # search existing windows
            wins = getattr(self, 'terminal_windows', []) or []
            for w in wins:
                try:
                    if getattr(w, '_device_item_id', None) == dev_id:
                        return w
                except Exception:
                    pass
            # Do not auto-open a new diagnostics window here; only return existing.
            return None
        except Exception:
            return None

    def _get_or_open_opc_terminal(self):
        """Return or open a dedicated OPC Diagnostics terminal window.

        注意：保留此 helper 以便其他組件可以檢索 (或選擇性建立) OPC diagnostics 窗口。
        不會自動建立視窗，但外部代碼可能動態呼叫此方法。
        """
        try:
            wins = getattr(self, 'terminal_windows', []) or []
            for w in wins:
                try:
                    if getattr(w, '_is_opc', False):
                        return w
                except Exception:
                    pass
            # Do not auto-create or show OPC diagnostics window; return None if not present.
            return None
        except Exception:
            return None

    # Methods called by TerminalWindow menu actions to update diagnostics flags
    def _set_diag_show_only_txrx(self, v: bool):
        try:
            self._diag_show_only_txrx = bool(v)
        except Exception:
            pass

    def _set_diag_show_raw(self, v: bool):
        try:
            self._diag_show_raw = bool(v)
        except Exception:
            pass

    # --- 🗑️ 多選刪除專用函數 ---
    def on_delete_selected_tags(self):
        """處理表格內的多選刪除"""
        current_node = self.tree.currentItem()
        if not current_node:
            return

        # 獲取表格中所有選中的列
        indices = self.tag_table.selectionModel().selectedRows()
        if not indices:
            return

        if (
            QMessageBox.question(
                self, "Delete", f"確定刪除選中的 {len(indices)} 個項目?"
            )
            == QMessageBox.StandardButton.Yes
        ):
            # collect selected rows and delete corresponding tag tree items
            try:
                rows = sorted([r.row() for r in indices], reverse=True)
                for r in rows:
                    try:
                        itm = self.tag_table.item(r, 0)
                        if itm is None:
                            continue
                        tag_tree_item = None
                        try:
                            tag_tree_item = itm.data(Qt.ItemDataRole.UserRole)
                        except Exception:
                            tag_tree_item = None
                        if tag_tree_item is not None:
                            try:
                                parent = tag_tree_item.parent() or self.tree.invisibleRootItem()
                                parent.removeChild(tag_tree_item)
                            except Exception:
                                pass
                    except Exception:
                        pass
            except Exception:
                pass

            # refresh UI and notify controller about structure change
            try:
                self.update_right_table(current_node, 0)
            except Exception:
                pass
            try:
                if hasattr(self, '_on_project_structure_changed'):
                    try:
                        self._on_project_structure_changed()
                    except Exception:
                        pass
            except Exception:
                pass

    def on_new_channel(self, parent_item):
        suggested = f"Channel{parent_item.childCount() + 1}"
        dialog = ChannelDialog(self, suggested_name=suggested)
        if dialog.exec():
            data = dialog.get_data()
            new_item = QTreeWidgetItem(parent_item)
            new_item.setData(0, Qt.ItemDataRole.UserRole, "Channel")
            self.controller.save_channel(new_item, data)
            parent_item.setExpanded(True)
            self.update_right_table(parent_item, 0)

    def on_new_device(self, channel_item):
        driver_name = channel_item.data(1, Qt.ItemDataRole.UserRole) or "Modbus RTU Serial"
        next_id = self.controller.calculate_next_id(channel_item)
        suggested_name = f"Device{channel_item.childCount() + 1}"

        dialog = DeviceDialog(
            self, suggested_name=suggested_name, driver_type=driver_name
        )
        dialog.load_data({"name": suggested_name, "device_id": next_id, "description": ""})

        if dialog.exec():
            data = dialog.get_data()
            new_item = QTreeWidgetItem(channel_item)
            new_item.setData(0, Qt.ItemDataRole.UserRole, "Device")
            self.controller.save_device(new_item, data)
            channel_item.setExpanded(True)
            self.update_right_table(channel_item, 0)

    def on_new_group(self, parent_item):
        group_count = sum(
            1
            for i in range(parent_item.childCount())
            if parent_item.child(i).data(0, Qt.ItemDataRole.UserRole) == "Group"
        )
        new_item = QTreeWidgetItem(parent_item)
        new_item.setText(0, f"Group{group_count + 1}")
        new_item.setData(0, Qt.ItemDataRole.UserRole, "Group")
        parent_item.setExpanded(True)
        self.update_right_table(parent_item, 0)

    def on_new_tag(self, parent_item):
        existing_tags = [
            parent_item.child(i)
            for i in range(parent_item.childCount())
            if parent_item.child(i).data(0, Qt.ItemDataRole.UserRole) == "Tag"
        ]

        used_names = [t.text(0) for t in existing_tags]
        used_addresses = [t.data(1, Qt.ItemDataRole.UserRole) for t in existing_tags]

        next_idx = 1
        while f"Tag{next_idx}" in used_names:
            next_idx += 1
        suggested_name = f"Tag{next_idx}"
        suggested_addr = self.controller.calculate_next_address(parent_item)

        dialog = TagDialog(self, suggested_name=suggested_name, suggested_addr=suggested_addr)

        while dialog.exec():
            data = dialog.get_data()
            tag_name = data["general"]["name"]
            tag_addr = data["general"]["address"]

            if tag_name in used_names:
                QMessageBox.warning(self, "警告", f"Tag 名稱 '{tag_name}' 已存在！")
                continue
            if tag_addr in used_addresses:
                QMessageBox.warning(self, "警告", f"位址 '{tag_addr}' 已被使用！")
                continue

            new_item = QTreeWidgetItem(parent_item)
            new_item.setData(0, Qt.ItemDataRole.UserRole, "Tag")
            new_item.setHidden(True)

            self.controller.save_tag(new_item, data)
            self.update_right_table(parent_item, 0)
            break

    # --- 💾 資料儲存與輔助邏輯 ---

    # NOTE: next-id/address and save_* are handled by AppController

    # --- 📊 介面更新與表格互動 ---

    def update_right_table(self, item, _column=0):
        if not item:
            return
        node_type = item.data(0, Qt.ItemDataRole.UserRole)
        self.tag_table.setRowCount(0)

        if node_type == "Connectivity":
            self._setup_table([
                "Channel Name",
                "Driver",
                "Connection",
                "Description",
            ])
            for i in range(item.childCount()):
                child = item.child(i)
                row = self.tag_table.rowCount()
                self.tag_table.insertRow(row)
                it0 = QTableWidgetItem(child.text(0))
                it0.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 0, it0)
                it1 = QTableWidgetItem(child.data(1, Qt.ItemDataRole.UserRole) or "")
                it1.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 1, it1)
                params = child.data(2, Qt.ItemDataRole.UserRole) or {}
                conn = params.get("com") or params.get("adapter") or ""
                it2 = QTableWidgetItem(str(conn))
                it2.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 2, it2)
                it3 = QTableWidgetItem(child.data(3, Qt.ItemDataRole.UserRole) or "")
                it3.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 3, it3)

        elif node_type == "Channel":
            self._setup_table(["Device Name", "Model", "ID", "Description"])
            for i in range(item.childCount()):
                child = item.child(i)
                row = self.tag_table.rowCount()
                self.tag_table.insertRow(row)
                it0 = QTableWidgetItem(child.text(0))
                it0.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 0, it0)
                it1 = QTableWidgetItem(child.data(1, Qt.ItemDataRole.UserRole) or "Modbus")
                it1.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 1, it1)
                it2 = QTableWidgetItem(str(child.data(2, Qt.ItemDataRole.UserRole) or ""))
                it2.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 2, it2)
                it3 = QTableWidgetItem(child.data(3, Qt.ItemDataRole.UserRole) or "")
                it3.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 3, it3)

        elif node_type in ["Device", "Group"]:
            self._setup_table([
                "Tag Name",
                "Address",
                "Data Type",
                "Scan Rate",
                "Scaling",
                "Description",
            ])

            def _add_tag_row(child):
                row = self.tag_table.rowCount()
                self.tag_table.insertRow(row)
                scaling_data = child.data(5, Qt.ItemDataRole.UserRole) or {}
                scale_type = scaling_data.get("type", "None")

                it0 = QTableWidgetItem(child.text(0))
                it0.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                self.tag_table.setItem(row, 0, it0)

                it1 = QTableWidgetItem(child.data(1, Qt.ItemDataRole.UserRole) or "")
                it1.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 1, it1)

                it2 = QTableWidgetItem(child.data(2, Qt.ItemDataRole.UserRole) or "")
                it2.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 2, it2)

                it3 = QTableWidgetItem(f"{child.data(4, Qt.ItemDataRole.UserRole) or '10'} ms")
                it3.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 3, it3)

                it4 = QTableWidgetItem(scale_type)
                it4.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 4, it4)

                it5 = QTableWidgetItem(child.data(3, Qt.ItemDataRole.UserRole) or "")
                it5.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 5, it5)

            if node_type == "Device":
                # show only direct Tag children of the device (do not include tags inside groups)
                for i in range(item.childCount()):
                    child = item.child(i)
                    if child.data(0, Qt.ItemDataRole.UserRole) == "Tag":
                        _add_tag_row(child)
            else:
                # Group: show only direct Tag children (no nested groups)
                for i in range(item.childCount()):
                    child = item.child(i)
                    if child.data(0, Qt.ItemDataRole.UserRole) == "Tag":
                        _add_tag_row(child)

    def _mark_dirty(self, v: bool = True):
        try:
            self._dirty = bool(v)
            base = "Modbus to OPC UA"
            try:
                if self._dirty:
                    self.setWindowTitle(base + " *")
                else:
                    self.setWindowTitle(base)
            except Exception:
                pass
        except Exception:
            pass

    def _on_project_structure_changed(self):
        """Called when channels/devices/tags structure changed.
        Keep monitor view and OPC UA nodes in sync with the tree.
        """
        try:
            # refresh right table for current selection
            try:
                cur = getattr(self, 'tree', None).currentItem() if getattr(self, 'tree', None) else None
                if cur is None:
                    cur = getattr(self, 'tree', None).conn_node if getattr(self, 'tree', None) else None
                if cur is not None:
                    try:
                        self.update_right_table(cur, 0)
                    except Exception:
                        pass
            except Exception:
                pass

            # Rebuild monitor entries if runtime is running or monitor already has items
            try:
                running = any(getattr(p, '_running', False) for p in getattr(self, 'pollers', []))
            except Exception:
                running = False
            try:
                has_monitor_items = getattr(self, 'monitor_table', None) is not None and self.monitor_table.rowCount() > 0
            except Exception:
                has_monitor_items = False

            if running or has_monitor_items:
                # if running, stop polling briefly to avoid races
                was_running = running
                try:
                    if was_running:
                        self.stop_polling()
                except Exception:
                    pass

                try:
                    # clear existing monitor state
                    if getattr(self, 'monitor_table', None) is not None:
                        self.monitor_table.setRowCount(0)
                    self.monitor_row.clear()
                    self.monitor_counts.clear()
                    self.monitor_last_values.clear()
                    self.monitored_tags.clear()
                except Exception:
                    pass

                try:
                    # re-add all tags to monitor
                    self.add_all_tags_to_monitor()
                except Exception:
                    pass

                try:
                    if was_running:
                        self.start_polling()
                except Exception:
                    pass

            # Update OPC UA nodes if server exists
            try:
                if getattr(self, 'opc_server', None) is not None:
                    root = getattr(self.tree, 'conn_node', None)
                    if root is not None:
                        try:
                            self.opc_server.setup_tags_from_tree(root)
                        except Exception as e:
                            try:
                                import traceback
                                try:
                                    self._write_opc_trace(f'OPC UA: setup_tags_from_tree failed on project-structure-change: {e}\n{traceback.format_exc()}')
                                except Exception:
                                    pass
                            except Exception:
                                pass
            except Exception:
                pass
        except Exception:
            pass

    def closeEvent(self, event):
        """Handle app close: save temp.json and prompt on unsaved changes."""
        try:
            # if dirty, ask user to save or cancel
            if getattr(self, '_dirty', False):
                msg = QMessageBox(self)
                msg.setIcon(QMessageBox.Icon.Warning)
                msg.setWindowTitle("有未儲存的變更")
                msg.setText("您有未儲存的變更。要現在儲存嗎？")
                save_btn = msg.addButton("儲存", QMessageBox.ButtonRole.AcceptRole)
                msg.addButton("取消", QMessageBox.ButtonRole.RejectRole)
                msg.exec()
                btn = msg.clickedButton()
                if btn == save_btn:
                    # try to save; if no current path, perform Save As
                    if not self.current_project_path:
                        self.save_project_as()
                    else:
                        self.save_project()
                    # if still dirty, user likely cancelled save-as dialog -> abort close
                    if getattr(self, '_dirty', False):
                        event.ignore()
                        return
                else:
                    # cancel -> abort close
                    event.ignore()
                    return

            # always write temp.json as snapshot before exit
            try:
                if getattr(self, '_temp_json', None):
                    self.controller.export_project_to_json(self._temp_json)
            except Exception:
                pass

            # stop runtime and pollers cleanly
            try:
                self.stop_runtime()
            except Exception:
                pass

            # stop OPC UA server if running
            try:
                if getattr(self, 'opc_server', None):
                    try:
                        self.opc_server.stop()
                    except Exception:
                        pass
                    self.opc_server = None
            except Exception:
                pass

            try:
                if getattr(self, 'diagnostics', None):
                    self.diagnostics.stop()
            except Exception:
                pass

            event.accept()
        except Exception:
            try:
                event.accept()
            except Exception:
                pass
            else:
                # Group: show only direct Tag children (do not include tags from nested groups)
                for i in range(item.childCount()):
                    child = item.child(i)
                    if (
                        child.data(0, Qt.ItemDataRole.UserRole) == "Tag"
                    ):
                        row = self.tag_table.rowCount()
                        self.tag_table.insertRow(row)
                        scaling_data = child.data(5, Qt.ItemDataRole.UserRole) or {}
                        scale_type = scaling_data.get("type", "None")
                        it0 = QTableWidgetItem(child.text(0))
                        it0.setTextAlignment(
                            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
                        )
                        self.tag_table.setItem(row, 0, it0)
                        it1 = QTableWidgetItem(child.data(1, Qt.ItemDataRole.UserRole) or "")
                        it1.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                        self.tag_table.setItem(row, 1, it1)
                        it2 = QTableWidgetItem(child.data(2, Qt.ItemDataRole.UserRole) or "")
                        it2.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                        self.tag_table.setItem(row, 2, it2)
                        it3 = QTableWidgetItem(f"{child.data(4, Qt.ItemDataRole.UserRole) or '10'} ms")
                        it3.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                        self.tag_table.setItem(row, 3, it3)
                        it4 = QTableWidgetItem(scale_type)
                        it4.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                        self.tag_table.setItem(row, 4, it4)
                        it5 = QTableWidgetItem(child.data(3, Qt.ItemDataRole.UserRole) or "")
                        it5.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                        self.tag_table.setItem(row, 5, it5)

    def _setup_table(self, headers):
        self.tag_table.setColumnCount(len(headers))
        self.tag_table.setHorizontalHeaderLabels(headers)
        # 欄位自動根據內容調整寬度；保留右側空白（不強制拉伸）
        header = self.tag_table.horizontalHeader()
        for c in range(len(headers)):
            header.setSectionResizeMode(c, QHeaderView.ResizeMode.ResizeToContents)
        header.setStretchLastSection(False)

    # --- 🖱️ 表格右鍵與雙擊邏輯 ---

    def on_table_cell_double_clicked(self, row, _column):
        current_node = self.tree.currentItem()
        if not current_node:
            return
        node_type = current_node.data(0, Qt.ItemDataRole.UserRole)

        if node_type in ["Device", "Group"]:
            tags = [
                current_node.child(i)
                for i in range(current_node.childCount())
                if current_node.child(i).data(0, Qt.ItemDataRole.UserRole) == "Tag"
            ]
            if 0 <= row < len(tags):
                self.on_edit_tag(tags[row])
        elif node_type == "Connectivity":
            child = current_node.child(row)
            if child:
                self.on_edit_channel(child)
        elif node_type == "Channel":
            child = current_node.child(row)
            if child:
                self.on_edit_device(child)

    def on_table_context_menu(self, pos):
        current_node = self.tree.currentItem()
        if not current_node:
            return
        node_type = current_node.data(0, Qt.ItemDataRole.UserRole)

        menu = QMenu()
        item_at = self.tag_table.itemAt(pos)
        current_row = self.tag_table.row(item_at) if item_at else -1

        # 判定選中狀態
        selected_rows = self.tag_table.selectionModel().selectedRows()
        is_multi = len(selected_rows) > 1

        # 1. 基本動作
        add_action = None
        if not is_multi:
            if node_type == "Connectivity":
                add_action = menu.addAction("➕ 新增 Channel")
            elif node_type == "Channel":
                add_action = menu.addAction("➕ 新增 Device")
            elif node_type in ["Device", "Group"]:
                add_action = menu.addAction("➕ 新增 Tag")

            paste_action = menu.addAction("📥 貼上")
            if not self.clipboard_manager.clipboard:
                paste_action.setEnabled(False)

            # CSV import/export available via left-tree context menu (Device node)

        # 2. 選中項目的動作
        edit_action = del_action = copy_action = cut_action = None
        if is_multi or item_at:
            menu.addSeparator()
            if not is_multi:
                cut_action = menu.addAction("✂️ 剪下")
                copy_action = menu.addAction("📋 複製")
                edit_action = menu.addAction("✏️ 內容")
            del_action = menu.addAction("❌ 刪除")

        action = menu.exec(self.tag_table.viewport().mapToGlobal(pos))

        # 3. 執行
        if action == add_action:
            if node_type == "Connectivity":
                self.on_new_channel(current_node)
            elif node_type == "Channel":
                self.on_new_device(current_node)
            elif node_type in ["Device", "Group"]:
                self.on_new_tag(current_node)
        elif "paste_action" in locals() and action == paste_action:
            target = (
                current_node.child(current_row) if (item_at and current_row != -1) else current_node
            )
            parent = self.clipboard_manager.paste(target)
            if parent:
                self.update_right_table(parent, 0)
        elif action == edit_action:
            self.on_table_cell_double_clicked(current_row, 0)
        elif action == copy_action:
            target_item = current_node.child(current_row)
            if target_item:
                self.on_copy_item(target_item)
        elif action == cut_action:
            target_item = current_node.child(current_row)
            if target_item:
                self.on_cut_item(target_item)
        elif action == del_action:
            self.on_delete_selected_tags()

    # --- ✏️ 編輯功能 ---

    def on_edit_item(self, item):
        node_type = item.data(0, Qt.ItemDataRole.UserRole)
        if node_type == "Channel":
            self.on_edit_channel(item)
        elif node_type == "Device":
            self.on_edit_device(item)
        elif node_type == "Tag":
            self.on_edit_tag(item)

    def on_edit_channel(self, item):
        current = {
            "name": item.text(0),
            "driver": item.data(1, Qt.ItemDataRole.UserRole),
            "params": item.data(2, Qt.ItemDataRole.UserRole),
            # ensure description is a string for the dialog
            "description": (item.data(3, Qt.ItemDataRole.UserRole) if not isinstance(item.data(3, Qt.ItemDataRole.UserRole), dict) else "") ,
        }
        dialog = ChannelDialog(self)
        dialog.load_data(current)
        if dialog.exec():
            self.controller.save_channel(item, dialog.get_data())
            self.update_right_table(item.parent(), 0)

    def on_edit_device(self, item):
        channel_item = item.parent()
        driver_name = (
            channel_item.data(1, Qt.ItemDataRole.UserRole) if channel_item else "Modbus RTU Serial"
        )
        current = {
            "name": item.text(0),
            "device_id": item.data(2, Qt.ItemDataRole.UserRole),
            "description": (item.data(3, Qt.ItemDataRole.UserRole) if not isinstance(item.data(3, Qt.ItemDataRole.UserRole), dict) else ""),
            "timing": item.data(4, Qt.ItemDataRole.UserRole),
            "data_access": item.data(5, Qt.ItemDataRole.UserRole),
            "encoding": item.data(6, Qt.ItemDataRole.UserRole),
            "block_sizes": item.data(7, Qt.ItemDataRole.UserRole),
            "ethernet": item.data(8, Qt.ItemDataRole.UserRole),
        }
        dialog = DeviceDialog(self, driver_type=driver_name)
        dialog.load_data(current)
        if dialog.exec():
            # capture returned data (suppress diagnostic noise)
            returned = dialog.get_data()
            self.controller.save_device(item, returned)
            self.update_right_table(item.parent(), 0)

    def on_edit_tag(self, item):
        # gather access from stored slot or attached model so dialog shows correct value
        access_val = None
        try:
            access_val = item.data(9, Qt.ItemDataRole.UserRole)
        except Exception:
            access_val = None
        try:
            if not access_val:
                mdl = item.data(0, Qt.ItemDataRole.UserRole + 1)
                if mdl is not None and hasattr(mdl, "access"):
                    access_val = getattr(mdl, "access")
        except Exception:
            pass
        if not access_val:
            access_val = "Read/Write"

        current = {
            "general": {
                "name": item.text(0),
                "address": item.data(1, Qt.ItemDataRole.UserRole),
                "data_type": item.data(2, Qt.ItemDataRole.UserRole),
                "description": item.data(3, Qt.ItemDataRole.UserRole),
                "scan_rate": item.data(4, Qt.ItemDataRole.UserRole),
                "access": access_val,
            },
            "scaling": item.data(5, Qt.ItemDataRole.UserRole) or {"type": "None"},
        }
        dialog = TagDialog(self)
        dialog.load_data(current)
        if dialog.exec():
            self.controller.save_tag(item, dialog.get_data())
            self.update_right_table(item.parent(), 0)

    # --- 📋 剪貼簿與單項刪除 ---

    def on_delete_item(self, item):
        if item.data(0, Qt.ItemDataRole.UserRole) in ["Project", "Connectivity"]:
            return
        if (
            QMessageBox.question(self, "Delete", f"確定刪除 '{item.text(0)}'?")
            == QMessageBox.StandardButton.Yes
        ):
            parent = item.parent() or self.tree.invisibleRootItem()
            parent.removeChild(item)
            self.update_right_table(parent, 0)
            try:
                if hasattr(self, '_on_project_structure_changed'):
                    try:
                        self._on_project_structure_changed()
                    except Exception:
                        pass
            except Exception:
                pass
            # ensure monitor is populated after opening (if currently empty)
            try:
                if getattr(self, 'monitor_table', None) is not None and self.monitor_table.rowCount() == 0:
                    try:
                        self.add_all_tags_to_monitor()
                    except Exception:
                        pass
            except Exception:
                pass

    def on_copy_item(self, item):
        self.clipboard_manager.copy(item)

    def on_cut_item(self, item):
        parent = self.clipboard_manager.cut(item)
        if parent:
            self.update_right_table(parent, 0)
            try:
                if hasattr(self, '_on_project_structure_changed'):
                    try:
                        self._on_project_structure_changed()
                    except Exception:
                        pass
            except Exception:
                pass

    def on_paste_item(self, target_item):
        # 現在由 ClipboardManager 處理貼上，並回傳被影響的父節點
        parent = self.clipboard_manager.paste(target_item)
        if parent:
            self.update_right_table(parent, 0)
            try:
                if hasattr(self, '_on_project_structure_changed'):
                    try:
                        self._on_project_structure_changed()
                    except Exception:
                        pass
            except Exception:
                pass

    def on_import_device_csv(self, device_item):
        import os
        desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
        path, _ = QFileDialog.getOpenFileName(self, "Import Device CSV", desktop_path, "CSV Files (*.csv)")
        if path:
            self.controller.import_device_from_csv(device_item, path)
            self.update_right_table(device_item, 0)

    def on_export_device_csv(self, device_item):
        import os
        desktop_path = os.path.join(os.path.expanduser("~"), "Desktop", "Device.csv")
        path, _ = QFileDialog.getSaveFileName(self, "Export Device CSV", desktop_path, "CSV Files (*.csv)")
        if path:
            if not path.lower().endswith(".csv"):
                path = path + ".csv"
            # ask encoding in a separate small dialog (preserves native file dialog appearance)
            enc, ok = QInputDialog.getItem(
                self, "Encoding", "Select file encoding:", ["UTF-8", "ANSI"], 0, False
            )
            if not ok:
                return
            encoding = "utf-8" if enc == "UTF-8" else "mbcs"
            self.controller.export_device_to_csv(device_item, path, encoding=encoding)

    # --- Project file operations ---
    def new_project(self):
        # clear all under Connectivity
        root = self.tree.conn_node
        # remove children
        while root.childCount() > 0:
            root.removeChild(root.child(0))
        self.current_project_path = None
        # 清除上次專案記錄
        try:
            if getattr(self, "_last_project_file", None) and os.path.exists(self._last_project_file):
                try:
                    os.remove(self._last_project_file)
                except Exception:
                    pass
        except Exception:
            pass
        # clearing project should clear dirty state and remove temp
        try:
            self._mark_dirty(False)
            if getattr(self, '_temp_json', None) and os.path.exists(self._temp_json):
                try:
                    os.remove(self._temp_json)
                except Exception:
                    pass
        except Exception:
            pass
        self.update_right_table(self.tree.conn_node, 0)
        try:
            if hasattr(self, '_on_project_structure_changed'):
                try:
                    self._on_project_structure_changed()
                except Exception:
                    pass
        except Exception:
            pass

    def open_project(self):
        import os
        desktop_path = os.path.join(os.path.expanduser("~"), "Desktop", "project.json")
        path, _ = QFileDialog.getOpenFileName(self, "Open Project", desktop_path, "JSON Files (*.json)")
        if not path:
            return

        # Load project file and update UI; keep logic simple and explicit
        try:
            self.controller.import_project_from_json(path)
        except Exception as e:
            QMessageBox.warning(self, "Open Failed", f"Failed to open project: {path}\n{e}")
            return

        self.current_project_path = path
        try:
            self.tree.expandAll()
        except Exception:
            pass

        # If opcua settings present, apply them (simulate pressing OK on settings)
        try:
            if getattr(self, 'opcua_settings', None):
                try:
                    self.apply_opcua_settings(self.opcua_settings)
                except Exception:
                    pass
        except Exception:
            pass

        # save last project path
        try:
            if getattr(self, "_last_project_file", None):
                with open(self._last_project_file, "w", encoding="utf-8") as _f:
                    _f.write(path)
        except Exception:
            pass

        # loaded project -> not dirty and remove temp
        try:
            self._mark_dirty(False)
            if getattr(self, '_temp_json', None) and os.path.exists(self._temp_json):
                try:
                    os.remove(self._temp_json)
                except Exception:
                    pass
        except Exception:
            pass

        # refresh UI and other subscribers
        try:
            self.update_right_table(self.tree.conn_node, 0)
        except Exception:
            pass
        try:
            if hasattr(self, '_on_project_structure_changed'):
                self._on_project_structure_changed()
        except Exception:
            pass

    def save_project(self):
        if not self.current_project_path:
            return self.save_project_as()
        try:
            self.controller.export_project_to_json(self.current_project_path)
            # 寫入 last project 檔案
            try:
                if getattr(self, "_last_project_file", None):
                    with open(self._last_project_file, "w", encoding="utf-8") as _f:
                        _f.write(self.current_project_path)
            except Exception:
                pass
            # saved -> clear dirty and remove temp
            try:
                self._mark_dirty(False)
                if getattr(self, '_temp_json', None) and os.path.exists(self._temp_json):
                    try:
                        os.remove(self._temp_json)
                    except Exception:
                        pass
            except Exception:
                pass
            # ensure monitor/OPC reflect saved project
            try:
                if hasattr(self, '_on_project_structure_changed'):
                    try:
                        self._on_project_structure_changed()
                    except Exception:
                        pass
            except Exception:
                pass
            # ensure monitor is populated after save (if currently empty)
            try:
                if getattr(self, 'monitor_table', None) is not None and self.monitor_table.rowCount() == 0:
                    try:
                        self.add_all_tags_to_monitor()
                    except Exception:
                        pass
            except Exception:
                pass
        except Exception:
            QMessageBox.warning(self, "Save Failed", f"Failed to save project: {self.current_project_path}")

    def save_project_as(self):
        import os
        desktop_path = os.path.join(os.path.expanduser("~"), "Desktop", "Project.json")
        path, _ = QFileDialog.getSaveFileName(self, "Save Project As", desktop_path, "JSON Files (*.json)")
        if not path:
            return
        if not path.lower().endswith(".json"):
            path = path + ".json"
        try:
            self.controller.export_project_to_json(path)
            self.current_project_path = path
            # 寫入 last project 檔案
            try:
                if getattr(self, "_last_project_file", None):
                    with open(self._last_project_file, "w", encoding="utf-8") as _f:
                        _f.write(path)
            except Exception:
                pass
            # saved -> clear dirty and remove temp
            try:
                self._mark_dirty(False)
                if getattr(self, '_temp_json', None) and os.path.exists(self._temp_json):
                    try:
                        os.remove(self._temp_json)
                    except Exception:
                        pass
            except Exception:
                pass
            # ensure monitor/OPC reflect saved project
            try:
                if hasattr(self, '_on_project_structure_changed'):
                    try:
                        self._on_project_structure_changed()
                    except Exception:
                        pass
            except Exception:
                pass
            # ensure monitor is populated after save-as (if currently empty)
            try:
                if getattr(self, 'monitor_table', None) is not None and self.monitor_table.rowCount() == 0:
                    try:
                        self.add_all_tags_to_monitor()
                    except Exception:
                        pass
            except Exception:
                pass
        except Exception:
            QMessageBox.warning(self, "Save Failed", f"Failed to save project: {path}")

    # (helper functions for unique naming and deserialization
    # have been moved to ClipboardManager and controller modules)

    # --- Modbus monitor helpers ---
    

    def add_selected_to_monitor(self):
        """Add currently selected Tag(s) to the Monitor view.

        此方法由 UI Action (右鍵選單 / toolbar) 觸發，可被動態綁定，
        因此即使靜態分析顯示未直接呼叫，也不要移除或重命名。
        """
        current = self.tree.currentItem()
        if not current:
            return
        node_type = current.data(0, Qt.ItemDataRole.UserRole)
        tags = []
        if node_type == "Tag":
            tags = [current]
        elif node_type in ["Device", "Group"]:
            for i in range(current.childCount()):
                c = current.child(i)
                if c.data(0, Qt.ItemDataRole.UserRole) == "Tag":
                    tags.append(c)
        else:
            if node_type == "Channel":
                for i in range(current.childCount()):
                    dev = current.child(i)
                    if dev.data(0, Qt.ItemDataRole.UserRole) == "Device":
                        for j in range(dev.childCount()):
                            t = dev.child(j)
                            if t.data(0, Qt.ItemDataRole.UserRole) == "Tag":
                                tags.append(t)

        for t in tags:
            tid = id(t)
            data_type = t.data(2, Qt.ItemDataRole.UserRole) or ""
            
            # 检测数组类型
            is_array = "Array" in str(data_type)
            array_size = 0
            if is_array:
                # 优先从Address字段解析数组大小
                array_size = self._get_array_size(t)
                if array_size is None:
                    # 尝试从tag的值中获取数组大小
                    try:
                        value = t.data(3, Qt.ItemDataRole.UserRole)
                        if isinstance(value, (list, tuple)):
                            array_size = len(value)
                        else:
                            array_size = 10
                    except Exception:
                        array_size = 10
                else:
                    array_size = int(array_size)
            
            # 检查是否已在monitor中
            if is_array:
                already_added = any((tid, idx) in self.monitor_row for idx in range(max(1, array_size)))
                if already_added:
                    continue
            else:
                if tid in self.monitor_row:
                    continue
            
            if is_array and array_size > 0:
                # 添加数组中的每个元素
                for idx in range(array_size):
                    row = self.monitor_table.rowCount()
                    self.monitor_table.insertRow(row)
                    item_id = QTableWidgetItem(self._format_item_id(t, idx))
                    try:
                        item_id.setData(Qt.ItemDataRole.UserRole, t)
                    except Exception:
                        pass
                    item_id.setTextAlignment(
                        Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
                    )
                    dt_item = QTableWidgetItem(data_type)
                    dt_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    val_item = QTableWidgetItem("")
                    val_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    ts_item = QTableWidgetItem("")
                    ts_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    q_item = QTableWidgetItem("Bad")
                    q_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    uc_item = QTableWidgetItem("0")
                    uc_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    self.monitor_table.setItem(row, 0, item_id)
                    self.monitor_table.setItem(row, 1, dt_item)
                    self.monitor_table.setItem(row, 2, val_item)
                    self.monitor_table.setItem(row, 3, ts_item)
                    self.monitor_table.setItem(row, 4, q_item)
                    self.monitor_table.setItem(row, 5, uc_item)
                    self.monitor_row[(tid, idx)] = row
                    self.monitor_counts[(tid, idx)] = 0
                    self.monitor_last_values[(tid, idx)] = None
            else:
                # 非数组，正常添加
                row = self.monitor_table.rowCount()
                self.monitor_table.insertRow(row)
                item_id = QTableWidgetItem(self._format_item_id(t))
                try:
                    item_id.setData(Qt.ItemDataRole.UserRole, t)
                except Exception:
                    pass
                item_id.setTextAlignment(
                    Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
                )
                dt_item = QTableWidgetItem(data_type)
                dt_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                val_item = QTableWidgetItem("")
                val_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                ts_item = QTableWidgetItem("")
                ts_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                q_item = QTableWidgetItem("Bad")
                q_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                uc_item = QTableWidgetItem("0")
                uc_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.monitor_table.setItem(row, 0, item_id)
                self.monitor_table.setItem(row, 1, dt_item)
                self.monitor_table.setItem(row, 2, val_item)
                self.monitor_table.setItem(row, 3, ts_item)
                self.monitor_table.setItem(row, 4, q_item)
                self.monitor_table.setItem(row, 5, uc_item)
                self.monitor_row[tid] = row
                self.monitor_counts[tid] = 0
                self.monitor_last_values[tid] = None
            
            # 如果还没有轮询运行，添加tag
            if t not in self.monitored_tags:
                self.monitored_tags.append(t)
            # 如果轮询已在运行，将tag添加到轮询器
            if self.pollers:
                try:
                    key = self._compute_tag_conn_key(t)
                    for p in self.pollers:
                        try:
                            pk = getattr(p, '__conn_key__', None)
                            if pk == key and hasattr(p, 'add_tag'):
                                p.add_tag(t)
                                break
                        except Exception:
                            continue
                except Exception:
                    pass

    def _format_item_id(self, tag_item, index=None):
        parts = []
        it = tag_item
        while it is not None and it.data(0, Qt.ItemDataRole.UserRole) != "Connectivity":
            parts.insert(0, it.text(0))
            it = it.parent()
        item_id = ".".join(parts)
        if index is not None:
            item_id = f"{item_id}[{index}]"
        return item_id

    def _compute_tag_conn_key(self, tag_item, default=None):
        """Return (host, port, unit, interval_seconds) used to select/create poller for a tag."""
        # defaults
        host = None
        port = None
        unit = None
        interval = None

        # find enclosing Device
        dev = tag_item.parent()
        while dev is not None and dev.data(0, Qt.ItemDataRole.UserRole) != "Device":
            dev = dev.parent()

        # device-level ethernet settings (data slot 8)
        if dev is not None:
            try:
                eth = dev.data(8, Qt.ItemDataRole.UserRole)
                if isinstance(eth, dict) and eth:
                    host = eth.get("ip") or eth.get("host") or eth.get("address")
                    try:
                        port = int(eth.get("port", port)) if eth.get("port", None) is not None else port
                    except Exception:
                        port = port
            except Exception:
                pass

            # unit stored at data(2)
            try:
                unit_val = dev.data(2, Qt.ItemDataRole.UserRole)
                if unit_val is not None:
                    try:
                        unit = int(unit_val)
                    except Exception:
                        unit = unit_val
            except Exception:
                pass

            # channel-level fallback for host/port
            try:
                if (not host or not port) and dev is not None:
                    ch = dev.parent()
                    if ch is not None and ch.data(0, Qt.ItemDataRole.UserRole) == "Channel":
                        ch_params = ch.data(2, Qt.ItemDataRole.UserRole)
                        if isinstance(ch_params, dict):
                            if not host:
                                host = ch_params.get("ip") or ch_params.get("host") or ch_params.get("address")
                            try:
                                if port is None:
                                    port = int(ch_params.get("port", port))
                            except Exception:
                                port = port
            except Exception:
                pass

        # compute interval from tag scan rate (data slot 4)
        try:
            scan = tag_item.data(4, Qt.ItemDataRole.UserRole)
            ms = None
            if scan is None or str(scan).strip() == "":
                ms = None
            else:
                s = str(scan).strip().lower()
                if s.endswith("ms"):
                    try:
                        ms = float(s[:-2].strip())
                    except Exception:
                        ms = None
                elif s.endswith("s"):
                    try:
                        sec = float(s[:-1].strip())
                        ms = sec * 1000.0
                    except Exception:
                        ms = None
                else:
                    try:
                        ms = float(s)
                    except Exception:
                        ms = None
            if ms is None:
                interval = None
            else:
                ms = max(10.0, min(ms, 99999990.0))
                ms = round(ms / 10.0) * 10.0
                interval = ms / 1000.0
        except Exception:
            interval = None

        if default is not None:
            try:
                dh, dp, du, di = default
            except Exception:
                dh = dp = du = di = None
            if host is None:
                host = dh
            if port is None:
                port = dp
            if unit is None:
                unit = du
            if interval is None:
                interval = di

        # determine function code (fc) from tag address prefix so pollers
        # can be separated per FC when desired. Follow Kepware mapping:
        # 0xxxx -> FC1 (coils), 1xxxx -> FC2 (discrete inputs),
        # 3xxxx -> FC4 (input registers), 4xxxx -> FC3 (holding regs)
        fc = 3
        try:
            addr_raw = tag_item.data(1, Qt.ItemDataRole.UserRole)
        except Exception:
            addr_raw = None
        try:
            def _digits(s):
                if s is None:
                    return ""
                return "".join(ch for ch in str(s) if ch.isdigit())
            nums = _digits(addr_raw)
            if len(nums) == 5 and nums.startswith("4"):
                nums = nums[0] + "0" + nums[1:]
            nums = nums.zfill(6)
            lead = nums[0] if nums else "4"
            if lead in ("0", "1"):
                fc = 1 if lead == "0" else 2
            elif lead == "3":
                fc = 4
            elif lead == "4":
                fc = 3
            else:
                fc = 3
        except Exception:
            fc = 3

        return (host, port, unit, interval, fc)

    def append_diagnostic(self, text: str, context=None):
        try:
            if getattr(self, 'diagnostics', None):
                self.diagnostics.emit(text, context=context)
                # Also proactively deliver to any already-open TerminalWindow
                # instances to ensure per-device windows update immediately
                try:
                    def _deliver_to_windows():
                        try:
                            wins = getattr(self, 'terminal_windows', []) or []
                            # try to build a structured context for better matching
                            try:
                                parsed_ctx = None
                                if getattr(self, 'diagnostics', None) and hasattr(self.diagnostics, '_parse_txrx_context'):
                                    try:
                                        parsed_ctx = self.diagnostics._parse_txrx_context(text, context)
                                    except Exception:
                                        parsed_ctx = context
                                else:
                                    parsed_ctx = context
                            except Exception:
                                parsed_ctx = context

                            for w in wins:
                                try:
                                    if not (hasattr(w, 'matches_message') and hasattr(w, 'add_message')):
                                        continue
                                    try:
                                        if w.matches_message(text, parsed_ctx):
                                            w.add_message(time.strftime("%H:%M:%S") + ".000", text, parsed_ctx)
                                    except Exception:
                                        pass
                                except Exception:
                                    pass
                        except Exception:
                            pass

                    # Ensure UI calls run on the main thread
                    try:
                        from PyQt6.QtCore import QTimer
                        QTimer.singleShot(0, _deliver_to_windows)
                    except Exception:
                        try:
                            _deliver_to_windows()
                        except Exception:
                            pass
                except Exception:
                    pass
                return
        except Exception:
            pass
        try:
            # fallback if diagnostics manager is unavailable
            logging.debug(str(text))
        except Exception:
            pass

    def _write_opc_trace(self, text: str):
        """Route OPC-related messages into diagnostics without auto-opening windows."""
        try:
            # Prefer to send as a normal diagnostic line prefixed with OPC so
            # it can be filtered into per-device windows if relevant.
            self.append_diagnostic(f"OPC: {text}")
        except Exception:
            pass

    # menu callbacks for TerminalWindow
    def _on_toggle_only_txrx(self, v):
        """Menu callback: toggle showing only TX/RX in diagnostics.

        Kept for backward compatibility and dynamic menu binding; do not remove.
        """
        try:
            if self.parent_window:
                self.parent_window._set_diag_show_only_txrx(bool(v))
        except Exception:
            pass

    def _on_toggle_show_raw(self, v):
        """Menu callback: toggle showing raw diagnostics text.

        Kept for backward compatibility and dynamic menu binding; do not remove.
        """
        try:
            if self.parent_window:
                self.parent_window._set_diag_show_raw(bool(v))
        except Exception:
            pass

    def clear_diagnostics(self):
        try:
            if getattr(self, 'diagnostics', None):
                self.diagnostics.clear()
        except Exception:
            pass
        try:
            wins = getattr(self, 'terminal_windows', []) or []
            for w in wins:
                try:
                    if w and hasattr(w, 'diagnostics_table'):
                        w.diagnostics_table.setRowCount(0)
                except Exception:
                    pass
        except Exception:
            pass

    def start_runtime(self):
        """啟動 Runtime - 自動添加所有 tag 到 monitor 並開始輪詢"""
        # 更新 runtime 指示為紅色 (running)
        try:
            try:
                self.runtime_indicator_action.setText("🔴 Runtime")
            except Exception:
                pass
        except Exception:
            pass

        # Ensure monitor is populated before starting pollers and log startup.
        try:
            try:
                from datetime import datetime as _dt
                t = _dt.now()
                ms = int(t.microsecond / 1000)
                ts = f"{t.strftime('%H:%M:%S')}.{ms:03d}"
                mon_exists = getattr(self, 'monitor_table', None) is not None
                rows = self.monitor_table.rowCount() if mon_exists else 'NA'
                try:
                    if getattr(self, 'diagnostics', None):
                        self.diagnostics.emit(f"RUNTIME_START: monitor_exists={mon_exists} rows={rows}")
                except Exception:
                    pass
            except Exception:
                pass

            if getattr(self, 'monitor_table', None) is not None:
                try:
                    self.add_all_tags_to_monitor()
                except Exception:
                    pass

            # Only control the pollers (start polling); monitor/OPC management is done elsewhere.
            try:
                self.start_polling()
            except Exception:
                pass
        except Exception:
            pass

    def stop_runtime(self):
        """停止 Runtime"""
        # 更新 runtime 指示為綠色 (stopped)
        try:
            try:
                self.runtime_indicator_action.setText("🟢 Runtime")
            except Exception:
                pass
        except Exception:
            pass
        self.stop_polling()
        # NOTE: do not clear monitor on stop_runtime — keep monitor contents intact

    def toggle_runtime(self):
        """Toggle runtime state: start if stopped, stop if running."""
        try:
            running = any(getattr(p, '_running', False) for p in self.pollers)
        except Exception:
            running = False
        try:
            if running:
                self.stop_runtime()
            else:
                self.start_runtime()
        except Exception:
            pass

    def open_opcua_settings(self):
        try:
            initial = getattr(self, 'opcua_settings', None) or {}
            dlg = OPCUADialog(self, initial=initial)
            if dlg.exec() != QDialog.DialogCode.Accepted:
                return

            vals = dlg.values()
            # store settings and apply them (same behaviour as pressing OK)
            self.opcua_settings = vals
            try:
                self._write_opc_trace(f"OPC UA settings saved: {vals}")
            except Exception:
                pass

            # apply settings (stop old, create/start new, setup nodes)
            self.apply_opcua_settings(vals)

            # create periodic timer to push broker snapshot to OPC server
            try:
                if getattr(self, 'opc_server', None) and getattr(self, 'data_broker', None):
                    if self._opc_update_timer is None:
                        self._opc_update_timer = QTimer(self)
                        self._opc_update_timer.setInterval(200)
                        self._opc_update_timer.timeout.connect(self._opc_push_and_sync)
                        self._opc_update_timer.start()
            except Exception:
                pass
        except Exception as e:
            try:
                self._write_opc_trace(f"Failed to open OPC UA settings: {e}")
            except Exception:
                pass

    def apply_opcua_settings(self, vals: dict):
        """Apply OPC UA settings as if the user pressed OK in the settings dialog.
        Stop any existing server, create/start a new one, and populate nodes from the tree.
        """
        try:
            self.opcua_settings = vals
        except Exception:
            pass

        # stop existing server
        if getattr(self, 'opc_server', None):
            try:
                self.opc_server.stop()
            except Exception:
                pass
            self.opc_server = None

        if OPCServer is None:
            try:
                self._write_opc_trace('OPC UA library not available; cannot start server')
            except Exception:
                pass
            return

        try:
            self.opc_server = OPCServer(vals)
            try:
                self.opc_server.start()
                try:
                    self._write_opc_trace('OPC UA: Running')
                except Exception:
                    pass
            except Exception as e:
                try:
                    self._write_opc_trace(f'OPC UA failed to start: {e}')
                except Exception:
                    pass
                self.opc_server = None
        except Exception:
            self.opc_server = None

        # create nodes from tree
        try:
            if getattr(self, 'opc_server', None) and getattr(self, 'tree', None):
                root = getattr(self.tree, 'conn_node', None)
                if root:
                    try:
                        self.opc_server.setup_tags_from_tree(root)
                        try:
                            # mark that nodes have been populated to avoid duplicate creations later
                            try:
                                self.opc_server._nodes_populated = True
                            except Exception:
                                pass
                        except Exception:
                            pass
                        try:
                            self._write_opc_trace('OPC UA: setup_tags_from_tree completed after OPC UA settings applied')
                        except Exception:
                            pass

                        # ensure the periodic OPC UA push timer exists when server and data broker are present
                        try:
                            if getattr(self, 'opc_server', None) and getattr(self, 'data_broker', None):
                                if self._opc_update_timer is None:
                                    self._opc_update_timer = QTimer(self)
                                    self._opc_update_timer.setInterval(200)
                                    self._opc_update_timer.timeout.connect(self._opc_push_and_sync)
                                    self._opc_update_timer.start()
                        except Exception:
                            pass
                    except Exception as e:
                        try:
                            import traceback
                            self._write_opc_trace(f'OPC UA: setup_tags_from_tree failed after OPC UA settings applied: {e}\n{traceback.format_exc()}')
                        except Exception:
                            pass
        except Exception:
            pass

    def add_all_tags_to_monitor(self):
        """添加所有 tag 到 monitor 視窗"""
        connectivity_node = self.tree.conn_node
        if not connectivity_node:
            return
        
        # 遍歷所有 Channel
        for ch_idx in range(connectivity_node.childCount()):
            channel = connectivity_node.child(ch_idx)
            if not channel or channel.data(0, Qt.ItemDataRole.UserRole) != "Channel":
                continue
            
            # 遍歷每個 Channel 下的 Device
            for dev_idx in range(channel.childCount()):
                device = channel.child(dev_idx)
                if not device or device.data(0, Qt.ItemDataRole.UserRole) != "Device":
                    continue
                
                # 遍歷 Device 下的 Tag
                self._add_tags_from_device(device)
                
                # 遍歷 Device 下的 Group
                for grp_idx in range(device.childCount()):
                    group = device.child(grp_idx)
                    if not group or group.data(0, Qt.ItemDataRole.UserRole) != "Group":
                        continue
                    
                    # 遍歷 Group 下的 Tag
                    self._add_tags_from_group(group)

    def _find_tag_item_by_canonical_key(self, key):
        """Traverse the tree to find a tag item whose canonical key matches `key`.
        Returns the QTreeWidgetItem or None."""
        try:
            root = getattr(self.tree, 'conn_node', None)
            if root is None:
                return None
            stack = [root]
            while stack:
                item = stack.pop()
                try:
                    role = item.data(0, Qt.ItemDataRole.UserRole) if Qt is not None else None
                except Exception:
                    role = None
                try:
                    if role == 'Tag' or (item.text(0) and item.childCount() == 0):
                        try:
                            ckey = type(self.data_broker)._make_key_from_tag_item(item)
                            if ckey == key:
                                return item
                        except Exception:
                            pass
                except Exception:
                    pass
                try:
                    for i in range(item.childCount()):
                        stack.append(item.child(i))
                except Exception:
                    pass
        except Exception:
            return None
        return None

    def _handle_tag_polled_maybe_update_broker(self, tag_item, value, timestamp, quality):
        """Called for each poll result; update DataBroker unless a recent
        OPC-originated write for the same canonical key is still in the grace period."""
        try:
            # compute canonical key
            try:
                canonical = type(self.data_broker)._make_key_from_tag_item(tag_item) if getattr(self, 'data_broker', None) is not None else None
            except Exception:
                canonical = None
            # check recent OPC writes
            skip = False
            try:
                rw_lock = getattr(self, '_recent_opc_writes_lock', None)
                recent = getattr(self, '_recent_opc_writes', None)
                if rw_lock and recent is not None and canonical is not None:
                    import time as _time
                    with rw_lock:
                        ts = recent.get(canonical)
                    if ts is not None and (_time.time() - ts) < 2.0:
                        skip = True
            except Exception:
                skip = False

            if not skip:
                try:
                    # forward to the original broker handler
                    self.data_broker.handle_polled(tag_item, value, timestamp, quality)
                except Exception:
                    pass
                else:
                    # suppressed broker update due to recent OPC write -> write to trace file
                    try:
                        try:
                            self._write_opc_trace(f"SUPPRESS_POLL_OVERWRITE: {canonical}")
                        except Exception:
                            pass
                    except Exception:
                        pass
        except Exception:
            pass

    def _opc_push_and_sync(self):
        """Push DataBroker snapshot to OPC (existing behaviour) and also detect
        OPC-side writes and forward them to Modbus via `controller.write_tag_value`.
        """
        try:
            if not getattr(self, 'opc_server', None) or not getattr(self, 'data_broker', None):
                return
            # Read current snapshots and node list
            snap = self.data_broker.snapshot()
            try:
                node_keys = list(getattr(self.opc_server, '_nodes', {}).keys())
            except Exception:
                node_keys = []

            # (debug output removed to avoid log flood)

            # First: detect OPC-side writes (OPC value differs from broker) and forward them to Modbus
            for k in node_keys:
                try:
                    opc_val = self.opc_server.read_tag_value(k)
                except Exception:
                    opc_val = None
                try:
                    broker_entry = snap.get(k)
                    broker_val = broker_entry.get('value') if broker_entry else None
                except Exception:
                    broker_val = None

                # If OPC has no value, skip.
                if opc_val is None:
                    continue

                # If broker has no polled value yet, skip forwarding OPC default/initial values
                # to Modbus — these are often node defaults from the OPC server and
                # cause noisy writes on startup. Record the fact to the trace file.
                if broker_entry is None:
                    try:
                        self._write_opc_trace(f"OPC_WRITE_IGNORED_NO_BROKER_VALUE: {k} opc_val={repr(opc_val)}")
                    except Exception:
                        pass
                    continue

                # Only forward when values actually differ
                if opc_val != broker_val:
                    try:
                        if not self.opc_server.is_tag_writable(k):
                            continue
                    except Exception:
                        continue

                    tag_item = self._find_tag_item_by_canonical_key(k)
                    # ensure we don't schedule a duplicate write while one is pending
                    try:
                        import time as _time
                        import threading as _thr
                        if not hasattr(self, '_pending_opc_writes_lock'):
                            self._pending_opc_writes_lock = _thr.RLock()
                        if not hasattr(self, '_pending_opc_writes'):
                            self._pending_opc_writes = {}
                        pending_skip = False
                        with self._pending_opc_writes_lock:
                            ts = self._pending_opc_writes.get(k)
                            if ts is not None and (_time.time() - ts) < 5.0:
                                pending_skip = True
                            else:
                                # mark pending now
                                self._pending_opc_writes[k] = _time.time()
                        if pending_skip:
                            try:
                                self._write_opc_trace(f"SKIP_DUPLICATE_PENDING_WRITE: {k} opc_val={repr(opc_val)}")
                            except Exception:
                                pass
                            continue
                    except Exception:
                        pass
                    try:
                        self._write_opc_trace(f"OPC_WRITE_DETECTED: key={k} opc_val={repr(opc_val)} broker_val={repr(broker_val)} tag_item_found={tag_item is not None}")
                    except Exception:
                        pass
                    if tag_item is None:
                        try:
                            self._write_opc_trace(f"OPC_WRITE_IGNORED_NO_TAG: {k}")
                        except Exception:
                            pass
                        continue
                    # perform Modbus write in background thread to avoid blocking UI
                    def _do_write(tag_item_local, opc_val_local, key_local):
                        try:
                            # persistent trace to a file for debugging write path
                            try:
                                self._write_opc_trace(f"WRITE_CALL START: {key_local} => {repr(opc_val_local)}")
                            except Exception:
                                pass
                            # log intent (use TX: prefix so append_diagnostic shows it)
                            try:
                                self._write_opc_trace(f"TX: OPC->Modbus WRITE_CALL: {key_local} => {repr(opc_val_local)}")
                            except Exception:
                                pass
                            # perform the potentially-blocking write
                            # wrap diag callback so emitted TX/RX include device context
                            try:
                                dev_item = None
                                try:
                                    dev_item = tag_item_local.parent()
                                    while dev_item is not None and dev_item.data(0, Qt.ItemDataRole.UserRole) != 'Device':
                                        dev_item = dev_item.parent()
                                except Exception:
                                    dev_item = None
                                dev_id_prefix = ''
                                try:
                                    if dev_item is not None:
                                        dev_id_prefix = f"DEV_ID={int(id(dev_item))} "
                                except Exception:
                                    dev_id_prefix = ''
                            except Exception:
                                dev_id_prefix = ''

                            def _diag_with_dev(msg):
                                try:
                                    # ensure TX/RX lines are visible in Diagnostics by adding device context
                                    self.append_diagnostic(f"{dev_id_prefix}{msg}")
                                except Exception:
                                    try:
                                        self.append_diagnostic(msg)
                                    except Exception:
                                        pass

                            self.controller.write_tag_value(tag_item_local, opc_val_local, diag_callback=_diag_with_dev)
                            try:
                                self._write_opc_trace(f"TX: OPC->Modbus WRITE_OK: {key_local}")
                            except Exception:
                                pass
                            try:
                                self._write_opc_trace(f"WRITE_OK: {key_local}")
                            except Exception:
                                pass
                            # record recent OPC write time to protect against immediate Broker->OPC overwrite
                            try:
                                import threading as _thr
                                import time as _time
                                if not hasattr(self, '_recent_opc_writes_lock'):
                                    self._recent_opc_writes_lock = _thr.RLock()
                                if not hasattr(self, '_recent_opc_writes'):
                                    self._recent_opc_writes = {}
                                with self._recent_opc_writes_lock:
                                    self._recent_opc_writes[key_local] = _time.time()
                                # clear pending flag
                                try:
                                    if hasattr(self, '_pending_opc_writes_lock') and hasattr(self, '_pending_opc_writes'):
                                        with self._pending_opc_writes_lock:
                                            if key_local in self._pending_opc_writes:
                                                del self._pending_opc_writes[key_local]
                                except Exception:
                                    pass
                            except Exception:
                                pass
                            # update broker (DataBroker is thread-safe)
                            try:
                                self.data_broker.handle_polled(tag_item_local, opc_val_local, time.time(), 'Good')
                            except Exception:
                                pass
                        except Exception as e:
                            try:
                                # schedule a trace write on main thread
                                from PyQt6.QtCore import QTimer
                                QTimer.singleShot(0, lambda: self._write_opc_trace(f"OPC->Modbus write failed for {key_local}: {e}"))
                            except Exception:
                                try:
                                    self._write_opc_trace(f"OPC->Modbus write failed for {key_local}: {e}")
                                except Exception:
                                    pass
                            try:
                                self._write_opc_trace(f"WRITE_EXCEPTION: {key_local} => {repr(e)}")
                            except Exception:
                                pass
                            # clear pending flag on failure as well
                            try:
                                if hasattr(self, '_pending_opc_writes_lock') and hasattr(self, '_pending_opc_writes'):
                                    with self._pending_opc_writes_lock:
                                        if key_local in self._pending_opc_writes:
                                            del self._pending_opc_writes[key_local]
                            except Exception:
                                pass

                    try:
                        import threading
                        t = threading.Thread(target=_do_write, args=(tag_item, opc_val, k), daemon=True)
                        t.start()
                    except Exception as e:
                            try:
                                self._write_opc_trace(f"OPC->Modbus spawn write thread failed for {k}: {e}")
                            except Exception:
                                pass

            # Refresh snapshot because we may have updated the broker above
            snap = self.data_broker.snapshot()

            # Then: Broker -> OPC — push canonical broker values to OPC nodes
            # Grace period (seconds) after an OPC-originated write during which
            # Broker->OPC updates will not overwrite the node.
            grace_seconds = 2.0
            import time as _time
            for k, v in snap.items():
                try:
                    try:
                        rw_lock = getattr(self, '_recent_opc_writes_lock', None)
                        recent = getattr(self, '_recent_opc_writes', None)
                        skip = False
                        if rw_lock and recent is not None:
                            try:
                                with rw_lock:
                                    ts = recent.get(k)
                            except Exception:
                                ts = None
                            try:
                                if ts is not None and (_time.time() - ts) < grace_seconds:
                                    skip = True
                            except Exception:
                                pass
                        if skip:
                            # optionally clear expired entries later; just skip update now
                            try:
                                self._write_opc_trace(f"SUPPRESS_POLL_OVERWRITE: {k}")
                            except Exception:
                                pass
                            continue
                    except Exception:
                        pass

                    val = None
                    try:
                        val = v.get('value') if isinstance(v, dict) else v
                    except Exception:
                        val = None

                    # Do not overwrite OPC with None values
                    if val is None:
                        continue

                    ok = False
                    try:
                        ok = bool(self.opc_server.update_tag(k, val))
                    except Exception:
                        ok = False
                    if not ok:
                        try:
                            self._write_opc_trace(f"UPDATE_FAIL: OPC update failed for {k} val={repr(val)}")
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass

    def _add_tags_from_device(self, device):
        """從 Device 添加所有 Tag"""
        for i in range(device.childCount()):
            child = device.child(i)
            if child and child.data(0, Qt.ItemDataRole.UserRole) == "Tag":
                self._add_single_tag_to_monitor(child)

    def _add_tags_from_group(self, group):
        """從 Group 添加所有 Tag"""
        for i in range(group.childCount()):
            child = group.child(i)
            if child and child.data(0, Qt.ItemDataRole.UserRole) == "Tag":
                self._add_single_tag_to_monitor(child)

    def _get_array_size(self, tag_item):
        """从Tag的Address字段中解析数组大小，格式: 400095 [25]"""
        try:
            address = tag_item.data(1, Qt.ItemDataRole.UserRole)  # Address通常在data slot 1
            if address:
                address_str = str(address).strip()
                # 查找 [数字] 格式
                import re
                match = re.search(r'\[(\d+)\]', address_str)
                if match:
                    return int(match.group(1))
        except Exception:
            pass
        return None

    def _extract_device_params(self, device_item):
        """從 Device item 提取所有參數信息"""
        params = {
            'device_item': device_item,
            'device_name': device_item.text(0) if device_item else "",
            'ip': "",
            'port': 502,
            'unit_id': 1,
            'fc05_enabled': False,  # Coil 寫入：FC05 enable?
            'fc06_enabled': False,  # Holding Register 寫入：FC06 enable?
            'first_word_low': True,  # First Word Low 編碼格式
        }
        
        try:
            # 遍歷所有 data slot 獲取 Device 參數
            for slot in range(15):
                try:
                    data = device_item.data(slot, Qt.ItemDataRole.UserRole)
                    if data is None:
                        continue
                    
                    if isinstance(data, dict):
                        # 檢查各種參數鍵
                        for key in data.keys():
                            key_lower = str(key).lower()
                            val = data[key]
                            
                            # 檢查 IP 地址
                            if "ip" in key_lower and ("address" in key_lower or key_lower == "ip"):
                                params['ip'] = str(data[key])
                            
                            # 檢查 Port
                            elif "port" in key_lower:
                                try:
                                    params['port'] = int(data[key])
                                except:
                                    pass
                            
                            # 檢查 Unit ID
                            elif "unit" in key_lower and "id" in key_lower:
                                try:
                                    params['unit_id'] = int(data[key])
                                except:
                                    pass
                            
                            # 檢查 Modbus Function 05 (Coil 寫入 - 單個)
                            elif ("function" in key_lower or "func" in key_lower) and "05" in key_lower:
                                is_enabled = str(val).strip().lower() == "enable" or val is True
                                params['fc05_enabled'] = is_enabled
                            
                            # 檢查 Modbus Function 06 (Holding Register 寫入 - 單個)
                            elif ("function" in key_lower or "func" in key_lower) and "06" in key_lower:
                                is_enabled = str(val).strip().lower() == "enable" or val is True
                                params['fc06_enabled'] = is_enabled
                            
                            # 檢查 First Word Low 編碼格式
                            elif "word" in key_lower and "low" in key_lower:
                                is_enabled = str(val).strip().lower() == "enable" or val is True
                                params['first_word_low'] = is_enabled
                
                except Exception as e:
                    pass
        except Exception as e:
            pass
        
        return params

    def _add_single_tag_to_monitor(self, tag_item):
        """添加單個 Tag 到 monitor"""
        tid = id(tag_item)
        data_type = tag_item.data(2, Qt.ItemDataRole.UserRole) or ""
        
        # 獲取對應的 Device 及其參數
        device_item = tag_item.parent()
        # 如果 Tag 在 Group 裡，需要先找到 Group 的 parent (Device)
        if device_item and device_item.data(0, Qt.ItemDataRole.UserRole) == "Group":
            device_item = device_item.parent()
        
        if not device_item or device_item.data(0, Qt.ItemDataRole.UserRole) != "Device":
            return  # 無法找到 Device，skip
        
        # 獲取 Device 的參數（IP、Port、Unit ID 等）
        device_params = self._extract_device_params(device_item)
        
        # 检测数组类型
        is_array = "Array" in str(data_type)
        array_size = 0
        if is_array:
            # 优先从Address字段解析数组大小
            array_size = self._get_array_size(tag_item)
            if array_size is None:
                # 尝试从tag的值中获取数组大小
                try:
                    value = tag_item.data(3, Qt.ItemDataRole.UserRole)  # 可能的初始值位置
                    if isinstance(value, (list, tuple)):
                        array_size = len(value)
                    else:
                        # 默认数组大小
                        array_size = 10
                except Exception:
                    array_size = 10
            else:
                # 确保是整数
                array_size = int(array_size)
        
        # 检查是否已在monitor中
        if is_array:
            # 对于数组，检查是否有任何索引已添加
            already_added = any((tid, idx) in self.monitor_row for idx in range(max(1, array_size)))
            if already_added:
                return
        else:
            if tid in self.monitor_row:
                return  # 已经在 monitor 中，跳过
        
        if is_array and array_size > 0:
            # 添加数组中的每个元素
            for idx in range(array_size):
                row = self.monitor_table.rowCount()
                self.monitor_table.insertRow(row)
                item_id = QTableWidgetItem(self._format_item_id(tag_item, idx))
                # 存儲 tag_item 和 device_params
                try:
                    item_id.setData(Qt.ItemDataRole.UserRole, (tag_item, device_params, idx))
                except Exception:
                    pass
                item_id.setTextAlignment(
                    Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
                )
                dt_item = QTableWidgetItem(data_type)
                dt_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                val_item = QTableWidgetItem("")
                val_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                ts_item = QTableWidgetItem("")
                ts_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                q_item = QTableWidgetItem("Bad")
                q_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                uc_item = QTableWidgetItem("0")
                uc_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.monitor_table.setItem(row, 0, item_id)
                self.monitor_table.setItem(row, 1, dt_item)
                self.monitor_table.setItem(row, 2, val_item)
                self.monitor_table.setItem(row, 3, ts_item)
                self.monitor_table.setItem(row, 4, q_item)
                self.monitor_table.setItem(row, 5, uc_item)
                self.monitor_row[(tid, idx)] = row
                # also store canonical key mapping for fallback lookups
                try:
                    if getattr(self, 'data_broker', None) is not None:
                        try:
                            ckey = type(self.data_broker)._make_key_from_tag_item(tag_item)
                        except Exception:
                            ckey = None
                        if ckey is not None:
                            self.monitor_row[(ckey, idx)] = row
                except Exception:
                    pass
                self.monitor_counts[(tid, idx)] = 0
                self.monitor_last_values[(tid, idx)] = None
                # MONITOR_ADD diagnostics suppressed to reduce noise
        else:
            # 非数组，正常添加
            row = self.monitor_table.rowCount()
            self.monitor_table.insertRow(row)
            item_id = QTableWidgetItem(self._format_item_id(tag_item))
            # 存儲 tag_item 和 device_params
            try:
                item_id.setData(Qt.ItemDataRole.UserRole, (tag_item, device_params, None))
            except Exception:
                pass
            item_id.setTextAlignment(
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
            )
            dt_item = QTableWidgetItem(data_type)
            dt_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            val_item = QTableWidgetItem("")
            val_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            ts_item = QTableWidgetItem("")
            ts_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            q_item = QTableWidgetItem("Bad")
            q_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            uc_item = QTableWidgetItem("0")
            uc_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.monitor_table.setItem(row, 0, item_id)
            self.monitor_table.setItem(row, 1, dt_item)
            self.monitor_table.setItem(row, 2, val_item)
            self.monitor_table.setItem(row, 3, ts_item)
            self.monitor_table.setItem(row, 4, q_item)
            self.monitor_table.setItem(row, 5, uc_item)
            self.monitor_row[tid] = row
            # also store canonical key mapping for fallback lookups
            try:
                if getattr(self, 'data_broker', None) is not None:
                    try:
                        ckey = type(self.data_broker)._make_key_from_tag_item(tag_item)
                    except Exception:
                        ckey = None
                    if ckey is not None:
                        self.monitor_row[ckey] = row
            except Exception:
                pass
            self.monitor_counts[tid] = 0
            self.monitor_last_values[tid] = None
            # MONITOR_ADD diagnostics suppressed to reduce noise
        
        if tag_item not in self.monitored_tags:
            self.monitored_tags.append(tag_item)

    def start_polling(self):
        if any(getattr(p, '_running', False) for p in self.pollers):
            return
        # determine connection info from first monitored tag's Device ethernet settings
        host = self.poll_settings.get("host", "127.0.0.1")
        port = self.poll_settings.get("port", 502)
        unit = self.poll_settings.get("unit", 1)
        interval = self.poll_settings.get("interval", 1.0)

        # Track which devices we've already logged diagnostics for
        logged_devices = set()

        if self.monitored_tags:
            first_tag = self.monitored_tags[0]
            # find enclosing Device
            dev = first_tag.parent()
            while dev is not None and dev.data(0, Qt.ItemDataRole.UserRole) != "Device":
                dev = dev.parent()
            if dev is not None:
                # ethernet settings stored at data(8)
                eth = dev.data(8, Qt.ItemDataRole.UserRole)
                if isinstance(eth, dict) and eth:
                    host = eth.get("ip") or eth.get("host") or eth.get("address") or host
                    try:
                        port = int(eth.get("port", port))
                    except Exception:
                        port = port
                    # record diagnostics source: show driver name and relevant params
                    try:
                        driver_name = "UnknownDriver"
                        ch = dev.parent()
                        if ch is not None and ch.data(0, Qt.ItemDataRole.UserRole) == "Channel":
                            driver_name = ch.data(1, Qt.ItemDataRole.UserRole) or driver_name
                    except Exception:
                        driver_name = "UnknownDriver"
                    try:
                        params = ", ".join(f"{k}={v}" for k, v in eth.items())
                    except Exception:
                        params = f"host={host},port={port}"
                    self.append_diagnostic(f"Using {driver_name}: {params}")
                    dev_id = id(dev)
                    logged_devices.add(dev_id)
                else:
                    # fallback: check channel params for IP/port
                    ch = dev.parent()
                    if ch is not None and ch.data(0, Qt.ItemDataRole.UserRole) == "Channel":
                        ch_params = ch.data(2, Qt.ItemDataRole.UserRole)
                        if isinstance(ch_params, dict):
                            # If channel params look like serial (com/adapter/method=rtu), show serial info;
                            # otherwise show host:port for TCP.
                            is_serial = False
                            try:
                                method = (ch_params.get("method") or "").lower()
                                if method in ("rtu", "serial"):
                                    is_serial = True
                            except Exception:
                                pass
                            if not is_serial:
                                if any(k in ch_params for k in ("com", "adapter", "serial_port")):
                                    is_serial = True

                            if is_serial:
                                try:
                                    serial_port = ch_params.get("com") or ch_params.get("adapter") or ch_params.get("serial_port") or ""
                                    baud = ch_params.get("baud") or ch_params.get("baudrate") or ""
                                    parity = ch_params.get("parity") or ""
                                    # data bits / bytesize
                                    bytesize = ch_params.get("bytesize") or ch_params.get("data_bits") or ch_params.get("databits") or ch_params.get("data") or ""
                                    # stop bits
                                    stopbits = ch_params.get("stopbits") or ch_params.get("stop_bits") or ch_params.get("stop") or ""
                                    # flow control (can be textual or flags)
                                    flow = ch_params.get("flow") or ch_params.get("flow_control") or ""
                                    if not flow:
                                        # derive from flags if present
                                        if ch_params.get("xonxoff"):
                                            flow = "xonxoff"
                                        elif ch_params.get("rtscts") and ch_params.get("dsrdtr"):
                                            flow = "rtscts+dsrdtr"
                                        elif ch_params.get("rtscts"):
                                            flow = "rtscts"
                                        elif ch_params.get("dsrdtr"):
                                            flow = "dsrdtr"
                                    self.append_diagnostic(f"Using Channel params for connection (serial): port={serial_port} baud={baud} bytesize={bytesize} stopbits={stopbits} parity={parity} flow={flow}")
                                except Exception:
                                    self.append_diagnostic("Using Channel params for connection (serial)")
                            else:
                                host = ch_params.get("ip") or ch_params.get("host") or ch_params.get("address") or host
                                try:
                                    port = int(ch_params.get("port", port))
                                except Exception:
                                    port = port
                                self.append_diagnostic(f"Using Channel params for connection: {host}:{port}")
                    dev_id = id(dev)
                    logged_devices.add(dev_id)
                # unit / slave id stored as device id at data(2)
                try:
                    unit_val = dev.data(2, Qt.ItemDataRole.UserRole)
                    if unit_val is not None:
                        unit = int(unit_val)
                except Exception:
                    pass
            
            # Log diagnostics for other devices (when there are multiple devices)
            for tag in self.monitored_tags[1:]:
                try:
                    tag_dev = tag.parent()
                    while tag_dev is not None and tag_dev.data(0, Qt.ItemDataRole.UserRole) != "Device":
                        tag_dev = tag_dev.parent()
                    if tag_dev is not None:
                        dev_id = id(tag_dev)
                        if dev_id not in logged_devices:
                            logged_devices.add(dev_id)
                            # Log this device's connection info
                            tag_eth = tag_dev.data(8, Qt.ItemDataRole.UserRole)
                            if isinstance(tag_eth, dict) and tag_eth:
                                tag_host = tag_eth.get("ip") or tag_eth.get("host") or tag_eth.get("address") or host
                                try:
                                    tag_port = int(tag_eth.get("port", port))
                                except Exception:
                                    tag_port = port
                                try:
                                    tag_driver_name = "UnknownDriver"
                                    tag_ch = tag_dev.parent()
                                    if tag_ch is not None and tag_ch.data(0, Qt.ItemDataRole.UserRole) == "Channel":
                                        tag_driver_name = tag_ch.data(1, Qt.ItemDataRole.UserRole) or tag_driver_name
                                except Exception:
                                    tag_driver_name = "UnknownDriver"
                                try:
                                    tag_params = ", ".join(f"{k}={v}" for k, v in tag_eth.items())
                                except Exception:
                                    tag_params = f"host={tag_host},port={tag_port}"
                                self.append_diagnostic(f"Using {tag_driver_name}: {tag_params}")
                            else:
                                # fallback: check channel params
                                tag_ch = tag_dev.parent()
                                if tag_ch is not None and tag_ch.data(0, Qt.ItemDataRole.UserRole) == "Channel":
                                    tag_ch_params = tag_ch.data(2, Qt.ItemDataRole.UserRole)
                                    if isinstance(tag_ch_params, dict):
                                        is_serial = False
                                        try:
                                            tmethod = (tag_ch_params.get("method") or "").lower()
                                            if tmethod in ("rtu", "serial"):
                                                is_serial = True
                                        except Exception:
                                            pass
                                        if not is_serial:
                                            if any(k in tag_ch_params for k in ("com", "adapter", "serial_port")):
                                                is_serial = True

                                        if is_serial:
                                            try:
                                                serial_port = tag_ch_params.get("com") or tag_ch_params.get("adapter") or tag_ch_params.get("serial_port") or ""
                                                baud = tag_ch_params.get("baud") or tag_ch_params.get("baudrate") or ""
                                                parity = tag_ch_params.get("parity") or ""
                                                bytesize = tag_ch_params.get("bytesize") or tag_ch_params.get("data_bits") or tag_ch_params.get("databits") or tag_ch_params.get("data") or ""
                                                stopbits = tag_ch_params.get("stopbits") or tag_ch_params.get("stop_bits") or tag_ch_params.get("stop") or ""
                                                flow = tag_ch_params.get("flow") or tag_ch_params.get("flow_control") or ""
                                                if not flow:
                                                    if tag_ch_params.get("xonxoff"):
                                                        flow = "xonxoff"
                                                    elif tag_ch_params.get("rtscts") and tag_ch_params.get("dsrdtr"):
                                                        flow = "rtscts+dsrdtr"
                                                    elif tag_ch_params.get("rtscts"):
                                                        flow = "rtscts"
                                                    elif tag_ch_params.get("dsrdtr"):
                                                        flow = "dsrdtr"
                                                self.append_diagnostic(f"Using Channel params for connection (serial): port={serial_port} baud={baud} bytesize={bytesize} stopbits={stopbits} parity={parity} flow={flow}")
                                            except Exception:
                                                self.append_diagnostic("Using Channel params for connection (serial)")
                                        else:
                                            tag_host = tag_ch_params.get("ip") or tag_ch_params.get("host") or tag_ch_params.get("address") or host
                                            try:
                                                tag_port = int(tag_ch_params.get("port", port))
                                            except Exception:
                                                tag_port = port
                                            self.append_diagnostic(f"Using Channel params for connection: {tag_host}:{tag_port}")
                except Exception:
                    pass
            # prefer tag scan rate if present; interpret values as milliseconds
            # per device spec: valid range 10..99999990 ms, step 10 ms, default 100 ms
            try:
                scan = first_tag.data(4, Qt.ItemDataRole.UserRole)
                ms = None
                if scan is None or str(scan).strip() == "":
                    ms = 100.0
                else:
                    s = str(scan).strip().lower()
                    # allow forms: '100', '100 ms', '100ms', '0.1 s', '0.1s'
                    if s.endswith("ms"):
                        try:
                            ms = float(s[:-2].strip())
                        except Exception:
                            ms = None
                    elif s.endswith("s"):
                        try:
                            sec = float(s[:-1].strip())
                            ms = sec * 1000.0
                        except Exception:
                            ms = None
                    else:
                        # plain number: interpret as milliseconds per spec
                        try:
                            ms = float(s)
                        except Exception:
                            ms = None

                if ms is None:
                    ms = 100.0

                # enforce device limits and 10 ms step
                try:
                    ms = float(ms)
                except Exception:
                    ms = 100.0
                ms = max(10.0, min(ms, 99999990.0))
                # quantize to nearest 10 ms
                ms = round(ms / 10.0) * 10.0
                interval = ms / 1000.0
            except Exception:
                interval = 0.1

        # group monitored tags by device connection parameters and create one poller per group
        groups = {}
        for t in list(self.monitored_tags):
            try:
                # include function code in the conn key (fc) so we can create
                # pollers per (host,port,unit,interval,fc)
                key = self._compute_tag_conn_key(t, default=(host, port, unit, interval, None))
                groups.setdefault(key, []).append(t)
            except Exception:
                groups.setdefault((host, port, unit, interval, None), []).append(t)

        self.pollers = []
        for (h, pnum, u, inv, fcc), tags_for_group in groups.items():
            try:
                poller = AsyncPoller(self.controller, host=h, port=pnum, unit=u, interval=inv, diag_callback=self.append_diagnostic)
                # attach serial metadata to poller so worker can emit appropriate diagnostics
                try:
                    first_tag_for_group = tags_for_group[0] if tags_for_group else None
                    is_serial_poll = False
                    serial_meta = {}
                    if first_tag_for_group:
                        dev = first_tag_for_group.parent()
                        while dev is not None and dev.data(0, Qt.ItemDataRole.UserRole) != "Device":
                            dev = dev.parent()
                        if dev is not None:
                            ch = dev.parent()
                            if ch is not None and ch.data(0, Qt.ItemDataRole.UserRole) == "Channel":
                                ch_params = ch.data(2, Qt.ItemDataRole.UserRole)
                                if isinstance(ch_params, dict):
                                    method = str(ch_params.get("method") or "").lower()
                                    if method in ("rtu", "serial"):
                                        is_serial_poll = True
                                    if not is_serial_poll and any(k in ch_params for k in ("com", "adapter", "serial_port")):
                                        is_serial_poll = True
                                    if is_serial_poll:
                                        serial_meta["port"] = ch_params.get("com") or ch_params.get("adapter") or ch_params.get("serial_port") or ""
                                        serial_meta["baud"] = ch_params.get("baud") or ch_params.get("baudrate") or ""
                                        serial_meta["parity"] = ch_params.get("parity") or ""
                                        serial_meta["bytesize"] = ch_params.get("bytesize") or ch_params.get("data_bits") or ch_params.get("databits") or ""
                                        serial_meta["stopbits"] = ch_params.get("stopbits") or ch_params.get("stop_bits") or ch_params.get("stop") or ""
                                        flow = ch_params.get("flow") or ch_params.get("flow_control") or ""
                                        if not flow:
                                            if ch_params.get("xonxoff"):
                                                flow = "xonxoff"
                                            elif ch_params.get("rtscts") and ch_params.get("dsrdtr"):
                                                flow = "rtscts+dsrdtr"
                                            elif ch_params.get("rtscts"):
                                                flow = "rtscts"
                                            elif ch_params.get("dsrdtr"):
                                                flow = "dsrdtr"
                                        serial_meta["flow"] = flow
                    setattr(poller, '_is_serial', bool(is_serial_poll))
                    setattr(poller, '_serial_params', serial_meta)
                except Exception:
                    # safe fallback: leave attributes unset if anything goes wrong
                    pass
                # attach a lightweight connection key for runtime matching
                try:
                    setattr(poller, '__conn_key__', (h, pnum, u, inv, fcc))
                except Exception:
                    pass
                # emit diagnostic listing tags assigned to this poller for clarity
                try:
                    try:
                        addrs = []
                        for t in tags_for_group:
                            try:
                                a = t.data(1, Qt.ItemDataRole.UserRole)
                                dt = t.data(2, Qt.ItemDataRole.UserRole)
                                addrs.append(f"{a}:{dt}")
                            except Exception:
                                addrs.append(str(t))
                        self.append_diagnostic(f"POLLER_CREATE: key={(h,pnum,u,inv,fcc)} tags={addrs}")
                    except Exception:
                        pass
                except Exception:
                    pass
                for t in tags_for_group:
                    try:
                        poller.add_tag(t)
                    except Exception:
                        pass
                poller.tag_polled.connect(self._on_tag_polled)
                try:
                    if getattr(self, 'data_broker', None) is not None:
                        # connect to wrapper that respects recent OPC writes
                        poller.tag_polled.connect(self._handle_tag_polled_maybe_update_broker)
                except Exception:
                    pass
                # diag routed via diag_callback -> DiagnosticsManager; keep signal unused to avoid duplicates
                poller.start()
                try:
                    self.append_diagnostic(f"POLLER_START: key={(h,pnum,u,inv)} running_tags={len(tags_for_group)}")
                except Exception:
                    pass
                self.pollers.append(poller)
            except Exception:
                pass

    def stop_polling(self):
        if self.pollers:
            for p in list(self.pollers):
                try:
                    p.stop()
                except Exception:
                    pass
            self.pollers = []
        # 清除 Monitor 緩存
        self.monitor_last_values.clear()
        self.monitor_counts.clear()
        self.append_diagnostic("[POLL] ✓ 已停止輪詢並清除緩存")

    def _on_tag_polled(self, tag_item, value, timestamp, quality):
        tid = id(tag_item)
        data_type = tag_item.data(2, Qt.ItemDataRole.UserRole) or ""
        is_array = "Array" in str(data_type)
        tag_name = tag_item.text(0) if tag_item else "?"
        # compute canonical key for fallback lookup
        try:
            if getattr(self, 'data_broker', None) is not None:
                try:
                    canonical_key = type(self.data_broker)._make_key_from_tag_item(tag_item)
                except Exception:
                    canonical_key = None
            else:
                canonical_key = None
        except Exception:
            canonical_key = None
        
        if is_array and isinstance(value, (list, tuple)):
            # 数组类型：为每个元素更新对应的行
            for idx, elem_value in enumerate(value):
                key = (tid, idx)
                row = self.monitor_row.get(key)
                # fallback to canonical_key if id-based lookup fails
                if row is None and canonical_key is not None:
                    row = self.monitor_row.get((canonical_key, idx))
                if row is None:
                    # diagnostic: record missing monitor row for this tag/index
                    # MONITOR_MISS diagnostics suppressed to reduce noise
                    continue
                
                # 更新值 (布林顯示為 1/0)
                if elem_value is None:
                    display = ""
                else:
                    try:
                        if isinstance(elem_value, bool):
                            display = "1" if elem_value else "0"
                        else:
                            display = str(elem_value)
                    except Exception:
                        display = str(elem_value)
                val_item = QTableWidgetItem(display)
                val_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.monitor_table.setItem(row, 2, val_item)
                
                # 更新时间戳
                import time as _time
                ms = int((timestamp % 1) * 1000)
                ts_text = _time.strftime("%H:%M:%S", _time.localtime(timestamp)) + f".{ms:03d}"
                ts_item = QTableWidgetItem(ts_text)
                ts_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.monitor_table.setItem(row, 3, ts_item)
                
                # 更新质量
                q_item = QTableWidgetItem(quality)
                q_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.monitor_table.setItem(row, 4, q_item)
                
                # 更新计数（检查值是否改变）
                last = self.monitor_last_values.get(key, None)
                changed = not (last == elem_value)
                if changed:
                    self.monitor_counts[key] = self.monitor_counts.get(key, 0) + 1
                    self.monitor_last_values[key] = elem_value
                uc_item = QTableWidgetItem(str(self.monitor_counts.get(key, 0)))
                uc_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.monitor_table.setItem(row, 5, uc_item)
        else:
            # 非数组类型：正常更新
            row = self.monitor_row.get(tid)
            # fallback to canonical key lookup when necessary
            if row is None and canonical_key is not None:
                row = self.monitor_row.get(canonical_key)
            if row is None:
                # diagnostic: missing scalar monitor row
                # MONITOR_MISS diagnostics suppressed to reduce noise
                return
            
            # 更新值 (布林顯示為 1/0)
            if value is None:
                display = ""
            else:
                try:
                    if isinstance(value, bool):
                        display = "1" if value else "0"
                    else:
                        display = str(value)
                except Exception:
                    display = str(value)
            val_item = QTableWidgetItem(display)
            val_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.monitor_table.setItem(row, 2, val_item)
            
            # 更新时间戳
            import time as _time
            ms = int((timestamp % 1) * 1000)
            ts_text = _time.strftime("%H:%M:%S", _time.localtime(timestamp)) + f".{ms:03d}"
            ts_item = QTableWidgetItem(ts_text)
            ts_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.monitor_table.setItem(row, 3, ts_item)
            
            # 更新质量
            q_item = QTableWidgetItem(quality)
            q_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.monitor_table.setItem(row, 4, q_item)
            
            # 更新计数
            last = self.monitor_last_values.get(tid, None)
            changed = not (last == value)
            if changed:
                self.monitor_counts[tid] = self.monitor_counts.get(tid, 0) + 1
                self.monitor_last_values[tid] = value
            uc_item = QTableWidgetItem(str(self.monitor_counts.get(tid, 0)))
            uc_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.monitor_table.setItem(row, 5, uc_item)

    def _on_monitor_value_context_menu(self, pos):
        """Monitor表格右键菜單，僅在Value列顯示"""
        item = self.monitor_table.itemAt(pos)
        if item is None:
            return
        
        col = self.monitor_table.column(item)
        row = self.monitor_table.row(item)
        
        # 只在Value列（列2）显示菜單
        if col != 2:
            return
        
        # 檢查是否允許寫入
        can_write, reason = self._can_write_to_tag(row)
        if not can_write:
            QMessageBox.warning(self, "不允許寫入", "該Tag不支持寫入操作")
            return
        
        menu = QMenu(self)
        write_action = menu.addAction("✏️ 寫入數值")
        act = menu.exec(self.monitor_table.viewport().mapToGlobal(pos))
        
        if act == write_action:
            self._write_monitor_value(row)

    def _can_write_to_tag(self, row):
        """檢查該Tag是否允許寫入：1.檢查Client Access 2.檢查Address類型"""
        try:
            # 獲取 Item ID 的 UserRole 數據
            item_id_item = self.monitor_table.item(row, 0)
            if not item_id_item:
                return False, "無法獲取Tag信息"
            
            data = item_id_item.data(Qt.ItemDataRole.UserRole)
            if not data:
                return False, "無法獲取Tag對象"
            
            # 解構 (tag_item, device_params, array_index)
            if isinstance(data, tuple) and len(data) == 3:
                tag_item, device_params, array_index = data
            else:
                tag_item = data
                device_params = {}
                array_index = None
            
            if not tag_item:
                return False, "無法獲取Tag對象"
            
            # 【第一步】檢查 Client Access 是否允許寫入
            # 嘗試所有可能的 data slot，找到包含 read/write 的值
            client_access = None
            for slot in range(10):
                ca = tag_item.data(slot, Qt.ItemDataRole.UserRole)
                if ca and isinstance(ca, str) and ("write" in str(ca).lower() or "read" in str(ca).lower()):
                    client_access = ca
                    break
            
            if client_access:
                access_str = str(client_access).strip().lower()
                # 只要包含 "write" 字樣就允許寫入
                if "write" not in access_str:
                    return False, f"Client Access 不允許寫入: {client_access}"
            # 如果 Client Access 為空，也允許寫入
            
            # 【第二步】獲取地址並檢查類型
            address = tag_item.data(1, Qt.ItemDataRole.UserRole)
            if not address:
                return False, "無法獲取地址"
            
            address_str = str(address).strip()
            address_start = address_str[0] if address_str else None
            
            if address_start not in ["0", "4"]:
                return False, f"地址類型不支持寫入 (需要 0 或 4 開頭，實際: {address_str})"
            
            # 【第三步】檢查對應的 FC 碼設置（只是參考）
            if address_start == "0":
                fc05_enabled = device_params.get('fc05_enabled', False)
                fc_info = f"Coil地址，FC05: {'enable' if fc05_enabled else 'disable'}"
            else:  # address_start == "4"
                fc06_enabled = device_params.get('fc06_enabled', False)
                fc_info = f"Holding Register，FC06: {'enable' if fc06_enabled else 'disable'}"
            
            return True, fc_info
            
        except Exception as e:
            return False, f"檢查失敗: {str(e)}"

    def _write_monitor_value(self, row):
        """写入monitor中指定行的数值"""
        try:
            # 再次檢查是否允許寫入
            can_write, reason = self._can_write_to_tag(row)
            if not can_write:
                QMessageBox.warning(self, "錯誤", f"該Tag不支持寫入操作\n\n原因: {reason}")
                return
            
            # 獲取Item ID對應的tag和參數
            item_id_item = self.monitor_table.item(row, 0)
            if not item_id_item:
                QMessageBox.warning(self, "錯誤", "無法獲取Tag信息")
                return
            
            data = item_id_item.data(Qt.ItemDataRole.UserRole)
            if not data:
                QMessageBox.warning(self, "錯誤", "無法獲取Tag對象")
                return
            
            # 解構 (tag_item, device_params, array_index)
            if isinstance(data, tuple) and len(data) == 3:
                tag_item, _, array_index = data
            else:
                tag_item = data
                array_index = None
            
            if not tag_item:
                QMessageBox.warning(self, "錯誤", "無法獲取Tag對象")
                return
            
            # 【重要】獲取當前值（用作預設值）
            value_item = self.monitor_table.item(row, 2)
            current_value = value_item.text() if value_item else ""
            
            # 彈出輸入框
            new_value, ok = QInputDialog.getText(
                self,
                "寫入數值",
                f"請輸入要寫入的數值（當前值: {current_value}）:",
                text=current_value
            )
            
            if not ok or not new_value:
                return
            
            # 【重要】獲取tag的地址和數據類型
            address = tag_item.data(1, Qt.ItemDataRole.UserRole)
            data_type = tag_item.data(2, Qt.ItemDataRole.UserRole)
            
            if not address:
                QMessageBox.warning(self, "錯誤", "無法獲取Tag地址")
                return
            
            # 【重要】找到對應的 Device item，重新提取最新參數
            device_item = tag_item.parent()
            # 如果 Tag 在 Group 裡，需要先找到 Group 的 parent (Device)
            if device_item and device_item.data(0, Qt.ItemDataRole.UserRole) == "Group":
                device_item = device_item.parent()
            
            if not device_item or device_item.data(0, Qt.ItemDataRole.UserRole) != "Device":
                QMessageBox.warning(self, "錯誤", "無法找到Device")
                return
            
            # 【重要】重新提取最新的 Device 參數（包括最新的 FC 設置）
            device_params = self._extract_device_params(device_item)
            
            # 【重要】呼叫寫入函數
            self._perform_modbus_write(
                tag_item=tag_item,
                device_params=device_params,
                address=address,
                data_type=data_type,
                value=new_value,
                array_index=array_index
            )
            
        except Exception as e:
            import traceback
            self.append_diagnostic(f"[WRITE ERROR] {str(e)}")
            QMessageBox.warning(self, "錯誤", f"寫入失敗: {str(e)}")

    def _determine_register_width(self, data_type):
        """判斷數據類型佔用的寄存器數量（1或2）"""
        data_type_str = str(data_type).upper()
        if any(x in data_type_str for x in ["FLOAT", "DOUBLE", "LONG", "DWORD"]):
            return 2  # 佔 2 個寄存器
        return 1  # 預設佔 1 個寄存器
    
    def _convert_kepware_to_modbus_addr(self, kepware_addr):
        """
        轉換 Kepware 地址格式到 Modbus 地址
        例: 400095 -> 95
        例: 000123 -> 123
        """
        addr_str = str(kepware_addr).zfill(5)
        if addr_str[0] in ["0", "4"]:
            return int(addr_str[1:])
        return kepware_addr & 0xFFFF
    
    def _calculate_actual_address(self, base_addr, array_index, regs_per_element):
        """
        計算實際地址（考慮陣列和寄存器寬度）
        公式: actual_addr = base_addr + (array_index × regs_per_element)
        例: base=95, idx=5, width=2 -> 95 + (5×2) = 105
        """
        if array_index is not None:
            return base_addr + (array_index * regs_per_element)
        return base_addr
    
    def _determine_fc_code(self, address_start, fc05_enabled, fc06_enabled):
        """
        判斷使用哪個 FC 碼
        邏輯:
        - Coil (0xxxx):      FC05 (enabled?) -> FC15
        - Holding Reg (4xxxx): FC06 (enabled?) -> FC16
        """
        if address_start == "0":
            # Coil 寫入
            return "FC05" if fc05_enabled else "FC15"
        else:  # address_start == "4"
            # Holding Register 寫入
            return "FC06" if fc06_enabled else "FC16"
    
    def _convert_value_to_registers(self, write_value, data_type, first_word_low=True):
        """
        根據數據類型將值轉換為寄存器列表
        返回: (regs_list, description)
        例: (100, Int) -> ([100], "Int: 1 reg")
        例: (1.5, Float) -> ([0x0000, 0x3fc0], "Float: 2 regs - First Word Low")
        
        Kepware 規則（First Word Low = TRUE）：
        - 對於 50.0 = 0x42480000（IEEE 754 big-endian）
        - 應該寫成: [0x0000, 0x4248]（低字在前）
        """
        import struct
        data_type_str = str(data_type).upper()
        
        if any(x in data_type_str for x in ["FLOAT", "DOUBLE"]):
            # Float 類型: 2 個寄存器
            b_all = struct.pack(">f", float(write_value))
            # 先按照 big-endian 打包為 [高16位, 低16位]
            regs = [int.from_bytes(b_all[i:i+2], 'big') for i in range(0, len(b_all), 2)]
            
            # 根據 First Word Low 調整字序
            if first_word_low:
                # First Word Low = TRUE（Kepware默認），交換為 [低16位, 高16位]
                regs = [regs[1], regs[0]]
            
            return regs, f"Float: 2 regs = {[hex(r) for r in regs]} (first_word_low={first_word_low})"
        else:
            # Int/Word 類型: 1 個寄存器
            reg_val = int(write_value) & 0xFFFF
            return [reg_val], f"Int: 1 reg = {hex(reg_val)}"
    def _perform_modbus_write(self, tag_item, device_params, address, data_type, value, array_index=None):
        """
        執行 Modbus 寫入操作 - 系統化判斷流程
        
        判斷流程:
        1. 解析地址 → 提取數值
        2. 判斷寄存器寬度 → 根據數據類型
        3. 轉換地址 → Kepware 格式轉 Modbus
        4. 計算實際地址 → 考慮陣列索引
        5. 驗證地址範圍 → 不超過 65535
        6. 轉換值 → 數值轉換
        7. 判斷 FC 碼 → 根據地址和設備設置
        8. 執行寫入 → 調用對應的 FC 函數
        """
        try:
            import re
            
            # ========== 步驟 1: 解析地址 ==========
            address_match = re.search(r'(\d+)', str(address))
            if not address_match:
                QMessageBox.warning(self, "錯誤", f"無法解析地址: {address}")
                return

            digit_str = address_match.group(1)
            full_addr_num = int(digit_str)
            # Determine leading digit from the original Kepware-style address string
            # (preserve leading zeros such as '000103' -> '0').
            try:
                address_start = digit_str.zfill(5)[0]
            except Exception:
                address_start = str(full_addr_num)[0]
            
            # 判斷寄存器寬度
            regs_per_element = self._determine_register_width(data_type)
            
            # 轉換地址
            base_addr = self._convert_kepware_to_modbus_addr(full_addr_num)
            
            # 計算實際地址
            actual_addr = self._calculate_actual_address(base_addr, array_index, regs_per_element)
            
            # 驗證地址範圍
            if actual_addr > 65535:
                QMessageBox.warning(self, "錯誤", f"Modbus地址超過上限: {actual_addr}")
                return
            
            # 轉換值（Boolean 以 1/0 處理）
            try:
                dt_s = str(data_type) if data_type is not None else ""
                if "Boolean" in dt_s or dt_s.lower().startswith("bool"):
                    s = str(value).strip().lower()
                    if s in ("1", "true", "yes", "on", "t", "y"):
                        write_value = 1
                    elif s in ("0", "false", "no", "off", "f", "n"):
                        write_value = 0
                    else:
                        try:
                            write_value = 1 if int(float(value)) != 0 else 0
                        except Exception:
                            QMessageBox.warning(self, "錯誤", f"數值轉換失敗(布林): {value}")
                            return
                elif "Int" in dt_s:
                    write_value = int(value)
                elif "Float" in dt_s:
                    write_value = float(value)
                else:
                    write_value = float(value)
            except ValueError:
                QMessageBox.warning(self, "錯誤", f"數值轉換失敗: {value}")
                return
            
            # 取得設備參數
            dev_ip = device_params.get('ip', '127.0.0.1')
            dev_port = device_params.get('port', 502)
            dev_unit = device_params.get('unit_id', 1)
            fc05_enabled = device_params.get('fc05_enabled', False)
            fc06_enabled = device_params.get('fc06_enabled', False)
            first_word_low = device_params.get('first_word_low', True)
            device_name = device_params.get('device_name', '未知')
            
            use_fc = self._determine_fc_code(address_start, fc05_enabled, fc06_enabled)
            
            # 轉換為寄存器值
            regs, _ = self._convert_value_to_registers(write_value, data_type, first_word_low)

            # prepare a device context prefix so Diagnostics lines (TX/RX)
            # can be associated with the correct device/terminal window
            try:
                dev_item = tag_item.parent()
                if dev_item and dev_item.data(0, Qt.ItemDataRole.UserRole) == 'Group':
                    dev_item = dev_item.parent()
            except Exception:
                dev_item = None
            try:
                dev_id_prefix = f"DEV_ID={int(id(dev_item))} " if dev_item is not None else ""
            except Exception:
                dev_id_prefix = ""
            
            # 確保該 Device 的 Diagnostics 視窗已開啟（monitor 寫值視為一種 poll）
            try:
                try:
                    self._get_or_open_device_terminal(dev_item)
                except Exception:
                    pass
            except Exception:
                pass

            # ========== 步驟 9: 執行寫入操作 ==========
            from modbus_client import ModbusClient
            import threading
            
            def perform_write():
                try:
                    import asyncio
                    
                    async def do_write():
                        # wrap diag_callback so ModbusClient emissions include device context
                        def _diag_with_dev_local(m):
                            try:
                                self.append_diagnostic(f"{dev_id_prefix}{m}")
                            except Exception:
                                try:
                                    self.append_diagnostic(m)
                                except Exception:
                                    pass

                        client = ModbusClient(
                                mode="tcp",
                                host=dev_ip,
                                port=dev_port,
                                unit=dev_unit,
                                diag_callback=_diag_with_dev_local
                            )
                        
                        try:
                            await client.connect_async()

                            # helpers for emitting TX/SY diagnostics (simple MBAP-like ADU)
                            def _hex(b: bytes) -> str:
                                try:
                                    return " ".join(f"{x:02X}" for x in b)
                                except Exception:
                                    try:
                                        return str(b)
                                    except Exception:
                                        return ""

                            def _format_adu(pdu: bytes) -> bytes:
                                try:
                                    # use IoTApp._txid counter so MBAP txid increments for each synth ADU
                                    txid = (getattr(self, '_txid', 0) + 1) & 0xFFFF
                                    try:
                                        self._txid = txid
                                    except Exception:
                                        pass
                                    proto = 0
                                    mbap_len = len(pdu) + 1
                                    mbap = txid.to_bytes(2, "big") + proto.to_bytes(2, "big") + mbap_len.to_bytes(2, "big") + int(dev_unit).to_bytes(1, "big")
                                    return mbap + pdu
                                except Exception:
                                    return pdu

                            result = None
                            
                            if use_fc == "FC05":
                                # Write Single Coil (FC05)
                                try:
                                    try:
                                        pdu_tx = bytes([5]) + int(actual_addr).to_bytes(2, "big") + (0xFF00 if bool(write_value) else 0x0000).to_bytes(2, "big")
                                        self.append_diagnostic(f"{dev_id_prefix}TX: | {_hex(_format_adu(pdu_tx))} |")
                                    except Exception:
                                        pass
                                    result = await client.write_coil_async(actual_addr, bool(write_value))
                                    try:
                                        enc = None
                                        if result is not None and hasattr(result, 'encode'):
                                            try:
                                                enc = result.encode()
                                            except Exception:
                                                enc = None
                                        # synthetic RX disabled
                                    except Exception:
                                        pass
                                except Exception as e:
                                    self.append_diagnostic(f"[WRITE ERROR] FC05: {str(e)}")
                                    raise
                            
                            elif use_fc == "FC15":
                                # Write Multiple Coils (FC15)
                                try:
                                    try:
                                        qty = 1
                                        coil_bytes = b"\x01" if bool(write_value) else b"\x00"
                                        pdu_tx = bytes([15]) + int(actual_addr).to_bytes(2, "big") + int(qty).to_bytes(2, "big") + int(len(coil_bytes)).to_bytes(1, "big") + coil_bytes
                                        self.append_diagnostic(f"{dev_id_prefix}TX: | {_hex(_format_adu(pdu_tx))} |")
                                    except Exception:
                                        pass
                                    result = await client.write_coils_async(actual_addr, [bool(write_value)])
                                    try:
                                        enc = None
                                        if result is not None and hasattr(result, 'encode'):
                                            try:
                                                enc = result.encode()
                                            except Exception:
                                                enc = None
                                        # synthetic RX disabled
                                    except Exception:
                                        pass
                                except Exception as e:
                                    self.append_diagnostic(f"[WRITE ERROR] FC15: {str(e)}")
                                    raise
                            
                            elif use_fc == "FC06":
                                # Write Single Register (FC06)
                                # 注: Float 需要 2 個寄存器，此時自動升級到 FC16
                                if len(regs) == 1:
                                    try:
                                        try:
                                            pdu_tx = bytes([6]) + int(actual_addr).to_bytes(2, "big") + int(regs[0]).to_bytes(2, "big")
                                            self.append_diagnostic(f"{dev_id_prefix}TX: | {_hex(_format_adu(pdu_tx))} |")
                                        except Exception:
                                            pass
                                        result = await client.write_register_async(actual_addr, regs[0])
                                        try:
                                            enc = None
                                            if result is not None and hasattr(result, 'encode'):
                                                try:
                                                    enc = result.encode()
                                                except Exception:
                                                    enc = None
                                            # synthetic RX disabled
                                        except Exception:
                                            pass
                                        self.append_diagnostic(f"[WRITE] FC06: 寫入結果 = {result}")
                                    except Exception as e:
                                        self.append_diagnostic(f"[WRITE ERROR] FC06: {str(e)}")
                                        raise
                                else:
                                    # Float 自動升級到 FC16
                                    try:
                                        self.append_diagnostic(f"[WRITE] FC16(upgraded): 準備寫入 addr={actual_addr}, regs={[hex(r) for r in regs]}")
                                        result = await client.write_registers_async(actual_addr, regs)
                                        self.append_diagnostic(f"[WRITE] FC16(upgraded): 寫入結果 = {result}")
                                    except Exception as e:
                                        self.append_diagnostic(f"[WRITE ERROR] FC16 (upgraded): {str(e)}")
                                        raise
                            
                            elif use_fc == "FC16":
                                # Write Multiple Registers (FC16)
                                try:
                                    try:
                                        qty = len(regs)
                                        data_bytes = b"".join(int(r & 0xFFFF).to_bytes(2, "big") for r in regs)
                                        pdu_tx = bytes([16]) + int(actual_addr).to_bytes(2, "big") + int(qty).to_bytes(2, "big") + int(len(data_bytes)).to_bytes(1, "big") + data_bytes
                                        self.append_diagnostic(f"{dev_id_prefix}TX: | {_hex(_format_adu(pdu_tx))} |")
                                    except Exception:
                                        pass
                                    result = await client.write_registers_async(actual_addr, regs)
                                    try:
                                        enc = None
                                        if result is not None and hasattr(result, 'encode'):
                                            try:
                                                enc = result.encode()
                                            except Exception:
                                                enc = None
                                            # synthetic RX disabled
                                    except Exception:
                                        pass
                                    self.append_diagnostic(f"[WRITE] FC16: 寫入結果 = {result}")
                                except Exception as e:
                                    self.append_diagnostic(f"[WRITE ERROR] FC16: {str(e)}")
                                    raise
                            
                            if result:
                                self.append_diagnostic(f"[WRITE] ✓ 寫入成功: {device_name} @ {actual_addr}")
                            else:
                                self.append_diagnostic(f"[WRITE] ✗ 寫入失敗: result={result}")
                        
                        except Exception as e:
                            self.append_diagnostic(f"[WRITE ERROR] {str(e)}")
                        finally:
                            try:
                                await client.close_async()
                            except:
                                pass
                    
                    # 在後台線程中執行異步操作
                    asyncio.run(do_write())
                    
                except Exception as write_err:
                    self.append_diagnostic(f"[WRITE ERROR] {str(write_err)}")
            
            # 在後台線程中執行寫入
            write_thread = threading.Thread(target=perform_write, daemon=True)
            write_thread.start()
            
            QMessageBox.information(
                self, 
                "寫入",
                f"已發送寫入請求\n\n"
                f"設備: {device_name}\n"
                f"IP: {dev_ip}:{dev_port}\n"
                f"Unit ID: {dev_unit}\n\n"
                f"Kepware地址: {full_addr_num}\n"
                f"Modbus地址: {actual_addr}\n"
                f"寫入值: {write_value}\n"
                f"FC碼: {use_fc}\n\n"
                f"結果將顯示在診斷視窗"
            )
        except Exception as e:
            QMessageBox.warning(self, "錯誤", f"Modbus寫入失敗: {str(e)}")
            self.append_diagnostic(f"[ERROR] 寫入異常: {str(e)}")


# Compatibility: expose module-level handlers as IoTApp methods for legacy callers/tests
try:
    IoTApp._on_diag_context_menu = _on_diag_context_menu
    IoTApp._on_only_txrx_toggled = _on_only_txrx_toggled
    IoTApp._on_show_raw_toggled = _on_show_raw_toggled
except Exception:
    pass


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = IoTApp()
    window.show()
    try:
        # If qasync is available, run a Qt-integrated asyncio event loop so
        # AsyncPoller will create tasks on the main loop instead of spawning
        # a background thread.
        from qasync import QEventLoop

        loop = QEventLoop(app)
        asyncio.set_event_loop(loop)
        with loop:
            loop.run_forever()
    except Exception:
        sys.exit(app.exec())
