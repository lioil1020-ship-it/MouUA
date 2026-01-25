from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QTableWidget, 
    QTableWidgetItem, QFileDialog, QMessageBox
)
from PyQt6.QtGui import QAction
from PyQt6.QtCore import Qt, QTimer
import threading

from core.config import GROUP_SEPARATOR


class TerminalWindow(QMainWindow):
    """诊断信息窗口 - 显示设备的诊断数据
    def __init__(self, parent=None, device_item=None, diagnostics_manager=None):
        super().__init__(parent)
        self.device_item = device_item
        # 保留對主視窗（IoTApp）的參照以供回呼使用
        self.parent_window = parent
        self._diag_manager = diagnostics_manager
        self._diag_listener_token = None
        self.setWindowTitle("Diagnostics" if device_item is None else f"Diagnostics - {self._device_path(device_item)}")
        self.resize(1000, 600)

        # 主容器
        main_widget = QWidget()
        # 診斷資訊表格
        layout = QVBoxLayout()
        self.diagnostics_table = QTableWidget()
        self.diagnostics_table.setColumnCount(5)
        self.diagnostics_table.setHorizontalHeaderLabels(["Date", "Time", "Event", "Length", "Data"])
        
        # 配置表头自动调整大小
        header = self.diagnostics_table.horizontalHeader()
        try:
            header.setSectionResizeMode(QTableWidget.ResizeMode.ResizeToContents)
        except Exception:
            pass
        
        layout.addWidget(self.diagnostics_table)
        main_widget.setLayout(layout)
        self.setCentralWidget(main_widget)

        self._device_tag_ids = set()
        self._device_path_str = None
        self._device_unit = None
        if device_item is not None:
            try:
                # 收集裝置底下的 Tag id
                for i in range(device_item.childCount()):
                    c = device_item.child(i)
                    if c.data(0, Qt.ItemDataRole.UserRole) == "Tag":
                        try:
                            self._device_tag_ids.add(id(c))
                        except Exception:
                            pass
                # 建立類似 Channel1.Device1 的裝置路徑
                self._device_path_str = self._device_path(device_item)
                # 嘗試從裝置讀取已設定的 unit id（通常存在於 role index 2）
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

                import re as _re
                lightweight_matcher = lambda t, c: bool(_re.search(r"\b(TX|RX)\b", str(t or "")))

                self._diag_listener_token = self._diag_manager.register_listener(
                    name=f"terminal-{id(self)}",
                    callback=_cb,
                    matcher=None,
                )
                try:
                    snap = self._diag_manager.snapshot()
                    for rec in snap:
                        try:
                            ctx = getattr(rec, 'context', None)
                            if self.matches_message(rec.text, ctx):
                                _cb(rec.timestamp, rec.text, ctx)
                        except Exception:
                            pass
                    try:
                        self._last_diag_index = len(snap)
                    except Exception:
                        self._last_diag_index = 0
                except Exception:
                    self._last_diag_index = 0
            except Exception:
                self._diag_listener_token = None

        self._setup_menu()

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
        return GROUP_SEPARATOR.join(parts)

    def matches_message(self, text: str, ctx=None) -> bool:
        if self.device_item is None:
            return True
        txt = str(text or "")
        import re
        if not re.search(r"\b(TX|RX)\b", txt):
            return False
        try:
            if isinstance(ctx, dict):
                dev_ctx = ctx.get("dev_id") or ctx.get("device_id")
                if dev_ctx is not None and getattr(self, "_device_item_id", None) is not None:
                    try:
                        if int(dev_ctx) == int(self._device_item_id):
                            return True
                    except Exception:
                        pass
                if self._device_unit is not None and ctx.get("unit") is not None:
                    try:
                        if int(ctx.get("unit")) != int(self._device_unit):
                            return False
                    except Exception:
                        pass
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
                fc = ctx.get('fc')
                if fc is not None:
                    try:
                        if int(fc) not in (1, 2, 3, 4, 5, 6, 15, 16):
                            return False
                    except Exception:
                        pass
                return True
        except Exception:
            pass
        m2 = re.search(r"DEV_ID=(\d+)", txt)
        if m2:
            try:
                if self._device_item_id is not None and int(m2.group(1)) == int(self._device_item_id):
                    return True
            except Exception:
                pass
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
        for m in re.finditer(r"id=(\d+)", txt):
            try:
                if int(m.group(1)) in self._device_tag_ids:
                    return True
            except Exception:
                pass
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
            try:
                date_str = _dt.now().strftime("%Y/%m/%d")
            except Exception:
                date_str = ""
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
        clear_action = QAction("🗑️ 清除", self)
        clear_action.triggered.connect(self._clear_diagnostics)
        try:
            self.menuBar().addAction(clear_action)
        except Exception:
            pass
        export_action = QAction("💾 匯出.txt", self)
        export_action.triggered.connect(self._export_to_txt)
        try:
            self.menuBar().addAction(export_action)
        except Exception:
            pass

    def _clear_diagnostics(self):
        self.diagnostics_table.setRowCount(0)
        if self.parent_window:
            try:
                self.parent_window.clear_diagnostics()
            except Exception:
                pass

    def _export_to_txt(self):
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
                    headers = []
                    for c in range(self.diagnostics_table.columnCount()):
                        h = self.diagnostics_table.horizontalHeaderItem(c)
                        headers.append(h.text() if h is not None else "")
                    f.write("\t".join(headers) + "\n")
                    f.write("-" * 100 + "\n")
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
        pass

    def _on_only_txrx_toggled(self, v: bool):
        return

    def _on_show_raw_toggled(self, v: bool):
        return

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
                    if self._diag_show_raw:
                        self.diagnostics.set_only_txrx(False)
                    else:
                        self.diagnostics.set_only_txrx(bool(self._diag_show_only_txrx))
            except Exception:
                pass
        except Exception:
            pass
