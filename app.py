import sys
import os
import time
import threading
import logging
import asyncio
from datetime import datetime

# Hide console window on Windows (before any other operations)
if sys.platform == "win32":
    try:
        import ctypes
        import subprocess

        # Try method 1: Hide using windll
        ctypes.windll.kernel32.GetConsoleWindow.restype = ctypes.c_void_p
        hwnd = ctypes.windll.kernel32.GetConsoleWindow()
        if hwnd:
            ctypes.windll.user32.ShowWindow(hwnd, 0)  # 0 = SW_HIDE
            logger_temp = logging.getLogger(__name__)
            logger_temp.debug("Console window hidden via windll")
    except Exception as e:
        pass  # Not critical if this fails

from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTableView,
    QHeaderView,
    QTreeWidgetItem,
    QMessageBox,
    QMenu,
    QFileDialog,
)
from PyQt6.QtGui import QShortcut, QKeySequence, QAction, QIcon
from PyQt6.QtCore import Qt, QTimer
from core.config import GROUP_SEPARATOR
from ui.dragdrop_tree import ConnectivityTree
from ui.dialogs.channel_dialog import ChannelDialog
from ui.dialogs.device_dialog import DeviceDialog
from ui.dialogs.tag_dialog import TagDialog
from ui.dialogs.opcua_dialog import OPCUADialog
from ui.dialogs.write_value_dialog import WriteValueDialog
from ui.components import (
    setup_table,
    to_numeric_flag,
    safe_data,
    call_controller,
    schedule_temp_export,
    collect_selected_tree_items,
)
from ui.theme import SPLITTER_STYLE, TREE_ITEM_STYLE

try:
    from core.controllers.data_manager import DataBroker
except Exception:
    DataBroker = None
try:
    from core.OPC_UA.opcua_server import OPCUAServer

    OPCServer = OPCUAServer
except Exception:
    OPCServer = None
from ui.clipboard import ClipboardManager
from core.controllers import AppController
from core.modbus import AsyncPoller, RuntimeMonitor
from core.diagnostics import DiagnosticsManager

try:
    from ui.terminal_window import TerminalWindow
except Exception:
    TerminalWindow = None

# 可透過環境變數 `SUPPRESS_TERMINAL_OUTPUT=1` 全域抑制所有 `print()` 輸出（方便在生產或測試時避免終端雜訊）
try:
    import builtins

    if os.getenv("SUPPRESS_TERMINAL_OUTPUT", "0") == "1":

        def _suppress_print(*args, **kwargs):
            return None

        builtins.print = _suppress_print
except Exception:
    pass

# 可透過環境變數 `SUPPRESS_PYMODBUS=0` 控制是否顯示 pymodbus 的日誌。
# 預設會將 pymodbus logger 設為 CRITICAL，以隱藏 ERROR 等噪音訊息。
try:
    if os.getenv("SUPPRESS_PYMODBUS", "1") == "1":
        logging.getLogger("pymodbus").setLevel(logging.CRITICAL)
        logging.getLogger("pymodbus.logging").setLevel(logging.CRITICAL)
except Exception:
    pass

# Suppress asyncua and OPC UA library warnings
try:
    logging.getLogger("asyncua").setLevel(logging.CRITICAL)
    logging.getLogger("asyncua.client").setLevel(logging.CRITICAL)
    logging.getLogger("asyncua.server").setLevel(logging.CRITICAL)
    logging.getLogger("opcua").setLevel(logging.CRITICAL)
except Exception:
    pass


class IoTApp(QMainWindow):
    """Placeholder IoTApp class. Actual methods are defined as functions
    later in the file and will be attached to this class at runtime.
    """

    def __init__(self):
        super().__init__()

        # 初始化主窗口
        self.setWindowTitle("ModUA")
        icon_path = os.path.join(os.path.dirname(__file__), "lioil.ico")
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))

        # 初始化核心属性
        self._current_group_item = None
        self.terminal_windows = []
        self._runtime_running = False
        self.runtime_monitor = None  # RuntimeMonitor instance

        # Create RuntimeMonitorSignals in main thread (for thread affinity)
        from core.modbus.modbus_monitor import RuntimeMonitorSignals

        self.runtime_signals = RuntimeMonitorSignals()

        # 初始化核心模块
        try:
            self.controller = AppController(self)
            self.clipboard_manager = ClipboardManager(self)
            self.diagnostics = DiagnosticsManager()
        except Exception as e:
            self.controller = self.clipboard_manager = self.diagnostics = None
            logging.warning(f"Failed to initialize core modules: {e}")

        # 初始化数据路径
        appdata_root = os.getenv("APPDATA") or os.path.join(
            os.path.expanduser("~"), ".modua"
        )
        self._appdata_dir = os.path.join(appdata_root, "ModUA")
        os.makedirs(self._appdata_dir, exist_ok=True)
        self._temp_json = os.path.join(self._appdata_dir, "temp.json")
        self._last_project_file = os.path.join(self._appdata_dir, "last_project.txt")

        # 初始化UI状态变量
        self.terminal_window = None
        self.splitter = QSplitter(Qt.Orientation.Horizontal)
        self._splitter_initialized = False
        self._rows_initialized = False
        self._splitter_user_moved = False
        self._fixed_row_height = 24
        self.splitter.splitterMoved.connect(
            lambda pos, idx: setattr(self, "_splitter_user_moved", True)
        )

        # Batched UI update mechanism is no longer needed with virtual scrolling
        # Virtual model only updates visible rows, so performance is inherently good

        # 初始化树形控件
        self.tree = ConnectivityTree()
        self.tree.setUniformRowHeights(True)

        # 应用树形控件的额外样式
        if hasattr(self.tree, "setStyleSheet"):
            base = self.tree.styleSheet() or ""
            if "QTreeWidget::item" not in base:
                base += TREE_ITEM_STYLE
            self.tree.setStyleSheet(base)

        if hasattr(self.tree, "header"):
            try:
                self.tree.header().setDefaultSectionSize(100)
            except Exception:
                pass
        try:
            self.data_broker = DataBroker()
        except Exception:
            self.data_broker = None
        self.opc_server = None
        self._opc_update_timer = None
        self.tree.request_new_channel.connect(self.on_new_channel)
        self.tree.request_new_device.connect(self.on_new_device)
        self.tree.request_new_group.connect(self.on_new_group)
        self.tree.request_new_tag.connect(self.on_new_tag)
        try:
            # 將顯示內容的請求透過專用處理器路由，以確保 UI 能正確刷新
            self.tree.request_show_content.connect(self.on_show_content)
        except Exception:
            pass
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
        self.tree.itemClicked.connect(self.update_right_table)
        self.splitter.addWidget(self.tree)
        self.tag_table = QTableWidget()
        self.tag_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tag_table.customContextMenuRequested.connect(self.on_table_context_menu)
        self.tag_table.cellDoubleClicked.connect(self.on_table_cell_double_clicked)
        self.tag_table.setSelectionMode(QTableWidget.SelectionMode.ExtendedSelection)
        self.tag_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)

        # 应用统一的表格样式
        setup_table(self.tag_table)

        self.del_shortcut = QShortcut(QKeySequence.StandardKey.Delete, self)
        self.del_shortcut.activated.connect(self._handle_delete_shortcut)
        # 剪下/複製/貼上 的快速鍵
        self.cut_shortcut = QShortcut(QKeySequence.StandardKey.Cut, self)
        self.copy_shortcut = QShortcut(QKeySequence.StandardKey.Copy, self)
        self.paste_shortcut = QShortcut(QKeySequence.StandardKey.Paste, self)
        self.cut_shortcut.activated.connect(lambda: self._handle_shortcut("cut"))
        self.copy_shortcut.activated.connect(lambda: self._handle_shortcut("copy"))
        self.paste_shortcut.activated.connect(lambda: self._handle_shortcut("paste"))
        self.splitter.addWidget(self.tag_table)
        self.splitter.setStretchFactor(0, 3)
        self.splitter.setStretchFactor(1, 7)
        self.vsplitter = QSplitter(Qt.Orientation.Vertical)
        self.vsplitter.addWidget(self.splitter)

        # Monitor table setup with virtual scrolling
        from core.ui_models import VirtualMonitorTableModel

        self.monitor_model = VirtualMonitorTableModel(buffer_ref=None)
        self.monitor_table = QTableView()
        self.monitor_table.setModel(self.monitor_model)
        self.monitor_table.setSelectionMode(
            QTableWidget.SelectionMode.ExtendedSelection
        )
        self.monitor_table.setSelectionBehavior(
            QTableWidget.SelectionBehavior.SelectRows
        )
        self.monitor_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.monitor_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.monitor_table.customContextMenuRequested.connect(
            self._on_monitor_value_context_menu
        )

        # Connect scroll to virtual scrolling updates
        self.monitor_table.verticalScrollBar().valueChanged.connect(
            self._on_monitor_scroll
        )

        # 配置列宽
        header = self.monitor_table.horizontalHeader()
        for c in range(self.monitor_model.columnCount()):
            try:
                header.setSectionResizeMode(c, QHeaderView.ResizeMode.ResizeToContents)
            except Exception:
                pass

        # 应用统一的表格样式
        setup_table(self.monitor_table)
        self.vsplitter.addWidget(self.monitor_table)

        # 应用统一的背景色
        self.splitter.setStyleSheet(SPLITTER_STYLE)
        self.vsplitter.setStyleSheet(SPLITTER_STYLE)
        self.setCentralWidget(self.vsplitter)
        self.pollers = []
        self.monitor_row = {}
        self.monitor_counts = {}
        self.monitor_last_values = {}
        self.poll_settings = {
            "host": "127.0.0.1",
            "port": 502,
            "unit": 1,
            "interval": 1.0,
        }
        self.monitored_tags = []
        file_menu = self.menuBar().addMenu("📁 File")
        new_action = file_menu.addAction("📄 New")
        open_action = file_menu.addAction("📂 Open...")
        save_action = file_menu.addAction("💾 Save")
        save_as_action = file_menu.addAction("💾 Save As...")
        new_action.triggered.connect(self.new_project)
        open_action.triggered.connect(self.open_project)
        save_action.triggered.connect(self.save_project)
        save_as_action.triggered.connect(self.save_project_as)
        runtime_indicator = QAction("🟢 Runtime", self)
        runtime_indicator.triggered.connect(self.toggle_runtime)
        self.menuBar().addAction(runtime_indicator)
        self.runtime_indicator_action = runtime_indicator
        # (Normalize Channels action removed)
        opcua_action = QAction("🔗 OPC UA", self)
        opcua_action.triggered.connect(self.open_opcua_settings)
        self.menuBar().addAction(opcua_action)
        # 終端機（Terminals）選單：列出 ConnectivityTree 下的 Device，選擇即開啟該 Device 的 TerminalWindow
        try:
            self._terminals_menu = self.menuBar().addMenu("📺 Terminals")
            # 每次選單顯示時重建裝置清單
            try:
                self._terminals_menu.aboutToShow.connect(self.update_terminals_menu)
            except Exception:
                pass
        except Exception:
            self._terminals_menu = None
        try:
            conn_node = getattr(self.tree, "conn_node", None)
            if conn_node is not None:
                try:
                    self.tree.setCurrentItem(conn_node)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            loaded = False
            if self._temp_json and os.path.exists(self._temp_json):
                try:
                    self._call_controller("import_project_from_json", self._temp_json)
                    self.current_project_path = None
                    loaded = True
                    # 若 temp.json 看起來不完整（例如 channel 缺少 driver 參數），則以 last_project_file 作為備援匯入
                    try:
                        conn_node = getattr(self.tree, "conn_node", None)
                        missing_params = True
                        if conn_node is not None:
                            for i in range(conn_node.childCount()):
                                ch = conn_node.child(i)
                                try:
                                    drv9 = self._safe_data(ch, 9, None)
                                    if isinstance(drv9, dict):
                                        p = drv9.get("params")
                                        if isinstance(p, dict) and len(p) > 0:
                                            missing_params = False
                                            break
                                except Exception:
                                    pass
                        if (
                            missing_params
                            and self._last_project_file
                            and os.path.exists(self._last_project_file)
                        ):
                            try:
                                # 嘗試匯入上次儲存的專案，該檔案可能包含較完整的資料
                                self._call_controller(
                                    "import_project_from_json", self._last_project_file
                                )
                                self.current_project_path = self._last_project_file
                            except Exception:
                                pass
                    except Exception:
                        pass
                except Exception:
                    loaded = False
            if (
                not loaded
                and self._last_project_file
                and os.path.exists(self._last_project_file)
            ):
                try:
                    with open(self._last_project_file, "r", encoding="utf-8") as _f:
                        last_path = _f.read().strip()
                except Exception:
                    last_path = None
                if last_path and os.path.exists(last_path):
                    try:
                        self._call_controller("import_project_from_json", last_path)
                        self.current_project_path = last_path
                    except Exception:
                        pass
                else:
                    pass
            # 若未載入任何專案且樹為空，嘗試載入附帶的範例專案
            try:
                conn = getattr(self.tree, "conn_node", None)
                empty = True
                if conn is not None and conn.childCount() > 0:
                    empty = False
                if empty and getattr(self, "controller", None):
                    sample = os.path.join(os.path.dirname(__file__), "Project.json")
                    if os.path.exists(sample):
                        try:
                            self._call_controller("import_project_from_json", sample)
                            self.current_project_path = sample
                            loaded = True
                        except Exception:
                            loaded = loaded
            except Exception:
                pass
            else:
                pass
        except Exception:
            pass
        # 將初始更新延後至事件迴圈，以便樹狀資料先完成填充。
        # 同時安排第二次略微延遲的刷新以處理較晚才匯入的資料。
        try:
            # 啟動後短時間內填充右側表格一次；勿自動選取或自動展開
            QTimer.singleShot(
                0, lambda: self.update_right_table(self.tree.conn_node, 0)
            )
            # Schedule a delayed ensure of initial splitter sizes after the
            # main window has a chance to be shown and layouts settled.
            try:
                QTimer.singleShot(250, lambda: self._ensure_initial_splitter_sizes())
            except Exception:
                pass
            # 故意不自動選取第一個 device/channel，也不安排進一步的延遲選取
        except Exception:
            # 若定時器不可用則退回直接呼叫
            try:
                self.update_right_table(self.tree.conn_node, 0)
            except Exception:
                pass
        try:
            if hasattr(self, "_on_project_structure_changed"):
                try:
                    self._on_project_structure_changed()
                except Exception:
                    pass
        except Exception:
            pass

        # _normalize_channels_action removed — normalization can be invoked programmatically via controller.normalize_all_channels()
        # Startup OPC UA auto-start disabled - OPC UA server will only start when:
        # 1. User opens OPC UA settings dialog and clicks Finish
        # 2. User opens a project (after existing server is stopped)
        # try:
        #     # Only apply OPC UA settings at startup if settings are complete and valid
        #     # Check if we have the minimum required settings
        #     opc_settings = getattr(self, "opcua_settings", None)
        #     if opc_settings is not None:
        #         try:
        #             # Check if settings are complete (at least have general config)
        #             gen = (
        #                 opc_settings.get("general", {})
        #                 if isinstance(opc_settings.get("general"), dict)
        #                 else {}
        #             )
        #             if gen:  # Only apply if we have some settings
        #                 self.apply_opcua_settings(opc_settings)
        #         except Exception as e:
        #             try:
        #                 import traceback

        #                 self._write_opc_trace(
        #                     f"Startup: apply_opcua_settings failed: {e}\n{traceback.format_exc()}"
        #                 )
        #             except Exception:
        #                 pass
        # except Exception:
        #     pass

    def _log_splitter_sizes(self, prefix):
        try:
            sizes = list(self.splitter.sizes())
            msg = f"SPLITTER_DEBUG {prefix}: sizes={sizes}"
            # debug log removed
            return sizes
        except Exception:
            return None

    def _log_row_heights(self, prefix, max_rows=8):
        try:
            cnt = min(max_rows, self.tag_table.rowCount())
            rows = []
            for r in range(cnt):
                try:
                    rows.append(self.tag_table.rowHeight(r))
                except Exception:
                    rows.append(None)
                try:
                    default = None
                    try:
                        default = self.tag_table.verticalHeader().defaultSectionSize()
                    except Exception:
                        default = None
                    # debug log removed
                except Exception:
                    pass
            return rows
        except Exception:
            return None

    def _set_splitter_sizes(self, sizes, reason=None):
        try:
            cur = list(self.splitter.sizes())
        except Exception:
            cur = None
        # debug log removed
        # recent-finish guard removed; keep honoring user manual moves
        try:
            if getattr(self, "_splitter_user_moved", False) and reason != "force":
                # do not override when user has manually moved splitter
                # debug log removed
                return
        except Exception:
            pass
        try:
            self.splitter.setSizes(list(sizes))
        except Exception:
            pass

    # previously had a _mark_recent_finish helper; removed while reverting recent-finish behavior

    def _ensure_initial_splitter_sizes(self):
        # Ensure sensible splitter sizes after window show; log sizes and window geometry
        try:
            w = int(self.width() or 0)
            h = int(self.height() or 0)
        except Exception:
            w = 0
            h = 0

        # debug log removed

        # determine screen geometry
        try:
            screen = QApplication.primaryScreen()
            if screen is None:
                scrs = QApplication.screens()
                screen = scrs[0] if scrs else None
            if screen:
                geo = screen.availableGeometry()
                desired_w = max(300, geo.width() // 2)
                desired_h = max(200, geo.height() // 2)
                # attempt to set window to half-screen and center it
                try:
                    self.resize(desired_w, desired_h)
                    cx = geo.x() + (geo.width() - desired_w) // 2
                    cy = geo.y() + (geo.height() - desired_h) // 2
                    self.move(cx, cy)
                    # debug log removed
                    total_w = desired_w
                except Exception:
                    total_w = w or geo.width()
            else:
                total_w = w or 800
        except Exception:
            total_w = w or 800

        # log current splitter sizes before changing
        try:
            self._log_splitter_sizes("ensure_initial BEFORE_SET")
        except Exception:
            pass

        # left-small/right-large split (30/70) but respect minimums and user moves
        try:
            if not getattr(self, "_splitter_user_moved", False):
                left_calc = max(150, int(total_w * 0.30))
                right_calc = max(150, int(total_w) - left_calc)
                self._set_splitter_sizes(
                    [left_calc, right_calc], reason="ensure_initial"
                )
            else:
                half = int(total_w) // 2
                left_w = max(150, half)
                w_req = max(150, int(total_w) - left_w)
                self._set_splitter_sizes(
                    [left_w, w_req], reason="ensure_initial-fallback"
                )
        except Exception:
            pass

        try:
            # mark splitter as initialized so update_right_table won't override
            self._splitter_initialized = True
        except Exception:
            pass
        try:
            self._log_splitter_sizes("ensure_initial AFTER_SET")
        except Exception:
            pass

        # apply fixed row heights once after initial layout settle
        try:
            rh = int(getattr(self, "_fixed_row_height", 24))
            try:
                self.tag_table.verticalHeader().setDefaultSectionSize(rh)
            except Exception:
                pass
            try:
                for r in range(self.tag_table.rowCount()):
                    try:
                        self.tag_table.setRowHeight(r, rh)
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                self._log_row_heights("ensure_initial ROWS_SET")
            except Exception:
                pass
            self._rows_initialized = True
        except Exception:
            pass

    def _on_group_cell_changed(self, row, column):
        # 最小化處理器：當右側表格編輯 Group 欄位時更新 Group 名稱/描述
        try:
            if not getattr(self, "_current_group_item", None):
                return
            if column != 1:
                return
            cell = self.tag_table.item(row, column)
            if cell is None:
                return
            text = cell.text()
            if row == 0:
                try:
                    self._current_group_item.setText(0, text)
                except Exception:
                    pass
            elif row == 1:
                try:
                    # Group description moved to role 1
                    self._current_group_item.setData(1, Qt.ItemDataRole.UserRole, text)
                except Exception:
                    pass
        except Exception:
            pass

    def show_terminal_window(self, device_item=None):
        # 建立並顯示 TerminalWindow；若提供 device_item 則為該 device 的過濾視窗
        if TerminalWindow is None:
            try:
                QMessageBox.warning(
                    self,
                    "Diagnostics Unavailable",
                    "TerminalWindow component is missing.",
                )
            except Exception:
                pass
            return None
        tw = TerminalWindow(self, device_item, getattr(self, "diagnostics", None))
        self.terminal_windows.append(tw)
        try:
            tw.destroyed.connect(lambda obj, t=tw: self._remove_terminal(t))
        except Exception:
            pass
        try:
            tw.show()
            tw.raise_()
            tw.activateWindow()
        except Exception:
            pass
        try:
            self.update_terminals_menu()
        except Exception:
            pass
        return tw

    def open_device_diagnostics(self, device_item):
        # 針對指定的 `device_item` 開啟一個過濾後的 Diagnostics 視窗
        return self.show_terminal_window(device_item)

    def open_opcua_settings(self):
        # Open the OPC UA settings dialog, apply and persist settings when accepted.
        try:
            from ui.dialogs.opcua_dialog import OPCUADialog
        except Exception:
            return

        initial = getattr(self, "opcua_settings", None)
        # Don't normalize on dialog open - only on apply
        # initial is already in canonical form from last apply

        # Don't apply settings on dialog open
        # Only apply when user clicks Finish in the dialog
        # This prevents creating multiple OPC UA servers

        try:
            dlg = OPCUADialog(self, initial=initial)
        except Exception:
            return

        try:
            if initial is not None:
                dlg.load_data(initial)
        except Exception:
            pass

        if dlg.exec():
            try:
                data = dlg.get_data()
                self.opcua_settings = data

                # Only apply OPC UA settings when user clicks Finish
                # This ensures we only create one server per session
                if hasattr(self, "apply_opcua_settings"):
                    try:
                        self.apply_opcua_settings(data)
                    except Exception:
                        pass

                # persist temp project with updated opcua settings
                try:
                    if getattr(self, "_temp_json", None):
                        QTimer.singleShot(
                            50,
                            lambda: self._call_controller(
                                "export_project_to_json", self._temp_json
                            ),
                        )
                except Exception:
                    pass

                    # Reload tags with clearing when OPC settings change
                    # This ensures tags are properly re-synchronized with new OPC configuration
                    try:
                        if getattr(self, "opc_server", None):
                            # Update data sources first
                            if hasattr(self.opc_server, "set_data_sources"):
                                self.opc_server.set_data_sources(
                                    tree_widget=getattr(self, "tree", None),
                                    data_buffer=getattr(
                                        self, "modbus_data_buffer", None
                                    ),
                                )
                            # Reload all tags
                            QTimer.singleShot(
                                100,
                                lambda: self.opc_server.load_all_tags()
                                if self.opc_server
                                else None,
                            )
                    except Exception:
                        pass
            except Exception:
                pass

    def apply_opcua_settings(self, settings):
        """Apply OPC UA settings: normalize, persist and (re)start OPC server if available.

        This method keeps OPC UA adapter selection independent from channel adapters.
        """
        from logging import getLogger

        logger = getLogger(__name__)

        # 1. Normalize settings via controller if available
        try:
            if getattr(self, "controller", None) and hasattr(
                self.controller, "normalize_opcua_settings"
            ):
                res = self._call_controller("normalize_opcua_settings", settings)
                if res is not None:
                    settings = res
        except Exception as e:
            logger.warning(f"Normalization failed: {e}")

        # 2. Canonicalize settings into flat+nested structure
        try:
            gen = (
                settings.get("general", {})
                if isinstance(settings.get("general"), dict)
                else {}
            )
            auth = (
                settings.get("authentication", {})
                if isinstance(settings.get("authentication"), dict)
                else {}
            )
            sec = (
                settings.get("security_policies", {})
                if isinstance(settings.get("security_policies"), dict)
                else {}
            )
            cert = (
                settings.get("certificate", {})
                if isinstance(settings.get("certificate"), dict)
                else {}
            )

            nested = {
                "general": {
                    "application_name": gen.get("application_name")
                    or gen.get("application_Name")
                    or "",
                    "namespace": gen.get("namespace", ""),
                    "port": gen.get("port", ""),
                    "product_uri": gen.get("product_uri", ""),
                    "network_adapter": gen.get("network_adapter", ""),
                    "network_adapter_ip": gen.get("network_adapter_ip", ""),
                    "max_sessions": gen.get("max_sessions", ""),
                    "publish_interval": gen.get("publish_interval", ""),
                },
                "authentication": {
                    "authentication": auth.get("authentication")
                    or auth.get("type")
                    or "Anonymous",
                    "username": auth.get("username", ""),
                    "password": auth.get("password", ""),
                },
                "security_policies": {
                    k: bool(v)
                    for k, v in (sec.items() if isinstance(sec, dict) else {})
                },
                "certificate": {
                    **cert,
                    "auto_generate": bool(cert.get("auto_generate", True)),
                    "common_name": cert.get("common_name", ""),
                },
            }

            flat = {
                **nested["general"],
                **nested["authentication"],
                **nested["security_policies"],
                **nested["certificate"],
            }

            # Aliases for OPCUADialog
            if "application_name" in flat and "application_Name" not in flat:
                flat["application_Name"] = flat.get("application_name")
            if "product_uri" not in flat:
                flat["product_uri"] = nested["general"].get("product_uri", "")
            if "network_adapter_ip" not in flat:
                flat["network_adapter_ip"] = nested["general"].get(
                    "network_adapter_ip", ""
                )

            self.opcua_settings = {**flat, **nested}
        except Exception as e:
            logger.error(f"Canonicalization failed: {e}")
            self.opcua_settings = settings

        # 3. Start/restart OPC UA server if implementation available
        try:
            if OPCServer is None:
                logger.warning("OPCServer not available")
                return

            existing_server = getattr(self, "opc_server", None)

            if existing_server is None:
                logger.info("First time starting OPC UA server")
                self._create_and_start_opc_server(flat, nested)
                return

            # Server exists - check if restart needed
            is_running = (
                existing_server.is_running
                if hasattr(existing_server, "is_running")
                else False
            )
            old_port = getattr(existing_server, "settings", {}).get("port") or getattr(
                existing_server, "settings", {}
            ).get("general", {}).get("port")
            new_port = flat.get("port") or nested.get("general", {}).get("port", "")
            port_changed = (old_port != new_port) if old_port is not None else False

            logger.info(
                f"OPC UA check: old_port={old_port}, new_port={new_port}, port_changed={port_changed}, is_running={is_running}"
            )

            if is_running and port_changed:
                logger.info(
                    f"Port changed from {old_port} to {new_port}, restarting..."
                )
                try:
                    existing_server.stop_server()
                except Exception as e:
                    logger.warning(f"Error stopping OPC server: {e}")
                time.sleep(1)
                self._create_and_start_opc_server(flat, nested)
            elif is_running and not port_changed:
                logger.info("Settings updated, reloading tags...")
                existing_server.settings = {**flat, **nested}
                QTimer.singleShot(
                    100,
                    lambda: existing_server.reload_tags() if existing_server else None,
                )
            else:
                logger.info("Starting OPC UA server...")
                self._create_and_start_opc_server(flat, nested)
        except Exception as e:
            logger.error(f"Error in apply_opcua_settings: {e}")
            self.opc_server = None

    def _create_and_start_opc_server(self, flat: dict, nested: dict):
        """Helper to create and start OPC UA server."""
        try:
            self.opc_server = OPCServer({**flat, **nested})

            # Set data sources
            self.opc_server.set_data_sources(
                tree_widget=getattr(self, "tree", None),
                data_buffer=getattr(self, "modbus_data_buffer", None),
                runtime_monitor=getattr(self, "runtime_monitor", None),
            )

            # Start in background thread
            def _init_opc_in_background():
                import time
                import traceback
                from logging import getLogger

                opc_logger = getLogger(__name__)

                try:
                    self.opc_server.start_server()

                    # Wait for server to be ready
                    max_wait = 10
                    waited = 0
                    while not self.opc_server.is_running and waited < max_wait:
                        time.sleep(0.1)
                        waited += 0.1

                    if not self.opc_server.is_running:
                        opc_logger.error(
                            "OPC UA server initialization timeout or failed to start"
                        )
                        return

                    # Server is ready, load tags
                    if self.opc_server:
                        try:
                            self.opc_server.load_all_tags()

                            # Start periodic sync
                            if not getattr(self, "_opc_update_timer", None):
                                from PyQt6.QtCore import QTimer

                                self._opc_update_timer = QTimer(self)
                                self._opc_update_timer.setInterval(200)
                                self._opc_update_timer.timeout.connect(
                                    lambda: self.opc_server.sync_values()
                                    if self.opc_server
                                    else None
                                )
                                self._opc_update_timer.start()
                        except Exception as e:
                            opc_logger.error(
                                f"Error loading OPC tags: {e}\n{traceback.format_exc()}"
                            )
                except Exception as e:
                    opc_logger.error(
                        f"Error in OPC background initialization: {e}\n{traceback.format_exc()}"
                    )

            # Launch in background thread
            opc_thread = threading.Thread(target=_init_opc_in_background, daemon=True)
            opc_thread.start()
        except Exception as e:
            import traceback

            self._write_opc_trace(
                f"Failed to start OPC initialization: {e}\n{traceback.format_exc()}"
            )

            # canonicalize into flat+nested structure expected by OPCUADialog.load_data/get_data
            try:
                gen = (
                    settings.get("general")
                    if isinstance(settings.get("general"), dict)
                    else {}
                )
            except Exception:
                gen = {}
            try:
                auth = (
                    settings.get("authentication")
                    if isinstance(settings.get("authentication"), dict)
                    else {}
                )
            except Exception:
                auth = {}
            try:
                sec = (
                    settings.get("security_policies")
                    if isinstance(settings.get("security_policies"), dict)
                    else {}
                )
            except Exception:
                sec = {}
            try:
                cert = (
                    settings.get("certificate")
                    if isinstance(settings.get("certificate"), dict)
                    else {}
                )
            except Exception:
                cert = {}

            nested = {
                "general": {
                    "application_name": gen.get("application_name")
                    or gen.get("application_Name")
                    or "",
                    "namespace": gen.get("namespace", ""),
                    "port": gen.get("port", ""),
                    "product_uri": gen.get("product_uri", ""),
                    "network_adapter": gen.get("network_adapter", ""),
                    "network_adapter_ip": gen.get("network_adapter_ip", ""),
                    "max_sessions": gen.get("max_sessions", ""),
                    "publish_interval": gen.get("publish_interval", ""),
                },
                "authentication": {
                    "authentication": auth.get("authentication")
                    or auth.get("type")
                    or "Anonymous",
                    "username": auth.get("username", ""),
                    "password": auth.get("password", ""),
                },
                "security_policies": {
                    k: bool(v)
                    for k, v in (sec.items() if isinstance(sec, dict) else {})
                },
                "certificate": {
                    **cert,
                    "auto_generate": bool(cert.get("auto_generate", True)),
                    "common_name": cert.get("common_name", ""),
                },
            }

            flat = {}
            try:
                flat.update(nested["general"])
                flat.update(nested["authentication"])
                flat.update(nested["security_policies"])
                flat.update(nested["certificate"])
            except Exception:
                pass

            # provide aliases expected by OPCUADialog/FormBuilder
            try:
                # Form uses 'application_Name' (mixed case) while canonical uses 'application_name'
                if "application_name" in flat and "application_Name" not in flat:
                    flat["application_Name"] = flat.get("application_name")
                # ensure product_uri present under expected key
                if "product_uri" not in flat and nested.get("general"):
                    flat["product_uri"] = nested["general"].get("product_uri", "")
                # ensure network adapter ip flat key
                if "network_adapter_ip" not in flat and nested.get("general"):
                    flat["network_adapter_ip"] = nested["general"].get(
                        "network_adapter_ip", ""
                    )
            except Exception:
                pass

            # persist canonical structure
            try:
                self.opcua_settings = {**flat, **nested}
            except Exception:
                self.opcua_settings = settings

            # stop existing server if any
            try:
                if getattr(self, "opc_server", None) is not None:
                    # Only stop if actually running or needs restart
                    opc = self.opc_server
                    is_server_running = (
                        opc.is_running if hasattr(opc, "is_running") else False
                    )

                    # Check if port changed - need full restart
                    old_port = None
                    try:
                        old_settings = getattr(opc, "settings", {})
                        old_port = old_settings.get("port") or old_settings.get(
                            "general", {}
                        ).get("port")
                    except Exception as e:
                        logger.warning(f"Error getting old port: {e}")

                    new_port = settings.get("port") or settings.get("general", {}).get(
                        "port"
                    )
                    port_changed = (
                        (old_port != new_port) if old_port is not None else False
                    )

                    # Debug logging
                    logger.info(
                        f"OPC UA port check: old_port={old_port}, new_port={new_port}, port_changed={port_changed}, is_running={is_server_running}"
                    )

                    if is_server_running and port_changed:
                        # Port changed - need full restart
                        logger.info(
                            f"Port changed from {old_port} to {new_port}, restarting OPC UA server..."
                        )
                        try:
                            opc.stop_server()
                            import time

                            time.sleep(2)  # Wait longer for cleanup
                        except Exception as e:
                            logger.warning(f"Error stopping OPC server: {e}")
                        # Force create new server (don't reuse existing)
                        self.opc_server = None
                    elif is_server_running and not port_changed:
                        # Settings changed but port same, just update and reload
                        try:
                            opc.settings = settings
                        except Exception as e:
                            logger.warning(f"Error updating OPC settings: {e}")
            except Exception:
                pass

            # create new OPC server if implementation available
            try:
                if OPCServer is not None:
                    # Check if we have an existing server
                    existing_server = getattr(self, "opc_server", None)

                    # Check if existing server is actually running
                    is_server_running = (
                        existing_server is not None
                        and existing_server.is_running
                        and existing_server.server_thread is not None
                        and existing_server.server_thread.is_alive()
                    )

                    # Helper function to create and start server
                    def create_and_start_server():
                        try:
                            self.opc_server = OPCServer(settings)

                            # Set data sources
                            self.opc_server.set_data_sources(
                                tree_widget=getattr(self, "tree", None),
                                data_buffer=getattr(self, "modbus_data_buffer", None),
                                runtime_monitor=getattr(self, "runtime_monitor", None),
                            )

                            # Start OPC server and load tags in background thread
                            def _init_opc_in_background():
                                import time
                                import traceback
                                from logging import getLogger

                                opc_logger = getLogger(__name__)

                                try:
                                    self.opc_server.start_server()

                                    # Wait for server to be ready
                                    max_wait = 10
                                    waited = 0
                                    while (
                                        not self.opc_server.is_running
                                        and waited < max_wait
                                    ):
                                        time.sleep(0.1)
                                        waited += 0.1

                                    if not self.opc_server.is_running:
                                        opc_logger.error(
                                            "OPC UA server initialization timeout or failed to start"
                                        )
                                        return

                                    # Server is ready, now load all tags
                                    if self.opc_server:
                                        try:
                                            self.opc_server.load_all_tags()

                                            # Start periodic synchronization
                                            if not getattr(
                                                self, "_opc_update_timer", None
                                            ):
                                                from PyQt6.QtCore import QTimer

                                                self._opc_update_timer = QTimer(self)
                                                self._opc_update_timer.setInterval(200)
                                                self._opc_update_timer.timeout.connect(
                                                    lambda: self.opc_server.sync_values()
                                                    if self.opc_server
                                                    else None
                                                )
                                                self._opc_update_timer.start()
                                        except Exception as e:
                                            opc_logger.error(
                                                f"Error loading OPC tags: {e}\n{traceback.format_exc()}"
                                            )
                                except Exception as e:
                                    opc_logger.error(
                                        f"Error in OPC background initialization: {e}\n{traceback.format_exc()}"
                                    )

                            # Launch in background thread
                            opc_thread = threading.Thread(
                                target=_init_opc_in_background, daemon=True
                            )
                            opc_thread.start()
                        except Exception as e:
                            import traceback

                            self._write_opc_trace(
                                f"Failed to start OPC initialization: {e}\n{traceback.format_exc()}"
                            )

                    if is_server_running:
                        # Server is already running, just update settings and reload tags
                        try:
                            existing_server.settings = settings

                            # Reload tags with clearing
                            QTimer.singleShot(
                                100,
                                lambda: existing_server.reload_tags()
                                if existing_server
                                else None,
                            )

                            logger.info(
                                "OPC UA server settings updated and tags reloaded"
                            )
                        except Exception as e:
                            logger.warning(f"Error reloading OPC tags: {e}")
                    else:
                        # Create new server (first time or after restart)
                        # Ensure any existing thread is stopped
                        if existing_server is not None:
                            if (
                                existing_server.server_thread is not None
                                and existing_server.server_thread.is_alive()
                            ):
                                logger.warning("Stopping stale OPC UA server thread...")
                                existing_server.stop_server()
                                import time

                                time.sleep(1)

                        create_and_start_server()
            except Exception:
                self.opc_server = None
                pass
        except Exception:
            pass

            # create new OPC server if implementation available
            try:
                if OPCServer is not None:
                    # Check if we have an existing server
                    existing_server = getattr(self, "opc_server", None)

                    # Check if existing server is actually running
                    # Also check if to thread is still alive
                    is_server_running = (
                        existing_server is not None
                        and existing_server.is_running
                        and existing_server.server_thread is not None
                        and existing_server.server_thread.is_alive()
                    )

                    if is_server_running:
                        # Server is already running and thread is alive
                        # Check if port changed - need full restart
                        old_port = None
                        try:
                            old_settings = getattr(existing_server, "settings", {})
                            old_port = old_settings.get("port") or old_settings.get(
                                "general", {}
                            ).get("port")
                        except Exception:
                            pass

                        new_port = settings.get("port") or settings.get(
                            "general", {}
                        ).get("port")
                        port_changed = (
                            (old_port != new_port) if old_port is not None else False
                        )

                        if port_changed:
                            # Port changed - need full restart
                            # Stop the old server first
                            logger.info(
                                f"Port changed from {old_port} to {new_port}, restarting OPC UA server..."
                            )
                            try:
                                existing_server.stop_server()
                            except Exception as e:
                                logger.warning(f"Error stopping OPC server: {e}")

                            # Wait for cleanup
                            import time

                            time.sleep(1)

                            # Create new server with new settings
                            try:
                                self.opc_server = OPCServer(settings)

                                # Set data sources
                                self.opc_server.set_data_sources(
                                    tree_widget=getattr(self, "tree", None),
                                    data_buffer=getattr(
                                        self, "modbus_data_buffer", None
                                    ),
                                    runtime_monitor=getattr(
                                        self, "runtime_monitor", None
                                    ),
                                )

                                # Start OPC server and load tags in background thread
                                # This prevents UI from blocking during initialization
                                def _init_opc_in_background():
                                    import time
                                    import traceback
                                    from logging import getLogger

                                    opc_logger = getLogger(__name__)

                                    try:
                                        self.opc_server.start_server()

                                        # Wait for server to be ready (with timeout)
                                        max_wait = 10  # Maximum 10 seconds to wait
                                        waited = 0
                                        while (
                                            not self.opc_server.is_running
                                            and waited < max_wait
                                        ):
                                            time.sleep(0.1)
                                            waited += 0.1

                                        if not self.opc_server.is_running:
                                            opc_logger.error(
                                                "OPC UA server initialization timeout or failed to start"
                                            )
                                            return

                                        # Server is ready, now load all tags
                                        if self.opc_server:
                                            try:
                                                self.opc_server.load_all_tags()

                                                # Start periodic synchronization of tag values
                                                if not getattr(
                                                    self, "_opc_update_timer", None
                                                ):
                                                    from PyQt6.QtCore import QTimer

                                                    self._opc_update_timer = QTimer(
                                                        self
                                                    )
                                                    self._opc_update_timer.setInterval(
                                                        200
                                                    )
                                                    self._opc_update_timer.timeout.connect(
                                                        lambda: self.opc_server.sync_values()
                                                        if self.opc_server
                                                        else None
                                                    )
                                                    self._opc_update_timer.start()
                                            except Exception as e:
                                                opc_logger.error(
                                                    f"Error loading OPC tags: {e}\n{traceback.format_exc()}"
                                                )
                                    except Exception as e:
                                        opc_logger.error(
                                            f"Error in OPC background initialization: {e}\n{traceback.format_exc()}"
                                        )

                                # Launch OPC initialization in background thread
                                opc_thread = threading.Thread(
                                    target=_init_opc_in_background, daemon=True
                                )
                                opc_thread.start()
                            except Exception as e:
                                import traceback

                                self._write_opc_trace(
                                    f"Failed to start OPC initialization: {e}\n{traceback.format_exc()}"
                                )
                        else:
                            # Settings changed but port same, just update and reload
                            try:
                                existing_server.settings = settings

                                # Reload tags with clearing (in case structure changed)
                                QTimer.singleShot(
                                    100,
                                    lambda: existing_server.reload_tags()
                                    if existing_server
                                    else None,
                                )

                                logger.info(
                                    "OPC UA server settings updated and tags reloaded"
                                )
                            except Exception as e:
                                logger.warning(f"Error reloading OPC tags: {e}")
                    else:
                        # Create new server (first time startup or after new project)
                        # Ensure any existing thread is stopped
                        if existing_server is not None:
                            if (
                                existing_server.server_thread is not None
                                and existing_server.server_thread.is_alive()
                            ):
                                logger.warning("Stopping stale OPC UA server thread...")
                                existing_server.stop_server()
                                import time

                                time.sleep(1)

                        try:
                            self.opc_server = OPCServer(settings)

                            # Set data sources
                            self.opc_server.set_data_sources(
                                tree_widget=getattr(self, "tree", None),
                                data_buffer=getattr(self, "modbus_data_buffer", None),
                                runtime_monitor=getattr(self, "runtime_monitor", None),
                            )

                            # Start OPC server and load tags in background thread
                            # This prevents UI from blocking during initialization
                            def _init_opc_in_background():
                                import time
                                import traceback
                                from logging import getLogger

                                opc_logger = getLogger(__name__)

                                try:
                                    self.opc_server.start_server()

                                    # Wait for server to be ready (with timeout)
                                    max_wait = 10  # Maximum 10 seconds to wait
                                    waited = 0
                                    while (
                                        not self.opc_server.is_running
                                        and waited < max_wait
                                    ):
                                        time.sleep(0.1)
                                        waited += 0.1

                                    if not self.opc_server.is_running:
                                        opc_logger.error(
                                            "OPC UA server initialization timeout or failed to start"
                                        )
                                        return

                                    # Server is ready, now load all tags
                                    if self.opc_server:
                                        try:
                                            self.opc_server.load_all_tags()

                                            # Start periodic synchronization of tag values
                                            if not getattr(
                                                self, "_opc_update_timer", None
                                            ):
                                                from PyQt6.QtCore import QTimer

                                                self._opc_update_timer = QTimer(self)
                                                self._opc_update_timer.setInterval(200)
                                                self._opc_update_timer.timeout.connect(
                                                    lambda: self.opc_server.sync_values()
                                                    if self.opc_server
                                                    else None
                                                )
                                                self._opc_update_timer.start()
                                        except Exception as e:
                                            opc_logger.error(
                                                f"Error loading OPC tags: {e}\n{traceback.format_exc()}"
                                            )
                                except Exception as e:
                                    opc_logger.error(
                                        f"Error in OPC background initialization: {e}\n{traceback.format_exc()}"
                                    )

                            # Launch OPC initialization in background thread
                            opc_thread = threading.Thread(
                                target=_init_opc_in_background, daemon=True
                            )
                            opc_thread.start()
                        except Exception as e:
                            import traceback

                            self._write_opc_trace(
                                f"Failed to start OPC initialization: {e}\n{traceback.format_exc()}"
                            )
            except Exception:
                self.opc_server = None
                pass
        except Exception:
            pass
        except Exception:
            pass
        except Exception:
            pass

    def _remove_terminal(self, tw):
        if tw in getattr(self, "terminal_windows", []):
            try:
                self.terminal_windows.remove(tw)
            except Exception:
                pass
        try:
            self.update_terminals_menu()
        except Exception:
            pass

        menu = getattr(self, "_terminals_menu", None)
        if menu is None:
            return
        # 使用統一 helper 重建 menu
        try:
            self._populate_terminals_menu(menu)
        except Exception:
            pass
        # (Intentionally do not list existing open TerminalWindow items here)

    def update_terminals_menu(self):
        """Rebuild the Terminals menu listing devices under Connectivity.

        This is invoked when the Terminals menu is shown or when terminals list changes.
        """
        menu = getattr(self, "_terminals_menu", None)
        if menu is None:
            return
        try:
            self._populate_terminals_menu(menu)
        except Exception:
            pass

    def _get_or_open_device_terminal(self, device_item):
        # 尋找已存在的 per-device `TerminalWindow`（若存在則回傳，否則不自動建立）。
        #
        # 設計理據：避免在非使用者主動要求時自動開啟視窗，僅當程式想要重用已開啟視窗時回傳。
        if device_item is None:
            return None
        dev_id = int(id(device_item))
        wins = getattr(self, "terminal_windows", []) or []
        for w in wins:
            try:
                if getattr(w, "_device_item_id", None) == dev_id:
                    return w
            except Exception:
                pass
        return None

    def _get_or_open_opc_terminal(self):
        # 尋找或回傳專用的 OPC diagnostics 視窗（若不存在則回傳 None）。
        #
        # 注意：僅為查詢/重用用途，不會自動建立或顯示新的 OPC 視窗。
        wins = getattr(self, "terminal_windows", []) or []
        for w in wins:
            try:
                if getattr(w, "_is_opc", False):
                    return w
            except Exception:
                pass
        return None

    def _populate_terminals_menu(self, menu):
        """Clear and populate the given Terminals menu from ConnectivityTree devices."""
        if menu is None:
            return
        try:
            try:
                menu.clear()
            except Exception:
                pass
            root = getattr(self.tree, "conn_node", None)
            if root is None:
                return
            for ci in range(root.childCount()):
                try:
                    ch = root.child(ci)
                except Exception:
                    continue
                for di in range(ch.childCount()):
                    try:
                        dev = ch.child(di)
                    except Exception:
                        continue
                    try:
                        if self._safe_data(dev, 0) == "Device":
                            label = f"{ch.text(0)} / {dev.text(0)}"
                            act = QAction(label, self)
                            act.triggered.connect(
                                lambda checked=False, d=dev: self.show_terminal_window(
                                    d
                                )
                            )
                            menu.addAction(act)
                    except Exception:
                        pass
        except Exception:
            pass

    def _set_diag_show_only_txrx(self, v: bool):
        # 設定 Diagnostics 的顯示模式為只顯示 TX/RX（布林）。
        #
        # 此方法會更新本地旗標，並嘗試更新 `DiagnosticsManager`（若存在）。
        self._diag_show_only_txrx = bool(v)

    def _set_diag_show_raw(self, v: bool):
        # 設定 Diagnostics 的 Raw 顯示模式（顯示完整原始訊息）。
        #
        # 若啟用 raw 顯示，會強制將 TX/RX filter 關閉。
        self._diag_show_raw = bool(v)

    def _safe_data(self, item, role, default=None):
        """安全地從 QTreeWidgetItem 讀取 UserRole 資料，失敗時回傳 default。"""
        return safe_data(item, role, default)

    def _schedule_temp_export(self, delay=50):
        """若存在 `_temp_json`，排程短延遲匯出以儲存暫存專案。"""
        schedule_temp_export(self, delay)

    def _call_controller(self, method_name, *args, **kwargs):
        """Safely call a controller method if available; otherwise return None."""
        return call_controller(self, method_name, *args, **kwargs)

    # --- Minimal handlers required by ConnectivityTree signals ---
    def on_edit_item(self, item):
        # 處理樹狀項目之編輯請求。
        #
        # 行為：依據節點類型（Channel/Device/Tag）分派到對應的編輯函式；
        # 若找不到對應實作，顯示資訊對話框提示使用者。
        #
        # 參數：
        # - item: QTreeWidgetItem 要編輯的節點。
        node_type = None
        node_type = self._safe_data(item, 0, None)

        # 補強推斷：若 UserRole 未正確設定，嘗試從子節點或名稱推斷類型
        if not isinstance(node_type, str):
            try:
                # 若子節點中有 Device，則此為 Channel；若有 Tag，則為 Device
                for i in range(item.childCount()):
                    try:
                        ctype = self._safe_data(item.child(i), 0, None)
                        if ctype == "Device":
                            node_type = "Channel"
                            break
                        if ctype == "Tag":
                            node_type = "Device"
                            break
                    except Exception:
                        pass
            except Exception:
                pass
        if not isinstance(node_type, str):
            try:
                import re

                m = re.match(r"^(Channel|Device|Tag)", (item.text(0) or ""))
                if m:
                    node_type = m.group(1)
            except Exception:
                pass
        if node_type == "Channel":
            try:
                self.on_edit_channel(item)
                return
            except Exception:
                pass
        if node_type == "Device":
            try:
                self.on_edit_device(item)
                return
            except Exception:
                pass
        if node_type == "Group":
            try:
                self.on_edit_group(item)
                return
            except Exception:
                pass
        if node_type == "Tag":
            try:
                self.on_edit_tag(item)
                return
            except Exception:
                pass
        QMessageBox.information(
            self, "Edit", "Edit action not available in this UI build"
        )

    def on_edit_channel(self, item):
        # 開啟 Channel 編輯對話方塊並儲存變更
        if item is None:
            return
        try:
            # new mapping: description -> role1, driver -> role2, communication -> role3
            flat_params = self._safe_data(item, 3, {}) or {}
            driver_val = self._safe_data(item, 2, None)
            desc = self._safe_data(item, 1, "") or ""
            from collections import OrderedDict as _OD

            # driver_val may be dict or string; ensure nested structure matches expected project tree
            drv_type = None
            drv_params = {}
            if isinstance(driver_val, dict):
                drv_type = driver_val.get("type")
                drv_params = (
                    driver_val.get("params")
                    if isinstance(driver_val.get("params"), dict)
                    else (driver_val.get("params") or {})
                )
            else:
                drv_type = driver_val
                drv_params = flat_params
            data = _OD(
                [
                    ("general", {"name": item.text(0) or "", "description": desc}),
                    ("driver", _OD([("type", drv_type), ("params", drv_params)])),
                    ("communication", flat_params),
                ]
            )
        except Exception:
            data = {"general": {"name": item.text(0) or ""}}
        dlg = ChannelDialog(
            self, suggested_name=(data.get("general", {}).get("name") or "Channel1")
        )
        try:
            dlg.load_data(data)
        except Exception:
            pass
        if dlg.exec():
            new_data = dlg.get_data()
            try:
                self._call_controller("save_channel", item, new_data)
            except Exception:
                pass
            # 確保在編輯 Channel 後更新 temp.json
            self._schedule_temp_export()
            # Show the edited channel itself on the right
            try:
                item.setExpanded(True)
            except Exception:
                pass
            try:
                self.tree.setCurrentItem(item)
            except Exception:
                pass
            try:
                self.update_right_table(item, 0)
            except Exception:
                pass

    def on_edit_device(self, item):
        # 開啟 Device 編輯對話方塊並儲存變更
        if item is None:
            return
        try:
            # 優先使用 device 專屬的 driver (if any); 否則從父 Channel 的 role2/role9 讀取
            driver = None
            try:
                # check device-level role9 (compat) first
                drv9 = self._safe_data(item, 9, None)
                if isinstance(drv9, dict) and drv9.get("type"):
                    driver = drv9.get("type")
            except Exception:
                driver = None
            if not driver:
                parent = item.parent() or None
                try:
                    if parent is not None:
                        pdrv = self._safe_data(parent, 2, None)
                        if isinstance(pdrv, dict):
                            # nested driver dict stored on channel
                            driver = pdrv.get("type") or pdrv
                        else:
                            driver = pdrv
                        # fallback to parent's role9 if needed
                        if not driver:
                            pr9 = self._safe_data(parent, 9, None)
                            if isinstance(pr9, dict):
                                driver = pr9.get("type")
                except Exception:
                    driver = None
            driver = driver or "Modbus RTU Serial"
            # ensure we pass a string driver_type to DeviceDialog
            try:
                if isinstance(driver, dict):
                    driver = driver.get("type") or "Modbus RTU Serial"
            except Exception:
                driver = "Modbus RTU Serial"

            general = {
                "name": item.text(0) or "",
                "device_id": self._safe_data(item, 2, None),
                # description moved to role 1
                "description": self._safe_data(item, 1, "") or "",
            }
            data = {
                "general": general,
                "timing": self._safe_data(item, 3, {}) or {},
                "data_access": self._safe_data(item, 4, {}) or {},
                "encoding": self._safe_data(item, 5, {}) or {},
                "block_sizes": self._safe_data(item, 6, {}) or {},
            }
        except Exception:
            driver = "Modbus RTU Serial"
            data = {"general": {"name": item.text(0) or ""}}
        dlg = DeviceDialog(
            self,
            suggested_name=(data.get("general", {}).get("name") or "Device1"),
            driver_type=driver,
        )
        try:
            dlg.load_data(data)
        except Exception:
            pass
        if dlg.exec():
            new_data = dlg.get_data()
            try:
                self._call_controller("save_device", item, new_data)
            except Exception:
                pass
            # 確保在建立或編輯 Device 後更新 temp.json
            try:
                if getattr(self, "_temp_json", None):
                    QTimer.singleShot(
                        50,
                        lambda: self._call_controller(
                            "export_project_to_json", self._temp_json
                        ),
                    )
            except Exception:
                pass
            # After editing a device, show the device's Tag view
            try:
                item_parent = item.parent() or getattr(self.tree, "conn_node", None)
                if item_parent:
                    item_parent.setExpanded(True)
            except Exception:
                pass
            try:
                self.tree.setCurrentItem(item)
            except Exception:
                pass
            try:
                self.update_right_table(item, 0)
            except Exception:
                pass

    def on_edit_tag(self, item):
        # 開啟 Tag 編輯對話方塊並儲存變更
        if item is None:
            return
        try:
            parent = item.parent() or None
            data = {
                "general": {
                    "name": item.text(0) or "",
                    "address": self._safe_data(item, 4, None),
                    "data_type": self._safe_data(item, 2, None),
                    "description": self._safe_data(item, 1, "") or "",
                    "scan_rate": self._safe_data(item, 5, "10") or "10",
                },
                "scaling": self._safe_data(item, 6, {}) or {},
            }
        except Exception:
            data = {"general": {"name": item.text(0) or ""}, "scaling": {}}
            parent = item.parent() or None
        suggested_addr = data.get("general", {}).get("address") or None
        dlg = TagDialog(
            self,
            suggested_name=data.get("general", {}).get("name") or "Tag1",
            suggested_addr=suggested_addr or "400000",
            is_new=False,
        )
        try:
            dlg.load_data(data)
        except Exception:
            pass
        if dlg.exec():
            new_data = dlg.get_data()
            try:
                self._call_controller("save_tag", item, new_data)
            except Exception:
                pass
            parent = parent or getattr(self.tree, "conn_node", None)
            if parent:
                self.update_right_table(parent, 0)

    def on_edit_group(self, item):
        # 開啟 Group 對話方塊以編輯名稱/描述
        if item is None:
            return
        try:
            data = {
                "general": {
                    "name": item.text(0) or "",
                    "description": self._safe_data(item, 3, "") or "",
                }
            }
        except Exception:
            data = {"general": {"name": item.text(0) or "", "description": ""}}
        from ui.dialogs.group_dialog import GroupDialog

        suggested = (
            (data.get("general") or {}).get("name") or data.get("name") or "Group1"
        )
        dlg = GroupDialog(self, suggested_name=suggested)
        try:
            dlg.load_data(data)
        except Exception:
            pass
        # preserve splitter sizes to avoid dialog-driven layout shifts
        try:
            _prev_sizes = list(self.splitter.sizes())
        except Exception:
            _prev_sizes = None
        try:
            self._log_splitter_sizes("on_edit_group BEFORE")
        except Exception:
            pass
        try:
            self._log_row_heights("on_edit_group BEFORE")
        except Exception:
            pass
        if dlg.exec():
            new = dlg.get_data()
            try:
                if getattr(self, "controller", None):
                    self._call_controller("save_group", item, new)
                else:
                    item.setText(0, new.get("name") or item.text(0))
                    item.setData(
                        1, Qt.ItemDataRole.UserRole, new.get("description") or ""
                    )
            except Exception:
                try:
                    item.setText(0, new.get("name") or item.text(0))
                    item.setData(
                        1, Qt.ItemDataRole.UserRole, new.get("description") or ""
                    )
                except Exception:
                    pass
            # After editing a group, show the group's Tag view
            try:
                try:
                    item.setExpanded(True)
                except Exception:
                    pass
                self.tree.setCurrentItem(item)
            except Exception:
                pass
            try:
                self.update_right_table(item, 0)
            except Exception:
                pass
        # restore previous splitter sizes (if recorded) only if user did NOT manually move splitter
        try:
            if _prev_sizes is not None and not getattr(
                self, "_splitter_user_moved", False
            ):
                self._set_splitter_sizes(_prev_sizes, reason="restore_on_new_group")
        except Exception:
            pass
        try:
            self._log_splitter_sizes("on_edit_group AFTER_RESTORE")
        except Exception:
            pass
        try:
            self._log_row_heights("on_edit_group AFTER")
        except Exception:
            pass

    def on_delete_item(self, item):
        # 刪除指定的樹狀節點。
        #
        # UI 行為：詢問使用者確認，若確認則將節點從其 parent 移除，並嘗試刷新右側表格顯示。
        if item is None:
            return
        try:
            label = item.text(0)
        except Exception:
            label = ""
        if (
            QMessageBox.question(self, "Delete", f"確定刪除 '{label}'?")
            == QMessageBox.StandardButton.Yes
        ):
            try:
                parent = item.parent() or self.tree.invisibleRootItem()
                parent.removeChild(item)
            except Exception:
                pass
            try:
                self.update_right_table(parent, 0)
            except Exception:
                pass

    def on_copy_item(self, item):
        # 將指定節點複製到內建 clipboard-manager。
        #
        # 此方法僅為 UI 操作層面的複製；實際貼上需由 `on_paste_item` 處理。
        try:
            self.clipboard_manager.copy(item)
        except Exception:
            pass

    def on_cut_item(self, item):
        # 剪下指定節點並將其放入 clipboard；若有 parent 回傳則刷新對應顯示。

        parent = self.clipboard_manager.cut(item)
        if parent:
            try:
                self.update_right_table(parent, 0)
            except Exception:
                pass

    def on_paste_item(self, target_item):
        # 將 clipboard 中的項目貼上到 `target_item`。貼上成功後會刷新父節點的表格顯示。
        #
        # 參數：
        # - target_item: 如果為 None，則貼到目前選中的節點或專案根節點。
        try:
            try:
                ttype = (
                    self._safe_data(target_item, 0, None)
                    if target_item is not None
                    else None
                )
                ttext = target_item.text(0) if target_item is not None else None
                # debug log removed
            except Exception:
                pass
        except Exception:
            pass

        parent = self.clipboard_manager.paste(target_item)
        if parent:
            try:
                self.update_right_table(parent, 0)
            except Exception:
                pass
        # 確保貼上操作後樹狀隱藏 Tag 節點
        try:
            if getattr(self, "tree", None) and hasattr(self.tree, "hide_all_tags"):
                try:
                    self.tree.hide_all_tags()
                except Exception:
                    pass
        except Exception:
            pass

    def on_import_device_csv(self, device_item):
        # 觸發 Device CSV 匯入流程的 UI 端 handler。
        #
        # Prompt user to select CSV file and import into the specified device
        try:
            from PyQt6.QtWidgets import QFileDialog, QMessageBox

            path, _ = QFileDialog.getOpenFileName(
                self, "Import Device CSV", "", "CSV Files (*.csv);;All Files (*)"
            )
            if not path:
                return

            try:
                self._call_controller("import_device_from_csv", device_item, path)
            except Exception as e:
                QMessageBox.warning(
                    self, "Import", f"匯入失敗，請檢查 CSV 格式: {str(e)}"
                )
                return

            try:
                # refresh UI for device
                self.update_right_table(device_item, 0)
            except Exception:
                pass

            QMessageBox.information(self, "Import", "匯入完成")
        except Exception as e:
            try:
                QMessageBox.information(self, "Import", f"匯入失敗: {str(e)}")
            except Exception:
                pass

    def on_export_device_csv(self, device_item):
        # 觸發 Device CSV 匯出的 UI handler。
        #
        try:
            from PyQt6.QtWidgets import QFileDialog, QMessageBox

            path, _ = QFileDialog.getSaveFileName(
                self,
                "Export Device CSV",
                "device_export.csv",
                "CSV Files (*.csv);;All Files (*)",
            )
            if not path:
                return
            try:
                # ensure extension
                if not path.lower().endswith(".csv"):
                    path = path + ".csv"
                self._call_controller("export_device_to_csv", device_item, path)
            except Exception:
                QMessageBox.warning(self, "Export", "匯出失敗")
                return
            QMessageBox.information(self, "Export", "匯出完成")
        except Exception:
            try:
                QMessageBox.information(self, "Export", "匯出失敗")
            except Exception:
                pass

    def on_table_cell_double_clicked(self, row, _column):
        # 處理表格中列的雙擊事件。
        #
        # 行為說明：
        # - 嘗試根據目前 tree 的選取節點與被雙擊的列來開啟或編輯對應的項目；
        # - 在精簡版中，僅以資訊視窗告知用戶，此功能為占位且未完整實作。
        #
        # 參數：
        # - row: 被雙擊的列索引
        # - _column: 被雙擊的欄位索引（目前未使用）
        # 嘗試開啟對應的樹狀項目編輯器（若可用）
        try:
            itm = self.tag_table.item(row, 0)
            if itm is not None:
                try:
                    tree_item = self._safe_data(itm, Qt.ItemDataRole.UserRole)
                except Exception:
                    tree_item = None
                if tree_item is not None:
                    # 轉交給既有的編輯函式處理
                    try:
                        self.on_edit_item(tree_item)
                        return
                    except Exception:
                        pass
        except Exception:
            pass

        QMessageBox.information(
            self,
            "Open",
            f"Double-click on row {row} (detail view not implemented in UI-only build)",
        )

    def on_table_context_menu(self, pos):
        # 在 `tag_table` 上顯示右鍵選單。
        #
        # 此實作為精簡版：顯示基本操作選項（Copy/Paste/Delete）。
        # 未來可依選取狀態啟用或停用某些項目。
        current_node = self.tree.currentItem()
        if not current_node:
            return

        # 根據目前 tree 選取節點，提供新增 (Add) 快捷項目於右側表格選單
        try:
            node_type = self._safe_data(current_node, 0, None)
        except Exception:
            node_type = None

        # 建構一個類似樹狀選單樣式的右鍵選單
        menu = QMenu()
        sel = self.tag_table.selectionModel().selectedRows()
        # 決定一個代表性的樹項（使用第一個選取的列）
        rep_tree_item = None
        if sel:
            try:
                first_row = sel[0].row()
                itm = self.tag_table.item(first_row, 0)
                if itm is not None:
                    rep_tree_item = self._safe_data(itm, Qt.ItemDataRole.UserRole)
            except Exception:
                rep_tree_item = None

        # 決定表格操作的實際目標：優先使用代表性選取的樹項
        effective_target = rep_tree_item or current_node

        # ------ 根據目前選取節點顯示新增項目（置頂） ------
        try:
            # 對於新增動作，始終使用樹的當前選取作為父節點。
            if node_type == "Connectivity":
                menu.addAction(
                    "➕ 新增 Channel",
                    lambda: self.tree.request_new_channel.emit(current_node),
                )
                menu.addSeparator()
            elif node_type == "Channel":
                menu.addAction(
                    "➕ 新增 Device",
                    lambda: self.tree.request_new_device.emit(current_node),
                )
                menu.addSeparator()
            elif node_type in ("Device", "Group"):
                menu.addAction(
                    "➕ 新增 Tag", lambda: self.tree.request_new_tag.emit(current_node)
                )
                menu.addSeparator()
        except Exception:
            pass

        # 剪下/複製 選取列：收集其對應的樹項並呼叫剪貼簿管理器
        def _collect_selected_tree_items():
            items = []
            if sel:
                try:
                    for s in sel:
                        row = s.row()
                        itm = self.tag_table.item(row, 0)
                        if itm is None:
                            continue
                        try:
                            tree_item = self._safe_data(itm, Qt.ItemDataRole.UserRole)
                        except Exception:
                            tree_item = None
                        if tree_item is not None:
                            items.append(tree_item)
                except Exception:
                    pass
            return items

        menu.addAction(
            "✂️ 剪下",
            lambda: (
                lambda l=_collect_selected_tree_items(): self.clipboard_manager.cut(l)
            )(),
        )
        menu.addAction(
            "📋 複製",
            lambda: (
                lambda l=_collect_selected_tree_items(): self.clipboard_manager.copy(l)
            )(),
        )

        # 貼上：目標為 effective_target（當表格有選取則優先使用代表性樹項）
        menu.addAction(
            "📥 貼上", lambda: self.tree.request_paste_item.emit(effective_target)
        )

        # 編輯 / 內容：為代表性樹項開啟內容編輯器
        if rep_tree_item is not None:
            menu.addAction(
                "✏️ 內容", lambda: self.tree.request_edit_item.emit(rep_tree_item)
            )
        else:
            menu.addAction("✏️ 內容")
        menu.addSeparator()

        # 刪除：重用現有的表格處理器以移除選取列
        menu.addAction("❌ 刪除", lambda: self.on_delete_selected_tags())

        menu.exec(self.tag_table.viewport().mapToGlobal(pos))

    def _on_monitor_value_context_menu(self, pos):
        # 監看表格（monitor_table）右鍵選單的處理函式。
        #
        # 功能:
        # 1. 寫入值 (如果標籤支援寫入)

        menu = QMenu()
        menu.addAction("✏️ 寫入值...", lambda: self._write_monitor_value(pos))
        menu.exec(self.monitor_table.viewport().mapToGlobal(pos))

    def _copy_monitor_value(self, pos):
        """複製監視表格的值"""
        try:
            index = self.monitor_table.indexAt(pos)
            if index.isValid():
                # 獲取值列 (通常是第3列)
                value_index = self.monitor_model.index(
                    index.row(), 2
                )  # Column 2 = Value
                value = self.monitor_model.data(value_index)

                from PyQt6.QtWidgets import QApplication

                QApplication.clipboard().setText(str(value))
                print(f"[UI] Copied value: {value}")
        except Exception as e:
            print(f"[ERROR] Failed to copy value: {e}")

    def _write_monitor_value(self, pos):
        """從監視表格寫入值"""
        try:
            index = self.monitor_table.indexAt(pos)
            if not index.isValid():
                QMessageBox.warning(self, "警告", "請選擇有效的行")
                return

            # 獲取該行的標籤資訊
            # 監視表格模型包含: TagPath, DataType, Access, Value, ...
            tag_path = self.monitor_model.data(self.monitor_model.index(index.row(), 0))
            current_value = self.monitor_model.data(
                self.monitor_model.index(index.row(), 3)
            )  # Column 3 = Value

            print(
                f"[DEBUG] Write value for tag_path: {tag_path}, current_value: {current_value}"
            )

            # 簡化方式：直接構造基本的標籤信息字典
            # 先從樹結構中查找完整的標籤數據
            tag_info = self._build_tag_info_from_tree(tag_path)
            if not tag_info:
                QMessageBox.warning(self, "錯誤", f"找不到標籤: {tag_path}")
                return

            print(f"[DEBUG] Found tag_info: {tag_info}")

            # 驗證標籤支援寫入
            read_write = tag_info.get("read_write", "Read Only")
            if "Read Only" in str(read_write):
                QMessageBox.warning(self, "警告", "此標籤為只讀，無法寫入！")
                return

            # 驗證函數碼支援寫入 (FC 5, 6, 15, 16)
            fc = tag_info.get("function_code")
            if fc not in (5, 6, 15, 16):
                QMessageBox.warning(self, "警告", f"函數碼 {fc} 不支援寫入")
                return

            # 打開寫值對話框
            dialog = WriteValueDialog(tag_info, current_value, self)
            print(f"[UI] WriteValueDialog created for tag: {tag_path}")

            # 連接信號
            def on_write_requested(addr, fc, val):
                print(
                    f"[UI] write_requested signal received: addr={addr}, fc={fc}, val={val}"
                )
                self._execute_tag_write(addr, fc, val, tag_info)
                # 在寫入完成後才關閉對話框
                dialog.accept()

            dialog.write_requested.connect(on_write_requested)
            print(f"[UI] Signal connected, showing dialog")
            dialog.exec()
            print(f"[UI] Dialog closed")

        except Exception as e:
            print(f"[ERROR] Failed to write value: {e}")
            import traceback

            traceback.print_exc()
            QMessageBox.critical(self, "錯誤", f"寫入失敗: {str(e)}")

    def _build_tag_info_from_tree(self, tag_path):
        """從樹結構中找標籤，返回完整的標籤信息字典"""
        try:
            if not tag_path:
                return None

            # 提取標籤名稱
            if GROUP_SEPARATOR in tag_path:
                parts = tag_path.split(GROUP_SEPARATOR)
                tag_name = parts[-1].strip()
                device_name = parts[1].strip() if len(parts) > 1 else None
            else:
                parts = tag_path.split("/")
                tag_name = parts[-1].strip()
                device_name = parts[1].strip() if len(parts) > 1 else None

            # 移除陣列索引
            import re

            tag_name_clean = re.sub(r"\s*\[\d+\]\s*$", "", tag_name).strip()

            print(
                f"[DEBUG] _build_tag_info_from_tree: looking for tag '{tag_name_clean}' in path '{tag_path}'"
            )

            # ===== 方式1：從項目配置中直接搜索 =====
            if hasattr(self, "project") and self.project and "channels" in self.project:
                for channel in self.project["channels"]:
                    if "children" not in channel:
                        continue
                    for device in channel["children"]:
                        if device.get("type") != "Device":
                            continue

                        device_id = device.get("general", {}).get("device_id")
                        data_access = device.get("data_access", {})

                        # 遞歸搜索此 device 下的所有 tag
                        def find_tag_recursive(item_list):
                            if not item_list:
                                return None
                            for item in item_list:
                                if (
                                    item.get("type") == "Tag"
                                    and item.get("text") == tag_name_clean
                                ):
                                    # 找到標籤 - 返回帶有設備信息的完整副本
                                    tag_data = dict(item)
                                    tag_data["device_id"] = device_id
                                    tag_data["device_data_access"] = data_access
                                    print(
                                        f"[DEBUG] Found tag in config: {tag_name_clean}, device_id={device_id}"
                                    )
                                    return tag_data

                                # 遞歸查看 Group
                                if item.get("type") == "Group" and "children" in item:
                                    result = find_tag_recursive(item["children"])
                                    if result:
                                        return result
                            return None

                        # 在此設備下查找標籤
                        tag_data = find_tag_recursive(device.get("children", []))
                        if tag_data:
                            return tag_data

            # ===== 方式2：從樹結構(UI)中直接搜索作為備選 =====
            print(f"[DEBUG] Not found in config, searching UI tree...")
            root = self.tree.invisibleRootItem()

            def search_tree_for_tag(node):
                """遍歷樹結構尋找標籤"""
                if not node:
                    return None

                # 檢查當前節點
                if node.data(0, Qt.ItemDataRole.UserRole) == "Tag":
                    node_text = node.text(0)
                    # 移除陣列索引進行比較
                    node_text_clean = re.sub(r"\s*\[\d+\]\s*$", "", node_text).strip()
                    if node_text_clean == tag_name_clean:
                        # 找到標籤節點，構建完整信息
                        # 使用正確的角色位置讀取地址和其他信息
                        tag_data = {
                            "text": node_text,
                            "type": "Tag",
                            "address": self._safe_data(node, 4, None),  # 地址在角色 4
                            "data_type": self._safe_data(
                                node, 2, "Word"
                            ),  # 數據類型在角色 2
                            "read_write": self._safe_data(
                                node, 3, "Read/Write"
                            ),  # 讀寫在角色 3
                            "function_code": 16,  # 默認值
                            "device_id": 1,  # 默認值
                            "device_data_access": {
                                "func_05": 1,
                                "func_06": 1,
                            },  # 默認值
                        }

                        print(
                            f"[DEBUG] Found tag in UI tree: {tag_name_clean}, address={tag_data.get('address')}, data_type={tag_data.get('data_type')}"
                        )

                        # 從樹中提取更多信息
                        parent = node.parent()
                        while parent and parent != root:
                            parent_text = parent.text(0)
                            parent_type = parent.data(0, Qt.ItemDataRole.UserRole)

                            if parent_type == "Device":
                                # 嘗試從設備節點提取 device_id
                                try:
                                    # 從項目配置中查找此設備
                                    if hasattr(self, "project") and self.project:
                                        for ch in self.project.get("channels", []):
                                            for dev in ch.get("children", []):
                                                if dev.get("text") == parent_text:
                                                    tag_data["device_id"] = dev.get(
                                                        "general", {}
                                                    ).get("device_id", 1)
                                                    tag_data["device_data_access"] = (
                                                        dev.get(
                                                            "data_access",
                                                            {
                                                                "func_05": 1,
                                                                "func_06": 1,
                                                            },
                                                        )
                                                    )
                                                    print(
                                                        f"[DEBUG] Updated device_id={tag_data['device_id']}, data_access={tag_data['device_data_access']}"
                                                    )
                                                    break
                                except:
                                    pass
                                break

                            parent = parent.parent()

                        return tag_data

                # 遞歸搜索子節點
                for i in range(node.childCount()):
                    result = search_tree_for_tag(node.child(i))
                    if result:
                        return result

                return None

            # 從根節點開始搜索
            for i in range(root.childCount()):
                result = search_tree_for_tag(root.child(i))
                if result:
                    return result

            print(f"[DEBUG] Tag '{tag_name_clean}' not found in config or UI tree")
            return None

        except Exception as e:
            print(f"[ERROR] Failed to build tag info from tree: {e}")
            import traceback

            traceback.print_exc()
            return None

    def _get_tag_info_by_name(self, tag_path):
        """從項目配置中找到標籤的詳細信息字典"""
        try:
            if not tag_path or not hasattr(self, "project") or not self.project:
                return None

            # 提取標籤名稱
            if GROUP_SEPARATOR in tag_path:
                parts = tag_path.split(GROUP_SEPARATOR)
                tag_name = parts[-1].strip()
            else:
                parts = tag_path.split("/")
                tag_name = parts[-1].strip()

            # 移除陣列索引
            import re

            tag_name_clean = re.sub(r"\s*\[\d+\]\s*$", "", tag_name).strip()

            print(f"[DEBUG] Looking for tag: {tag_name_clean}")

            # 遍歷項目中的所有 Channel、Device、Group 找標籤
            def search_in_config(item_list):
                if not item_list:
                    return None
                for item in item_list:
                    if item.get("type") == "Tag" and item.get("text") == tag_name_clean:
                        # 找到！返回完整的標籤數據
                        print(f"[DEBUG] Found tag in config: {item}")
                        return item
                    # 遞歸搜索子項
                    if "children" in item:
                        result = search_in_config(item["children"])
                        if result:
                            return result
                return None

            # 從 project 中的 channels 開始搜索
            if "channels" in self.project:
                for channel in self.project["channels"]:
                    if "children" in channel:
                        tag_data = search_in_config(channel["children"])
                        if tag_data:
                            return tag_data

            print(f"[DEBUG] Tag {tag_name_clean} not found in project config")
            return None
        except Exception as e:
            print(f"[ERROR] Failed to get tag info: {e}")
            import traceback

            traceback.print_exc()
            return None

    def _find_tag_in_tree_by_name(self, tag_path):
        """從樹形結構中找標籤，支援多種路徑格式"""
        try:
            if not tag_path:
                return None

            # 提取標籤名稱（路徑中的最後一部分）
            if GROUP_SEPARATOR in tag_path:
                tag_name = tag_path.split(GROUP_SEPARATOR)[-1]
            else:
                tag_name = tag_path.split("/")[-1]

            # 移除陣列索引 (例如 "Tag [0]" -> "Tag")
            import re

            tag_name_clean = re.sub(r"\s*\[\d+\]\s*$", "", tag_name).strip()

            print(f"[DEBUG] Searching for tag: {tag_name_clean} from path: {tag_path}")

            # 遞歸搜索整棵樹
            def search_tree(node):
                # 檢查當前節點
                try:
                    node_type = self._safe_data(node, 0, None)
                    node_text = node.text(0) if hasattr(node, "text") else ""

                    if node_type == "Tag" and node_text == tag_name_clean:
                        # 找到匹配的標籤
                        tag_data = node.data(0, Qt.ItemDataRole.UserRole)
                        print(
                            f"[DEBUG] Found tag node, tag_data type: {type(tag_data)}, value: {tag_data if not isinstance(tag_data, dict) else 'dict'}"
                        )

                        # 確保返回字典
                        if isinstance(tag_data, dict):
                            return tag_data
                        else:
                            print(
                                f"[ERROR] tag_data is not a dict, it's {type(tag_data)}"
                            )
                            return None
                except Exception as e:
                    print(f"[DEBUG] Error checking node: {e}")
                    pass

                # 搜索子節點
                for i in range(node.childCount()):
                    result = search_tree(node.child(i))
                    if result:
                        return result

                return None

            # 從根節點開始搜索
            root = self.tree.invisibleRootItem()
            for i in range(root.childCount()):
                result = search_tree(root.child(i))
                if result:
                    return result

            print(f"[DEBUG] Tag {tag_name_clean} not found in tree")
            return None
        except Exception as e:
            print(f"[ERROR] Failed to find tag in tree: {e}")
            import traceback

            traceback.print_exc()
            return None

    def _find_tag_by_path(self, tag_path):
        """根據 tag_path 找到 tag 項目"""
        try:
            # tag_path 格式可能是: "Channel1.Device1.Set.CT_Pri" (點分隔) 或 "Channel1/Device1/Set/CT_Pri" (斜線分隔)
            if not tag_path:
                return None

            # 從樹形結構中遍歷尋找
            def _search_tree(node, parts, idx=0):
                if idx >= len(parts):
                    return node.get("_tag_data") if hasattr(node, "get") else None

                node_name = node.text(0) if hasattr(node, "text") else None
                if node_name == parts[idx]:
                    if idx == len(parts) - 1:
                        # 到達目標 tag
                        return node.data(0, Qt.ItemDataRole.UserRole)

                    for i in range(node.childCount()):
                        result = _search_tree(node.child(i), parts, idx + 1)
                        if result:
                            return result

                return None

            # 支持點分隔 (監視表格格式) 或斜線分隔
            if GROUP_SEPARATOR in tag_path and "/" not in tag_path:
                parts = tag_path.split(GROUP_SEPARATOR)
            else:
                parts = tag_path.split("/")

            root = self.tree.invisibleRootItem()
            for i in range(root.childCount()):
                result = _search_tree(root.child(i), parts)
                if result:
                    return result

            return None
        except Exception as e:
            print(f"[ERROR] Failed to find tag by path: {e}")
            return None

    def _normalize_data_type(self, data_type_raw):
        """
        標準化 data_type 到標準 Modbus 資料型別

        映射規則:
        - 'float' → 'float32'
        - 'int' → 'int16'
        - 'uint', 'word' → 'uint16'
        - 'bool', 'coil' → 'bool'
        - 'double', 'float64' → 'float64'
        - 'long', 'int32' → 'int32'
        - 'ulong', 'uint32' → 'uint32'
        - 'int64' → 'int64'
        - 'uint64' → 'uint64'
        """
        if not isinstance(data_type_raw, str):
            return data_type_raw

        data_type_lower = data_type_raw.lower().strip()

        # Mapping of common type names to standard Modbus types
        type_map = {
            "float": "float32",
            "float32": "float32",
            "float64": "float64",
            "double": "float64",
            "int": "int16",
            "int16": "int16",
            "int32": "int32",
            "int64": "int64",
            "long": "int32",
            "uint": "uint16",
            "uint16": "uint16",
            "uint32": "uint32",
            "uint64": "uint64",
            "ulong": "uint32",
            "word": "uint16",
            "dword": "uint32",
            "qword": "uint64",
            "bool": "bool",
            "boolean": "bool",
            "coil": "bool",
            "bit": "bool",
        }

        normalized = type_map.get(data_type_lower, data_type_lower)

        if normalized != data_type_lower:
            return normalized
        return data_type_raw

    def _execute_tag_write(self, address, fc, value, tag_item):
        """執行標籤寫入操作"""
        print(
            f"[EXECUTE_WRITE] Called with: address={address}, fc={fc}, value={value}, tag_item keys={tag_item.keys() if isinstance(tag_item, dict) else 'N/A'}"
        )
        try:
            # 檢查 Runtime 狀態
            if not hasattr(self, "runtime_monitor") or not self.runtime_monitor:
                print(f"[ERROR] runtime_monitor not available")
                QMessageBox.warning(
                    self, "警告", "運行時監視未啟動\n請先點擊 'Runtime' 按鈕啟動"
                )
                return

            # 檢查 workers 是否已初始化（RuntimeMonitor 使用 _workers）
            workers = getattr(self.runtime_monitor, "_workers", {})
            print(f"[EXECUTE_WRITE] workers: {list(workers.keys())}")
            if not workers:
                print(f"[ERROR] No workers available")
                QMessageBox.warning(
                    self,
                    "警告",
                    "設備 worker 未初始化\n請確保 Runtime 已正確啟動並連接設備",
                )
                return

            print(f"[DEBUG] Available workers: {list(workers.keys())}")

            # 獲取設備 ID（可能是整數或字符串）
            device_id = tag_item.get("device_id")
            print(
                f"[DEBUG] device_id from tag_item: {device_id} (type: {type(device_id)})"
            )

            if not device_id:
                QMessageBox.warning(self, "警告", "找不到設備 ID")
                return

            # 從 _workers 字典中查找
            # workers 的鍵通常是 config_id 格式
            worker = None
            config_id = None

            # 嘗試各種可能的格式匹配
            for key, w in workers.items():
                # 檢查 worker 是否包含相關的設備信息
                # 鍵通常是 "Channel1-Device1" 之類的格式
                if f"-Device{device_id}" in str(key) or f"Device{device_id}" in str(
                    key
                ):
                    worker = w
                    config_id = key
                    print(f"[DEBUG] Found worker by config_id: {key}")
                    break
                elif f"-{device_id}" in str(key) or str(device_id) in str(key):
                    worker = w
                    config_id = key
                    print(f"[DEBUG] Found worker by device match: {key}")
                    break

            # 如果還是找不到，就用第一個 worker
            if not worker and workers:
                config_id = list(workers.keys())[0]
                worker = workers[config_id]
                print(f"[DEBUG] Using first available worker: {config_id}")

            if not worker:
                available_keys = list(workers.keys()) if workers else "無"
                QMessageBox.warning(
                    self, "警告", f"找不到設備的 worker\n可用設備: {available_keys}"
                )
                return

            print(f"[DEBUG] Found worker with config_id: {config_id}")

            # 根據設備設定和數據類型選擇合適的 function_code
            selected_fc = self._select_write_function_code(tag_item, value)
            print(f"[DEBUG] Selected FC: {selected_fc} (provided: {fc})")

            # 讀取設備的 DataEncoding 設定
            device_encoding = {}
            device_id_str = str(device_id)
            # 從 Project.json 讀取編碼設定
            try:
                import json
                import io

                project_path = (
                    getattr(self, "current_project_path", None) or "Project.json"
                )
                if not os.path.isabs(project_path):
                    project_path = os.path.join(os.path.dirname(__file__), project_path)
                if os.path.exists(project_path):
                    # 用 io.open 確保 UTF-8 編碼
                    with io.open(
                        project_path, "r", encoding="utf-8", errors="ignore"
                    ) as f:
                        project_data = json.load(f)

                    for channel in project_data.get("channels", []):
                        for device in channel.get("children", []):
                            if device.get("type") == "Device":
                                dev_id_cfg = device.get("general", {}).get("device_id")
                                if (
                                    str(dev_id_cfg) == device_id_str
                                    or dev_id_cfg == device_id
                                ):
                                    device_encoding = device.get("encoding", {})
                                    break
                        if device_encoding:
                            break
            except Exception as e:
                import traceback

                traceback.print_exc()

            # 如果找不到，使用預設值
            if not device_encoding:
                print(f"[WARNING] Device encoding not found, using defaults")
                device_encoding = {
                    "byte_order": 1,
                    "word_order": 1,
                    "dword_order": 1,
                    "bit_order": 0,
                }

            # 轉換編碼設定
            from core.modbus.modbus_mapping import map_endian_names_to_constants

            mapped_encoding = map_endian_names_to_constants(
                device_encoding.get("byte_order"),
                device_encoding.get("word_order"),
                device_encoding.get("bit_order"),
                device_encoding.get("dword_order"),
                device_encoding.get("treat_longs_as_decimals", False),
            )
            # 組合寫入資訊，包括數據型別和編碼設定，確保寫入時能正確使用 Modbus 資料編碼
            tag_info = {
                "address": address,
                "function_code": selected_fc,
                "name": tag_item.get("name", tag_item.get("text", "Unknown")),
                "device_id": device_id,
                "data_type": self._normalize_data_type(
                    tag_item.get("data_type", "uint16")
                ),
                "byte_order": tag_item.get("byte_order")
                or mapped_encoding.get("byte_order"),
                "word_order": tag_item.get("word_order")
                or mapped_encoding.get("word_order"),
                "dword_order": tag_item.get("dword_order")
                or mapped_encoding.get("dword_order"),
                "bit_order": tag_item.get("bit_order")
                or mapped_encoding.get("bit_order"),
                "treat_longs_as_decimals": tag_item.get(
                    "treat_longs_as_decimals", False
                )
                or mapped_encoding.get("treat_longs_as_decimals", False),
            }

            success = worker._write_queue.enqueue(address, selected_fc, value, tag_info)

            if success:
                fc_name = {
                    5: "FC 5 (寫單一線圈)",
                    6: "FC 6 (寫單一暫存器)",
                    15: "FC 15 (寫多個線圈)",
                    16: "FC 16 (寫多個暫存器)",
                }.get(selected_fc, f"FC {selected_fc}")

                QMessageBox.information(
                    self,
                    "成功",
                    f"寫入請求已加入隊列\n地址: {address}\n值: {value}\n方式: {fc_name}\n\n將在讀取 {worker.duty_cycle_ratio} 次後執行",
                )
                print(
                    f"[UI] Write enqueued: addr={address} fc={selected_fc} value={value}"
                )
            else:
                QMessageBox.warning(self, "警告", "寫入隊列已滿，請稍後重試")

        except Exception as e:
            print(f"[ERROR] Failed to execute write: {e}")
            import traceback

            traceback.print_exc()
            QMessageBox.critical(self, "錯誤", f"執行寫入失敗: {str(e)}")

    def _select_write_function_code(self, tag_item, value):
        """根據地址範圍、設備設定和數據類型選擇合適的 function_code

        邏輯:
        - 0xxxx (Coils): func_05==1 → FC5, else → FC15
        - 4xxxx (Holding Registers):
            - 如果是 float/long/dword/double → FC16
            - 否則: func_06==1 → FC6, else → FC16

        注意：Modbus 地址是 1-based 的（400005 表示第 5 個 holding register），
        所以需要提取地址的前4位數字來判斷類型。
        """
        try:
            # 獲取設備的數據訪問設定
            data_access = tag_item.get("device_data_access", {})
            address = tag_item.get("address")
            data_type = str(tag_item.get("data_type", "Word")).lower()

            print(f"[DEBUG] tag_item keys: {tag_item.keys()}")
            print(f"[DEBUG] address: {address} (type: {type(address)})")
            print(f"[DEBUG] data_type: {data_type}")
            print(f"[DEBUG] device_data_access: {data_access}")

            # 讀取 func_05 和 func_06 設定（可能是 0/1 或 True/False）
            func_05_raw = data_access.get("func_05")
            func_06_raw = data_access.get("func_06")

            print(f"[DEBUG] func_05_raw: {func_05_raw} (type: {type(func_05_raw)})")
            print(f"[DEBUG] func_06_raw: {func_06_raw} (type: {type(func_06_raw)})")

            # 轉換為布爾值 (1/True 表示啟用，0/False/None 表示停用)
            func_05_enabled = func_05_raw in (1, True, "1", "true")
            func_06_enabled = func_06_raw in (1, True, "1", "true")

            print(
                f"[DEBUG] func_05_enabled: {func_05_enabled}, func_06_enabled: {func_06_enabled}"
            )

            # 判斷數據類型是否需要多個 register
            needs_multiple = (
                "float" in data_type
                or "double" in data_type
                or "long" in data_type
                or "dword" in data_type
            )

            print(f"[DEBUG] needs_multiple: {needs_multiple}")

            # 將 address 轉為整數以判斷地址範圍
            # Modbus 地址是 1-based 的 (400005 表示第 5 個 holding register)
            # 提取地址的前 4 位數字來判斷類型（0xxxx 或 4xxxx）
            try:
                addr_str = str(address).strip() if address else "0"
                # 提取前 4 位數字（如果地址是 400005，取 "4000"；如果是 5，取 "5"）
                addr_prefix = addr_str[:4] if len(addr_str) >= 4 else addr_str
                addr_category = int(
                    addr_prefix
                )  # 如果是 "0005" 會變 5，如果是 "4000" 會變 4000
                print(
                    f"[DEBUG] addr_str: {addr_str}, addr_prefix: {addr_prefix}, addr_category: {addr_category}"
                )
            except (ValueError, TypeError) as e:
                print(f"[DEBUG] Failed to parse address: {e}, defaulting to 0")
                addr_category = 0

            # 根據地址類型選擇（Modbus 1-based 編碼）：
            # 00001-09999 = Coils (0xxxx)
            # 10001-19999 = Discrete Inputs (1xxxx)
            # 30001-39999 = Input Registers (3xxxx)
            # 40001-49999 = Holding Registers (4xxxx)

            if addr_category < 1000:  # 0xxxx range (Coils)
                # Coil 範圍 (0xxxx)
                if func_05_enabled:
                    print(f"[DEBUG] Selecting FC 5 (Coil, func_05 enabled)")
                    return 5
                else:
                    print(f"[DEBUG] Selecting FC 15 (Coil, func_05 disabled)")
                    return 15
            else:  # 4xxxx range (Holding Registers)
                # Holding Register 範圍 (4xxxx)
                # 先檢查數據類型
                if needs_multiple:
                    print(
                        f"[DEBUG] Selecting FC 16 (multiple registers needed for {data_type})"
                    )
                    return 16

                # 再檢查 func_06 設定
                if func_06_enabled:
                    print(f"[DEBUG] Selecting FC 6 (Holding Register, func_06 enabled)")
                    return 6
                else:
                    print(
                        f"[DEBUG] Selecting FC 16 (Holding Register, func_06 disabled)"
                    )
                    return 16

        except Exception as e:
            print(f"[ERROR] Failed to select FC: {e}")
            import traceback

            traceback.print_exc()
            return 16  # 默認使用 FC 16

    def on_delete_selected_tags(self):
        # 處理在 `tag_table` 中選取多列後的批次刪除動作。
        #
        # 行為：
        # - 以確認對話方塊詢問使用者；
        # - 若確認，依選取的列從 tree 中移除相對應的 Tag 節點；
        # - 刪除後刷新右側表格，並呼叫 `_on_project_structure_changed` hook。
        current_node = self.tree.currentItem()
        if not current_node:
            return
        indices = self.tag_table.selectionModel().selectedRows()
        if not indices:
            return
        if (
            QMessageBox.question(
                self, "Delete", f"確定刪除選中的 {len(indices)} 個項目?"
            )
            == QMessageBox.StandardButton.Yes
        ):
            try:
                rows = sorted([r.row() for r in indices], reverse=True)
                for r in rows:
                    try:
                        itm = self.tag_table.item(r, 0)
                        if itm is None:
                            continue
                        tag_tree_item = None
                        try:
                            tag_tree_item = self._safe_data(
                                itm, Qt.ItemDataRole.UserRole
                            )
                        except Exception:
                            tag_tree_item = None
                        if tag_tree_item is not None:
                            try:
                                parent = (
                                    tag_tree_item.parent()
                                    or self.tree.invisibleRootItem()
                                )
                                parent.removeChild(tag_tree_item)
                            except Exception:
                                pass
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                self.update_right_table(current_node, 0)
            except Exception:
                pass
            try:
                if hasattr(self, "_on_project_structure_changed"):
                    try:
                        self._on_project_structure_changed()
                    except Exception:
                        pass
            except Exception:
                pass

    # --- Keyboard shortcut helpers ---
    def _collect_selected_tree_items(self):
        items = []
        try:
            sel = self.tag_table.selectionModel().selectedRows()
            for s in sel:
                try:
                    row = s.row()
                    itm = self.tag_table.item(row, 0)
                    if itm is None:
                        continue
                    tree_item = None
                    try:
                        tree_item = self._safe_data(itm, Qt.ItemDataRole.UserRole)
                    except Exception:
                        tree_item = None
                    if tree_item is not None:
                        items.append(tree_item)
                except Exception:
                    pass
        except Exception:
            pass
        return items

    def _handle_shortcut(self, action):
        # 判斷焦點所在：樹或表格
        fw = self.focusWidget()
        # 若焦點在 tag_table 或其子元件，對選取列執行相應操作
        is_table = fw is self.tag_table or (
            hasattr(fw, "parent") and getattr(fw, "parent") is self.tag_table
        )
        if is_table:
            if action == "copy":
                items = self._collect_selected_tree_items()
                if items:
                    self.clipboard_manager.copy(items if len(items) > 1 else items[0])
            elif action == "cut":
                items = self._collect_selected_tree_items()
                if items:
                    parent = self.clipboard_manager.cut(
                        items if len(items) > 1 else items[0]
                    )
                    if parent:
                        try:
                            self.update_right_table(parent, 0)
                        except Exception:
                            pass
            elif action == "paste":
                target = self.tree.currentItem()
                if target:
                    parent = self.clipboard_manager.paste(target)
                    if parent:
                        try:
                            self.update_right_table(parent, 0)
                        except Exception:
                            pass
            return

        # 否則（焦點在樹或其它處），對目前樹選項執行操作
        cur = self.tree.currentItem()
        if cur is None:
            return
        if action == "copy":
            try:
                self.on_copy_item(cur)
            except Exception:
                pass
        elif action == "cut":
            try:
                self.on_cut_item(cur)
            except Exception:
                pass
        elif action == "paste":
            try:
                self.on_paste_item(cur)
            except Exception:
                pass

    def _handle_delete_shortcut(self):
        fw = self.focusWidget()
        is_table = fw is self.tag_table or (
            hasattr(fw, "parent") and getattr(fw, "parent") is self.tag_table
        )
        if is_table:
            try:
                self.on_delete_selected_tags()
            except Exception:
                pass
            return
        cur = self.tree.currentItem()
        if cur is None:
            return
        try:
            self.on_delete_item(cur)
        except Exception:
            pass

    def on_new_channel(self, parent_item):
        # 建立新的 Channel 節點。
        #
        # 參數：
        # - parent_item: 父節點（通常為 Connectivity 根節點）。
        #
        # 行為：
        # - 顯示 `ChannelDialog` 以取得使用者輸入的欄位；
        # - 呼叫 `self.controller.save_channel` 儲存設定並在 tree 上新增節點；
        # - 展開父節點並刷新右側表格。
        # 建議名稱改為掃描現有 channel 名稱的最大編號並 +1，避免使用簡單 childCount() 導致重複/錯誤編號
        try:
            import re

            existing_names = [
                parent_item.child(i).text(0) for i in range(parent_item.childCount())
            ]
            nums = []
            for n in existing_names:
                try:
                    m = re.match(r"Channel(\d+)$", n)
                    if m:
                        nums.append(int(m.group(1)))
                except Exception:
                    pass
            if nums:
                next_idx = max(nums) + 1
            else:
                next_idx = parent_item.childCount() + 1
            suggested = f"Channel{next_idx}"
        except Exception:
            suggested = f"Channel{parent_item.childCount() + 1}"
        dialog = ChannelDialog(self, suggested_name=suggested)
        if dialog.exec():
            data = dialog.get_data()
            try:
                # normalize channel data before saving/creating the tree node
                if getattr(self, "controller", None):
                    try:
                        res = self._call_controller("normalize_channel", data, None)
                        if res is not None:
                            data = res
                    except Exception:
                        pass
            except Exception:
                pass
            new_item = QTreeWidgetItem(parent_item)
            new_item.setData(0, Qt.ItemDataRole.UserRole, "Channel")
            self._call_controller("save_channel", new_item, data)
            parent_item.setExpanded(True)
            # 確保在建立 Channel 後更新 temp.json，以便啟動時能載入完整資料
            try:
                if getattr(self, "_temp_json", None):
                    QTimer.singleShot(
                        50,
                        lambda: self._call_controller(
                            "export_project_to_json", self._temp_json
                        ),
                    )
            except Exception:
                pass
            try:
                # select and show the newly created channel after event loop settles
                try:
                    QTimer.singleShot(
                        150,
                        lambda ni=new_item: (
                            self.tree.setCurrentItem(ni),
                            self.update_right_table(ni, 0),
                        ),
                    )
                except Exception:
                    pass
            except Exception:
                try:
                    self.update_right_table(parent_item, 0)
                except Exception:
                    pass

    def on_new_device(self, channel_item):
        # 在指定的 Channel 底下建立新的 Device。
        #
        # 流程：
        # - 由 `AppController.calculate_next_id` 取得預設 device id；
        # - 顯示 `DeviceDialog` 供使用者調整參數；
        # - 呼叫 `self.controller.save_device` 並刷新 UI。
        # Channel now stores driver in role2 (and role9 for full dict); fall back to role9.type
        try:
            pdrv = self._safe_data(channel_item, 2, None)
            if isinstance(pdrv, dict):
                driver_name = pdrv.get("type") or None
            else:
                driver_name = pdrv
            if not driver_name:
                pr9 = self._safe_data(channel_item, 9, None)
                if isinstance(pr9, dict):
                    driver_name = pr9.get("type")
        except Exception:
            driver_name = None
        driver_name = driver_name or "Modbus RTU Serial"
        next_id = self._call_controller("calculate_next_id", channel_item) or 1
        # 建議名稱改為掃描現有 device 名稱的最大編號並 +1，避免使用 childCount() 導致重複
        try:
            import re

            existing_names = [
                channel_item.child(i).text(0) for i in range(channel_item.childCount())
            ]
            nums = []
            for n in existing_names:
                try:
                    m = re.match(r"Device(\d+)$", n)
                    if m:
                        nums.append(int(m.group(1)))
                except Exception:
                    pass
            if nums:
                next_idx = max(nums) + 1
            else:
                next_idx = channel_item.childCount() + 1
            suggested_name = f"Device{next_idx}"
        except Exception:
            suggested_name = f"Device{channel_item.childCount() + 1}"
        dialog = DeviceDialog(
            self, suggested_name=suggested_name, driver_type=driver_name
        )
        # load_data 接受巢狀的 'general' 結構；為一致性使用巢狀格式
        dialog.load_data(
            {
                "general": {
                    "name": suggested_name,
                    "device_id": next_id,
                    "description": "",
                }
            }
        )
        if dialog.exec():
            data = dialog.get_data()
            new_item = QTreeWidgetItem(channel_item)
            new_item.setData(0, Qt.ItemDataRole.UserRole, "Device")
            self._call_controller("save_device", new_item, data)
            channel_item.setExpanded(True)
            # show the new device's Tag view on the right (not the channel device list)
            try:
                # select and show the new device after event loop settles
                try:
                    QTimer.singleShot(
                        150,
                        lambda ni=new_item: (
                            self.tree.setCurrentItem(ni),
                            self.update_right_table(ni, 0),
                        ),
                    )
                except Exception:
                    pass
            except Exception:
                try:
                    self.update_right_table(channel_item, 0)
                except Exception:
                    pass

    def on_new_group(self, parent_item):
        # 透過對話方塊建立位於 parent_item 底下的 Group（名稱/描述）
        group_count = sum(
            1
            for i in range(parent_item.childCount())
            if self._safe_data(parent_item.child(i), 0, None) == "Group"
        )
        suggested_name = f"Group{group_count + 1}"
        from ui.dialogs.group_dialog import GroupDialog

        dlg = GroupDialog(self, suggested_name=suggested_name)
        dlg.load_data({"general": {"name": suggested_name, "description": ""}})
        try:
            _prev_sizes = list(self.splitter.sizes())
        except Exception:
            _prev_sizes = None
        # log sizes before opening dialog
        try:
            self._log_splitter_sizes("on_new_group BEFORE")
        except Exception:
            pass
        try:
            self._log_row_heights("on_new_group BEFORE")
        except Exception:
            pass
        if dlg.exec():
            data = dlg.get_data()
            new_item = QTreeWidgetItem(parent_item)
            new_item.setData(0, Qt.ItemDataRole.UserRole, "Group")
            try:
                if getattr(self, "controller", None):
                    self._call_controller("save_group", new_item, data)
                else:
                    new_item.setText(
                        0,
                        (data.get("general") or {}).get("name")
                        or data.get("name")
                        or suggested_name,
                    )
                    new_item.setData(
                        1,
                        Qt.ItemDataRole.UserRole,
                        (data.get("general") or {}).get("description")
                        or data.get("description")
                        or "",
                    )
            except Exception:
                try:
                    new_item.setText(
                        0,
                        (data.get("general") or {}).get("name")
                        or data.get("name")
                        or suggested_name,
                    )
                    new_item.setData(
                        1,
                        Qt.ItemDataRole.UserRole,
                        (data.get("general") or {}).get("description")
                        or data.get("description")
                        or "",
                    )
                except Exception:
                    pass
            parent_item.setExpanded(True)
            self.update_right_table(parent_item, 0)
            parent_item.setExpanded(True)
            self.update_right_table(parent_item, 0)
        try:
            if _prev_sizes is not None:
                self._set_splitter_sizes(_prev_sizes, reason="restore_on_edit_group")
        except Exception:
            pass
        try:
            self._log_row_heights("on_new_group AFTER")
        except Exception:
            pass

    def on_new_tag(self, parent_item):
        # 在 parent_item 下新增一個 Tag 節點（含名稱與地址檢查）。
        #
        # 流程：
        # - 收集 parent_item 下現有 Tag 名稱與地址以避免重複；
        # - 使用 `TagDialog` 讓使用者填寫/確認欄位；
        # - 驗證名稱與地址不重複後呼叫 `self.controller.save_tag`，並刷新 UI。
        try:
            try:
                ptype = (
                    self._safe_data(parent_item, 0, None)
                    if parent_item is not None
                    else None
                )
                ptext = parent_item.text(0) if parent_item is not None else None
                # debug log removed
            except Exception:
                pass
        except Exception:
            pass

        existing_tags = [
            parent_item.child(i)
            for i in range(parent_item.childCount())
            if self._safe_data(parent_item.child(i), 0, None) == "Tag"
        ]
        used_names = [t.text(0) for t in existing_tags]
        used_addresses = [self._safe_data(t, 4, None) for t in existing_tags]
        next_idx = 1
        while f"Tag{next_idx}" in used_names:
            next_idx += 1
        try:
            self._log_splitter_sizes("on_new_tag BEFORE")
        except Exception:
            pass
        suggested_name = f"Tag{next_idx}"
        # 如 controller 可用，建議下一個位址
        suggested_addr = None
        try:
            if getattr(self, "controller", None):
                try:
                    # Determine prefix based on access type
                    # Default: Boolean -> "0" (Read/Write) or "1" (Read Only)
                    #         Others -> "4" (Read/Write) or "3" (Read Only)
                    prefix = "4"  # Default for Word/DWord etc.
                    new_type = "Word"  # Default type
                    res = self._call_controller(
                        "calculate_next_address",
                        parent_item,
                        prefix=prefix,
                        new_type=new_type,
                    )
                    suggested_addr = res if res is not None else None
                except Exception:
                    suggested_addr = None
        except Exception:
            suggested_addr = None

        # Let TagDialog use its default suggested_addr if calculation fails
        # (TagDialog.__init__ already has suggested_addr="400000" as default)

        try:
            from ui.dialogs.tag_dialog import TagDialog
            from PyQt6.QtWidgets import QMessageBox

            dlg = TagDialog(
                self,
                suggested_name=suggested_name,
                suggested_addr=suggested_addr,
                target_item=parent_item,
                is_new=True,
            )
            # 載入最小預設值
            try:
                dlg.load_data(
                    {
                        "general": {
                            "name": suggested_name,
                            "address": suggested_addr or "",
                            "data_type": "Word",
                            "description": "",
                        },
                        "scaling": {"type": "None"},
                    }
                )
            except Exception:
                pass
            if dlg.exec():
                new_data = dlg.get_data()
                # 驗證是否有重複項目
                new_name = (new_data.get("general") or {}).get("name") or new_data.get(
                    "name"
                )
                new_addr = (new_data.get("general") or {}).get(
                    "address"
                ) or new_data.get("address")
                if new_name in used_names:
                    QMessageBox.warning(
                        self, "新增 Tag 失敗", f"名稱 {new_name} 已存在。"
                    )
                    return
                if new_addr in used_addresses:
                    QMessageBox.warning(
                        self, "新增 Tag 失敗", f"位址 {new_addr} 已存在。"
                    )
                    return

                new_item = QTreeWidgetItem(parent_item)
                new_item.setData(0, Qt.ItemDataRole.UserRole, "Tag")
                new_item.setHidden(True)
                try:
                    if getattr(self, "controller", None):
                        self._call_controller("save_tag", new_item, new_data)
                    else:
                        # 復原機制：設定文字與部分 data role
                        try:
                            new_item.setText(
                                0,
                                (new_data.get("general") or {}).get("name")
                                or new_data.get("name")
                                or suggested_name,
                            )
                        except Exception:
                            pass
                        try:
                            new_item.setData(
                                1,
                                Qt.ItemDataRole.UserRole,
                                (new_data.get("general") or {}).get("address")
                                or new_data.get("address"),
                            )
                        except Exception:
                            pass
                except Exception:
                    try:
                        new_item.setText(
                            0,
                            (new_data.get("general") or {}).get("name")
                            or new_data.get("name")
                            or suggested_name,
                        )
                    except Exception:
                        pass
                parent_item.setExpanded(True)
                try:
                    self.update_right_table(parent_item, 0)
                except Exception:
                    pass
                # 建立後確保左側樹狀顯示隱藏 Tag 節點
                try:
                    if getattr(self, "tree", None) and hasattr(
                        self.tree, "hide_all_tags"
                    ):
                        try:
                            self.tree.hide_all_tags()
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            return

    def on_show_content(self, item):
        # 處理顯示樹項內容的請求（例如 Group 雙擊）
        try:
            if item is None:
                return
            try:
                # 確保樹狀的選取反映此項目
                self.tree.setCurrentItem(item)
            except Exception:
                pass
            try:
                # 明確更新右側內容
                self.update_right_table(item, 0)
                # 若為 Group 雙擊，作為備援打開 modal 的 GroupDialog
                try:
                    node_type = self._safe_data(item, 0, None)
                except Exception:
                    node_type = None
                if node_type == "Group":
                    try:
                        from ui.dialogs.group_dialog import GroupDialog

                        try:
                            data = {
                                "general": {
                                    "name": item.text(0) or "",
                                    "description": self._safe_data(item, 3, "") or "",
                                }
                            }
                        except Exception:
                            data = {
                                "general": {
                                    "name": item.text(0) or "",
                                    "description": "",
                                }
                            }
                        suggested = (
                            (data.get("general") or {}).get("name")
                            or data.get("name")
                            or "Group1"
                        )
                        dlg = GroupDialog(self, suggested_name=suggested)
                        try:
                            dlg.load_data(data)
                        except Exception:
                            pass
                        try:
                            _prev_sizes = list(self.splitter.sizes())
                        except Exception:
                            _prev_sizes = None
                        try:
                            self._log_splitter_sizes("on_show_content BEFORE")
                        except Exception:
                            pass
                        try:
                            self._log_row_heights("on_show_content BEFORE")
                        except Exception:
                            pass
                        if dlg.exec():
                            new = dlg.get_data()
                            try:
                                if getattr(self, "controller", None):
                                    self._call_controller("save_group", item, new)
                                else:
                                    item.setText(0, new.get("name") or item.text(0))
                                    item.setData(
                                        1,
                                        Qt.ItemDataRole.UserRole,
                                        new.get("description") or "",
                                    )
                            except Exception:
                                pass
                            try:
                                self.update_right_table(item, 0)
                            except Exception:
                                pass
                            try:
                                if _prev_sizes is not None and not getattr(
                                    self, "_splitter_user_moved", False
                                ):
                                    self._set_splitter_sizes(
                                        _prev_sizes, reason="restore_on_show_content"
                                    )
                            except Exception:
                                pass
                        try:
                            self._log_splitter_sizes("on_show_content AFTER_RESTORE")
                        except Exception:
                            pass
                        try:
                            self._log_row_heights("on_show_content AFTER")
                        except Exception:
                            pass
                    except Exception:
                        pass
            except Exception:
                pass
            # 強制重繪/更新表格，讓 UI 立即反映變更
            try:
                # 確保 splitter 為右側面板分配可見寬度
                try:
                    # 只在第一次更新時設定初始 splitter 大小，避免每次操作都移動隔線
                    if not getattr(self, "_splitter_initialized", False):
                        # 如果視窗尚未被顯示或寬度為預設小值（如 <300），
                        # 改用螢幕可用寬度來計算初始 splitter sizes
                        total_w = self.width()
                        try:
                            if not total_w or total_w < 300:
                                screen = QApplication.primaryScreen()
                                if screen is None:
                                    scrs = QApplication.screens()
                                    screen = scrs[0] if scrs else None
                                if screen:
                                    total_w = screen.availableGeometry().width()
                        except Exception:
                            pass
                        # 設定初始為左右對半（但保留最小寬度保障）
                        half = int(total_w) // 2
                        left_w = max(150, half)
                        right_w = max(150, int(total_w) - left_w)
                        try:
                            self._set_splitter_sizes(
                                [left_w, right_w],
                                reason="update_right_table_initial_half",
                            )
                        except Exception:
                            pass
                        self._splitter_initialized = True
                except Exception:
                    pass
                self.tag_table.show()
                self.tag_table.repaint()
                try:
                    self.tag_table.viewport().update()
                except Exception:
                    pass
                try:
                    # 確保欄調整大小以顯示內容；僅在啟動時調整列高一次，避免對話窗關閉時改變高度
                    # 調整欄寬以顯示內容（列高固定化改為啟動後一次性設定）
                    self.tag_table.resizeColumnsToContents()
                except Exception:
                    pass
                try:
                    # 選取第一個值的 cell 以觸發可見焦點
                    if (
                        self.tag_table.rowCount() > 0
                        and self.tag_table.columnCount() > 1
                    ):
                        self.tag_table.setCurrentCell(0, 1)
                except Exception:
                    pass
                try:
                    # 將主視窗置頂並確保其擁有焦點
                    try:
                        self.raise_()
                        self.activateWindow()
                    except Exception:
                        pass
                    try:
                        self.tag_table.raise_()
                        self.tag_table.setFocus()
                    except Exception:
                        pass
                except Exception:
                    pass
            except Exception:
                pass
        except Exception:
            pass

    def update_right_table(self, item, _column=0):
        # 根據目前 `item`（樹狀節點）更新右側 `tag_table` 的顯示內容。
        #
        # 根據節點類型（Connectivity / Channel / Device / Group）顯示不同的欄位與內容。
        #
        # 參數：
        # - item: QTreeWidgetItem 要顯示的節點
        # - _column: 觸發欄位（未使用）
        # 除錯：記錄呼叫來源與項目資訊
        try:
            it_name = item.text(0) if item is not None else None
            it_type = self._safe_data(item, 0, None)
        except Exception:
            it_name = None
            it_type = None
        # 在填充表格時，確保群組專用的 cell handler 已斷開連接
        try:
            try:
                self.tag_table.cellChanged.disconnect(self._on_group_cell_changed)
            except Exception:
                pass
            self._current_group_item = None
        except Exception:
            pass

        if not item:
            return
        node_type = self._safe_data(item, 0, None)
        self.tag_table.setRowCount(0)
        if node_type == "Connectivity":
            self._setup_table(
                [
                    "Channel Name",
                    "Driver",
                    "Connection",
                    "Parameters",
                ]
            )
            # 除錯：啟動時列印子節點的 role 值到 stdout，以協助診斷遺漏欄位
            debug_startup = hasattr(self, "tree") and item is getattr(
                self.tree, "conn_node", None
            )
            for i in range(item.childCount()):
                child = item.child(i)
                self._append_channel_row(self.tag_table, child)
        elif node_type == "Group":
            # 顯示 Group 的完整 Tag 表格，與 Device 視圖採用相同欄位
            self._setup_table(
                [
                    "Tag Name",
                    "Data Type",
                    "Client Access",
                    "Address",
                    "Scan Rate",
                    "Scale Type",
                    "RL",
                    "RH",
                    "S DataType",
                    "SL",
                    "SH",
                    "CL",
                    "CH",
                    "Units",
                ]
            )
            source = item
            for i in range(source.childCount()):
                child = source.child(i)
                if self._safe_data(child, 0) != "Tag":
                    continue
                self._append_tag_row(self.tag_table, child)
        elif node_type == "Channel":
            # 顯示 Device 列表，包含簡潔的 Timing 與子頁摘要
            headers = [
                "Device Name",
                "Unit ID",
                "Timing",
                "DataAccess",
                "DataEncoding",
                "Block Sizes",
            ]
            self._setup_table(headers)
            for i in range(item.childCount()):
                child = item.child(i)
                if self._safe_data(child, 0, None) != "Device":
                    continue
                row = self.tag_table.rowCount()
                self.tag_table.insertRow(row)
                # 裝置名稱
                it0 = QTableWidgetItem(child.text(0))
                try:
                    it0.setData(Qt.ItemDataRole.UserRole, child)
                except Exception:
                    pass
                it0.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 0, it0)
                # 裝置 ID（role 2）
                it1 = QTableWidgetItem(str(self._safe_data(child, 2, "") or ""))
                it1.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 1, it1)

                # Timing 欄位順序：req_timeout, attempts, inter_req_delay
                try:
                    timing = self._safe_data(child, 3, {}) or {}
                    # determine channel driver type to decide which timing fields to show
                    try:
                        drv_val = self._safe_data(item, 2, None)
                        if isinstance(drv_val, dict):
                            drv_type = str(drv_val.get("type") or "")
                        else:
                            drv_type = str(drv_val or "")
                    except Exception:
                        drv_type = ""
                    drv_type = (drv_type or "").lower()

                    if isinstance(timing, dict):
                        if "rtu over tcp" in drv_type:
                            vals = [
                                timing.get("connect_timeout")
                                or timing.get("req_timeout")
                                or "",
                                timing.get("connect_attempts")
                                or timing.get("attempts")
                                or timing.get("attempts_before_timeout")
                                or "",
                                timing.get("request_timeout")
                                or timing.get("req_timeout")
                                or "",
                                timing.get("attempts_before_timeout")
                                or timing.get("attempts")
                                or "",
                                timing.get("inter_request_delay")
                                or timing.get("inter_req_delay")
                                or "",
                            ]
                        elif "tcp" in drv_type and (
                            "ethernet" in drv_type or "modbus tcp" in drv_type
                        ):
                            vals = [
                                timing.get("connect_timeout")
                                or timing.get("req_timeout")
                                or "",
                                timing.get("request_timeout")
                                or timing.get("req_timeout")
                                or "",
                                timing.get("attempts_before_timeout")
                                or timing.get("attempts")
                                or "",
                                timing.get("inter_request_delay")
                                or timing.get("inter_req_delay")
                                or "",
                            ]
                        else:
                            # serial or unknown: show request_timeout, attempts, inter_req_delay
                            vals = [
                                timing.get("request_timeout")
                                or timing.get("req_timeout")
                                or "",
                                timing.get("attempts_before_timeout")
                                or timing.get("attempts")
                                or "",
                                timing.get("inter_request_delay")
                                or timing.get("inter_req_delay")
                                or "",
                            ]
                        timing_str = ",".join(
                            [str(x) for x in vals if x is not None and str(x) != ""]
                        )
                    else:
                        timing_str = str(timing or "")
                except Exception:
                    timing_str = ""
                it2 = QTableWidgetItem(timing_str)
                it2.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 2, it2)

                # DataAccess（role5）：以預設順序顯示欄位值
                try:
                    access = self._safe_data(child, 4, {}) or {}
                    if isinstance(access, dict):
                        access_keys = [
                            "zero_based",
                            "zero_based_bit",
                            "bit_writes",
                            "func_06",
                            "func_05",
                        ]
                        vals = []
                        for k in access_keys:
                            if k in access:
                                try:
                                    nv = to_numeric_flag(access.get(k))
                                    vals.append(str(nv))
                                except Exception:
                                    vals.append(str(access.get(k)))
                        access_str = ",".join(vals)
                    else:
                        access_str = str(access or "")
                except Exception:
                    access_str = ""
                it4 = QTableWidgetItem(access_str)
                it4.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 3, it4)

                # DataEncoding（role6）：僅顯示其值
                try:
                    enc = self._safe_data(child, 5, {}) or {}
                    if isinstance(enc, dict):
                        vals = []
                        for v in enc.values():
                            if v is not None:
                                try:
                                    nv = to_numeric_flag(v)
                                    vals.append(str(nv))
                                except Exception:
                                    vals.append(str(v))
                        enc_str = ",".join(vals)
                    else:
                        enc_str = str(enc or "")
                except Exception:
                    enc_str = ""
                it5 = QTableWidgetItem(enc_str)
                it5.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 4, it5)

                # Block Sizes（role7）：以常用順序僅顯示數值
                try:
                    blocks = self._safe_data(child, 6, {}) or {}
                    if isinstance(blocks, dict):
                        block_keys = ["out_coils", "in_coils", "int_regs", "hold_regs"]
                        block_vals = [
                            str(blocks.get(k))
                            for k in block_keys
                            if blocks.get(k) is not None
                        ]
                        blocks_str = ",".join(block_vals)
                    else:
                        blocks_str = str(blocks or "")
                except Exception:
                    blocks_str = ""
                it6 = QTableWidgetItem(blocks_str)
                it6.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tag_table.setItem(row, 5, it6)
        elif node_type == "Device":
            # 遵循 TagDialog 的欄位順序（不包含 Description）：
            # 名稱、資料型態、Client 存取、位址、掃描速率、縮放類型、
            # RL、RH、縮放後資料型態、SL、SH、CL、CH、單位
            self._setup_table(
                [
                    "Tag Name",
                    "Data Type",
                    "Client Access",
                    "Address",
                    "Scan Rate",
                    "Scale Type",
                    "RL",
                    "RH",
                    "S DataType",
                    "SL",
                    "SH",
                    "CL",
                    "CH",
                    "Units",
                ]
            )
            source = item
            # 只顯示直接為 Device 子項的 Tag（不包含 Group 中的 Tag）
            for i in range(source.childCount()):
                child = source.child(i)
                if self._safe_data(child, 0) != "Tag":
                    continue
                self._append_tag_row(self.tag_table, child)
        else:
            self.tag_table.setRowCount(0)

        # 若未顯示 group，斷開任何群組專用的 cell handler
        try:
            if node_type != "Group":
                try:
                    self.tag_table.cellChanged.disconnect(self._on_group_cell_changed)
                except Exception:
                    pass
                self._current_group_item = None
        except Exception:
            pass

        # 更新 monitor 視窗以反映目前右側顯示的 Tag（如果適用）
        try:
            try:
                self._update_monitor_table(item)
            except Exception as e:
                logging.error(f"Error in _update_monitor_table: {e}", exc_info=True)
        except Exception as e:
            logging.error(
                f"Unexpected error updating monitor table: {e}", exc_info=True
            )

    def _setup_table(self, headers):
        # 協助設定 `tag_table` 的欄位標題與欄寬行為。
        #
        # 參數：
        # - headers: 欄位標題字串列表
        try:
            self.tag_table.setColumnCount(len(headers))
            self.tag_table.setHorizontalHeaderLabels(headers)
            header = self.tag_table.horizontalHeader()
            for c in range(len(headers)):
                try:
                    header.setSectionResizeMode(
                        c, QHeaderView.ResizeMode.ResizeToContents
                    )
                except Exception:
                    pass
            try:
                header.setStretchLastSection(False)
            except Exception:
                pass
        except Exception:
            pass

    def _append_tag_row(self, table, t):
        """Append a Tag row to given table using common column mapping.

        Expects Tag UI fields stored on roles consistent with TagDialog.
        """
        try:
            row = table.rowCount()
            table.insertRow(row)
        except Exception:
            return

        try:
            it0 = QTableWidgetItem(t.text(0) or "")
            it0.setTextAlignment(
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter
            )
            try:
                it0.setData(Qt.ItemDataRole.UserRole, t)
            except Exception:
                pass
            table.setItem(row, 0, it0)
        except Exception:
            pass

        # data type
        try:
            dtype = self._safe_data(t, 2, "") or ""
        except Exception:
            dtype = ""
        it1 = QTableWidgetItem(str(dtype))
        it1.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        table.setItem(row, 1, it1)

        # client access
        try:
            access = self._safe_data(t, 3, "") or ""
        except Exception:
            access = ""
        it2 = QTableWidgetItem(str(access))
        it2.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        table.setItem(row, 2, it2)

        # address
        try:
            addr = self._safe_data(t, 4, "") or ""
        except Exception:
            addr = ""
        it3 = QTableWidgetItem(str(addr))
        it3.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        table.setItem(row, 3, it3)

        # scan rate
        try:
            scan = self._safe_data(t, 5, None)
            scan_disp = f"{scan} ms" if scan is not None and str(scan) != "" else ""
        except Exception:
            scan_disp = ""
        it4 = QTableWidgetItem(str(scan_disp))
        it4.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        table.setItem(row, 4, it4)

        # scaling type and related fields
        try:
            scaling_data = self._safe_data(t, 6, {}) or {}
            scale_type = (
                scaling_data.get("type", "None")
                if isinstance(scaling_data, dict)
                else str(scaling_data or "")
            )
        except Exception:
            scaling_data = {}
            scale_type = ""
        it5 = QTableWidgetItem(str(scale_type))
        it5.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        table.setItem(row, 5, it5)

        show_scaling = isinstance(scale_type, str) and scale_type != "None"

        def _mk(col_idx, value):
            try:
                it = QTableWidgetItem(str(value))
                it.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                table.setItem(row, col_idx, it)
            except Exception:
                pass

        rl = (
            scaling_data.get("raw_low", "")
            if isinstance(scaling_data, dict) and show_scaling
            else ""
        )
        _mk(6, rl)
        rh = (
            scaling_data.get("raw_high", "")
            if isinstance(scaling_data, dict) and show_scaling
            else ""
        )
        _mk(7, rh)
        sdt = (
            scaling_data.get("scaled_type", "")
            if isinstance(scaling_data, dict) and show_scaling
            else ""
        )
        _mk(8, sdt)
        sl = (
            scaling_data.get("scaled_low", "")
            if isinstance(scaling_data, dict) and show_scaling
            else ""
        )
        _mk(9, sl)
        sh = (
            scaling_data.get("scaled_high", "")
            if isinstance(scaling_data, dict) and show_scaling
            else ""
        )
        _mk(10, sh)
        cl = (
            scaling_data.get("clamp_low", "")
            if isinstance(scaling_data, dict) and show_scaling
            else ""
        )
        _mk(11, cl)
        ch = (
            scaling_data.get("clamp_high", "")
            if isinstance(scaling_data, dict) and show_scaling
            else ""
        )
        _mk(12, ch)
        units = (
            scaling_data.get("units", "")
            if isinstance(scaling_data, dict) and show_scaling
            else ""
        )
        _mk(13, units)

    def _append_channel_row(self, table, ch):
        """Append a Channel row to the given table (Channel list under Connectivity)."""
        try:
            row = table.rowCount()
            table.insertRow(row)
        except Exception:
            return

        try:
            it0 = QTableWidgetItem(ch.text(0))
            try:
                it0.setData(Qt.ItemDataRole.UserRole, ch)
            except Exception:
                pass
            it0.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            table.setItem(row, 0, it0)
        except Exception:
            pass

        # driver
        try:
            val1 = self._safe_data(ch, 2, None)
            if isinstance(val1, dict):
                drv_type = val1.get("type") or str(val1)
            else:
                drv_type = str(val1 or "")
        except Exception:
            drv_type = ""
        it1 = QTableWidgetItem(str(drv_type))
        it1.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        table.setItem(row, 1, it1)

        # connection
        try:
            params = self._safe_data(ch, 3, {}) or {}
            if not params:
                drv9 = self._safe_data(ch, 9, None)
                if isinstance(drv9, dict):
                    p = drv9.get("params")
                    if isinstance(p, dict):
                        params = p
            if not params:
                drv2 = self._safe_data(ch, 2, None)
                if isinstance(drv2, dict):
                    p = drv2.get("params")
                    if isinstance(p, dict):
                        params = p
        except Exception:
            params = {}

        # derive a simple connection string
        try:
            drv_low = (str(drv_type) or "").lower()
            serial_like = False
            if ("serial" in drv_low) or (
                "rtu" in drv_low and "over tcp" not in drv_low
            ):
                serial_like = True
            if serial_like:
                conn = params.get("com") or ""
            else:
                ip = params.get("ip")
                port = params.get("port")
                adapter_ip = (
                    params.get("network_adapter_ip")
                    or params.get("adapter_ip")
                    or params.get("ip")
                )
                adapter = (
                    params.get("network_adapter")
                    or params.get("adapter")
                    or params.get("adapter_name")
                )
                if adapter:
                    conn = str(adapter)
                elif ip and port:
                    conn = f"{ip}:{port}"
                else:
                    conn = ""
        except Exception:
            conn = ""
        it2 = QTableWidgetItem(str(conn))
        it2.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        table.setItem(row, 2, it2)

        # parameters summary
        try:
            # for serial show baud etc., otherwise show adapter ip or TCP label
            param_str = ""
            try:
                if serial_like:
                    baud = params.get("baud") or ""
                    data_bits = params.get("data_bits") or ""
                    parity = params.get("parity") or ""
                    stop = params.get("stop") or ""
                    parts = [str(x) for x in (baud, data_bits, parity, stop) if x]
                    param_str = ",".join(parts)
                else:
                    nap = (
                        params.get("network_adapter_ip")
                        or params.get("adapter_ip")
                        or params.get("ip")
                    )
                    if nap:
                        param_str = str(nap)
                    else:
                        parts = []
                        if params.get("ip"):
                            parts.append(str(params.get("ip")))
                        if params.get("port"):
                            parts.append(str(params.get("port")))
                        param_str = ",".join(parts)
            except Exception:
                param_str = str(self._safe_data(ch, 3, "") or "")
        except Exception:
            param_str = ""
        it3 = QTableWidgetItem(str(param_str))
        it3.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        table.setItem(row, 3, it3)

    def _get_tree_path(self, item):
        """获取树项的路径，例如 channel1.device1.group1.tag1"""
        if not item:
            return ""
        path_parts = []
        current = item
        while current and current != self.tree.root_node:
            try:
                text = current.text(0)
                if text and text != "Connectivity":
                    path_parts.insert(0, text)
            except Exception:
                pass
            current = current.parent()
        return GROUP_SEPARATOR.join(path_parts)

    def _collect_tags_from_item(self, item, include_path=True, ancestors=None):
        """递归收集项及其下所有 tag，返回 (tag_item, path) 的列表"""
        if ancestors is None:
            ancestors = []

        results = []
        if not item:
            return results

        try:
            node_type = self._safe_data(item, 0, None)
        except Exception:
            node_type = None

        # Build path for container nodes (Channel, Device, Group)
        current_ancestors = ancestors
        try:
            if node_type in ("Channel", "Device", "Group"):
                text = item.text(0)
                if text and text != "Connectivity":
                    current_ancestors = ancestors + [text]
        except Exception:
            pass

        if node_type == "Tag":
            # 这是一个 tag
            try:
                text = item.text(0)
                if include_path and current_ancestors:
                    path = GROUP_SEPARATOR.join(current_ancestors + [text])
                else:
                    path = text
            except Exception:
                path = ""
            results.append((item, path))
        elif node_type in ("Channel", "Device", "Group", "Connectivity"):
            # 递归处理子项
            for i in range(item.childCount()):
                try:
                    child = item.child(i)
                    results.extend(
                        self._collect_tags_from_item(
                            child, include_path, current_ancestors
                        )
                    )
                except Exception:
                    continue

        return results

    def _update_monitor_table(self, item):
        """
        Populate Monitor table with tags from selected node.
        Uses virtual scrolling model - only keeps visible rows in memory.
        """
        if item is None:
            return

        try:
            node_type = self._safe_data(item, 0, None)
        except Exception:
            node_type = None

        # Support Device, Group, Channel, and Connectivity
        if node_type not in ("Device", "Group", "Channel", "Connectivity"):
            return

        # Collect tags based on node type
        tags = []  # List of (tag_item, tag_path) tuples

        if node_type == "Device":
            # For Device: only show direct tags, exclude Group items
            for i in range(item.childCount()):
                try:
                    child = item.child(i)
                    child_type = self._safe_data(child, 0, None)
                    if child_type == "Tag":
                        path = self._get_tree_path(child)
                        tags.append((child, path))
                except Exception:
                    continue

        elif node_type == "Group":
            # For Group: only show tags directly under this group
            for i in range(item.childCount()):
                try:
                    child = item.child(i)
                    child_type = self._safe_data(child, 0, None)
                    if child_type == "Tag":
                        path = self._get_tree_path(child)
                        tags.append((child, path))
                except Exception:
                    continue

        else:  # Channel or Connectivity
            # For Channel/Connectivity: show all tags recursively
            tags = self._collect_tags_from_item(item, include_path=True)

        # Convert to model format: list of (tag_path, data_type, client_access)
        model_tags = []
        import re

        for tag_item, tag_path in tags:
            # Get metadata
            try:
                scaling = self._safe_data(tag_item, 6, {}) or {}
                sdt = scaling.get("scaled_type") if isinstance(scaling, dict) else None
                scale_type = scaling.get("type") if isinstance(scaling, dict) else None
                rawdt = self._safe_data(tag_item, 2, "") or ""
                client_access = self._safe_data(tag_item, 3, "") or ""

                # Normalize dtype for display
                # Show original data type, and append scaled type if scaling is enabled and different
                dtype_display = rawdt or ""
                if scale_type and scale_type != "None" and sdt and sdt != rawdt:
                    dtype_display = f"{rawdt}→{sdt}"
                dtype = re.sub(
                    r"\(Array\)|\bArray\b|\[\]|\s*\[\s*\d+\s*\]",
                    "",
                    str(dtype_display),
                    flags=re.IGNORECASE,
                ).strip()

                # Check if array tag
                meta = self._safe_data(tag_item, 7, {}) or {}
                is_array = isinstance(meta, dict) and meta.get("is_array")
                if not is_array and isinstance(rawdt, str) and "array" in rawdt.lower():
                    is_array = True
                if not is_array:
                    addr = self._safe_data(tag_item, 4, "") or ""
                    is_array = isinstance(addr, str) and re.search(
                        r"\[\s*\d+\s*\]", addr
                    )

                # Add array elements with [idx] suffix for UI display
                if is_array:
                    addr = self._safe_data(tag_item, 4, "") or ""
                    m = re.search(r"\[\s*(\d+)\s*\]", str(addr))
                    cnt = int(m.group(1)) if m else 0
                    for idx in range(cnt):
                        model_tags.append((f"{tag_path} [{idx}]", dtype, client_access))
                else:
                    model_tags.append((tag_path, dtype, client_access))
            except Exception:
                # Fallback: simple tag
                model_tags.append((tag_path, "", ""))

        # Update model with all tags
        if hasattr(self, "monitor_model") and self.monitor_model:
            self.monitor_model.set_all_tags(model_tags)
            logging.info(
                f"[MONITOR_TABLE] Loaded {len(model_tags)} tags for display (virtual scrolling)"
            )
            # DEBUG: show first few tag paths
            for i, (path, dtype, access) in enumerate(model_tags[:5]):
                logging.debug(f"[MONITOR_TABLE] Tag[{i}]: {path}")
            if len(model_tags) > 5:
                logging.debug(
                    f"[MONITOR_TABLE] ... and {len(model_tags) - 5} more tags"
                )

        # Reset scroll to top
        try:
            self.monitor_table.verticalScrollBar().setValue(0)
        except Exception:
            pass

        return

    # --- Project file operations (minimal implementations) ---
    def new_project(self):
        # 建立一個全新的專案（清空 Connectivity tree）。
        #
        # 行為：
        # - 清除 `self.tree.conn_node` 底下的所有子項；
        # - 重設 `current_project_path` 與 last_project 檔案；
        # - 標記專案為未修改（dirty=False）並刷新 UI。
        root = getattr(self.tree, "conn_node", None)
        if root is None:
            return
        while root.childCount() > 0:
            try:
                root.removeChild(root.child(0))
            except Exception:
                break
        self.current_project_path = None
        try:
            if getattr(self, "_last_project_file", None) and os.path.exists(
                self._last_project_file
            ):
                try:
                    os.remove(self._last_project_file)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            self._mark_dirty(False)
        except Exception:
            pass
        try:
            self.update_right_table(self.tree.conn_node, 0)
        except Exception:
            pass

        # Stop OPC UA server when creating new project
        # This prevents port conflicts when creating a new project
        try:
            if getattr(self, "opc_server", None) is not None:
                logger.info("Stopping OPC UA server for new project...")
                self.opc_server.stop_server()
        except Exception:
            pass

    def open_project(self):
        # 開啟現有的 project.json 檔案並載入到 tree。
        #
        # 使用者會看到檔案選擇對話方塊；若載入失敗會顯示錯誤提醒。
        import os

        desktop_path = os.path.join(os.path.expanduser("~"), "Desktop", "project.json")
        path, _ = QFileDialog.getOpenFileName(
            self, "Open Project", desktop_path, "JSON Files (*.json)"
        )
        if not path:
            return
        try:
            self._call_controller("import_project_from_json", path)

            # Reload OPC UA tags when a new project is loaded (with clearing)
            try:
                if getattr(self, "opc_server", None) and getattr(self, "tree", None):
                    # Use reload_tags() instead of load_all_tags()
                    # reload_tags() clears old nodes before loading new ones
                    QTimer.singleShot(
                        100,
                        lambda: self.opc_server.reload_tags()
                        if self.opc_server
                        else None,
                    )
            except Exception:
                pass

            # 匯入專案檔後確保 UI 進行刷新
            try:
                QTimer.singleShot(
                    0, lambda: self.update_right_table(self.tree.conn_node, 0)
                )
                QTimer.singleShot(
                    250, lambda: self.update_right_table(self.tree.conn_node, 0)
                )
                QTimer.singleShot(
                    1000, lambda: self.update_right_table(self.tree.conn_node, 0)
                )
            except Exception:
                try:
                    self.update_right_table(self.tree.conn_node, 0)
                except Exception:
                    pass
            self.current_project_path = path
            pass
            # 成功開啟專案後，記錄 last project 路徑並同步建立 temp.json 以便下次快速回復
            try:
                if getattr(self, "_last_project_file", None):
                    try:
                        with open(self._last_project_file, "w", encoding="utf-8") as f:
                            f.write(path)
                    except Exception:
                        pass
                if getattr(self, "_temp_json", None):
                    try:
                        # 還原目前專案到 temp.json（覆寫），使用 controller 的匯出函式
                        self._call_controller("export_project_to_json", self._temp_json)
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                self._mark_dirty(False)
            except Exception:
                pass
        except Exception:
            QMessageBox.warning(self, "Open Failed", f"Failed to open project: {path}")

    def save_project(self):
        # 將當前專案儲存到 `current_project_path`。
        #
        # 若尚未指定路徑，會呼叫 `save_project_as()` 以提示使用者選擇儲存位置。
        if not self.current_project_path:
            return self.save_project_as()
        try:
            self._call_controller("export_project_to_json", self.current_project_path)
            try:
                # 成功儲存後更新 last_project.txt 與 temp.json
                if getattr(self, "_last_project_file", None):
                    try:
                        with open(self._last_project_file, "w", encoding="utf-8") as f:
                            f.write(self.current_project_path)
                    except Exception:
                        pass
                if getattr(self, "_temp_json", None):
                    try:
                        # 安排短延遲的匯出，以確保 UI handler 已更新相關 data role
                        try:
                            QTimer.singleShot(
                                50,
                                lambda: self._call_controller(
                                    "export_project_to_json", self._temp_json
                                ),
                            )
                        except Exception:
                            self._call_controller(
                                "export_project_to_json", self._temp_json
                            )
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                self._mark_dirty(False)
            except Exception:
                pass
        except Exception:
            QMessageBox.warning(
                self,
                "Save Failed",
                f"Failed to save project: {self.current_project_path}",
            )

    def save_project_as(self):
        # 提示使用者選擇儲存路徑並將專案序列化存成 JSON 檔案。
        #
        # 成功儲存後會更新 `current_project_path` 並將 dirty flag 設為 False。
        import os

        desktop_path = os.path.join(os.path.expanduser("~"), "Desktop", "Project.json")
        path, _ = QFileDialog.getSaveFileName(
            self, "Save Project As", desktop_path, "JSON Files (*.json)"
        )
        if not path:
            return
        if not path.lower().endswith(".json"):
            path = path + ".json"
        try:
            self._call_controller("export_project_to_json", path)
            self.current_project_path = path
            try:
                # 成功另存後更新 last_project.txt 與 temp.json
                if getattr(self, "_last_project_file", None):
                    try:
                        with open(self._last_project_file, "w", encoding="utf-8") as f:
                            f.write(path)
                    except Exception:
                        pass
                if getattr(self, "_temp_json", None):
                    try:
                        self._call_controller("export_project_to_json", self._temp_json)
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                self._mark_dirty(False)
            except Exception:
                pass
        except Exception:
            QMessageBox.warning(self, "Save Failed", f"Failed to save project: {path}")

    def toggle_runtime(self):
        """Toggle Modbus runtime polling.

        🔴 Red = Running (actively polling)
        🟢 Green = Stopped (idle)
        """
        if getattr(self, "_runtime_running", False):
            # Stop runtime
            self.stop_runtime_polling()
        else:
            # Start runtime
            self.start_runtime_polling()

    def start_runtime_polling(self):
        """Start Modbus polling of all project tags."""
        if self._runtime_running:
            logging.warning("Runtime already running")
            return

        try:
            # Initialize Modbus data buffer (thread-safe storage for all tag values)
            from core.modbus.data_buffer import ModbusDataBuffer

            if (
                not hasattr(self, "modbus_data_buffer")
                or self.modbus_data_buffer is None
            ):
                self.modbus_data_buffer = ModbusDataBuffer()
                logging.info("ModbusDataBuffer initialized")

            # Clear monitor table model (with virtual scrolling, just reset the model)
            if hasattr(self, "monitor_model") and self.monitor_model:
                self.monitor_model.set_all_tags([])

            # IMPORTANT: Initialize Monitor table BEFORE starting Modbus polling
            # This ensures tag list is populated before data arrives
            #
            # With virtual scrolling, we load ALL tags from current selection
            # But only display 30 at a time. User can scroll to see more.
            # This avoids lag from QTableWidget.insertRow() calls

            selected_item = self.tree.currentItem()

            if selected_item:
                # User has selected a specific node
                self._update_monitor_table(selected_item)
            elif hasattr(self.tree, "conn_node") and self.tree.conn_node:
                # No selection: load from Connectivity root (all 839 tags)
                self._update_monitor_table(self.tree.conn_node)

            # Link model to buffer for real-time value updates
            if hasattr(self, "modbus_data_buffer") and self.monitor_model:
                self.monitor_model.buffer_ref = self.modbus_data_buffer

            # Create RuntimeMonitor if not exists
            if self.runtime_monitor is None:
                self.runtime_monitor = RuntimeMonitor(
                    tree_root_item=self.tree.conn_node,
                    signals_instance=self.runtime_signals,
                )
                logging.info(f"Created RuntimeMonitor with signals instance")

                # Connect signals only once during creation
                try:
                    self.runtime_monitor.signal_tag_updated.connect(
                        self._on_runtime_tag_updated
                    )
                    logging.info("signal_tag_updated connected")
                except Exception as e:
                    logging.error(f"Failed to connect signal_tag_updated: {e}")

                try:
                    self.runtime_monitor.signal_error.connect(self._on_runtime_error)
                    logging.info("signal_error connected")
                except Exception as e:
                    logging.error(f"✗ Failed to connect signal_error: {e}")

                try:
                    self.runtime_monitor.signal_stopped.connect(
                        self._on_runtime_stopped
                    )
                    logging.info("✓ signal_stopped connected")
                except Exception as e:
                    logging.error(f"✗ Failed to connect signal_stopped: {e}")

                # Mark that all callbacks are now connected and ready
                self.runtime_monitor.mark_callbacks_connected()
                logging.info(
                    "✓ All callbacks marked as connected, ready to receive signals"
                )
            else:
                logging.info(
                    "✓ RuntimeMonitor already created, reusing existing instance"
                )

            # NOW start monitoring (data will flow into Monitor table which is already initialized)
            if self.runtime_monitor.start():
                self._runtime_running = True
                # 🔴 Red = Running
                self.runtime_indicator_action.setText("🔴 Runtime")
                logging.info("Runtime monitoring started with ModbusDataBuffer")
                # print(f"Runtime started - Monitor displays tags from tree selection, data synced to buffer")
            else:
                logging.error("Failed to start runtime polling")
        except Exception as e:
            logging.error(f"Error starting runtime polling: {e}", exc_info=True)
            QMessageBox.critical(self, "Runtime Error", f"Failed to start polling: {e}")

    def stop_runtime_polling(self):
        """Stop Modbus polling."""
        if not self._runtime_running:
            return

        try:
            # Clear buffer data
            if hasattr(self, "modbus_data_buffer") and self.modbus_data_buffer:
                self.modbus_data_buffer.clear()

            # Clear tracking
            if hasattr(self, "_monitor_tag_rows"):
                self._monitor_tag_rows.clear()

            if self.runtime_monitor:
                self.runtime_monitor.stop()

            self._runtime_running = False
            # 🟢 Green = Stopped
            self.runtime_indicator_action.setText("🟢 Runtime")
            logging.info("Runtime polling stopped")
        except Exception as e:
            logging.error(f"Error stopping runtime polling: {e}", exc_info=True)

    def _on_monitor_scroll(self, value):
        """Handle Monitor table scroll - update visible rows for virtual scrolling."""
        if hasattr(self, "monitor_model") and self.monitor_model:
            # Use indexAt() to get the actual first visible row
            # This is more reliable than calculating from scroll position
            from PyQt6.QtCore import QPoint
            import logging

            first_visible_index = self.monitor_table.indexAt(QPoint(0, 0))
            if first_visible_index.isValid():
                first_visible_row = first_visible_index.row()
            else:
                first_visible_row = 0
            logging.info(
                f"[SCROLL_EVENT] value={value}, first_visible_row={first_visible_row}"
            )
            self.monitor_model.update_visible_rows(first_visible_row)

    def _on_runtime_tag_updated(
        self, tag_name: str, value, timestamp: float, quality: str, update_count: int
    ):
        """Modbus worker callback - write data to buffer, update model."""
        try:
            # Write to central data buffer (not directly to UI)
            if hasattr(self, "modbus_data_buffer") and self.modbus_data_buffer:
                self.modbus_data_buffer.update_tag_value(
                    tag_name, value, timestamp, quality, update_count
                )

                # DEBUG: log first few updates
                if update_count <= 3 or update_count % 100 == 0:
                    logging.debug(f"[TAG_UPDATE] {tag_name}={value} quality={quality}")

                # Update model if tag is in visible range
                if hasattr(self, "monitor_model") and self.monitor_model:
                    timestamp_str = datetime.fromtimestamp(timestamp).strftime(
                        "%Y-%m-%d %H:%M:%S.%f"
                    )[:-3]
                    self.monitor_model.update_tag_value(
                        tag_name, value, timestamp_str, quality, update_count
                    )

        except Exception as e:
            logging.error(
                f"Error in _on_runtime_tag_updated callback: {tag_name}: {e}",
                exc_info=True,
            )

    def _on_runtime_error(self, error_msg: str):
        """Callback when runtime encounters an error."""
        logging.error(f"Runtime error: {error_msg}")
        # Optionally show in UI
        try:
            QMessageBox.warning(self, "Runtime Error", error_msg)
        except Exception:
            pass

    def _on_runtime_stopped(self):
        """Callback when runtime is stopped."""
        # Clear buffer and tracking when stopped
        if hasattr(self, "modbus_data_buffer") and self.modbus_data_buffer:
            self.modbus_data_buffer.clear()
        if hasattr(self, "_monitor_tag_rows"):
            self._monitor_tag_rows.clear()
        logging.info("Runtime monitor stopped and buffer cleared")

    def _set_table_item(self, table, row: int, col: int, text: str):
        """Helper to set table cell content."""
        item = QTableWidgetItem(text)
        item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)  # Read-only
        table.setItem(row, col, item)

    def closeEvent(self, event):
        """Handle application close event - cleanup OPC server and runtime."""
        try:
            # Stop runtime if running
            if getattr(self, "_runtime_running", False):
                try:
                    self.toggle_runtime()
                except Exception:
                    pass
        except Exception:
            pass

        try:
            # Stop OPC UA server if running - CRITICAL for clean shutdown
            if getattr(self, "opc_server", None) is not None:
                try:
                    logger.info("Closing OPC UA server...")
                    self.opc_server.stop_server()
                    logger.info("OPC UA server stopped")
                except Exception as e:
                    logger.warning(f"Error stopping OPC server: {e}")
                try:
                    self.opc_server = None
                except Exception:
                    pass
        except Exception:
            pass

        # Accept the close event
        event.accept()

        try:
            # Close all terminal windows
            for tw in getattr(self, "terminal_windows", []):
                try:
                    tw.close()
                except Exception:
                    pass
        except Exception:
            pass

        try:
            super().closeEvent(event)
        except Exception:
            event.accept()

    def _write_opc_trace(self, message: str):
        """Write OPC UA trace message to debug output or logger.

        Args:
            message: The trace message to output
        """
        try:
            import logging

            logger = logging.getLogger("OPC_UA")
            logger.info(message)
        except Exception:
            pass

        try:
            print(f"[OPC UA] {message}")
        except Exception:
            pass

    # duplicate open_opcua_settings removed (kept earlier implementation)

    # （省略多個方法以求簡潔；IoTApp 保持原 ui/app_full 的實作）

    # 將看似方法的頂層函式附加到 IoTApp 類別上。
    # 這有助於回復意外取消縮排而變成頂層函式的方法
    # 在編輯過程中發生的情況。我們會將第一個參數名為 'self' 的函式
    # 綁定為 `IoTApp` 的方法。
    try:
        import inspect

        for _name, _obj in list(globals().items()):
            try:
                if inspect.isfunction(_obj):
                    sig = inspect.signature(_obj)
                    params = list(sig.parameters.keys())
                    if params and params[0] == "self":
                        setattr(IoTApp, _name, _obj)
            except Exception:
                pass
    except Exception:
        pass

    # 相容性賦值


try:
    IoTApp._on_diag_context_menu = TerminalWindow._on_diag_context_menu
    IoTApp._on_only_txrx_toggled = TerminalWindow._on_only_txrx_toggled
    IoTApp._on_show_raw_toggled = TerminalWindow._on_show_raw_toggled
except Exception:
    pass


def main():
    # Hide console window VERY EARLY - before anything else
    if sys.platform == "win32":
        try:
            import ctypes
            import time

            ctypes.windll.kernel32.GetConsoleWindow.restype = ctypes.c_void_p
            hwnd = ctypes.windll.kernel32.GetConsoleWindow()
            if hwnd:
                # Aggressively hide console using FreeConsole
                ctypes.windll.user32.ShowWindow(hwnd, 0)  # SW_HIDE = 0
                # Don't free console, just hide it
        except Exception:
            pass

    app = QApplication(sys.argv)

    # Immediately hide any console windows that might appear
    if sys.platform == "win32":
        try:
            import ctypes
            import time

            ctypes.windll.kernel32.GetConsoleWindow.restype = ctypes.c_void_p
            hwnd = ctypes.windll.kernel32.GetConsoleWindow()
            if hwnd:
                # Triple-hide to be absolutely sure
                ctypes.windll.user32.ShowWindow(hwnd, 0)  # SW_HIDE = 0
                time.sleep(0.01)
                ctypes.windll.user32.ShowWindow(hwnd, 0)  # SW_HIDE = 0
        except Exception:
            pass

    # Process pending events to ensure any console windows are truly hidden
    app.processEvents()

    # 應用程式圖示（使用專案根目錄的 lioil.ico）
    try:
        icon_path = os.path.join(os.path.dirname(__file__), "lioil.ico")
        if os.path.exists(icon_path):
            app.setWindowIcon(QIcon(icon_path))
    except Exception:
        pass

    # Create and show main window
    window = IoTApp()
    window.show()

    # Hide console one more time after showing main window
    if sys.platform == "win32":
        try:
            import ctypes

            ctypes.windll.kernel32.GetConsoleWindow.restype = ctypes.c_void_p
            hwnd = ctypes.windll.kernel32.GetConsoleWindow()
            if hwnd:
                ctypes.windll.user32.ShowWindow(hwnd, 0)  # SW_HIDE = 0
        except Exception:
            pass

    try:
        from qasync import QEventLoop

        loop = QEventLoop(app)
        asyncio.set_event_loop(loop)
        with loop:
            loop.run_forever()
    except Exception:
        sys.exit(app.exec())


if __name__ == "__main__":
    main()
