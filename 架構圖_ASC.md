# ModUA 系統架構圖 (視覺化)

## 🏗️ 完整系統架構

```
═══════════════════════════════════════════════════════════════════════════════
                              📱 PyQt6 GUI 層
═══════════════════════════════════════════════════════════════════════════════

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃                          🪟 IoTApp (app.py)                             ┃
┃                                                                         ┃
┃  ┌─────────────────────────────────┬─────────────────────────────────┐ ┃
┃  │  🌳 樹狀面板                    │  📊 監視表格 & 設定面板        │ ┃
┃  │  (ConnectivityTree)             │  (QTableWidget + QTreeWidget) │ ┃
┃  │                                 │                               │ ┃
┃  │  Channel ┐                      │  Tag Name | Value | Status   │ ┃
┃  │  ├─ Device ┐                    │  ──────────────────────────   │ ┃
┃  │  │ ├─ Group ┐                   │  CT_Pri  | 1245.5 | ✅       │ ┃
┃  │  │ │ ├─ Tag1                    │  Freq    | 50.0   | ✅       │ ┃
┃  │  │ │ ├─ Tag2                    │  Status  | OK     | ✅       │ ┃
┃  │  │ │ └─ ...                     │                               │ ┃
┃  │  │ └─ Tag N                     │                               │ ┃
┃  │  └─ Device N                    │                               │ ┃
┃  │                                 │                               │ ┃
┃  └─────────────────────────────────┴─────────────────────────────────┘ ┃
┃                                                                         ┃
┃  菜單: 📁 File | 🔴 Runtime | 🔧 OPC UA | 🔍 Diagnostics              ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
        │                              │                           │
        │ triggers                     │ reads/writes              │ displays
        ▼                              ▼                           ▼
┌───────────────────────────────────────────────────────────────────────────┐
│ 📁 File Ops & Tree Mgmt                                                   │
│ ├─ new_project() / open_project() / save_project()                        │
│ ├─ on_new_channel() / on_new_device() / on_new_group() / on_new_tag()     │
│ ├─ _execute_tag_write()                                                   │
│ └─ update_right_table() → shows Channel/Device/Group/Tag details          │
└───────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  📦 UI 子包 (ui/)                                                         │
├───────────────────────────────────────────────────────────────────────────┤
│  ├─ dragdrop_tree.py: 樹狀控件 (D&D, 右鍵菜單)                            │
│  ├─ dialogs/:                                                             │
│  │   ├─ channel_dialog.py: Driver Type, Network Params                    │
│  │   ├─ device_dialog.py: Timing, DataAccess, Encoding, Block Sizes      │
│  │   ├─ group_dialog.py: Name, Description                                │
│  │   ├─ tag_dialog.py: Address, Data Type, Scan Rate, Scaling             │
│  │   ├─ opcua_dialog.py: Application Name, Endpoints, Security            │
│  │   └─ write_value_dialog.py: Write value & Function Code                │
│  ├─ components.py: 通用 UI 工具函數                                        │
│  ├─ terminal_window.py: 診斷窗口 (TX/RX, Events)                          │
│  ├─ clipboard.py: 複製/貼上 tag                                            │
│  └─ theme.py: 樣式                                                        │
└───────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════
                         🎮 控制層 (core/controllers/)
═══════════════════════════════════════════════════════════════════════════════

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃                      🎛️ AppController (base_controller.py)              ┃
┃                                                                         ┃
┃  主要方法:                                                               ┃
┃  ├─ save_channel(item, data)      │ Role: 1=Desc, 2=Driver, 3=Comm      ┃
┃  ├─ save_device(item, data)       │ Role: 1=Desc, 2=ID, 3=Timing...     ┃
┃  ├─ save_tag(item, data)          │ Role: 1=Desc, 2=Type, 3=Access...   ┃
┃  ├─ normalize_*()                 │ 規範化配置參數                        ┃
┃  ├─ export_project_to_json()      │ 保存到 Project.json                  ┃
┃  ├─ import_project_from_json()    │ 從 Project.json 載入                 ┃
┃  ├─ export_device_to_csv()        │ Tag 導出 CSV                         ┃
┃  └─ import_device_from_csv()      │ Tag 導入 CSV                         ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
        │                              │                      │
        ▼                              ▼                      ▼
  ┌──────────────┐          ┌────────────────┐        ┌──────────────┐
  │ validators   │          │ config_builder │        │ serializers  │
  │              │          │                │        │              │
  │ ✓ normalize_ │          │ ✓ normalize_   │        │ ✓ export_    │
  │   dict_flags │          │   communication│        │   tags_to_csv│
  │ ✓ to_numeric │          │   _params      │        │ ✓ is_array_  │
  │   _flag      │          │ ✓ detect_      │        │   tag        │
  │ ✓ is_tcp_    │          │   interface_   │        │ ✓ normalize_ │
  │   like_      │          │   for_ip       │        │   address_   │
  │   driver     │          └────────────────┘        │   number     │
  │              │                                    │              │
  └──────────────┘                                    └──────────────┘
        │                         │                      │
        └─────────────────────────┴──────────────────────┘
                        │ manages
                        ▼
                ┌──────────────────┐
                │  DataBroker      │
                │ (data_manager.py)│
                │                  │
                │ 線程安全的快取   │
                │ ✓ handle_polled()│
                │ ✓ get()          │
                │ ✓ snapshot()     │
                └──────────────────┘

═══════════════════════════════════════════════════════════════════════════════
                     📡 Modbus 層 (core/modbus/) - 核心輪詢
═══════════════════════════════════════════════════════════════════════════════

                    用戶點擊 [🔴 Runtime] 啟動輪詢
                              │
                              ▼
            ┌────────────────────────────────────┐
            │  RuntimeMonitor.start()            │
            │                                    │
            │  1. _extract_all_tags()            │
            │     → 遍歷 tree，收集所有 tag      │
            │                                    │
            │  2. _group_tags_by_config()        │
            │     → 按 driver + device_id 分組   │
            │                                    │
            │  3. for each group:                │
            │     ├─ _create_modbus_client()     │
            │     ├─ _create_worker_for_group()  │
            │     └─ ModbusWorker.start()        │
            │                                    │
            └────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
        ┌─────────────────────┐ ┌──────────────────┐
        │  ModbusClient       │ │ ModbusWorker     │
        │ (modbus_client.py)  │ │(modbus_worker.py)│
        │                     │ │                  │
        │ ✓ RTU/TCP connect   │ │ ✓ 輪詢線程      │
        │ ✓ Async wrapper     │ │ ✓ 調度管理      │
        │ ✓ 重試機制          │ │ ✓ 資料緩衝      │
        │ ✓ read/write ops    │ │ ✓ 寫入隊列      │
        │                     │ │                  │
        └─────────────────────┘ └──────────────────┘
                    │                   │
                    └─────────┬─────────┘
                              │ uses
                    ┌─────────┴──────────────────┐
                    ▼                            ▼
        ┌─────────────────────┐    ┌────────────────────┐
        │  ModbusMapping      │    │  DataBuffer        │
        │(modbus_mapping.py)  │    │ (data_buffer.py)   │
        │                     │    │                    │
        │ ✓ Address parsing   │    │ ✓ Value storage    │
        │ ✓ Data type convert │    │ ✓ Timestamp        │
        │ ✓ Byte order        │    │ ✓ Quality indicator│
        │ ✓ Scaling (raw→eng) │    │ ✓ Thread-safe      │
        │                     │    │                    │
        └─────────────────────┘    └────────────────────┘
                                            │
                                            │ stores
                                            ▼
                                ┌──────────────────────┐
                                │ Modbus Value Cache   │
                                │                      │
                                │ tag_path → {         │
                                │   value,             │
                                │   timestamp,         │
                                │   quality,           │
                                │   update_count       │
                                │ }                    │
                                │                      │
                                └──────────────────────┘
                                            │
                                    ┌───────┴────────┐
                                    │  emit signal   │
                                    ▼                ▼
                            ┌──────────────┐  ┌────────────────────┐
                            │ tag_updated  │  │ UI 表格 & OPC UA    │
                            │ signal       │  │ 自動更新            │
                            └──────────────┘  └────────────────────┘

═══════════════════════════════════════════════════════════════════════════════
                     🔧 OPC UA 層 (core/OPC_UA/)
═══════════════════════════════════════════════════════════════════════════════

📍 當前狀態: 輕量級 Shim
                                        
            ┌──────────────────────────┐
            │   OPCServer (shim)       │
            │                          │
            │ self._nodes = {          │
            │   tag_id: {              │
            │     value,               │
            │     meta (type, access)  │
            │   }                      │
            │ }                        │
            └──────────────────────────┘
                    │
        ┌───────────┼───────────┐
        │           │           │
        ▼           ▼           ▼
    add_tag()  update_tag() read_tag_value()
        │           │           │
        └─ 管理元數據──┴─ 實時更新──┴─ 查詢當前值

🔄 與 Modbus 集成:
        
    tag_updated signal (from Modbus)
              │
              ▼
    IoTApp._on_tag_polled()
              │
              ▼
    if OPCServer:
        OPCServer.update_tag(tag_path, value)
              │
              ▼
    OPC UA 客戶端可讀取最新值 ✅

⚠️ 缺失:
    ❌ 實際 OPC UA Server 實現 (asyncua/opcua)
    ❌ 端點監聽
    ❌ 客戶端連接管理
    ❌ 安全策略
    ❌ Method calls

📋 實現路線圖:
    
    core/OPC_UA/opcua_server.py (新增)
        │
        ├─ from asyncua import Server
        ├─ class ModUAOPCServer(OPCServer):
        │   ├─ async start()
        │   ├─ async add_tag_to_opcua(tag_path, meta)
        │   ├─ sync_with_modbus_values()
        │   └─ handle_client_write()
        │
        └─ 集成到 IoTApp:
            ├─ apply_opcua_settings() 啟動
            ├─ toggle_runtime() 同步控制
            └─ _on_tag_polled() 自動同步

═══════════════════════════════════════════════════════════════════════════════
                     🔍 診斷層 (core/diagnostics/)
═══════════════════════════════════════════════════════════════════════════════

        ┌─────────────────────────────────┐
        │  DiagnosticsManager             │
        │                                 │
        │  ✓ log_event()                  │
        │  ✓ log_modbus_tx() / rx()       │
        │  ✓ export_to_csv()              │
        │  ✓ get_stats()                  │
        │                                 │
        └─────────────────────────────────┘
                    │
        ┌───────────┼───────────┐
        │           │           │
        ▼           ▼           ▼
    Event Log   Modbus Trace  Stats
    (內存)      (內存/DB)      (計算)
        │           │           │
        └───────────┴───────────┘
                    │
                    ▼ triggered by menu
            ┌──────────────────────┐
            │  TerminalWindow      │
            │                      │
            │  QTableWidget:       │
            │  Date|Time|Event|Len │
            │  ───────────────────  │
            │  [Diag records]       │
            │                       │
            │  Features:            │
            │  ✓ 搜索/過濾          │
            │  ✓ CSV 導出           │
            │  ✓ 按 device 過濾      │
            │                       │
            └──────────────────────┘

💡 增強建議:
    1. 性能儀表板 (Device Status %)
    2. 實時流模式 (vs 回顧模式)
    3. 告警系統 (error_rate, response_time)
    4. 歷史查詢 (SQLite 持久化)

═══════════════════════════════════════════════════════════════════════════════
                          📁 配置文件格式
═══════════════════════════════════════════════════════════════════════════════

Project.json (或 Project_<name>.json)
├─ channels[]
│  ├─ type: "Channel"
│  ├─ text: "Channel1"
│  ├─ general: { channel_name, description }
│  ├─ driver: { type, params }
│  ├─ communication: { flat params dict }
│  ├─ children[]
│  │  ├─ type: "Device"
│  │  ├─ text: "Device1"
│  │  ├─ general: { name, description, device_id }
│  │  ├─ timing, data_access, encoding, block_sizes
│  │  ├─ children[]
│  │  │  ├─ type: "Group" (optional)
│  │  │  ├─ text: "DataGroup1"
│  │  │  ├─ children[]
│  │  │  │  ├─ type: "Tag"
│  │  │  │  ├─ text: "Temperature"
│  │  │  │  ├─ general: { address, data_type, access, description }
│  │  │  │  └─ scaling: { type, raw_low, raw_high, ... }
│  │  │  │
│  │  │  └─ ...
│  │  │
│  │  └─ ...
│  │
│  └─ ...
│
└─ opcua_settings: { general, endpoints, authentication, ... }

Tree 角色映射 (QTreeWidgetItem roles):
├─ col 0: 節點類型 (UserRole = "Channel"/"Device"/"Group"/"Tag")
├─ col 1: Description (UserRole)
├─ col 2: Device ID / Driver (UserRole)
├─ col 3: Timing / Communication (UserRole)
├─ col 4: Data Access (UserRole)
├─ col 5: Encoding (UserRole)
├─ col 6: Block Sizes (UserRole)
└─ col 7: Metadata (UserRole = {addrnum, is_array, array_size})

═══════════════════════════════════════════════════════════════════════════════
                          🔄 主要數據流
═══════════════════════════════════════════════════════════════════════════════

流程 1: 項目載入
┌───────────────────┐
│ 用戶選擇 JSON    │
└────────┬──────────┘
         ▼
┌──────────────────────────────────┐
│ AppController.import_project_... │
│  ├─ 解析 JSON                   │
│  ├─ 建立 TreeWidgetItem         │
│  └─ 儲存配置到 roles            │
└────────┬──────────────────────────┘
         ▼
┌──────────────────────────────────┐
│ UI 刷新樹狀結構                 │
└──────────────────────────────────┘

流程 2: Modbus 輪詢 (實時)
┌────────────────────────────────────┐
│ RuntimeMonitor._poll_loop()       │
│ (ModbusWorker 線程)               │
└────────┬─────────────────────────────┘
         │ 按 scan_rate 調度
         ▼
┌────────────────────────────────────┐
│ ModbusClient.read_holding_regs()  │
│ (async, 重試)                      │
└────────┬─────────────────────────────┘
         │ 成功
         ▼
┌────────────────────────────────────┐
│ ModbusMapping.map_tag_...()       │
│ - 解析地址                        │
│ - 轉換數據類型                    │
│ - 套用 Scaling                    │
└────────┬─────────────────────────────┘
         ▼
┌────────────────────────────────────┐
│ DataBuffer.update()                │
│ (緩衝值, 時間戳, 品質)            │
└────────┬─────────────────────────────┘
         ▼
┌────────────────────────────────────┐
│ tag_updated.emit()                │
│ (Qt 信號)                          │
└────────┬─────────────────────────────┘
         ▼
┌────────────────────────────────────┐
│ UI 表格更新                        │
│ OPC UA 值同步                      │
│ 診斷日誌記錄                       │
└────────────────────────────────────┘

流程 3: 標籤寫入
┌────────────────────────────────────┐
│ 用戶在表格雙擊寫入值               │
└────────┬─────────────────────────────┘
         ▼
┌────────────────────────────────────┐
│ WriteValueDialog 彈出               │
│ 用戶輸入值 + Function Code         │
└────────┬─────────────────────────────┘
         ▼
┌────────────────────────────────────┐
│ IoTApp._execute_tag_write()        │
│ ├─ 查找 tag 項目                   │
│ ├─ 提取 address, scaling          │
│ └─ ModbusClient.write_register()   │
└────────┬─────────────────────────────┘
         ▼
┌────────────────────────────────────┐
│ 成功/失敗回覆                      │
└────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════
                      🚀 集成新功能的建議步驟
═══════════════════════════════════════════════════════════════════════════════

要添加新的 Modbus 功能:
    1. 在 core/modbus/ 新增模塊 (如 modbus_advanced_features.py)
    2. 在 RuntimeMonitor 中集成
    3. 在 IoTApp 中暴露 UI/菜單項
    4. 連接到 DataBroker 進行數據共享

要添加新的協議 (如 EtherNet/IP):
    1. 建立 core/ethernet_ip/ 包
    2. 實現類似 ModbusClient/Worker 的架構
    3. 在 AppController 中添加驅動類型檢測
    4. 在 RuntimeMonitor 中添加協議調度邏輯

要完成 OPC UA:
    1. core/OPC_UA/opcua_server.py 實現真實服務器
    2. 集成到 RuntimeMonitor 信號
    3. 在 apply_opcua_settings() 中初始化
    4. 測試客戶端連接

要增強診斷:
    1. 添加性能指標計算 (success_rate, avg_latency)
    2. 實現告警系統 (DiagnosticsAlert)
    3. 集成 SQLite 歷史存儲 (optional)
    4. 增強 TerminalWindow 展示

═══════════════════════════════════════════════════════════════════════════════
```

---

## 📊 層級交互矩陣

```
         UI    控制   Modbus  OPC UA  診斷
UI       -     ✓✓     ✓✓      ✓       ✓
控制      ✓✓    -      ✓✓      ✓       ✓
Modbus   ✓✓    ✓       -      ✓✓      ✓✓
OPC UA   ✓     ✓      ✓✓       -      ✓
診斷      ✓     ✓      ✓✓      ✓       -

✓  = 有交互
✓✓ = 強耦合/重要交互
-  = 自身層
```

---

## 🎯 關鍵性能指標

| 指標 | 目標值 | 當前狀態 |
|-----|--------|--------|
| 樹加載時間 (1000 tags) | < 500ms | TBD |
| 表格更新延遲 | < 50ms | TBD |
| Modbus 輪詢精度 | ±5% | TBD |
| OPC UA 客戶端連接時間 | < 1s | ⚠️ 未實現 |
| 診斷日誌寫入 | < 1ms/記錄 | TBD |
| 內存占用 (100 tags) | < 50MB | TBD |

---

**架構圖完成** ✅  
更新時間: 2026年1月24日

