# ModUA
# ModUA

ModUA 是一個用於將 Modbus 裝置與 OPC UA 整合的桌面工具／橋接程式，提供 GUI 專案管理、輪詢 (polling)、監控與診斷功能。

**主要功能**
- 圖形化管理專案與標籤（使用 PyQt6）。
- 支援 Modbus TCP (overtcp) 與 RTU（透過 pymodbus 的同步客戶端，程式內以非同步包裝）。
- 支援 OPC UA（內建輕量 shim；可整合完整 OPC UA server 實作）。
- 批次讀取、位元/字節順序與常見資料型別的解碼。
- 內建診斷與記錄導出（`core/diagnostics.py`）。

**快速開始**
1. 建議使用虛擬環境並安裝相依套件：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. 執行桌面應用程式：

```powershell
python app.py
```

3. 若要以 headless 或測試方式使用 core 模組，可在 Python REPL 或自訂腳本中 import `core` 下的模組。

**相依套件（範例）**
- PyQt6（GUI）
- pymodbus（Modbus）
- opcua（OPC UA, 如需完整 server 功能）
- pyserial（RTU 支援）

詳細版本請見 `requirements.txt`。

**專案結構（重點）**
- `app.py`：應用程式入口與主要 UI（IoTApp 類）。
- `core/`：核心邏輯（controllers、modbus、OPC_UA、diagnostics、ui_models 等）。
	- `core/modbus/modbus_client.py`：Modbus 客戶端封裝（非同步包裝及重試機制）。
	- `core/OPC_UA/OPC_UA.py`：輕量 OPC UA shim（可替換為更完整實作）。
	- `core/controllers/`：專案、資料與匯入/匯出邏輯（AppController 與 DataBroker）。
- `ui/`：PyQt6 UI 元件、對話框與工具（`dialogs/`、`components.py`、`dragdrop_tree.py` 等）。
- `archived_py/`：歸檔的舊版本或備份程式碼（保留以便參考）。
- `Project.json`（若存在）：範例專案設定（app 啟動時若無專案會嘗試載入）。

**設計重點與實作觀察**
- Modbus 實作：以同步 pymodbus API 為基底，透過 `asyncio.to_thread` 包裝成非同步介面；提供重試、連線重試與地址格式（如 400005）處理。
- OPC UA：目前內建一個簡化的 shim 類 (`OPCServer`) 提供最小的介面以供 UI 使用，實際部署時可替換為完整 server 庫。
- UI 與核心分離：`app.py` 大量使用 controller/ broker 來處理資料，UI 主要負責呈現與事件路由。
- 診斷支援：`core/diagnostics.py` 可收集與匯出運行資料，對除錯與連線問題很有幫助。

**開發與除錯建議**
- 若要測試 Modbus 通訊，先建立模擬伺服器（或使用已知的實體裝置），確認 `requirements.txt` 中的 `pymodbus` 與 `pyserial` 版本相容。
- 若要替換 OPC UA 為真實伺服器，請在 `core/OPC_UA/OPC_UA.py` 或 `archived_py/` 找到較完整實作並整合。
- 在開發時建議打開診斷記錄並檢查 `DiagnosticsManager` 以捕捉重試/連線細節。

**測試**
- 若有 pytest 測試，可執行：

```powershell
python -m pytest
```

**授權與聯絡**
- 授權：請參考專案根目錄的 `LICENSE`。
- 作者/維護者：lioil1020（倉庫內註記）。

---

如果你希望我把 README 再細分成「使用者導向快速教學」與「開發者導向設置說明」，或補上範例 Project.json 與常見問題段落，我可以接著加上範本與範例設定。 
