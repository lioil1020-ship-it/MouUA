# ModUA

Modbus to OPC UA bridge / utility.

簡介
- 這個專案將 Modbus 裝置與 OPC UA 整合，可作為橋接或工具使用。

快速開始
1. 建議建立虛擬環境並安裝套件：
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```
2. 執行應用程式：
```powershell
python app.py
```

注意
- 本倉庫已忽略常見的暫存檔與安裝程式（見 `.gitignore`）。

聯絡
- 作者: lioil1020

---

## Cleanup notes (automated tidy)
- `quick_diag_trace.py` and `inspect_trace.py` were moved to `deprecated/` during cleanup.
- `MonitorWindow` implementation was moved to `deprecated/moved_monitor_window.py` and replaced with a lightweight stub in `app.py` to avoid dynamic import/runtime issues while keeping a reference for restoration.

## Diagnostics and tests
- Diagnostics are centralized through `core/diagnostics.py`. UI windows register as listeners and you can export buffered records via the menu or `DiagnosticsManager.export_to_txt`.
- Sync Modbus client helpers now block when an asyncio loop is already running; prefer the async methods inside event loops.
- New tests cover diagnostics buffering and representative decode paths. Run `python -m pytest` from the repository root.
