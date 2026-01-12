import threading
import time
import uuid
import re
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Any


@dataclass
class DiagnosticRecord:
    timestamp: str
    text: str
    context: Optional[Any] = None


class DiagnosticsManager:
    """Thread-safe diagnostics aggregator with pluggable listeners.

    - `emit(text, context=None)` records a message and forwards it to listeners.
    - Listeners can provide a matcher to filter messages they want to receive.
    - `export_to_txt(path)` writes all buffered records to a tab-separated file.
    """

    def __init__(self, capacity: int = 5000, logger=None, only_txrx: bool = False):
        self._lock = threading.RLock()
        self._capacity = max(1, int(capacity))
        self._logger = logger
        self._only_txrx = bool(only_txrx)
        self._records: List[DiagnosticRecord] = []
        self._listeners: dict[str, dict[str, Any]] = {}

    def set_only_txrx(self, value: bool):
        with self._lock:
            self._only_txrx = bool(value)

    def register_listener(self, name: str, callback: Callable[[str, str], None], matcher: Optional[Callable[[str, Optional[Any]], bool]] = None) -> str:
        token = str(uuid.uuid4())
        with self._lock:
            self._listeners[token] = {"name": name, "callback": callback, "matcher": matcher}
        return token

    def unregister_listener(self, token: str):
        with self._lock:
            self._listeners.pop(token, None)

    def clear(self):
        with self._lock:
            self._records.clear()

    def snapshot(self) -> List[DiagnosticRecord]:
        with self._lock:
            return list(self._records)

    def _should_emit(self, text: str, context: Optional[Any] = None) -> bool:
        if not self._only_txrx:
            return True
        t = text or ""
        if ("TX:" in t) or ("RX:" in t):
            return True
        try:
            if isinstance(context, dict):
                dir_val = str(context.get("direction") or "").upper()
                if dir_val in ("TX", "RX"):
                    return True
        except Exception:
            pass
        return False

    def _parse_txrx_context(self, text: str, context: Optional[Any]) -> Optional[dict]:
        """Parse TX/RX text into structured context (fc/unit/host/port/hex/length).

        Merges any provided dict `context` and returns the merged dict, or None when
        parsing does not yield useful metadata.
        """
        base_ctx = context if isinstance(context, dict) else {}
        merged = dict(base_ctx)
        try:
            s = str(text or "")
        except Exception:
            s = ""

        try:
            m = re.search(r"\b(TX|RX)\s*:\s*\|\s*([0-9A-Fa-f\s]+)\s*\|", s)
        except Exception:
            m = None

        if not m:
            return merged if merged else None

        direction = (m.group(1) or "").upper()
        hex_part = m.group(2) or ""
        merged.setdefault("direction", direction)

        data_bytes_list = []
        try:
            parts = [p for p in hex_part.split() if p]
            for p in parts:
                try:
                    data_bytes_list.append(int(p, 16))
                except Exception:
                    continue
        except Exception:
            parts = []

        data_bytes = bytes(data_bytes_list)
        merged.setdefault("hex", " ".join(f"{b:02X}" for b in data_bytes) if data_bytes else hex_part.strip())
        merged.setdefault("length", len(data_bytes))
        merged.setdefault("raw_text", s)

        # parse host/port/unit hints from the text prefix when present
        try:
            mh = re.search(r"HOST=([^\s]+)", s)
            if mh:
                merged.setdefault("host", mh.group(1))
        except Exception:
            pass
        try:
            mp = re.search(r"PORT=([0-9]+)", s)
            if mp:
                try:
                    merged.setdefault("port", int(mp.group(1)))
                except Exception:
                    merged.setdefault("port", mp.group(1))
        except Exception:
            pass
        try:
            mu = re.search(r"UNIT=([0-9]+)", s, flags=re.IGNORECASE)
            if mu:
                try:
                    merged.setdefault("unit", int(mu.group(1)))
                except Exception:
                    merged.setdefault("unit", mu.group(1))
        except Exception:
            pass
        try:
            md = re.search(r"DEV_ID=([0-9]+)", s, flags=re.IGNORECASE)
            if md:
                try:
                    merged.setdefault("dev_id", int(md.group(1)))
                except Exception:
                    merged.setdefault("dev_id", md.group(1))
        except Exception:
            pass

        # infer unit/function code from frame bytes
        fc = None
        unit_from_frame = None
        try:
            if data_bytes:
                if len(data_bytes) >= 7:
                    try:
                        fc = int(data_bytes[7])
                        unit_from_frame = int(data_bytes[6])
                    except Exception:
                        fc = None
                if fc is None and len(data_bytes) >= 2:
                    try:
                        fc = int(data_bytes[1])
                        unit_from_frame = int(data_bytes[0])
                    except Exception:
                        fc = None
        except Exception:
            fc = None

        try:
            if unit_from_frame is not None and "unit" not in merged:
                merged["unit"] = unit_from_frame
        except Exception:
            pass

        try:
            if fc is not None:
                merged.setdefault("fc", fc)
                merged.setdefault("fc_supported", fc in (1, 2, 3, 4, 5, 6, 15, 16))
        except Exception:
            pass

        return merged if merged else None

    def emit(self, text: str, context: Optional[Any] = None, timestamp: Optional[str] = None):
        if text is None:
            return
        if not self._should_emit(str(text), context):
            return

        if timestamp is None:
            try:
                now = time.time()
                ms = int((now - int(now)) * 1000)
                timestamp = time.strftime("%H:%M:%S", time.localtime(now)) + f".{ms:03d}"
            except Exception:
                timestamp = ""

        try:
            parsed_ctx = self._parse_txrx_context(text, context)
        except Exception:
            parsed_ctx = context

        record = DiagnosticRecord(timestamp=timestamp, text=str(text), context=parsed_ctx)

        listeners: List[dict[str, Any]]
        with self._lock:
            self._records.append(record)
            if len(self._records) > self._capacity:
                self._records = self._records[-self._capacity:]
            listeners = list(self._listeners.values())

        # forward to listeners outside the lock
        for item in listeners:
            try:
                matcher = item.get("matcher")
                if matcher is not None and not matcher(record.text, record.context):
                    continue
                cb = item.get("callback")
                if cb:
                    try:
                        cb(record.timestamp, record.text, record.context)
                    except TypeError:
                        try:
                            cb(record.timestamp, record.text)
                        except Exception:
                            pass
                    except Exception:
                        pass
            except Exception:
                # keep diagnostics resilient; optionally log to provided logger
                try:
                    if self._logger:
                        self._logger.exception("Diagnostics listener failed")
                except Exception:
                    pass

        try:
            if self._logger:
                self._logger.debug(record.text)
        except Exception:
            pass

    def export_to_txt(self, path: str):
        with self._lock:
            rows = list(self._records)
        with open(path, "w", encoding="utf-8") as f:
            f.write("Date\tTime\tMessage\n")
            f.write("-" * 80 + "\n")
            for r in rows:
                try:
                    date_part = ""
                    if r.timestamp and len(r.timestamp) >= 8:
                        # prepend current date (local) for readability
                        date_part = time.strftime("%Y/%m/%d")
                    time_part = r.timestamp or ""
                    f.write(f"{date_part}\t{time_part}\t{r.text}\n")
                except Exception:
                    continue

    def stop(self):
        with self._lock:
            self._listeners.clear()
            self._records.clear()
