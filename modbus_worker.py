from PyQt6.QtCore import QObject, pyqtSignal, QTimer, Qt
import time
import struct
import asyncio
import threading
import math
import uuid

# Global bus lock registry to serialize requests per physical/logical connection
# Keying: ('serial', serial_port) or ('tcp', host, port)
_BUS_LOCKS: dict = {}
_BUS_LOCKS_REG_LOCK = threading.Lock()

class _BusLock:
    def __init__(self, lock: threading.Lock):
        self._lock = lock

    async def __aenter__(self):
        # Acquire the threading.Lock in a thread to avoid blocking the event loop
        await asyncio.to_thread(self._lock.acquire)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            self._lock.release()
        except Exception:
            pass

def get_bus_lock(host=None, port=None, client_params=None, client_mode=None):
    """Return an async context manager for the bus lock for this connection.

    Prefer serial key if client_mode indicates RTU or client_params contains
    a serial port; otherwise use TCP host:port tuple.
    """
    # detect serial identifier
    serial_id = None
    try:
        cm = (client_mode or "").strip().lower() if client_mode is not None else ""
        if cm in ("rtu", "serial"):
            # try client_params for explicit port name
            if isinstance(client_params, dict):
                serial_id = client_params.get("serial_port") or client_params.get("com") or client_params.get("adapter") or client_params.get("port")
        if serial_id is None and isinstance(client_params, dict):
            # some mappings use 'serial_port' or 'com'
            serial_id = client_params.get("serial_port") or client_params.get("com") or client_params.get("adapter")
        # normalize numeric COM names
        if serial_id is not None:
            try:
                s = str(serial_id).strip()
                if s.isdigit():
                    serial_id = f"COM{int(s)}"
                else:
                    serial_id = s
            except Exception:
                pass
    except Exception:
        serial_id = None

    if serial_id:
        key = ("serial", serial_id)
    else:
        key = ("tcp", str(host), int(port) if port is not None else 0)

    with _BUS_LOCKS_REG_LOCK:
        lk = _BUS_LOCKS.get(key)
        if lk is None:
            lk = threading.Lock()
            _BUS_LOCKS[key] = lk
    return _BusLock(lk)

def decode_bytes_for_tag(base_dtype, db_slice, arr_len=1, is_bits=False, start_bit=0, bit_order_enable=False, treat_long=False, diag_callback=None):
    """Decode a bytes slice for a tag according to dtype and flags.

    base_dtype: string like 'Float', 'Long', 'Word', 'Boolean'
    db_slice: bytes containing the raw normalized data for that tag slice
    arr_len: number of elements expected
    is_bits: True when decoding bit arrays
    start_bit: starting bit offset inside first byte
    bit_order_enable: when True, use Modicon MSB-first bit ordering
    treat_long: when True, decode 32-bit as High*10000 + Low
    """
    try:
        # defensive normalizations
        if db_slice is None:
            return None
        if is_bits:
            elems = []
            try:
                total_bits = int(arr_len)
            except Exception:
                total_bits = 0
            if total_bits <= 0:
                return None
            for i in range(total_bits):
                abs_bit = start_bit + i
                byte_idx = abs_bit // 8
                bit_idx = abs_bit % 8
                try:
                    b = db_slice[byte_idx]
                    if bit_order_enable:
                        bit_val = (b >> (7 - bit_idx)) & 1
                    else:
                        bit_val = (b >> bit_idx) & 1
                    elems.append(bool(bit_val))
                except Exception:
                    elems.append(False)
            return elems if len(elems) > 1 else elems[0]

        elems = []
        base_l = (base_dtype or "").lower()
        if ("float" in base_l) or (base_l.strip() == "f"):
            elem_bytes = 4
            for i in range(arr_len):
                start = i * elem_bytes
                chunk = db_slice[start:start + elem_bytes]
                if len(chunk) < elem_bytes:
                    break
                try:
                    # Try multiple byte/word permutations to auto-detect correct ordering
                    def _candidates(b):
                        # b is 4-byte sequence representing two 2-byte registers
                        # variants: original, swap words, swap bytes-in-words, swap both
                        if len(b) != 4:
                            return [None]
                        w0 = b[0:2]
                        w1 = b[2:4]
                        orig = b
                        swap_words = w1 + w0
                        swap_bytes = w0[::-1] + w1[::-1]
                        swap_both = (w1[::-1] + w0[::-1])
                        return [orig, swap_words, swap_bytes, swap_both]

                    cand_vals = []
                    seen = []
                    for c in _candidates(chunk):
                        try:
                            v = struct.unpack(">f", c)[0]
                        except Exception:
                            try:
                                v = float(int.from_bytes(c, 'big', signed=False))
                            except Exception:
                                v = None
                        cand_vals.append((c, v))
                        seen.append(v)

                    # choose best candidate: prefer finite, non-NaN, and magnitude between 1e-6 and 1e6
                    def _is_reasonable(v):
                        try:
                            if v is None:
                                return False
                            if isinstance(v, float):
                                if v != v:
                                    return False
                                if abs(v) < 1e-9:
                                    return False
                                if abs(v) > 1e7:
                                    return False
                            return True
                        except Exception:
                            return False

                    chosen = None
                    for c, v in cand_vals:
                        if _is_reasonable(v):
                            chosen = v
                            break
                    if chosen is None:
                        # fallback: pick first non-None
                        for c, v in cand_vals:
                            if v is not None:
                                chosen = v
                                break
                    elems.append(chosen)
                except Exception:
                    elems.append(None)
        elif any(k in base_l for k in ("double", "qword", "llong", "int64")):
            elem_bytes = 8
            for i in range(arr_len):
                start = i * elem_bytes
                chunk = db_slice[start:start + elem_bytes]
                if len(chunk) < elem_bytes:
                    break
                try:
                    # Try variants for 8-byte double: original and swapped 4-word halves and byte swaps
                    def _double_candidates(b):
                        if len(b) != 8:
                            return [None]
                        # treat as 4x2-byte words: w0,w1,w2,w3
                        w = [b[i:i+2] for i in range(0,8,2)]
                        orig = b
                        swap_words_pair = b[4:8] + b[0:4]
                        swap_bytes_each = b[0:2][::-1] + b[2:4][::-1] + b[4:6][::-1] + b[6:8][::-1]
                        swap_both = swap_bytes_each[4:8] + swap_bytes_each[0:4]
                        return [orig, swap_words_pair, swap_bytes_each, swap_both]

                    cand_vals = []
                    for c in _double_candidates(chunk):
                        try:
                            v = struct.unpack(">d", c)[0]
                        except Exception:
                            try:
                                v = float(int.from_bytes(c, 'big', signed=False))
                            except Exception:
                                v = None
                        cand_vals.append((c, v))

                    chosen = None
                    def _is_reasonable_d(v):
                        try:
                            if v is None:
                                return False
                            if isinstance(v, float):
                                if v != v:
                                    return False
                                if abs(v) < 1e-9:
                                    return False
                                if abs(v) > 1e12:
                                    return False
                            return True
                        except Exception:
                            return False

                    for c, v in cand_vals:
                        if _is_reasonable_d(v):
                            chosen = v
                            break
                    if chosen is None:
                        for c, v in cand_vals:
                            if v is not None:
                                chosen = v
                                break
                    elems.append(chosen)
                except Exception:
                    elems.append(None)
        elif any(k in base_l for k in ("long", "dword", "int32")):
            elem_bytes = 4
            for i in range(arr_len):
                start = i * elem_bytes
                chunk = db_slice[start:start + elem_bytes]
                if len(chunk) < elem_bytes:
                    break
                try:
                    if treat_long:
                        high = int.from_bytes(chunk[0:2], "big", signed=False)
                        low = int.from_bytes(chunk[2:4], "big", signed=False)
                        elems.append(high * 10000 + low)
                    else:
                        elems.append(int.from_bytes(chunk, "big", signed=False))
                except Exception:
                    elems.append(None)
        else:
            elem_bytes = 2
            for i in range(arr_len):
                start = i * elem_bytes
                chunk = db_slice[start:start + elem_bytes]
                if len(chunk) < elem_bytes:
                    break
                try:
                    elems.append(int.from_bytes(chunk, "big", signed=False))
                except Exception:
                    elems.append(None)

        if not elems:
            return None
        return elems if len(elems) > 1 else elems[0]
    except Exception:
        return None



class AsyncPoller(QObject):
    """Async poller that runs the polling loop as asyncio Tasks.

    - If an asyncio loop is already running in the current thread, the poller
      creates a task on that loop.
    - Otherwise it falls back to starting a background thread that runs its
      own asyncio loop and executes the same coroutine.

    Emits `tag_polled(tag_item, value, timestamp, quality)` when a read
    completes, and `diag_signal(str)` for diagnostics.
    """

    tag_polled = pyqtSignal(object, object, float, str)
    diag_signal = pyqtSignal(str)

    def __init__(self, controller, host="127.0.0.1", port=502, unit=1, interval=1.0):
        super().__init__()
        self.controller = controller
        self.host = host
        self.port = port
        self.unit = unit
        self.interval = float(interval)
        self._running = False
        self._tags = []
        self._task = None
        self._bg_thread = None
        self._bg_loop = None
        self._in_main_loop = False
        self._current_tag = None
        self._last_emit_times = {}

    def add_tag(self, tag_item):
        if tag_item not in self._tags:
            self._tags.append(tag_item)
            try:
                self._emit_diag(f"Poller: added tag {getattr(tag_item, 'text', lambda x: '?')(0)}")
            except Exception:
                pass

    def remove_tag(self, tag_item):
        try:
            self._tags.remove(tag_item)
        except ValueError:
            pass

    def clear_tags(self):
        self._tags = []

    def set_connection(self, host, port, unit):
        self.host = host
        self.port = port
        self.unit = unit

    def set_interval(self, interval_seconds):
        self.interval = float(interval_seconds)

    def _emit_diag(self, text):
        # If we are running in the main asyncio loop (i.e. same thread as Qt),
        # emitting signals directly is fine. Otherwise post to Qt main thread.
        if self._in_main_loop:
            try:
                self.diag_signal.emit(text)
            except Exception:
                pass
        else:
            # emitting the Qt signal from a background thread is thread-safe
            # (it will be queued to the main thread). Just use emit directly.
            try:
                self.diag_signal.emit(text)
            except Exception:
                pass

        # NOTE: RX parsing from pymodbus Processing lines is intentionally
        # NOT used to emit `tag_polled` directly. We rely on the read result
        # (`res.data_bytes`) path in the poller loop which applies device
        # encoding settings consistently. Keeping RX diagnostics here only
        # (no tag_polled emission) prevents duplicate/conflicting values.

    def _emit_tag_polled(self, tag, value, ts, quality):
        def _safe_emit(tg, val, tstamp, qual):
            try:
                self.tag_polled.emit(tg, val, tstamp, qual)
                try:
                    self._last_emit_times[id(tg)] = time.time()
                except Exception:
                    pass
            except Exception as e:
                try:
                    import traceback

                    tb = traceback.format_exc()
                    self._emit_diag(f"TAG_POLLED_EMIT_ERROR: {e}\n{tb}")
                except Exception:
                    pass

        # Always emit directly (Qt will queue across threads when needed).
        try:
            try:
                # emit a concise diagnostic for every tag emission
                try:
                    self.diag_signal.emit(f"EMIT_TAG_POLLED: id={id(tag)} val={repr(value)} qual={quality}")
                except Exception:
                    pass
            except Exception:
                pass
            _safe_emit(tag, value, ts, quality)
        except Exception as e:
            try:
                self._emit_diag(f"TAG_POLLED_SCHEDULE_ERROR: {e}")
            except Exception:
                pass

    async def _main_loop_async(self):
        # notify loop start (suppressed to keep diagnostics compact)

        while self._running:
            # per-iteration diagnostics suppressed to avoid noise
            start = time.time()
            tags_snapshot = list(self._tags)
            try:
                self._emit_diag(f"LOOP_TAG_COUNT: {len(tags_snapshot)}")
            except Exception:
                pass
            # Build per-tag metadata (offset/count/fc) so we can group and merge reads
            tag_infos = []
            import re

            def _parse_tag_address_and_dtype(tag_item):
                try:
                    addr_raw = tag_item.data(1, Qt.ItemDataRole.UserRole)
                except Exception:
                    addr_raw = None
                try:
                    dtype = (tag_item.data(2, Qt.ItemDataRole.UserRole) or "Word")
                except Exception:
                    dtype = "Word"

                def _digits(s):
                    if s is None:
                        return ""
                    return "".join(ch for ch in str(s) if ch.isdigit())

                # extract bracketed length from address like 40095[3]
                array_len_from_addr = None
                try:
                    m_addr = re.search(r"\[\s*(\d+)\s*\]", str(addr_raw))
                    if m_addr:
                        array_len_from_addr = int(m_addr.group(1))
                        addr_no_brackets = re.sub(r"\[\s*\d+\s*\]", "", str(addr_raw))
                    else:
                        addr_no_brackets = str(addr_raw)
                except Exception:
                    addr_no_brackets = str(addr_raw)

                nums = _digits(addr_no_brackets)
                if len(nums) == 5 and nums.startswith("4"):
                    nums = nums[0] + "0" + nums[1:]
                nums = nums.zfill(6)
                lead = nums[0]

                # Determine zero-based flags from Device
                zero_based = False
                zero_based_bit = True
                try:
                    dev = tag_item.parent()
                    while dev is not None and dev.data(0, Qt.ItemDataRole.UserRole) != "Device":
                        dev = dev.parent()
                    if dev is not None:
                        da = dev.data(5, Qt.ItemDataRole.UserRole) or {}
                        try:
                            zb_raw = da.get("zero_based", "Disable")
                            zb_s = str(zb_raw).strip().lower()
                            # Kepware mapping: UI 'Disable' -> send raw_index (no +1)
                            #                 UI 'Enable'  -> send raw_index + 1
                            if zb_s == "enable":
                                zero_based = True
                            else:
                                zero_based = False
                        except Exception:
                            zero_based = False
                        try:
                            zb_bit_raw = da.get("zero_based_bit", "Enable")
                            zero_based_bit = str(zb_bit_raw).strip().lower() == "enable"
                        except Exception:
                            zero_based_bit = True
                except Exception:
                    pass

                try:
                    raw_index = int(nums[1:])
                except Exception:
                    raw_index = 0
                # Apply requested zero-based semantics:
                # - UI 'Enable' -> send raw_index - 1
                # - UI 'Disable' -> send raw_index unchanged
                try:
                    if zero_based:
                        offset = max(0, raw_index - 1)
                    else:
                        offset = max(0, raw_index)
                except Exception:
                    offset = max(0, raw_index)
                    

                # parse array length from dtype like Float[3] or Float(Array)
                array_len = 1
                base_dtype = dtype
                try:
                    m = re.match(r"^\s*([A-Za-z0-9_]+)\s*\[\s*(\d+)\s*\]\s*$", str(dtype))
                    if m:
                        base_dtype = m.group(1)
                        array_len = max(1, int(m.group(2)))
                    else:
                        m2 = re.match(r"^\s*([A-Za-z0-9_]+)\s*\(\s*array\s*\)\s*$", str(dtype), flags=re.IGNORECASE)
                        if m2:
                            base_dtype = m2.group(1)
                except Exception:
                    base_dtype = dtype

                try:
                    if array_len_from_addr is not None:
                        array_len = max(1, int(array_len_from_addr))
                except Exception:
                    pass

                # Enforce address-prefix semantics: addresses starting with
                # '0' or '1' are boolean/coils and should use FC1/FC2.
                try:
                    if str(lead) in ("0", "1"):
                        base_dtype = "Boolean"
                        array_len = max(1, int(array_len))
                except Exception:
                    pass

                # determine per-element registers and function code
                # Determine per-element registers and function code strictly
                # based on Kepware address prefix semantics:
                # - 0xxxx -> coils (FC1) boolean
                # - 1xxxx -> discrete inputs (FC2) boolean
                # - 3xxxx -> input registers (FC4) read-only
                # - 4xxxx -> holding registers (FC3)
                try:
                    if str(lead) in ("0", "1"):
                        # Force boolean semantics for coil/discrete
                        per_elem_regs = 1
                        fc = 1 if str(lead) == "0" else 2
                        base_dtype = "Boolean"
                    else:
                        dt = base_dtype.lower()
                        if any(k in dt for k in ("double", "qword", "llong")):
                            per_elem_regs = 4
                        elif any(k in dt for k in ("long", "dword", "float")):
                            per_elem_regs = 2
                        else:
                            per_elem_regs = 1
                        # Strict mapping by leading digit
                        if str(addr_no_brackets).strip().startswith("3") or lead == "3":
                            fc = 4
                        elif lead == "4":
                            fc = 3
                        else:
                            fc = 3
                except Exception:
                    # fallback conservative defaults
                    per_elem_regs = 1
                    fc = 3

                # Adjust offset semantics for bit vs register addressing
                try:
                    if "Boolean" in dtype or dtype.lower().startswith("bool"):
                        if zero_based_bit:
                            offset = max(0, raw_index)
                        else:
                            offset = max(0, raw_index - 1)
                    else:
                        if zero_based:
                            offset = max(0, raw_index - 1)
                        else:
                            offset = max(0, raw_index)
                except Exception:
                    pass

                count = per_elem_regs * max(1, int(array_len))
                return offset, count, fc, per_elem_regs, base_dtype, int(array_len), tag_item

            for tag in tags_snapshot:
                try:
                    info = _parse_tag_address_and_dtype(tag)
                    tag_infos.append(info)
                    # emit mapping diagnostic for each parsed tag
                    try:
                        off, cnt, fc, per_regs, base_dtype, array_len, tag_item = info
                        try:
                            self._emit_diag(f"TAG_MAPPING: addr={tag.data(1, Qt.ItemDataRole.UserRole)} -> offset={off} count={cnt} fc={fc} per_elem_regs={per_regs} base_dtype={base_dtype} array_len={array_len}")
                        except Exception:
                            pass
                    except Exception:
                        pass
                except Exception:
                    try:
                        self._emit_diag(f"ERR parsing tag addr/dtype {getattr(tag, 'text', lambda x: '?')(0)}")
                    except Exception:
                        pass

            # group tags by connection (host,port,unit) and function code and device parent
            groups = {}
            for (offset, count, fc, per_elem_regs, base_dtype, array_len, tag_item) in tag_infos:
                try:
                    # determine connection details
                    host = self.host
                    port = self.port
                    unit = self.unit
                    dev = tag_item.parent()
                    while dev is not None and dev.data(0, Qt.ItemDataRole.UserRole) != "Device":
                        dev = dev.parent()
                    if dev is not None:
                        eth = dev.data(8, Qt.ItemDataRole.UserRole)
                        if isinstance(eth, dict) and eth:
                            host = eth.get("ip") or eth.get("host") or eth.get("address") or host
                            try:
                                port = int(eth.get("port", port))
                            except Exception:
                                pass
                        else:
                            ch = dev.parent()
                            if ch is not None and ch.data(0, Qt.ItemDataRole.UserRole) == "Channel":
                                ch_params = ch.data(2, Qt.ItemDataRole.UserRole)
                                if isinstance(ch_params, dict):
                                    host = ch_params.get("ip") or ch_params.get("host") or ch_params.get("address") or host
                                    try:
                                        port = int(ch_params.get("port", port))
                                    except Exception:
                                        pass
                        try:
                            unit_val = dev.data(2, Qt.ItemDataRole.UserRole)
                            if unit_val is not None:
                                unit = int(unit_val)
                        except Exception:
                            pass
                    key = (str(host), int(port), int(unit), int(fc), id(dev))
                    groups.setdefault(key, []).append((offset, count, per_elem_regs, base_dtype, array_len, tag_item, dev))
                except Exception:
                    pass

            # Emit diagnostic of built group keys so we can see which FC groups exist
            try:
                self._emit_diag(f"GROUP_KEYS: {list(groups.keys())}")
            except Exception:
                pass


            # wrapper that delegates to module-level decoder
            def _decode_bytes_for_tag(*args, **kwargs):
                try:
                    return decode_bytes_for_tag(*args, **kwargs)
                except Exception:
                    return None

            # iterate grouped connections
            for key, items in groups.items():
                host, port, unit, fc, dev_id = key
                try:
                    self._emit_diag(f"PROCESS_GROUP: host={host} port={port} unit={unit} fc={fc} dev_id={dev_id} members={len(items)}")
                except Exception:
                    pass
                # sort by offset
                items.sort(key=lambda x: x[0])

                # determine block size limit from device if present
                # items entries: (offset, count, per_elem_regs, base_dtype, array_len, tag_item, dev)
                # items[0][5] is tag_item, items[0][6] is the Device item
                dev = items[0][6]
                try:
                    bs = dev.data(7, Qt.ItemDataRole.UserRole) or {}
                except Exception:
                    bs = {}
                # Normalize block_sizes keys to canonical names so UI field names
                # like 'Holding Registers' or 'holding_registers' are respected.
                def _norm_block_sizes(raw):
                    out = {}
                    try:
                        if raw is None:
                            return out
                        # if user supplied a single int, apply to both register types
                        if isinstance(raw, (int, float)):
                            v = int(raw)
                            out['hold_regs'] = v
                            out['int_regs'] = v
                            out['out_coils'] = v
                            out['in_coils'] = v
                            return out
                        if isinstance(raw, str) and raw.strip().isdigit():
                            v = int(raw.strip())
                            out['hold_regs'] = v
                            out['int_regs'] = v
                            out['out_coils'] = v
                            out['in_coils'] = v
                            return out
                        # if it's a dict, map common variants
                        if isinstance(raw, dict):
                            for k, val in raw.items():
                                if val is None:
                                    continue
                                lk = str(k).strip().lower()
                                try:
                                    vi = int(val)
                                except Exception:
                                    # try numeric inside string
                                    try:
                                        vi = int(str(val).strip())
                                    except Exception:
                                        continue
                                if 'hold' in lk or 'holding' in lk:
                                    out['hold_regs'] = vi
                                elif 'input' in lk and ('reg' in lk or 'int' in lk):
                                    out['int_regs'] = vi
                                elif 'int' in lk and 'reg' in lk:
                                    out['int_regs'] = vi
                                elif 'out' in lk and 'coil' in lk:
                                    out['out_coils'] = vi
                                elif 'in' in lk and 'coil' in lk:
                                    out['in_coils'] = vi
                                elif 'coil' in lk and 'out' not in lk and 'in' not in lk:
                                    # generic coil value -> apply to both
                                    out['out_coils'] = vi
                                    out['in_coils'] = vi
                                # accept some short keys
                                elif lk in ('hold_regs', 'holding_regs', 'holding', 'hold'):
                                    out['hold_regs'] = vi
                                elif lk in ('int_regs', 'input_regs', 'input_registers'):
                                    out['int_regs'] = vi
                                elif lk in ('out_coils', 'output_coils'):
                                    out['out_coils'] = vi
                                elif lk in ('in_coils', 'input_coils', 'discrete_inputs'):
                                    out['in_coils'] = vi
                        # if certain keys missing, fill defaults later
                    except Exception:
                        pass
                    return out

                # emit raw stored value for debugging
                try:
                    try:
                        raw_bs = dev.data(7, Qt.ItemDataRole.UserRole)
                    except Exception:
                        pass
                except Exception:
                    pass

                bs = _norm_block_sizes(bs)
                # normalized block sizes are available in `bs` (no diagnostic emitted)

                # also read device encoding and data_access so we can pass them to controller/client
                try:
                    encoding = dev.data(6, Qt.ItemDataRole.UserRole) or {}
                except Exception:
                    encoding = {}
                try:
                    data_access = dev.data(5, Qt.ItemDataRole.UserRole) or {}
                except Exception:
                    data_access = {}

            # fallback: if normalization produced empty and there might be alternative locations,
            # try reading common alternate roles/indices (4:timing,2:params,3:description)
            try:
                if not bs:
                    alt = None
                    try:
                        alt = dev.data(2, Qt.ItemDataRole.UserRole)
                    except Exception:
                        alt = None
                    try:
                        if not alt:
                            alt = dev.data(3, Qt.ItemDataRole.UserRole)
                    except Exception:
                        pass
                    try:
                        if not alt:
                            alt = dev.data(4, Qt.ItemDataRole.UserRole)
                    except Exception:
                        pass
                    if alt:
                        alt_norm = _norm_block_sizes(alt)
                        if alt_norm:
                            bs = alt_norm
                            try:
                                self._emit_diag(f"BLOCK_SIZES_NORMALIZED_FROM_ALT: {bs}")
                            except Exception:
                                pass
            except Exception:
                pass

            # Final fallback: check for an attached model stored at UserRole+1
            try:
                if not bs and dev is not None:
                    try:
                        mdl = dev.data(0, Qt.ItemDataRole.UserRole + 1)
                    except Exception:
                        mdl = None
                    found = None
                    if mdl is not None:
                        try:
                            # if model is a dict-like
                            if isinstance(mdl, dict):
                                for k in ("block_sizes", "blockSizes", "blocks", "block_sizes_map"):
                                    if k in mdl and mdl.get(k):
                                        found = mdl.get(k)
                                        break
                            else:
                                # try attributes or to_dict
                                if hasattr(mdl, "block_sizes"):
                                    found = getattr(mdl, "block_sizes")
                                elif hasattr(mdl, "to_dict"):
                                    try:
                                        md = mdl.to_dict()
                                        for k in ("block_sizes", "blockSizes", "blocks"):
                                            if k in md and md.get(k):
                                                found = md.get(k)
                                                break
                                    except Exception:
                                        pass
                        except Exception:
                            found = None
                    if found:
                        f_norm = _norm_block_sizes(found)
                        if f_norm:
                            bs = f_norm
                            try:
                                self._emit_diag(f"BLOCK_SIZES_NORMALIZED_FROM_MODEL: {bs}")
                            except Exception:
                                pass
            except Exception:
                pass
            # Map function code to block size parameter from Device block_sizes
            try:
                if fc in (3,):
                    # Holding registers
                    max_block = int(bs.get('hold_regs', 120) or 120)
                elif fc in (4,):
                    # Input registers
                    max_block = int(bs.get('int_regs', 120) or 120)
                elif fc in (1,):
                    # Coils (outputs)
                    max_block = int(bs.get('out_coils', 2000) or 2000)
                elif fc in (2,):
                    # Discrete inputs (input coils)
                    max_block = int(bs.get('in_coils', 2000) or 2000)
                else:
                    max_block = int(bs.get('hold_regs', 120) or 120)
            except Exception:
                max_block = 120

            # merge contiguous ranges respecting block size
            merged_ranges = []  # list of (start, count, list_of_items)
            cur_start = None
            cur_end = None
            cur_items = []
            for off, cnt, per_regs, base_dtype, arr_len, tag_item, dev in items:
                if cur_start is None:
                    cur_start = off
                    cur_end = off + cnt
                    cur_items = [(off, cnt, per_regs, base_dtype, arr_len, tag_item)]
                    continue
                # if next begins before or at cur_end, extend
                if off <= cur_end:
                    cur_end = max(cur_end, off + cnt)
                    cur_items.append((off, cnt, per_regs, base_dtype, arr_len, tag_item))
                else:
                    # gap between cur_end and off. decide if we can merge within block
                    gap = off - cur_end
                    potential_count = (off + cnt) - cur_start
                    if potential_count <= max_block:
                        # merge including gap (will read unused registers)
                        cur_end = off + cnt
                        cur_items.append((off, cnt, per_regs, base_dtype, arr_len, tag_item))
                    else:
                        merged_ranges.append((cur_start, cur_end - cur_start, cur_items))
                        cur_start = off
                        cur_end = off + cnt
                        cur_items = [(off, cnt, per_regs, base_dtype, arr_len, tag_item)]
            if cur_start is not None and cur_items:
                merged_ranges.append((cur_start, cur_end - cur_start, cur_items))

            # for each merged range, perform chunked reads not exceeding max_block and decode overlapping members
            for start, total_count, members in merged_ranges:
                try:
                    # determine chunking parameters
                    chunk_size = max_block if max_block and max_block > 0 else total_count
                    chunk_starts = list(range(start, start + total_count, chunk_size))

                    for cstart in chunk_starts:
                        ccount = min(chunk_size, start + total_count - cstart)

                        # build a fake tag object for this chunk
                        class _FakeTag:
                            def __init__(self, addr, dtype, dev):
                                self._addr = addr
                                self._dtype = dtype
                                self._dev = dev

                            def data(self, idx, role=None):
                                if idx == 1:
                                    return str(self._addr)
                                if idx == 2:
                                    return str(self._dtype)
                                return None

                        # map function code to address prefix correctly:
                        # FC1 -> coils (0), FC2 -> discrete inputs (1)
                        # FC3 -> holding registers (4), FC4 -> input registers (3)
                        if fc in (1, 2):
                            fake_dtype = f"Boolean[{ccount}]"
                            prefix = "0" if fc == 1 else "1"
                        else:
                            fake_dtype = f"Word[{ccount}]"
                            prefix = "4" if fc == 3 else "3"
                        fake_addr = f"{prefix}{str(cstart).zfill(5)}"
                        fake_tag = _FakeTag(fake_addr, fake_dtype, dev)

                        # mark current tag to help diagnostics mapping
                        self._current_tag = members[0][5] if members and members[0] and len(members[0]) > 5 else None

                        # determine device timing for this device (map to connect/request timeouts)
                        device_timing = {}
                        try:
                            device_timing = dev.data(4, Qt.ItemDataRole.UserRole) or {}
                        except Exception:
                            device_timing = {}

                        try:
                            mapped_connect_timeout = float(device_timing.get('connect_timeout', 3.0))
                        except Exception:
                            mapped_connect_timeout = 3.0
                        try:
                            if 'req_timeout' in device_timing and device_timing.get('req_timeout') is not None:
                                mapped_request_timeout = max(0.0, float(device_timing.get('req_timeout')) / 1000.0)
                            else:
                                mapped_request_timeout = 1.0
                        except Exception:
                            mapped_request_timeout = 1.0
                        try:
                            inter_delay_local = max(0.0, float(device_timing.get('inter_req_delay', 0)) / 1000.0)
                        except Exception:
                            inter_delay_local = 0.0
                        # attempts and inter-request delay in ms (integers)
                        try:
                            attempts_local = int(device_timing.get('attempts', 1))
                        except Exception:
                            attempts_local = 1
                        try:
                            inter_ms = int(device_timing.get('inter_req_delay', 0))
                        except Exception:
                            inter_ms = 0

                        # determine which member tags overlap this chunk
                        sub_members = []
                        for m in members:
                            off, cnt, per_regs, base_dtype, arr_len, tag_item = m
                            if (off < cstart + ccount) and (off + cnt > cstart):
                                sub_members.append(m)

                        # perform read for this chunk
                        res = None
                        try:
                            # centralize Kepware -> pymodbus mapping
                            from kepware_mapping import map_kepware_to_pymodbus

                            # ch may be None; obtain driver and params when available
                            ch = dev.parent() if dev is not None else None
                            drv = None
                            ch_params = {}
                            if ch is not None and ch.data(0, Qt.ItemDataRole.UserRole) == "Channel":
                                drv = ch.data(1, Qt.ItemDataRole.UserRole)
                                ch_params = ch.data(2, Qt.ItemDataRole.UserRole) or {}

                            client_mode_local, host_local, port_local, client_params_local = map_kepware_to_pymodbus(
                                driver_name=drv,
                                ch_params=ch_params,
                                encoding=encoding,
                                block_sizes=bs,
                                data_access=data_access,
                                device_timing=device_timing,
                                host=host,
                                port=port,
                            )

                            try:
                                self._emit_diag(f"POLL_CALL: mode={client_mode_local} host={host_local} port={port_local} unit={unit} addr={fake_addr}")
                            except Exception:
                                pass
                            try:
                                # wrap diag_callback to include device context so diagnostics
                                # can be filtered per-device (DEV_ID / HOST / PORT)
                                try:
                                    dev_id_ctx = id(dev) if dev is not None else 0
                                except Exception:
                                    dev_id_ctx = 0
                                prefix = f"DEV_ID={dev_id_ctx} HOST={host_local} PORT={port_local} "
                                # assign an operation id so we can correlate logs for a single controller read
                                try:
                                    op_id = uuid.uuid4().hex[:8]
                                except Exception:
                                    op_id = str(time.time())
                                poller_key = getattr(self, '__conn_key__', id(self))
                                prefix2 = f"OP={op_id} POLLER={poller_key} {prefix}"
                                def _diag_with_ctx(msg):
                                    try:
                                        self._emit_diag(prefix2 + str(msg))
                                    except Exception:
                                        try:
                                            self._emit_diag(str(msg))
                                        except Exception:
                                            pass

                                # serialize requests on the same physical/logical bus
                                try:
                                    self._emit_diag(f"BUS_LOCK_LOOKUP: HOST={host_local} PORT={port_local} MODE={client_mode_local}")
                                except Exception:
                                    pass
                                lock = get_bus_lock(host=host_local, port=port_local, client_params=client_params_local, client_mode=client_mode_local)
                                async with lock:
                                    try:
                                        self._emit_diag(f"BUS_LOCK_ACQUIRED: HOST={host_local} PORT={port_local} MODE={client_mode_local}")
                                    except Exception:
                                        pass
                                    res = await self.controller.read_tag_value_async(
                                        fake_tag,
                                        host=host_local,
                                        port=port_local,
                                        unit=unit,
                                        timeout=mapped_request_timeout,
                                        connect_timeout=mapped_connect_timeout,
                                        diag_callback=_diag_with_ctx,
                                        client_mode=client_mode_local,
                                        client_params=client_params_local,
                                        inter_request_delay=inter_delay_local,
                                        )
                                    try:
                                        self._emit_diag(f"BUS_LOCK_RELEASE: HOST={host_local} PORT={port_local} MODE={client_mode_local}")
                                    except Exception:
                                        pass
                            except Exception as e:
                                try:
                                    import traceback
                                    tb = traceback.format_exc()
                                    self._emit_diag(f"POLL_CALL_EXCEPTION: addr={fake_addr} exc={tb}")
                                except Exception:
                                    pass
                                raise
                        except Exception:
                            try:
                                for (_, _, _, _, _, tag_item) in sub_members:
                                    ts = time.time()
                                    self._emit_tag_polled(tag_item, None, ts, "Bad")
                            except Exception:
                                pass
                            continue

                        # obtain normalized bytes for this response
                        db = getattr(res, 'data_bytes', None)
                        regs = []
                        # Special-case coil/discrete responses: many pymodbus versions
                        # expose a `.bits` list rather than `registers` or `data_bytes`.
                        # Convert bit list -> bytes (LSB in byte 0) so decoder can
                        # uniformly parse boolean payloads.
                        try:
                            if fc in (1, 2) and hasattr(res, 'bits'):
                                bits = getattr(res, 'bits') or []
                                if bits:
                                    b_chunks = []
                                    i = 0
                                    while i < len(bits):
                                        byte_val = 0
                                        for j in range(8):
                                            if i + j < len(bits) and bits[i + j]:
                                                byte_val |= (1 << j)
                                        b_chunks.append(byte_val.to_bytes(1, 'big'))
                                        i += 8
                                    db = b"".join(b_chunks)
                        except Exception:
                            pass

                        if not db:
                            try:
                                regs = getattr(res, 'registers', None) or []
                                db = b"".join(int(r & 0xFFFF).to_bytes(2, 'big') for r in regs)
                            except Exception:
                                db = b""
                        else:
                            try:
                                regs = getattr(res, 'registers', None) or []
                            except Exception:
                                regs = []

                        # Emit diagnostic about this chunk's payload vs expected length
                        try:
                            exp_len = None
                            try:
                                # expected bytes correspond to this chunk's register count
                                exp_len = int(ccount) * 2
                            except Exception:
                                exp_len = None
                            self._emit_diag(f"DECODE_CHUNK: addr={fake_addr} fc={fc} chunk_start={cstart} chunk_count={ccount} expected_bytes={exp_len} got_bytes={len(db)} regs_len={len(regs)} regs={list(regs) if hasattr(regs, '__iter__') else regs}")
                        except Exception:
                            pass

                        # read encoding flags for this device
                        try:
                            encoding = dev.data(6, Qt.ItemDataRole.UserRole) or {}
                        except Exception:
                            encoding = {}

                        def _is_enabled(v):
                            if isinstance(v, bool):
                                return v
                            try:
                                return str(v).strip().lower() in ("enable", "true", "1", "yes", "on")
                            except Exception:
                                return False

                        bit_order_enable = _is_enabled(encoding.get('bit_order', False))
                        treat_long = _is_enabled(encoding.get('treat_long', False))

                        # for each overlapping member tag, extract its slice relative to cstart and decode
                        for off, cnt, per_regs, base_dtype, arr_len, tag_item in sub_members:
                            try:
                                rel_off = off - cstart
                                need_fallback = False
                                # compute slice bounds depending on bit/register
                                if fc in (1, 2):
                                    bit_start = rel_off
                                    byte_start = bit_start // 8
                                    need_bits = (bit_start % 8) + cnt
                                    byte_len = math.ceil(need_bits / 8)
                                    # if chunk payload too short for this member, mark fallback
                                    if len(db) < (byte_start + byte_len):
                                        need_fallback = True
                                    slice_bytes = db[byte_start:byte_start + byte_len]
                                    if not need_fallback:
                                        val = _decode_bytes_for_tag(base_dtype, slice_bytes, arr_len, is_bits=True, start_bit=(bit_start % 8), bit_order_enable=bit_order_enable)
                                else:
                                    byte_start = rel_off * 2
                                    byte_len = cnt * 2
                                    if len(db) < (byte_start + byte_len):
                                        need_fallback = True
                                    slice_bytes = db[byte_start:byte_start + byte_len]
                                    if not need_fallback:
                                        val = _decode_bytes_for_tag(base_dtype, slice_bytes, arr_len, is_bits=False, start_bit=0, bit_order_enable=bit_order_enable, treat_long=treat_long)

                                # If this member's bytes are missing or incomplete in the chunk, try a single-tag read fallback
                                if need_fallback:
                                    try:
                                        self._emit_diag(f"CHUNK_MISS_FALLBACK: member_addr={tag_item.data(1, Qt.ItemDataRole.UserRole)} chunk_start={cstart} chunk_count={ccount}")
                                    except Exception:
                                        pass
                                    # perform single-tag read using same client mapping
                                    try:
                                        try:
                                            dev_id_ctx = id(dev) if dev is not None else 0
                                        except Exception:
                                            dev_id_ctx = 0
                                        prefix = f"DEV_ID={dev_id_ctx} HOST={host_local} PORT={port_local} "
                                        try:
                                            op_id2 = uuid.uuid4().hex[:8]
                                        except Exception:
                                            op_id2 = str(time.time())
                                        poller_key2 = getattr(self, '__conn_key__', id(self))
                                        prefix_single = f"OP={op_id2} POLLER={poller_key2} {prefix}"
                                        def _diag_single_with_ctx(msg):
                                            try:
                                                self._emit_diag(prefix_single + str(msg))
                                            except Exception:
                                                try:
                                                    self._emit_diag(str(msg))
                                                except Exception:
                                                    pass

                                        try:
                                            self._emit_diag(f"BUS_LOCK_LOOKUP_SINGLE: HOST={host_local} PORT={port_local} MODE={client_mode_local}")
                                        except Exception:
                                            pass
                                        lock_single = get_bus_lock(host=host_local, port=port_local, client_params=client_params_local, client_mode=client_mode_local)
                                        async with lock_single:
                                            try:
                                                self._emit_diag(f"BUS_LOCK_ACQUIRED_SINGLE: HOST={host_local} PORT={port_local} MODE={client_mode_local}")
                                            except Exception:
                                                pass
                                            single_res = await self.controller.read_tag_value_async(
                                                tag_item,
                                                host=host_local,
                                                port=port_local,
                                                unit=unit,
                                                timeout=mapped_request_timeout,
                                                connect_timeout=mapped_connect_timeout,
                                                diag_callback=_diag_single_with_ctx,
                                                client_mode=client_mode_local,
                                                client_params=client_params_local,
                                                inter_request_delay=inter_delay_local,
                                            )
                                            try:
                                                self._emit_diag(f"BUS_LOCK_RELEASE_SINGLE: HOST={host_local} PORT={port_local} MODE={client_mode_local}")
                                            except Exception:
                                                pass
                                    except Exception:
                                        single_res = None

                                    db_single = None
                                    regs_single = []
                                    if single_res is not None:
                                        db_single = getattr(single_res, 'data_bytes', None)
                                        try:
                                            regs_single = getattr(single_res, 'registers', None) or []
                                        except Exception:
                                            regs_single = []
                                        if not db_single:
                                            try:
                                                db_single = b"".join(int(r & 0xFFFF).to_bytes(2, 'big') for r in regs_single)
                                            except Exception:
                                                db_single = b""

                                    if db_single:
                                        try:
                                            if fc in (1, 2):
                                                # single read for bits: decode from db_single starting at bit 0
                                                val = _decode_bytes_for_tag(base_dtype, db_single, arr_len, is_bits=True, start_bit=0, bit_order_enable=bit_order_enable)
                                            else:
                                                val = _decode_bytes_for_tag(base_dtype, db_single, arr_len, is_bits=False, start_bit=0, bit_order_enable=bit_order_enable, treat_long=treat_long)
                                        except Exception:
                                            val = None
                                    else:
                                        val = None

                                if val is not None:
                                    tid = id(tag_item)
                                    last = self._last_emit_times.get(tid)
                                    now = time.time()
                                    if last is None or (now - last) > 0.05:
                                        self._emit_tag_polled(tag_item, val, now, 'Good')
                                    else:
                                        pass
                                else:
                                    self._emit_tag_polled(tag_item, None, time.time(), 'Bad')
                            except Exception:
                                try:
                                    self._emit_tag_polled(tag_item, None, time.time(), 'Bad')
                                except Exception:
                                    pass

                        # clear current tag marker
                        self._current_tag = None
                        # inter-request delay is handled by controller.read_tag_value_async
                        # (controller will wait after RX before returning when configured)
                except Exception:
                    try:
                        for (_, _, _, _, _, tag_item) in members:
                            self._emit_tag_polled(tag_item, None, time.time(), 'Bad')
                    except Exception:
                        pass

            elapsed = time.time() - start
            to_wait = max(0.0, self.interval - elapsed)
            try:
                await asyncio.sleep(to_wait)
            except asyncio.CancelledError:
                break

    def _normalize_register_bytes(self, regs, encoding):
        """Return a bytes object for `regs` after applying encoding options.

        `encoding` is the device.encoding dict with keys like 'byte_order',
        'word_low', 'dword_low', 'bit_order'. Values are 'Enable'/'Disable'.
        """
        def _flag(enc, *keys, default="Enable"):
            try:
                for k in keys:
                    if k in enc and enc.get(k) is not None:
                        return str(enc.get(k)).lower() in ("enable", "true", "1", "yes")
            except Exception:
                pass
            return str(default).lower() in ("enable", "true", "1", "yes")

        try:
            byte_order_enable = _flag(encoding, "byte_order", "modbus_byte_order", "byteOrder", default="Enable")
            first_word_low = _flag(encoding, "word_low", "first_word_low", "firstWordLow", default="Enable")
            first_dword_low = _flag(encoding, "dword_low", "first_dword_low", "firstDwordLow", default="Enable")
        except Exception:
            byte_order_enable = True
            first_word_low = True
            first_dword_low = True

        # build list of 2-byte chunks (as bytes)
        chunks = []
        for r in regs:
            b = int(r & 0xFFFF).to_bytes(2, "big")
            if not byte_order_enable:
                b = b[::-1]
            chunks.append(b)

        # Historically the UI label "First Word Low" mapped to swapping adjacent
        # words. We invert the logic here so that the device UI option aligns
        # with the actual data layout expected by common devices: when the
        # device setting indicates the conventional ordering, we DON'T swap.
        # To change ordering for devices that use the opposite convention,
        # the flag will be False and we perform the swap.
        if not first_word_low:
            for i in range(0, len(chunks) - 1, 2):
                try:
                    chunks[i], chunks[i + 1] = chunks[i + 1], chunks[i]
                except Exception:
                    pass

        # Same idea for dword ordering: invert behavior similar to words.
        if not first_dword_low:
            for i in range(0, len(chunks) - 3, 4):
                try:
                    # swap [i:i+2] with [i+2:i+4]
                    chunks[i:i+4] = chunks[i + 2 : i + 4] + chunks[i : i + 2]
                except Exception:
                    pass

        return b"".join(chunks)


    def start(self):
        if self._running:
            return
        # Prefer running the poller in a background thread to avoid any chance
        # of scheduling heavy IO on the application's main thread event loop.
        # This also avoids depending on qasync being present. If running in
        # the main asyncio loop is desired, this behaviour can be changed.
        try:
            # intentionally ignore any running loop and use background thread
            loop = None
            self._in_main_loop = False
        except Exception:
            loop = None
            self._in_main_loop = False

        self._running = True
        try:
            if self._in_main_loop:
                self._emit_diag("Poller started on main asyncio loop")
            # emit concise startup diagnostics including connection info
            try:
                # If the poller was marked as a serial/RTU poller by the creator,
                # emit a serial-style startup line; otherwise emit host/port.
                if getattr(self, '_is_serial', False):
                    sp = getattr(self, '_serial_params', {}) or {}
                    port_s = sp.get('port', '')
                    baud_s = sp.get('baud', '')
                    bytesize_s = sp.get('bytesize', '')
                    stopbits_s = sp.get('stopbits', '')
                    parity_s = sp.get('parity', '')
                    flow_s = sp.get('flow', '')
                    self._emit_diag(
                        f"Poller START: mode=rtu port={port_s} baud={baud_s} bytesize={bytesize_s} stopbits={stopbits_s} parity={parity_s} flow={flow_s} unit={self.unit} interval={self.interval}"
                    )
                else:
                    self._emit_diag(f"Poller START: host={self.host} port={self.port} unit={self.unit} interval={self.interval}")
            except Exception:
                pass
            # when running in a background thread we intentionally do not emit
            # a startup diagnostic line to keep diagnostics compact.
        except Exception:
            pass
        if loop is not None:
            # schedule on current loop
            try:
                self._task = loop.create_task(self._main_loop_async())
            except Exception:
                self._running = False
        else:
            # fallback: run an asyncio loop in a background thread
            def _bg_runner():
                new_loop = asyncio.new_event_loop()
                self._bg_loop = new_loop
                asyncio.set_event_loop(new_loop)
                try:
                    # indicate background loop is starting
                    self._emit_diag("Poller: background loop starting")
                except Exception:
                    pass
                try:
                    # background runner starts silently (no startup diag to keep UI compact)

                    # create a task for the main loop and keep a reference so
                    # the main thread can cancel it via call_soon_threadsafe.
                    try:
                        self._bg_task = new_loop.create_task(self._main_loop_async())
                        new_loop.run_until_complete(self._bg_task)
                    except asyncio.CancelledError:
                        # expected when stop() cancels the task
                        pass
                except Exception as e:
                    try:
                        import traceback

                        tb = traceback.format_exc()
                        self._emit_diag(f"Poller background runner exception: {e}\n{tb}")
                    except Exception:
                        pass
                finally:
                    try:
                        # cancel any remaining tasks and wait for them to finish
                        pending = asyncio.all_tasks(loop=new_loop)
                        for t in pending:
                            try:
                                t.cancel()
                            except Exception:
                                pass
                        if pending:
                            try:
                                new_loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                            except Exception:
                                pass
                    except Exception:
                        pass
                    try:
                        new_loop.close()
                    except Exception:
                        pass

            self._bg_thread = threading.Thread(target=_bg_runner, daemon=True)
            self._bg_thread.start()
            try:
                # emit quick diagnostic that background thread was started
                self._emit_diag("Poller: background thread started")
            except Exception:
                pass

    def stop(self):
        self._running = False
        # cancel task if present
        try:
            if self._task is not None:
                self._task.cancel()
        except Exception:
            pass
        # stop background loop if used
        try:
            if self._bg_loop is not None:
                try:
                    # prefer cancelling the bg task rather than stopping the loop abruptly
                    if getattr(self, '_bg_task', None) is not None:
                        self._bg_loop.call_soon_threadsafe(self._bg_task.cancel)
                    else:
                        self._bg_loop.call_soon_threadsafe(self._bg_loop.stop)
                except Exception:
                    try:
                        self._bg_loop.call_soon_threadsafe(self._bg_loop.stop)
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            if self._bg_thread is not None:
                self._bg_thread.join(timeout=1.0)
        except Exception:
            pass