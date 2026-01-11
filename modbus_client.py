"""Lightweight wrapper around pymodbus clients.

Provides a compact, robust `ModbusClient` with the async/sync surface
used by the poller: `connect_async`, `close_async`, and `read_async`.

The implementation focuses on being conservative and readable while
normalizing read results by attaching `result.data_bytes` for the
decoder in `modbus_worker.py`.
"""

from __future__ import annotations

import asyncio
import inspect
import traceback
from typing import Optional, Any

ModbusTcpClient = None
ModbusSerialClient = None
_import_errors: list[tuple[str, str]] = []
try:
    # Try common import locations for pymodbus across versions
    from pymodbus.client.sync import ModbusTcpClient, ModbusSerialClient  # type: ignore
except Exception as e:  # pragma: no cover
    _import_errors.append(("pymodbus.client.sync", str(e)))
    try:
        from pymodbus.client import ModbusTcpClient, ModbusSerialClient  # type: ignore
    except Exception as e2:  # pragma: no cover
        _import_errors.append(("pymodbus.client", str(e2)))


class ModbusClient:
    def __init__(self, mode: str = "tcp", host: str | None = None, port: int = 502, unit: int = 1, connect_timeout: float = 3.0, request_timeout: float = 2.0, diag_callback: Optional[Any] = None, **kwargs):
        self.mode = (mode or "tcp").lower()
        self.host = host
        self.port = int(port or 502)
        self.unit = int(unit or 1)
        self.connect_timeout = float(connect_timeout or 3.0)
        self.request_timeout = float(request_timeout or 2.0)
        self.kwargs = kwargs or {}
        self._client = None
        self.diag_callback = diag_callback
        # transaction id counter for synthetic MBAP generation
        try:
            self._txid = 0
        except Exception:
            self._txid = 0

    def _transport_trace_installed(self) -> bool:
        """Return True if a lower-level transport trace was installed for the underlying client.

        When True, avoid emitting duplicate/partial RX from high-level `res.encode()` calls
        because the transport-level trace will emit canonical frames.
        """
        try:
            c = getattr(self, '_client', None)
            if c is None:
                return False
            # Only consider transport-level tracing installed when the
            # explicit `_trace_installed` flag is present and truthy.
            try:
                if bool(getattr(c, '_trace_installed', False)):
                    return True
            except Exception:
                pass
            # Some trace installers register a global host:port fallback
            # instead of instance-level wrappers. Check the global registry
            # in `pymodbus_trace` for an entry matching this client's host/port
            # so we can avoid emitting a synthetic TX when a global tracer
            # will already emit the wire bytes.
            try:
                import pymodbus_trace as _pmtr
                try:
                    reg = getattr(_pmtr, '_GLOBAL_ADDR_DIAG_REG', None)
                    if isinstance(reg, dict):
                        key = (str(getattr(self, 'host', None)), int(getattr(self, 'port', 502)))
                        if key in reg:
                            return True
                except Exception:
                    pass
            except Exception:
                pass
            return False
        except Exception:
            return False

    # --- sync convenience ---
    def connect(self) -> bool:
        return asyncio.run(self.connect_async())

    def close(self) -> None:
        return asyncio.run(self.close_async())

    def read(self, address: int, count: int, function_code: int, encoding: Optional[dict] = None):
        return asyncio.run(self.read_async(address, count, function_code, encoding=encoding))

    # --- async implementations ---
    async def connect_async(self) -> bool:
        if self.mode in ("tcp", "overtcp"):
            if ModbusTcpClient is None:
                msg = "pymodbus is required for ModbusClient (TCP)."
                try:
                    msg += " Import attempts: " + ", ".join(f"{p}:{e}" for p, e in _import_errors)
                except Exception:
                    pass
                raise ImportError(msg)

            def _sync():
                client_kw = {"host": self.host, "port": self.port, "timeout": self.connect_timeout}
                try:
                    self._client = ModbusTcpClient(**client_kw)
                except Exception:
                    try:
                        self._client = ModbusTcpClient(self.host, self.port)
                    except Exception:
                        self._client = None

                ok = False
                try:
                    if self._client is not None:
                        ok = bool(self._client.connect())
                except Exception:
                    ok = False

                # preserve diagnostic txid counter on underlying client object
                try:
                    if self._client is not None:
                        if not hasattr(self._client, '_diag_txid'):
                            setattr(self._client, '_diag_txid', getattr(self, '_txid', 0))
                        else:
                            try:
                                self._txid = int(getattr(self._client, '_diag_txid', getattr(self, '_txid', 0)))
                            except Exception:
                                pass
                except Exception:
                    pass

                # attempt to install transport trace for this client if requested
                try:
                    if self._client is not None and self.diag_callback:
                        try:
                            from pymodbus_trace import install_trace_for_client
                            try:
                                install_trace_for_client(self._client, self.diag_callback)
                                # Only register a host:port global fallback if the
                                # instance-level wrappers were NOT successfully
                                # installed. This prevents duplicate global emits
                                # when both instance and global wrappers exist.
                                try:
                                    if not getattr(self._client, '_trace_installed', False):
                                        from pymodbus_trace import register_addr_diag
                                        try:
                                            register_addr_diag(self.host, self.port, self.diag_callback)
                                        except Exception:
                                            pass
                                except Exception:
                                    pass
                            except Exception:
                                pass
                        except Exception:
                            pass
                except Exception:
                    pass

                # emit diagnostic about what underlying transport attributes exist
                try:
                    if self._client is not None and self.diag_callback:
                        try:
                            c = self._client
                            attrs = []
                            for an in ('socket', '_socket', '_sock', 'sock', 'transport', 'serial', '_transport'):
                                try:
                                    v = getattr(c, an, None)
                                    if v is None:
                                        attrs.append(f"{an}=None")
                                    else:
                                        attrs.append(f"{an}={type(v).__name__}@{hex(id(v))}")
                                except Exception:
                                    attrs.append(f"{an}=<err>")
                            ti = getattr(c, '_trace_installed', False)
                            orig = bool(getattr(c, '_trace_orig', None))
                            accum = bool(getattr(c, '_trace_accum', None))
                            try:
                                self.diag_callback(f"TRACE_INFO: trace_installed={ti} _trace_orig={orig} _trace_accum={accum} attrs={' '.join(attrs)}")
                            except Exception:
                                pass
                        except Exception:
                            pass
                except Exception:
                    pass

                return ok

            return await asyncio.to_thread(_sync)

        elif self.mode == "rtu":
            if ModbusSerialClient is None:
                raise ImportError("pymodbus is required for ModbusClient (RTU).")

            def _sync():
                ser_port = self.kwargs.get("serial_port") or self.kwargs.get("port") or self.host
                try:
                    if isinstance(ser_port, str):
                        s = ser_port.strip()
                        if s.isdigit():
                            ser_port = f"COM{int(s)}"
                        else:
                            import re

                            m = re.match(r"(?i)^com(\d+)$", s)
                            if m:
                                ser_port = f"COM{int(m.group(1))}"
                    else:
                        if isinstance(ser_port, (int, float)):
                            ser_port = f"COM{int(ser_port)}"
                except Exception:
                    pass

                base_kw = {"port": ser_port, "baudrate": int(self.kwargs.get("baudrate", 9600)), "timeout": self.request_timeout}
                for k in ("parity", "stopbits", "bytesize", "rtscts", "xonxoff", "framer"):
                    if k in self.kwargs:
                        base_kw[k] = self.kwargs[k]

                try:
                    self._client = ModbusSerialClient(**base_kw)
                except Exception:
                    try:
                        self._client = ModbusSerialClient(ser_port)
                    except Exception:
                        self._client = None

                ok = False
                try:
                    if self._client is not None:
                        ok = bool(self._client.connect())
                except Exception:
                    ok = False

                # install optional transport trace for serial clients as well
                try:
                    if self._client is not None and self.diag_callback:
                        try:
                            from pymodbus_trace import install_trace_for_client
                            try:
                                install_trace_for_client(self._client, self.diag_callback)
                                # Only register host:port fallback if instance-level
                                # wrappers were not installed to avoid duplicate emits.
                                try:
                                    if not getattr(self._client, '_trace_installed', False):
                                        from pymodbus_trace import register_addr_diag
                                        try:
                                            host_var = getattr(self._client, 'host', None) or self.host
                                            port_var = getattr(self._client, 'port', None) or self.port
                                            register_addr_diag(host_var, port_var, self.diag_callback)
                                        except Exception:
                                            pass
                                except Exception:
                                    pass
                            except Exception:
                                pass
                        except Exception:
                            pass
                except Exception:
                    pass

                # emit diagnostic about underlying transport attributes for serial
                try:
                    if self._client is not None and self.diag_callback:
                        try:
                            c = self._client
                            attrs = []
                            for an in ('socket', '_socket', '_sock', 'sock', 'transport', 'serial', '_transport'):
                                try:
                                    v = getattr(c, an, None)
                                    if v is None:
                                        attrs.append(f"{an}=None")
                                    else:
                                        attrs.append(f"{an}={type(v).__name__}@{hex(id(v))}")
                                except Exception:
                                    attrs.append(f"{an}=<err>")
                            ti = getattr(c, '_trace_installed', False)
                            orig = bool(getattr(c, '_trace_orig', None))
                            accum = bool(getattr(c, '_trace_accum', None))
                            try:
                                self.diag_callback(f"TRACE_INFO: trace_installed={ti} _trace_orig={orig} _trace_accum={accum} attrs={' '.join(attrs)}")
                            except Exception:
                                pass
                        except Exception:
                            pass
                except Exception:
                    pass

                return ok

            return await asyncio.to_thread(_sync)

        else:
            raise ValueError(f"Unsupported mode: {self.mode}")

    async def close_async(self) -> None:
        def _sync():
            try:
                if self._client:
                    # try uninstalling trace wrappers if present
                    try:
                        from pymodbus_trace import uninstall_trace_for_client
                        try:
                            uninstall_trace_for_client(self._client)
                        except Exception:
                            pass
                    except Exception:
                        pass
                    try:
                        self._client.close()
                    except Exception:
                        pass
            except Exception:
                pass
            self._client = None

        await asyncio.to_thread(_sync)

    async def _call_method_flexible_async(self, method, address, count):
        """Attempt several common call signatures for a pymodbus method.

        This version is async-friendly: it will `await` coroutine methods
        and return the call result (possibly already-resolved sync result).
        """
        # first try calling by parameter name if possible
        try:
            sig = inspect.signature(method)
            params = list(sig.parameters.keys())
            kw = {}
            for p in params:
                pn = p.lower()
                if pn in ("address", "addr", "starting_address", "start", "startingaddress"):
                    kw[p] = int(address)
                elif pn in ("count", "quantity", "qty"):
                    kw[p] = int(count)
                elif pn in ("unit", "slave", "device_id", "unit_id", "slave_id", "device", "deviceid", "default_unit"):
                    kw[p] = int(self.unit)
            if kw:
                try:
                    res = method(**kw)
                    if inspect.isawaitable(res):
                        return await res
                    return res
                except Exception:
                    if self.diag_callback:
                        try:
                            self.diag_callback(f"CALL_FAIL_KW: method={getattr(method,'__name__',repr(method))} kwargs={kw} err={traceback.format_exc().splitlines()[-1]}")
                        except Exception:
                            pass
        except Exception:
            pass

        # helper to await if needed
        async def _maybe_await(r):
            try:
                if inspect.isawaitable(r):
                    return await r
            except Exception:
                pass
            return r

        attempts = [
            ("pos_unit_kw", lambda: method(int(address), int(count), unit=int(self.unit))),
            ("pos_slave_kw", lambda: method(int(address), int(count), slave=int(self.unit))),
            ("pos_unit_pos", lambda: method(int(address), int(count), int(self.unit))),
            ("simple_pos", lambda: method(int(address), int(count))),
            ("simple_pos_str", lambda: method(str(address), str(count))),
            ("addr_only", lambda: method(int(address))),
            ("addr_unit_kw", lambda: method(int(address), unit=int(self.unit))),
            ("addr_count_deviceid", lambda: method(int(address), int(count), device_id=int(self.unit))),
        ]

        for i, (name, attempt) in enumerate(attempts, start=1):
            try:
                res = attempt()
                res = await _maybe_await(res)
                return res
            except Exception:
                if self.diag_callback:
                    try:
                        self.diag_callback(f"FALLBACK_FAIL[{i}:{name}]: method={getattr(method,'__name__',repr(method))} err={traceback.format_exc().splitlines()[-1]}")
                    except Exception:
                        pass
                continue

        # last-ditch: try positional
        try:
            res = method(int(address), int(count))
            res = await _maybe_await(res)
            return res
        except Exception:
            if self.diag_callback:
                try:
                    self.diag_callback(f"CALL_EXHAUSTED: method={getattr(method,'__name__',repr(method))}")
                except Exception:
                    pass
            raise

    @staticmethod
    def _registers_to_bytes(registers: list[int]) -> bytes:
        b = bytearray()
        try:
            for r in registers:
                v = int(r) & 0xFFFF
                b.extend(v.to_bytes(2, "big"))
        except Exception:
            pass
        return bytes(b)

    @staticmethod
    def _bits_to_bytes(bits: list[bool]) -> bytes:
        # pack bits into bytes, LSB first per byte
        b = bytearray()
        try:
            for i in range(0, len(bits), 8):
                byte = 0
                for bit_i in range(8):
                    if i + bit_i < len(bits) and bits[i + bit_i]:
                        byte |= (1 << bit_i)
                b.append(byte)
        except Exception:
            pass
        return bytes(b)

    async def read_async(self, address: int, count: int, function_code: int, encoding: Optional[dict] = None):
        # choose method name by FC
        if function_code == 1:
            method_name = "read_coils"
        elif function_code == 2:
            method_name = "read_discrete_inputs"
        elif function_code == 3:
            method_name = "read_holding_registers"
        elif function_code == 4:
            method_name = "read_input_registers"
        else:
            raise ValueError(f"Unsupported function code: {function_code}")

        if self._client is None:
            # try lazy connect
            try:
                await self.connect_async()
            except Exception:
                pass

        method = None
        try:
            method = getattr(self._client, method_name)
        except Exception:
            method = None

        if method is None:
            raise AttributeError(f"Underlying client missing method {method_name}")

        res = await self._call_method_flexible_async(method, address, count)
        # emit synthetic TX hex for read call so diagnostics show TX even when
        # transport-layer wrapping fails to capture send bytes. When a
        # transport-level trace is installed we MUST NOT emit a synthetic TX
        # here to avoid duplicate TX diagnostics.
        try:
            if self.diag_callback and not self._transport_trace_installed():
                try:
                    func_byte = int(function_code) & 0xFF
                    addr_b = int(address).to_bytes(2, "big")
                    cnt_b = int(count).to_bytes(2, "big")
                    # build PDU (function + addr + count)
                    pdu_pdu = bytes([func_byte]) + addr_b + cnt_b
                    try:
                        unit_val = int(self.unit) & 0xFF
                    except Exception:
                        unit_val = 0
                    # For TCP clients, build synthetic MBAP header: txid(2), proto(2=0), length(2), unit(1), PDU
                    try:
                        mode_lower = (self.mode or "").lower()
                        if mode_lower == "tcp":
                            # prefer per-client counter if available so txid
                            # increments consistently for the same connection
                            try:
                                if hasattr(self, '_client') and hasattr(self._client, '_diag_txid'):
                                    client_tid = (int(getattr(self._client, '_diag_txid', 0)) + 1) & 0xFFFF
                                    self._client._diag_txid = client_tid
                                    self._txid = client_tid
                                else:
                                    self._txid = (getattr(self, '_txid', 0) + 1) & 0xFFFF
                            except Exception:
                                self._txid = (getattr(self, '_txid', 0) + 1) & 0xFFFF
                            txid_b = int(self._txid).to_bytes(2, 'big')
                            proto_b = (0).to_bytes(2, 'big')
                            length_b = (len(pdu_pdu) + 1).to_bytes(2, 'big')
                            pdu = txid_b + proto_b + length_b + bytes([unit_val]) + pdu_pdu
                        elif mode_lower in ("rtu", "overtcp"):
                            # RTU ADU: unit + PDU + CRC16 (little-endian)
                            adu_no_crc = bytes([unit_val]) + pdu_pdu
                            try:
                                # compute CRC16 (Modbus RTU polynomial 0xA001)
                                crc = 0xFFFF
                                for b in adu_no_crc:
                                    crc ^= b
                                    for _ in range(8):
                                        if crc & 1:
                                            crc = (crc >> 1) ^ 0xA001
                                        else:
                                            crc >>= 1
                                crc &= 0xFFFF
                                crc_bytes = crc.to_bytes(2, 'little')
                                pdu = adu_no_crc + crc_bytes
                            except Exception:
                                pdu = adu_no_crc
                        else:
                            # unknown mode: fallback to unit-first
                            pdu = bytes([unit_val]) + pdu_pdu
                    except Exception:
                        pdu = bytes([unit_val]) + pdu_pdu
                    hex_s = ' '.join(f"{c:02X}" for c in pdu)
                    try:
                        self.diag_callback(f"TX: | {hex_s} |")
                    except Exception:
                        pass
                except Exception:
                    pass
        except Exception:
            pass

        # normalize: attach data_bytes for decoding path
        try:
            if hasattr(res, "registers") and res.registers is not None:
                res.data_bytes = self._registers_to_bytes(list(res.registers))
            elif hasattr(res, "bits") and res.bits is not None:
                # coils/discrete
                res.data_bytes = self._bits_to_bytes(list(res.bits))
            else:
                # fallback: try raw payload attrs
                res.data_bytes = getattr(res, "data", b"") or b""
        except Exception:
            try:
                res.data_bytes = b""
            except Exception:
                pass

        # emit RX hex if possible (use encode() if available, fallback to data_bytes)
        try:
            if self.diag_callback:
                data = None
                try:
                    if hasattr(res, "encode"):
                        try:
                            data = res.encode()
                        except Exception:
                            data = None
                except Exception:
                    data = None

                # fallback to normalized data_bytes (set earlier) when encode() is empty
                if not data:
                    try:
                        db = getattr(res, 'data_bytes', None)
                        if db:
                            data = db
                    except Exception:
                        data = data

                if data:
                    # if transport-level trace is installed, skip high-level RX emit
                    if not self._transport_trace_installed():
                        try:
                            hex_s = " ".join(f"{c:02X}" for c in data)
                        except Exception:
                            try:
                                hex_s = " ".join(f"{c:02X}" for c in bytes(data))
                            except Exception:
                                hex_s = str(data)
                        try:
                            self.diag_callback(f"RX: | {hex_s} |")
                        except Exception:
                            pass
        except Exception:
            pass

        return res

    async def _call_method_flexible_any(self, method, mapping: dict):
        """Flexible caller that maps known parameter names to provided mapping.

        `mapping` keys should be lower-case hints like 'address','value','unit','count','values'.
        This will try to call the method using keyword args matched to the method
        signature first, then fall back to common positional forms.
        """
        # try keyword mapping by signature
        try:
            sig = inspect.signature(method)
            params = list(sig.parameters.keys())
            kw = {}
            for p in params:
                pn = p.lower()
                if pn in mapping:
                    kw[p] = mapping[pn]
            if kw:
                res = method(**kw)
                if inspect.isawaitable(res):
                    return await res
                return res
        except Exception:
            pass

        # helper to await if needed
        async def _maybe_await(r):
            try:
                if inspect.isawaitable(r):
                    return await r
            except Exception:
                pass
            return r

        # common positional fallbacks
        attempts = [
            ("addr_val_unit", lambda: method(int(mapping.get('address', 0)), mapping.get('value'), int(mapping.get('unit', self.unit)))),
            ("addr_val", lambda: method(int(mapping.get('address', 0)), mapping.get('value'))),
            ("addr_vals", lambda: method(int(mapping.get('address', 0)), mapping.get('values'))),
            ("addr_vals_unit", lambda: method(int(mapping.get('address', 0)), mapping.get('values'), int(mapping.get('unit', self.unit)))),
            ("addr_only", lambda: method(int(mapping.get('address', 0)))),
        ]

        for i, (name, attempt) in enumerate(attempts, start=1):
            try:
                res = attempt()
                res = await _maybe_await(res)
                return res
            except Exception:
                if self.diag_callback:
                    try:
                        self.diag_callback(f"FALLBACK_FAIL_ANY[{i}:{name}]: method={getattr(method,'__name__',repr(method))} err={traceback.format_exc().splitlines()[-1]}")
                    except Exception:
                        pass
                continue

        # exhausted
        if self.diag_callback:
            try:
                self.diag_callback(f"CALL_EXHAUSTED_ANY: method={getattr(method,'__name__',repr(method))}")
            except Exception:
                pass
        raise

    async def write_coil_async(self, address: int, value: bool):
        if self._client is None:
            await self.connect_async()
        # emit synthetic TX for writes when no transport-level trace is installed
        try:
            if self.diag_callback and not self._transport_trace_installed():
                try:
                    func_byte = 5
                    addr_b = int(address).to_bytes(2, "big")
                    val_b = (0xFF00 if bool(value) else 0x0000).to_bytes(2, "big")
                    pdu_pdu = bytes([func_byte]) + addr_b + val_b
                    try:
                        unit_val = int(self.unit) & 0xFF
                    except Exception:
                        unit_val = 0
                    mode_lower = (self.mode or "").lower()
                    if mode_lower == "tcp":
                        try:
                            if hasattr(self, '_client') and hasattr(self._client, '_diag_txid'):
                                client_tid = (int(getattr(self._client, '_diag_txid', 0)) + 1) & 0xFFFF
                                self._client._diag_txid = client_tid
                                self._txid = client_tid
                            else:
                                self._txid = (getattr(self, '_txid', 0) + 1) & 0xFFFF
                        except Exception:
                            self._txid = (getattr(self, '_txid', 0) + 1) & 0xFFFF
                        txid_b = int(self._txid).to_bytes(2, 'big')
                        proto_b = (0).to_bytes(2, 'big')
                        length_b = (len(pdu_pdu) + 1).to_bytes(2, 'big')
                        pdu = txid_b + proto_b + length_b + bytes([unit_val]) + pdu_pdu
                    elif mode_lower in ("rtu", "overtcp"):
                        adu_no_crc = bytes([unit_val]) + pdu_pdu
                        try:
                            crc = 0xFFFF
                            for b in adu_no_crc:
                                crc ^= b
                                for _ in range(8):
                                    if crc & 1:
                                        crc = (crc >> 1) ^ 0xA001
                                    else:
                                        crc >>= 1
                            crc &= 0xFFFF
                            crc_bytes = crc.to_bytes(2, 'little')
                            pdu = adu_no_crc + crc_bytes
                        except Exception:
                            pdu = adu_no_crc
                    else:
                        pdu = bytes([unit_val]) + pdu_pdu
                    hex_s = ' '.join(f"{c:02X}" for c in pdu)
                    try:
                        self.diag_callback(f"TX: | {hex_s} |")
                    except Exception:
                        pass
                except Exception:
                    pass
        except Exception:
            pass
        method = getattr(self._client, 'write_coil', None)
        if method is None:
            raise AttributeError('Underlying client missing write_coil')
        res = await self._call_method_flexible_any(method, {'address': int(address), 'value': bool(value), 'unit': int(self.unit)})
        try:
            if self.diag_callback and hasattr(res, 'encode'):
                try:
                    data = res.encode()
                except Exception:
                    data = None
                if data:
                    hex_s = ' '.join(f"{c:02X}" for c in data)
                    try:
                        self.diag_callback(f"RX: | {hex_s} |")
                    except Exception:
                        pass
        except Exception:
            pass
        return res

    async def write_coils_async(self, address: int, values: list[bool]):
        if self._client is None:
            await self.connect_async()
        try:
            if self.diag_callback and not self._transport_trace_installed():
                try:
                    func_byte = 15
                    addr_b = int(address).to_bytes(2, "big")
                    qty = int(len(values))
                    coil_bytes = self._bits_to_bytes([bool(v) for v in values])
                    pdu_pdu = bytes([func_byte]) + addr_b + int(qty).to_bytes(2, 'big') + int(len(coil_bytes)).to_bytes(1, 'big') + coil_bytes
                    try:
                        unit_val = int(self.unit) & 0xFF
                    except Exception:
                        unit_val = 0
                    mode_lower = (self.mode or "").lower()
                    if mode_lower == "tcp":
                        try:
                            if hasattr(self, '_client') and hasattr(self._client, '_diag_txid'):
                                client_tid = (int(getattr(self._client, '_diag_txid', 0)) + 1) & 0xFFFF
                                self._client._diag_txid = client_tid
                                self._txid = client_tid
                            else:
                                self._txid = (getattr(self, '_txid', 0) + 1) & 0xFFFF
                        except Exception:
                            self._txid = (getattr(self, '_txid', 0) + 1) & 0xFFFF
                        txid_b = int(self._txid).to_bytes(2, 'big')
                        proto_b = (0).to_bytes(2, 'big')
                        length_b = (len(pdu_pdu) + 1).to_bytes(2, 'big')
                        pdu = txid_b + proto_b + length_b + bytes([unit_val]) + pdu_pdu
                    elif mode_lower in ("rtu", "overtcp"):
                        adu_no_crc = bytes([unit_val]) + pdu_pdu
                        try:
                            crc = 0xFFFF
                            for b in adu_no_crc:
                                crc ^= b
                                for _ in range(8):
                                    if crc & 1:
                                        crc = (crc >> 1) ^ 0xA001
                                    else:
                                        crc >>= 1
                            crc &= 0xFFFF
                            crc_bytes = crc.to_bytes(2, 'little')
                            pdu = adu_no_crc + crc_bytes
                        except Exception:
                            pdu = adu_no_crc
                    else:
                        pdu = bytes([unit_val]) + pdu_pdu
                    hex_s = ' '.join(f"{c:02X}" for c in pdu)
                    try:
                        self.diag_callback(f"TX: | {hex_s} |")
                    except Exception:
                        pass
                except Exception:
                    pass
        except Exception:
            pass
        method = getattr(self._client, 'write_coils', None)
        if method is None:
            raise AttributeError('Underlying client missing write_coils')
        res = await self._call_method_flexible_any(method, {'address': int(address), 'values': list(values), 'unit': int(self.unit)})
        try:
            if self.diag_callback and hasattr(res, 'encode'):
                try:
                    data = res.encode()
                except Exception:
                    data = None
                if data:
                    hex_s = ' '.join(f"{c:02X}" for c in data)
                    try:
                        self.diag_callback(f"RX: | {hex_s} |")
                    except Exception:
                        pass
        except Exception:
            pass
        return res

    async def write_register_async(self, address: int, value: int):
        if self._client is None:
            await self.connect_async()
        try:
            if self.diag_callback and not self._transport_trace_installed():
                try:
                    func_byte = 6
                    addr_b = int(address).to_bytes(2, "big")
                    val_b = int(value & 0xFFFF).to_bytes(2, 'big')
                    pdu_pdu = bytes([func_byte]) + addr_b + val_b
                    try:
                        unit_val = int(self.unit) & 0xFF
                    except Exception:
                        unit_val = 0
                    mode_lower = (self.mode or "").lower()
                    if mode_lower == "tcp":
                        try:
                            if hasattr(self, '_client') and hasattr(self._client, '_diag_txid'):
                                client_tid = (int(getattr(self._client, '_diag_txid', 0)) + 1) & 0xFFFF
                                self._client._diag_txid = client_tid
                                self._txid = client_tid
                            else:
                                self._txid = (getattr(self, '_txid', 0) + 1) & 0xFFFF
                        except Exception:
                            self._txid = (getattr(self, '_txid', 0) + 1) & 0xFFFF
                        txid_b = int(self._txid).to_bytes(2, 'big')
                        proto_b = (0).to_bytes(2, 'big')
                        length_b = (len(pdu_pdu) + 1).to_bytes(2, 'big')
                        pdu = txid_b + proto_b + length_b + bytes([unit_val]) + pdu_pdu
                    elif mode_lower in ("rtu", "overtcp"):
                        adu_no_crc = bytes([unit_val]) + pdu_pdu
                        try:
                            crc = 0xFFFF
                            for b in adu_no_crc:
                                crc ^= b
                                for _ in range(8):
                                    if crc & 1:
                                        crc = (crc >> 1) ^ 0xA001
                                    else:
                                        crc >>= 1
                            crc &= 0xFFFF
                            crc_bytes = crc.to_bytes(2, 'little')
                            pdu = adu_no_crc + crc_bytes
                        except Exception:
                            pdu = adu_no_crc
                    else:
                        pdu = bytes([unit_val]) + pdu_pdu
                    hex_s = ' '.join(f"{c:02X}" for c in pdu)
                    try:
                        self.diag_callback(f"TX: | {hex_s} |")
                    except Exception:
                        pass
                except Exception:
                    pass
        except Exception:
            pass
        method = getattr(self._client, 'write_register', None)
        if method is None:
            raise AttributeError('Underlying client missing write_register')
        res = await self._call_method_flexible_any(method, {'address': int(address), 'value': int(value), 'unit': int(self.unit)})
        try:
            if self.diag_callback and hasattr(res, 'encode'):
                try:
                    data = res.encode()
                except Exception:
                    data = None
                if data:
                    hex_s = ' '.join(f"{c:02X}" for c in data)
                    try:
                        self.diag_callback(f"RX: | {hex_s} |")
                    except Exception:
                        pass
        except Exception:
            pass
        return res

    async def write_registers_async(self, address: int, values: list[int]):
        if self._client is None:
            await self.connect_async()
        try:
            if self.diag_callback and not self._transport_trace_installed():
                try:
                    func_byte = 16
                    addr_b = int(address).to_bytes(2, "big")
                    qty = int(len(values))
                    data_bytes = self._registers_to_bytes(list(values))
                    pdu_pdu = bytes([func_byte]) + addr_b + int(qty).to_bytes(2, 'big') + int(len(data_bytes)).to_bytes(1, 'big') + data_bytes
                    try:
                        unit_val = int(self.unit) & 0xFF
                    except Exception:
                        unit_val = 0
                    mode_lower = (self.mode or "").lower()
                    if mode_lower == "tcp":
                        try:
                            if hasattr(self, '_client') and hasattr(self._client, '_diag_txid'):
                                client_tid = (int(getattr(self._client, '_diag_txid', 0)) + 1) & 0xFFFF
                                self._client._diag_txid = client_tid
                                self._txid = client_tid
                            else:
                                self._txid = (getattr(self, '_txid', 0) + 1) & 0xFFFF
                        except Exception:
                            self._txid = (getattr(self, '_txid', 0) + 1) & 0xFFFF
                        txid_b = int(self._txid).to_bytes(2, 'big')
                        proto_b = (0).to_bytes(2, 'big')
                        length_b = (len(pdu_pdu) + 1).to_bytes(2, 'big')
                        pdu = txid_b + proto_b + length_b + bytes([unit_val]) + pdu_pdu
                    elif mode_lower in ("rtu", "overtcp"):
                        adu_no_crc = bytes([unit_val]) + pdu_pdu
                        try:
                            crc = 0xFFFF
                            for b in adu_no_crc:
                                crc ^= b
                                for _ in range(8):
                                    if crc & 1:
                                        crc = (crc >> 1) ^ 0xA001
                                    else:
                                        crc >>= 1
                            crc &= 0xFFFF
                            crc_bytes = crc.to_bytes(2, 'little')
                            pdu = adu_no_crc + crc_bytes
                        except Exception:
                            pdu = adu_no_crc
                    else:
                        pdu = bytes([unit_val]) + pdu_pdu
                    hex_s = ' '.join(f"{c:02X}" for c in pdu)
                    try:
                        self.diag_callback(f"TX: | {hex_s} |")
                    except Exception:
                        pass
                except Exception:
                    pass
        except Exception:
            pass
        method = getattr(self._client, 'write_registers', None)
        if method is None:
            raise AttributeError('Underlying client missing write_registers')
        res = await self._call_method_flexible_any(method, {'address': int(address), 'values': list(values), 'unit': int(self.unit)})
        try:
            if self.diag_callback and hasattr(res, 'encode'):
                try:
                    data = res.encode()
                except Exception:
                    data = None
                if data:
                    hex_s = ' '.join(f"{c:02X}" for c in data)
                    try:
                        self.diag_callback(f"RX: | {hex_s} |")
                    except Exception:
                        pass
        except Exception:
            pass
        return res

    async def mask_write_register_async(self, address: int, and_mask: int, or_mask: int):
        if self._client is None:
            await self.connect_async()
        try:
            if self.diag_callback and not self._transport_trace_installed():
                try:
                    func_byte = 22
                    addr_b = int(address).to_bytes(2, "big")
                    and_b = int(and_mask & 0xFFFF).to_bytes(2, 'big')
                    or_b = int(or_mask & 0xFFFF).to_bytes(2, 'big')
                    pdu_pdu = bytes([func_byte]) + addr_b + and_b + or_b
                    try:
                        unit_val = int(self.unit) & 0xFF
                    except Exception:
                        unit_val = 0
                    mode_lower = (self.mode or "").lower()
                    if mode_lower == "tcp":
                        try:
                            if hasattr(self, '_client') and hasattr(self._client, '_diag_txid'):
                                client_tid = (int(getattr(self._client, '_diag_txid', 0)) + 1) & 0xFFFF
                                self._client._diag_txid = client_tid
                                self._txid = client_tid
                            else:
                                self._txid = (getattr(self, '_txid', 0) + 1) & 0xFFFF
                        except Exception:
                            self._txid = (getattr(self, '_txid', 0) + 1) & 0xFFFF
                        txid_b = int(self._txid).to_bytes(2, 'big')
                        proto_b = (0).to_bytes(2, 'big')
                        length_b = (len(pdu_pdu) + 1).to_bytes(2, 'big')
                        pdu = txid_b + proto_b + length_b + bytes([unit_val]) + pdu_pdu
                    elif mode_lower in ("rtu", "overtcp"):
                        adu_no_crc = bytes([unit_val]) + pdu_pdu
                        try:
                            crc = 0xFFFF
                            for b in adu_no_crc:
                                crc ^= b
                                for _ in range(8):
                                    if crc & 1:
                                        crc = (crc >> 1) ^ 0xA001
                                    else:
                                        crc >>= 1
                            crc &= 0xFFFF
                            crc_bytes = crc.to_bytes(2, 'little')
                            pdu = adu_no_crc + crc_bytes
                        except Exception:
                            pdu = adu_no_crc
                    else:
                        pdu = bytes([unit_val]) + pdu_pdu
                    hex_s = ' '.join(f"{c:02X}" for c in pdu)
                    try:
                        self.diag_callback(f"TX: | {hex_s} |")
                    except Exception:
                        pass
                except Exception:
                    pass
        except Exception:
            pass
        method = getattr(self._client, 'mask_write_register', None)
        if method is None:
            raise AttributeError('Underlying client missing mask_write_register')
        res = await self._call_method_flexible_any(method, {'address': int(address), 'and_mask': int(and_mask), 'or_mask': int(or_mask), 'unit': int(self.unit)})
        try:
            if self.diag_callback and hasattr(res, 'encode'):
                try:
                    data = res.encode()
                except Exception:
                    data = None
                if data:
                    hex_s = ' '.join(f"{c:02X}" for c in data)
                    try:
                        self.diag_callback(f"RX: | {hex_s} |")
                    except Exception:
                        pass
        except Exception:
            pass
        return res
